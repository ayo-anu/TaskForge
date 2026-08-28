"""PostgreSQL-backed authoritative fixed-window rate counters."""

from __future__ import annotations

import asyncio

from sqlalchemy import delete, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.metrics import add as add_metric
from taskforge.rate_limits import (
    RateLimit,
    RateLimitDecision,
    RateLimitPolicy,
    RateLimitRepositoryUnavailable,
)
from taskforge.rate_limits_schema import rate_limit_counters

_CONSUME = text(
    """
    WITH observed AS (SELECT clock_timestamp() AS now),
    consumed AS (
      INSERT INTO rate_limit_counters
        (policy, key_digest, window_started_at, count, updated_at)
      SELECT :policy, :key_digest, now, 1, now FROM observed
      ON CONFLICT (policy, key_digest) DO UPDATE SET
        window_started_at = CASE
          WHEN rate_limit_counters.window_started_at
               + make_interval(secs => :window_seconds) <= clock_timestamp()
          THEN clock_timestamp() ELSE rate_limit_counters.window_started_at END,
        count = CASE
          WHEN rate_limit_counters.window_started_at
               + make_interval(secs => :window_seconds) <= clock_timestamp()
          THEN 1 ELSE rate_limit_counters.count + 1 END,
        updated_at = clock_timestamp()
      WHERE rate_limit_counters.window_started_at
                + make_interval(secs => :window_seconds) <= clock_timestamp()
         OR rate_limit_counters.count < :limit
      RETURNING count, window_started_at,
        CEIL(EXTRACT(EPOCH FROM
          window_started_at + make_interval(secs => :window_seconds)
          - clock_timestamp()))::integer AS retry_after
    )
    SELECT count, window_started_at, GREATEST(1, retry_after) retry_after
    FROM consumed
    """
)
_LOCK_CURRENT = text(
    """
    SELECT count, window_started_at,
      GREATEST(1, CEIL(EXTRACT(EPOCH FROM
        window_started_at + make_interval(secs => :window_seconds)
        - clock_timestamp()))::integer) AS retry_after,
      window_started_at + make_interval(secs => :window_seconds)
        <= clock_timestamp() AS expired
    FROM rate_limit_counters
    WHERE policy = :policy AND key_digest = :key_digest
    FOR UPDATE
    """
)


class SQLAlchemyRateLimitRepository:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        timeout_seconds: float,
        cleanup_retention_seconds: int,
        cleanup_batch_size: int = 100,
    ) -> None:
        self._sessions = sessions
        self._timeout_seconds = timeout_seconds
        self._cleanup_retention_seconds = cleanup_retention_seconds
        self._cleanup_batch_size = cleanup_batch_size
        self._next_cleanup_at = 0.0

    async def consume(
        self, policy: RateLimitPolicy, key_digest: bytes, limit: RateLimit
    ) -> RateLimitDecision:
        parameters = {
            "policy": policy.value,
            "key_digest": key_digest,
            "window_seconds": limit.window_seconds,
            "limit": limit.count,
        }
        try:
            async with asyncio.timeout(self._timeout_seconds):
                for _attempt in range(2):
                    async with self._sessions.begin() as session:
                        row = (
                            (await session.execute(_CONSUME, parameters))
                            .mappings()
                            .one_or_none()
                        )
                        if row is not None:
                            decision = RateLimitDecision(True, row["retry_after"])
                            break
                        current = (
                            (await session.execute(_LOCK_CURRENT, parameters))
                            .mappings()
                            .one()
                        )
                        if not current["expired"]:
                            decision = RateLimitDecision(False, current["retry_after"])
                            break
                else:
                    raise RateLimitRepositoryUnavailable
        except asyncio.CancelledError:
            raise
        except RateLimitRepositoryUnavailable:
            raise
        except Exception as error:
            raise RateLimitRepositoryUnavailable from error
        await self._maybe_cleanup()
        return decision

    async def check(
        self, policy: RateLimitPolicy, key_digest: bytes, limit: RateLimit
    ) -> RateLimitDecision:
        parameters = {
            "policy": policy.value,
            "key_digest": key_digest,
            "window_seconds": limit.window_seconds,
        }
        try:
            async with asyncio.timeout(self._timeout_seconds):
                async with self._sessions() as session:
                    row = (
                        (await session.execute(_LOCK_CURRENT, parameters))
                        .mappings()
                        .one_or_none()
                    )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            raise RateLimitRepositoryUnavailable from error
        if row is None or row["expired"]:
            return RateLimitDecision(True, limit.window_seconds)
        return RateLimitDecision(row["count"] < limit.count, row["retry_after"])

    async def _maybe_cleanup(self) -> None:
        loop = asyncio.get_running_loop()
        now = loop.time()
        if now < self._next_cleanup_at:
            return
        self._next_cleanup_at = now + 60.0
        try:
            async with asyncio.timeout(self._timeout_seconds):
                async with self._sessions.begin() as session:
                    stale = rate_limit_counters.c.updated_at < text(
                        "clock_timestamp() - make_interval(secs => "
                        ":cleanup_retention_seconds)"
                    )
                    identifiers = session.execute(
                        rate_limit_counters.select()
                        .with_only_columns(
                            rate_limit_counters.c.policy,
                            rate_limit_counters.c.key_digest,
                        )
                        .where(stale)
                        .order_by(rate_limit_counters.c.updated_at)
                        .limit(self._cleanup_batch_size)
                        .with_for_update(skip_locked=True),
                        {"cleanup_retention_seconds": self._cleanup_retention_seconds},
                    )
                    rows = (await identifiers).all()
                    for policy, digest in rows:
                        await session.execute(
                            delete(rate_limit_counters).where(
                                rate_limit_counters.c.policy == policy,
                                rate_limit_counters.c.key_digest == digest,
                                stale,
                            ),
                            {
                                "cleanup_retention_seconds": self._cleanup_retention_seconds
                            },
                        )
        except Exception:
            add_metric("taskforge.rate_limit.cleanup_failures")
            return
