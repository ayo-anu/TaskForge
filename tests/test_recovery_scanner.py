"""Focused tests for bounded advisory recovery candidate scanning."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy.dialects import postgresql

from taskforge.persistence.recovery import (
    _expired_claim_statement,
    _stale_worker_session_statement,
)
from taskforge.recovery.domain import (
    MAX_RECOVERY_SCAN_BATCH_SIZE,
    ExpiredClaimCandidate,
    ExpiredClaimCandidatePage,
    ExpiredClaimScanCursor,
    StaleWorkerSessionCandidate,
    StaleWorkerSessionCandidatePage,
    StaleWorkerSessionScanCursor,
)
from taskforge.recovery.persistence_ports import (
    RecoveryScanPersistenceInvariantViolation,
    RecoveryScanPersistenceUnavailable,
)
from taskforge.recovery.scanner import (
    RecoveryCandidateScanner,
    RecoveryScanInvariantError,
    RecoveryScanServiceUnavailable,
)

NOW = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)


def expired_candidate(*, offset: int = 0) -> ExpiredClaimCandidate:
    return ExpiredClaimCandidate(
        uuid4(),
        uuid4(),
        uuid4(),
        1,
        1,
        uuid4(),
        NOW - timedelta(seconds=offset),
        NOW,
    )


def stale_candidate(*, offset: int = 60) -> StaleWorkerSessionCandidate:
    return StaleWorkerSessionCandidate(
        uuid4(), uuid4(), 0, NOW - timedelta(seconds=offset), False, NOW
    )


@dataclass
class FakeRecoveryRepository:
    claim_result: ExpiredClaimCandidatePage | Exception
    stale_result: StaleWorkerSessionCandidatePage | Exception
    calls: list[tuple[str, int, object]] = field(default_factory=list)

    async def scan_expired_claims(
        self, *, limit: int, cursor: ExpiredClaimScanCursor | None
    ) -> ExpiredClaimCandidatePage:
        self.calls.append(("claims", limit, cursor))
        if isinstance(self.claim_result, Exception):
            raise self.claim_result
        return self.claim_result

    async def scan_stale_worker_sessions(
        self,
        *,
        stale_after_seconds: int,
        limit: int,
        cursor: StaleWorkerSessionScanCursor | None,
    ) -> StaleWorkerSessionCandidatePage:
        self.calls.append(("sessions", limit, (stale_after_seconds, cursor)))
        if isinstance(self.stale_result, Exception):
            raise self.stale_result
        return self.stale_result


def repository() -> FakeRecoveryRepository:
    return FakeRecoveryRepository(
        ExpiredClaimCandidatePage((), NOW, None),
        StaleWorkerSessionCandidatePage((), NOW, 30, None),
    )


@pytest.mark.parametrize("limit", (True, 0, -1, 1.5, MAX_RECOVERY_SCAN_BATCH_SIZE + 1))
def test_scan_limit_is_an_exact_bounded_integer(limit: object) -> None:
    repo = repository()
    scanner = RecoveryCandidateScanner(repo, worker_stale_after_seconds=30)
    with pytest.raises(ValueError):
        asyncio.run(scanner.scan_expired_claims(limit=limit))  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        asyncio.run(scanner.scan_stale_worker_sessions(limit=limit))  # type: ignore[arg-type]
    assert repo.calls == []


@pytest.mark.parametrize("limit", (1, MAX_RECOVERY_SCAN_BATCH_SIZE))
def test_valid_limits_and_cursors_are_delegated(limit: int) -> None:
    claim = expired_candidate()
    claim_cursor = ExpiredClaimScanCursor(
        NOW, claim.lease_expires_at, claim.task_attempt_id, claim.generation
    )
    stale = stale_candidate()
    stale_cursor = StaleWorkerSessionScanCursor(
        NOW, stale.last_seen_at, stale.worker_session_id, 30
    )
    repo = repository()
    scanner = RecoveryCandidateScanner(repo, worker_stale_after_seconds=30)

    assert (
        asyncio.run(scanner.scan_expired_claims(limit=limit, cursor=claim_cursor))
        is repo.claim_result
    )
    assert (
        asyncio.run(
            scanner.scan_stale_worker_sessions(limit=limit, cursor=stale_cursor)
        )
        is repo.stale_result
    )
    assert repo.calls == [
        ("claims", limit, claim_cursor),
        ("sessions", limit, (30, stale_cursor)),
    ]


def test_stale_cursor_threshold_must_match_scanner() -> None:
    repo = repository()
    scanner = RecoveryCandidateScanner(repo, worker_stale_after_seconds=30)
    cursor = StaleWorkerSessionScanCursor(NOW, NOW - timedelta(seconds=60), uuid4(), 31)
    with pytest.raises(ValueError):
        asyncio.run(scanner.scan_stale_worker_sessions(limit=1, cursor=cursor))
    assert repo.calls == []


@pytest.mark.parametrize(
    ("failure", "expected"),
    (
        (RecoveryScanPersistenceInvariantViolation(), RecoveryScanInvariantError),
        (RecoveryScanPersistenceUnavailable(), RecoveryScanServiceUnavailable),
    ),
)
def test_persistence_errors_are_translated(
    failure: Exception, expected: type[Exception]
) -> None:
    repo = FakeRecoveryRepository(failure, failure)
    scanner = RecoveryCandidateScanner(repo, worker_stale_after_seconds=30)
    with pytest.raises(expected):
        asyncio.run(scanner.scan_expired_claims(limit=1))
    with pytest.raises(expected):
        asyncio.run(scanner.scan_stale_worker_sessions(limit=1))


def test_queries_are_bounded_ordered_database_timed_and_lock_free() -> None:
    claim_sql = str(
        _expired_claim_statement(7, None).compile(
            dialect=postgresql.dialect(),  # type: ignore[no-untyped-call]
            compile_kwargs={"literal_binds": True},
        )
    )
    stale_sql = str(
        _stale_worker_session_statement(30, 9, None).compile(
            dialect=postgresql.dialect(),  # type: ignore[no-untyped-call]
            compile_kwargs={"literal_binds": True},
        )
    )
    assert "statement_timestamp()" in claim_sql
    assert "terminated_at IS NULL" in claim_sql
    assert "lease_expires_at <= statement_timestamp()" in claim_sql
    assert "ORDER BY task_attempt_claims.lease_expires_at" in claim_sql
    assert "LIMIT 7" in claim_sql
    assert "FOR UPDATE" not in claim_sql and "SKIP LOCKED" not in claim_sql
    assert "statement_timestamp()" in stale_sql
    assert "ended_at IS NULL" in stale_sql
    assert "last_seen_at <= statement_timestamp()" in stale_sql
    assert "ORDER BY worker_session_health.last_seen_at" in stale_sql
    assert "LIMIT 9" in stale_sql
    assert "FOR UPDATE" not in stale_sql and "SKIP LOCKED" not in stale_sql


def test_continuation_uses_database_observation_time_and_ordering_keys() -> None:
    claim = expired_candidate(offset=10)
    claim_cursor = ExpiredClaimScanCursor(
        NOW, claim.lease_expires_at, claim.task_attempt_id, claim.generation
    )
    stale = stale_candidate()
    stale_cursor = StaleWorkerSessionScanCursor(
        NOW, stale.last_seen_at, stale.worker_session_id, 30
    )
    claim_sql = str(
        _expired_claim_statement(5, claim_cursor).compile(
            dialect=postgresql.dialect(),  # type: ignore[no-untyped-call]
            compile_kwargs={"literal_binds": True},
        )
    )
    stale_sql = str(
        _stale_worker_session_statement(30, 5, stale_cursor).compile(
            dialect=postgresql.dialect(),  # type: ignore[no-untyped-call]
            compile_kwargs={"literal_binds": True},
        )
    )
    assert "statement_timestamp()" not in claim_sql
    assert "task_attempt_claims.task_attempt_id >" in claim_sql
    assert "task_attempt_claims.generation > 1" in claim_sql
    assert "statement_timestamp()" not in stale_sql
    assert "worker_sessions.id >" in stale_sql
