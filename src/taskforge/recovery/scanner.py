"""Bounded, lock-free discovery of advisory crash-recovery candidates."""

from __future__ import annotations

from taskforge.recovery.domain import (
    MAX_RECOVERY_SCAN_BATCH_SIZE,
    ExpiredClaimCandidatePage,
    ExpiredClaimScanCursor,
    StaleWorkerSessionCandidatePage,
    StaleWorkerSessionScanCursor,
)
from taskforge.recovery.persistence_ports import (
    RecoveryCandidateRepository,
    RecoveryScanPersistenceInvariantViolation,
    RecoveryScanPersistenceUnavailable,
)


class RecoveryScanInvariantError(Exception):
    """Durable recovery candidate state is internally inconsistent."""


class RecoveryScanServiceUnavailable(Exception):
    """Recovery candidate persistence is operationally unavailable."""


class RecoveryCandidateScanner:
    def __init__(
        self,
        repository: RecoveryCandidateRepository,
        *,
        worker_stale_after_seconds: int,
    ) -> None:
        if not 1 <= worker_stale_after_seconds <= 3600:
            raise ValueError("worker stale threshold is out of range")
        self._repository = repository
        self._worker_stale_after_seconds = worker_stale_after_seconds

    async def scan_expired_claims(
        self, *, limit: int, cursor: ExpiredClaimScanCursor | None = None
    ) -> ExpiredClaimCandidatePage:
        _validate_limit(limit)
        try:
            return await self._repository.scan_expired_claims(
                limit=limit, cursor=cursor
            )
        except RecoveryScanPersistenceInvariantViolation as error:
            raise RecoveryScanInvariantError from error
        except RecoveryScanPersistenceUnavailable as error:
            raise RecoveryScanServiceUnavailable from error

    async def scan_stale_worker_sessions(
        self,
        *,
        limit: int,
        cursor: StaleWorkerSessionScanCursor | None = None,
    ) -> StaleWorkerSessionCandidatePage:
        _validate_limit(limit)
        if (
            cursor is not None
            and cursor.stale_after_seconds != self._worker_stale_after_seconds
        ):
            raise ValueError("stale-session cursor threshold does not match scanner")
        try:
            return await self._repository.scan_stale_worker_sessions(
                stale_after_seconds=self._worker_stale_after_seconds,
                limit=limit,
                cursor=cursor,
            )
        except RecoveryScanPersistenceInvariantViolation as error:
            raise RecoveryScanInvariantError from error
        except RecoveryScanPersistenceUnavailable as error:
            raise RecoveryScanServiceUnavailable from error


def _validate_limit(limit: int) -> None:
    if type(limit) is not int or not 1 <= limit <= MAX_RECOVERY_SCAN_BATCH_SIZE:
        raise ValueError("recovery scan limit is outside the supported bounds")
