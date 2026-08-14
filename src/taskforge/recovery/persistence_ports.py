"""Persistence boundary for read-only crash-recovery candidate discovery."""

from __future__ import annotations

from typing import Protocol

from taskforge.recovery.domain import (
    ExpiredClaimCandidatePage,
    ExpiredClaimScanCursor,
    StaleWorkerSessionCandidatePage,
    StaleWorkerSessionScanCursor,
)


class RecoveryScanPersistenceInvariantViolation(Exception):
    """Persisted recovery facts violate an established invariant."""


class RecoveryScanPersistenceUnavailable(Exception):
    """Recovery candidate persistence is operationally unavailable."""


class RecoveryCandidateRepository(Protocol):
    async def scan_expired_claims(
        self, *, limit: int, cursor: ExpiredClaimScanCursor | None
    ) -> ExpiredClaimCandidatePage: ...

    async def scan_stale_worker_sessions(
        self,
        *,
        stale_after_seconds: int,
        limit: int,
        cursor: StaleWorkerSessionScanCursor | None,
    ) -> StaleWorkerSessionCandidatePage: ...
