"""Bounded, lock-free discovery of advisory crash-recovery candidates."""

from __future__ import annotations

import logging
from uuid import uuid4

from taskforge.logging import bind_log_context, log_event
from taskforge.metrics import add as add_metric
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


logger = logging.getLogger(__name__)


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
        with bind_log_context(**{"operation.id": uuid4()}):
            try:
                page = await self._repository.scan_expired_claims(
                    limit=limit, cursor=cursor
                )
                add_metric(
                    "taskforge.recovery.scan.candidates",
                    len(page.items),
                    {"taskforge.scan.kind": "expired_claim"},
                )
                add_metric(
                    "taskforge.recovery.scan.operations",
                    attributes={
                        "taskforge.scan.kind": "expired_claim",
                        "taskforge.outcome": "completed",
                    },
                )
                log_event(
                    logger,
                    logging.INFO,
                    "scheduler.expired_claim_scan.completed",
                    {"examined": len(page.items)},
                )
                return page
            except RecoveryScanPersistenceInvariantViolation as error:
                add_metric(
                    "taskforge.recovery.scan.operations",
                    attributes={
                        "taskforge.scan.kind": "expired_claim",
                        "taskforge.outcome": "invariant_failure",
                    },
                )
                log_event(
                    logger,
                    logging.ERROR,
                    "scheduler.expired_claim_scan.failed",
                    {"error.category": "persistence_invariant", "outcome": "failed"},
                    error=error,
                )
                raise RecoveryScanInvariantError from error
            except RecoveryScanPersistenceUnavailable as error:
                add_metric(
                    "taskforge.recovery.scan.operations",
                    attributes={
                        "taskforge.scan.kind": "expired_claim",
                        "taskforge.outcome": "persistence_failure",
                    },
                )
                log_event(
                    logger,
                    logging.WARNING,
                    "scheduler.expired_claim_scan.failed",
                    {"error.category": "persistence_unavailable", "outcome": "failed"},
                    error=error,
                )
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
        with bind_log_context(**{"operation.id": uuid4()}):
            try:
                page = await self._repository.scan_stale_worker_sessions(
                    stale_after_seconds=self._worker_stale_after_seconds,
                    limit=limit,
                    cursor=cursor,
                )
                add_metric(
                    "taskforge.recovery.scan.candidates",
                    len(page.items),
                    {"taskforge.scan.kind": "stale_worker_session"},
                )
                add_metric(
                    "taskforge.recovery.scan.operations",
                    attributes={
                        "taskforge.scan.kind": "stale_worker_session",
                        "taskforge.outcome": "completed",
                    },
                )
                log_event(
                    logger,
                    logging.INFO,
                    "scheduler.stale_worker_scan.completed",
                    {"examined": len(page.items)},
                )
                return page
            except RecoveryScanPersistenceInvariantViolation as error:
                add_metric(
                    "taskforge.recovery.scan.operations",
                    attributes={
                        "taskforge.scan.kind": "stale_worker_session",
                        "taskforge.outcome": "invariant_failure",
                    },
                )
                log_event(
                    logger,
                    logging.ERROR,
                    "scheduler.stale_worker_scan.failed",
                    {"error.category": "persistence_invariant", "outcome": "failed"},
                    error=error,
                )
                raise RecoveryScanInvariantError from error
            except RecoveryScanPersistenceUnavailable as error:
                add_metric(
                    "taskforge.recovery.scan.operations",
                    attributes={
                        "taskforge.scan.kind": "stale_worker_session",
                        "taskforge.outcome": "persistence_failure",
                    },
                )
                log_event(
                    logger,
                    logging.WARNING,
                    "scheduler.stale_worker_scan.failed",
                    {"error.category": "persistence_unavailable", "outcome": "failed"},
                    error=error,
                )
                raise RecoveryScanServiceUnavailable from error


def _validate_limit(limit: int) -> None:
    if type(limit) is not int or not 1 <= limit <= MAX_RECOVERY_SCAN_BATCH_SIZE:
        raise ValueError("recovery scan limit is outside the supported bounds")
