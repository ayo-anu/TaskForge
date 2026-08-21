"""Persistence boundaries for crash-recovery discovery and transitions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from types import TracebackType
from typing import Protocol

from taskforge.recovery.domain import (
    ExpiredClaimCandidate,
    ExpiredClaimCandidatePage,
    ExpiredClaimScanCursor,
    PreparedCancellationSettlement,
    PreparedExpiredClaimRecovery,
    StaleWorkerSessionCandidate,
    StaleWorkerSessionCandidatePage,
    StaleWorkerSessionScanCursor,
)
from taskforge.retries.domain import RetryNotScheduledReason
from taskforge.retries.persistence_ports import NewScheduledRetryAttempt


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


class ExpiredClaimRecoveryPersistenceInvariantViolation(Exception):
    """Durable recovery state violates an established invariant."""


class ExpiredClaimRecoveryPersistenceUnavailable(Exception):
    """Expired-claim recovery persistence is operationally unavailable."""


class ExpiredClaimRecoveryNoOpReason(StrEnum):
    CANDIDATE_NO_LONGER_EXPIRED = "candidate_no_longer_expired"
    CLAIM_ALREADY_TERMINATED = "claim_already_terminated"
    ATTEMPT_NO_LONGER_LATEST = "attempt_no_longer_latest"
    TASK_NOT_ELIGIBLE = "task_not_eligible"
    WORKFLOW_NOT_ELIGIBLE = "workflow_not_eligible"
    RESULT_ALREADY_ACCEPTED = "result_already_accepted"
    ALREADY_RECOVERED = "already_recovered"


@dataclass(frozen=True)
class ExpiredClaimRecoveryNoOp:
    reason: ExpiredClaimRecoveryNoOpReason


ExpiredClaimRecoveryPreparation = (
    PreparedExpiredClaimRecovery
    | PreparedCancellationSettlement
    | ExpiredClaimRecoveryNoOp
)


class ExpiredClaimRecoveryTransaction(Protocol):
    async def prepare_recovery(
        self, candidate: ExpiredClaimCandidate
    ) -> ExpiredClaimRecoveryPreparation: ...

    async def schedule_retry(
        self,
        prepared: PreparedExpiredClaimRecovery,
        attempt: NewScheduledRetryAttempt,
    ) -> None: ...

    async def exhaust(
        self,
        prepared: PreparedExpiredClaimRecovery,
        reason: RetryNotScheduledReason,
    ) -> None: ...

    async def settle_cancellation(
        self,
        prepared: PreparedCancellationSettlement,
    ) -> None: ...


class ExpiredClaimRecoveryTransactionContext(Protocol):
    async def __aenter__(self) -> ExpiredClaimRecoveryTransaction: ...

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...


class ExpiredClaimRecoveryRepository(Protocol):
    def recovery_transaction(self) -> ExpiredClaimRecoveryTransactionContext: ...


class StaleWorkerSessionRecoveryPersistenceInvariantViolation(Exception):
    """Durable worker-session recovery facts violate an invariant."""


class StaleWorkerSessionRecoveryPersistenceUnavailable(Exception):
    """Worker-session recovery persistence is operationally unavailable."""


class StaleWorkerSessionRecoveryNoOpReason(StrEnum):
    CANDIDATE_REFRESHED = "candidate_refreshed"
    SESSION_ALREADY_ENDED = "session_already_ended"


@dataclass(frozen=True)
class EndedStaleWorkerSession:
    ended_at: datetime


StaleWorkerSessionRecoveryResult = (
    EndedStaleWorkerSession | StaleWorkerSessionRecoveryNoOpReason
)


class StaleWorkerSessionRecoveryRepository(Protocol):
    async def end_stale_session(
        self,
        candidate: StaleWorkerSessionCandidate,
        *,
        stale_after_seconds: int,
    ) -> StaleWorkerSessionRecoveryResult: ...
