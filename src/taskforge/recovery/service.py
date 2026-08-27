"""Application service for one authoritative expired-claim recovery."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from uuid import UUID, uuid4

from taskforge.recovery.domain import (
    ExpiredClaimCandidate,
    PreparedCancellationSettlement,
    PreparedExpiredClaimRecovery,
    StaleWorkerSessionCandidate,
)
from taskforge.recovery.persistence_ports import (
    EndedStaleWorkerSession,
    ExpiredClaimRecoveryNoOp,
    ExpiredClaimRecoveryPersistenceInvariantViolation,
    ExpiredClaimRecoveryPersistenceUnavailable,
    ExpiredClaimRecoveryRepository,
    ExpiredClaimRecoveryTransaction,
    StaleWorkerSessionRecoveryPersistenceInvariantViolation,
    StaleWorkerSessionRecoveryPersistenceUnavailable,
    StaleWorkerSessionRecoveryRepository,
)
from taskforge.retries.domain import (
    InvalidPersistedRetryPolicy,
    RetryCalculationError,
    RetryDecisionKind,
    RetryNotScheduledReason,
    decide_retry,
    resolve_persisted_retry_policy,
)
from taskforge.retries.persistence_ports import NewScheduledRetryAttempt
from taskforge.tracing import set_attributes, set_error, span


class ExpiredClaimRecoveryOutcome(StrEnum):
    CANCELLED = "cancelled"
    RETRY_SCHEDULED = "retry_scheduled"
    FAILED_NO_POLICY = "failed_no_policy"
    FAILED_EXHAUSTED = "failed_exhausted"
    CANDIDATE_NO_LONGER_EXPIRED = "candidate_no_longer_expired"
    CLAIM_ALREADY_TERMINATED = "claim_already_terminated"
    ATTEMPT_NO_LONGER_LATEST = "attempt_no_longer_latest"
    TASK_NOT_ELIGIBLE = "task_not_eligible"
    WORKFLOW_NOT_ELIGIBLE = "workflow_not_eligible"
    RESULT_ALREADY_ACCEPTED = "result_already_accepted"
    ALREADY_RECOVERED = "already_recovered"


class ExpiredClaimRecoveryInvariantError(Exception):
    """Durable recovery state cannot be interpreted safely."""


class ExpiredClaimRecoveryServiceUnavailable(Exception):
    """Expired-claim recovery persistence is unavailable."""


@dataclass(frozen=True)
class ExpiredClaimRecoveryReceipt:
    outcome: ExpiredClaimRecoveryOutcome
    task_attempt_id: UUID
    task_run_id: UUID
    recovered_at: datetime | None = None
    scheduled_attempt_id: UUID | None = None
    scheduled_attempt_number: int | None = None
    next_eligible_at: datetime | None = None


class ExpiredClaimRecoveryService:
    def __init__(self, repository: ExpiredClaimRecoveryRepository) -> None:
        self._repository = repository

    async def recover_expired_claim(
        self, candidate: ExpiredClaimCandidate
    ) -> ExpiredClaimRecoveryReceipt:
        with span(
            "taskforge.recovery.expired_claim",
            attributes={
                "db.system.name": "postgresql",
                "taskforge.task_attempt.id": str(candidate.task_attempt_id),
            },
        ) as active_span:
            try:
                receipt = await self._recover_expired_claim(candidate)
            except (
                ExpiredClaimRecoveryInvariantError,
                ExpiredClaimRecoveryServiceUnavailable,
            ) as error:
                set_error(active_span, error, "expired_claim_recovery_failure")
                raise
            set_attributes(active_span, {"taskforge.outcome": receipt.outcome.value})
            return receipt

    async def _recover_expired_claim(
        self, candidate: ExpiredClaimCandidate
    ) -> ExpiredClaimRecoveryReceipt:
        try:
            async with self._repository.recovery_transaction() as transaction:
                preparation = await transaction.prepare_recovery(candidate)
                if isinstance(preparation, ExpiredClaimRecoveryNoOp):
                    return ExpiredClaimRecoveryReceipt(
                        ExpiredClaimRecoveryOutcome(preparation.reason.value),
                        candidate.task_attempt_id,
                        candidate.task_run_id,
                    )
                if isinstance(preparation, PreparedCancellationSettlement):
                    await transaction.settle_cancellation(preparation)
                    return ExpiredClaimRecoveryReceipt(
                        ExpiredClaimRecoveryOutcome.CANCELLED,
                        preparation.task_attempt_id,
                        preparation.task_run_id,
                        preparation.recovered_at,
                    )
                return await self._apply_policy(transaction, preparation)
        except (
            InvalidPersistedRetryPolicy,
            RetryCalculationError,
            ExpiredClaimRecoveryPersistenceInvariantViolation,
        ) as error:
            raise ExpiredClaimRecoveryInvariantError from error
        except ExpiredClaimRecoveryPersistenceUnavailable as error:
            raise ExpiredClaimRecoveryServiceUnavailable from error

    async def _apply_policy(
        self,
        transaction: ExpiredClaimRecoveryTransaction,
        prepared: PreparedExpiredClaimRecovery,
    ) -> ExpiredClaimRecoveryReceipt:
        policy = resolve_persisted_retry_policy(
            prepared.workflow_execution_policy, prepared.step_execution_policy
        )
        decision = decide_retry(
            policy=policy,
            failed_attempt_number=prepared.attempt_number,
            completed_at=prepared.recovered_at,
        )
        if decision.kind is RetryDecisionKind.RETRY_ALLOWED:
            if (
                decision.next_attempt_number is None
                or decision.next_eligible_at is None
            ):
                raise RetryCalculationError(
                    "retry-allowed recovery decision is incomplete"
                )
            attempt = NewScheduledRetryAttempt(
                uuid4(),
                prepared.task_run_id,
                decision.next_attempt_number,
                decision.next_eligible_at,
            )
            await transaction.schedule_retry(prepared, attempt)
            return ExpiredClaimRecoveryReceipt(
                ExpiredClaimRecoveryOutcome.RETRY_SCHEDULED,
                prepared.task_attempt_id,
                prepared.task_run_id,
                prepared.recovered_at,
                attempt.id,
                attempt.attempt_number,
                attempt.next_eligible_at,
            )
        reason = (
            RetryNotScheduledReason.NO_POLICY
            if decision.kind is RetryDecisionKind.NO_POLICY
            else RetryNotScheduledReason.EXHAUSTED
        )
        await transaction.exhaust(prepared, reason)
        outcome = (
            ExpiredClaimRecoveryOutcome.FAILED_NO_POLICY
            if decision.kind is RetryDecisionKind.NO_POLICY
            else ExpiredClaimRecoveryOutcome.FAILED_EXHAUSTED
        )
        return ExpiredClaimRecoveryReceipt(
            outcome,
            prepared.task_attempt_id,
            prepared.task_run_id,
            prepared.recovered_at,
        )


class StaleWorkerSessionRecoveryOutcome(StrEnum):
    SESSION_ENDED = "session_ended"
    CANDIDATE_REFRESHED = "candidate_refreshed"
    SESSION_ALREADY_ENDED = "session_already_ended"


class StaleWorkerSessionRecoveryInvariantError(Exception):
    """Durable worker-session recovery state is inconsistent."""


class StaleWorkerSessionRecoveryServiceUnavailable(Exception):
    """Worker-session recovery persistence is unavailable."""


@dataclass(frozen=True)
class StaleWorkerSessionRecoveryReceipt:
    outcome: StaleWorkerSessionRecoveryOutcome
    worker_session_id: UUID
    ended_at: datetime | None = None


class StaleWorkerSessionRecoveryService:
    def __init__(self, repository: StaleWorkerSessionRecoveryRepository) -> None:
        self._repository = repository

    async def end_stale_session(
        self,
        candidate: StaleWorkerSessionCandidate,
        *,
        stale_after_seconds: int,
    ) -> StaleWorkerSessionRecoveryReceipt:
        with span(
            "taskforge.recovery.stale_session",
            attributes={
                "db.system.name": "postgresql",
                "taskforge.worker.session.id": str(candidate.worker_session_id),
            },
        ) as active_span:
            try:
                receipt = await self._end_stale_session(
                    candidate, stale_after_seconds=stale_after_seconds
                )
            except (
                StaleWorkerSessionRecoveryInvariantError,
                StaleWorkerSessionRecoveryServiceUnavailable,
            ) as error:
                set_error(active_span, error, "stale_session_recovery_failure")
                raise
            set_attributes(active_span, {"taskforge.outcome": receipt.outcome.value})
            return receipt

    async def _end_stale_session(
        self,
        candidate: StaleWorkerSessionCandidate,
        *,
        stale_after_seconds: int,
    ) -> StaleWorkerSessionRecoveryReceipt:
        if type(stale_after_seconds) is not int or not 1 <= stale_after_seconds <= 3600:
            raise ValueError("stale-session recovery threshold is out of range")
        try:
            result = await self._repository.end_stale_session(
                candidate, stale_after_seconds=stale_after_seconds
            )
        except StaleWorkerSessionRecoveryPersistenceInvariantViolation as error:
            raise StaleWorkerSessionRecoveryInvariantError from error
        except StaleWorkerSessionRecoveryPersistenceUnavailable as error:
            raise StaleWorkerSessionRecoveryServiceUnavailable from error
        if isinstance(result, EndedStaleWorkerSession):
            return StaleWorkerSessionRecoveryReceipt(
                StaleWorkerSessionRecoveryOutcome.SESSION_ENDED,
                candidate.worker_session_id,
                result.ended_at,
            )
        return StaleWorkerSessionRecoveryReceipt(
            StaleWorkerSessionRecoveryOutcome(result.value),
            candidate.worker_session_id,
        )
