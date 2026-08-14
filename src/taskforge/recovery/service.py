"""Application service for one authoritative expired-claim recovery."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from uuid import UUID, uuid4

from taskforge.recovery.domain import (
    ExpiredClaimCandidate,
    PreparedExpiredClaimRecovery,
)
from taskforge.recovery.persistence_ports import (
    ExpiredClaimRecoveryNoOp,
    ExpiredClaimRecoveryPersistenceInvariantViolation,
    ExpiredClaimRecoveryPersistenceUnavailable,
    ExpiredClaimRecoveryRepository,
    ExpiredClaimRecoveryTransaction,
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


class ExpiredClaimRecoveryOutcome(StrEnum):
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
        try:
            async with self._repository.recovery_transaction() as transaction:
                preparation = await transaction.prepare_recovery(candidate)
                if isinstance(preparation, ExpiredClaimRecoveryNoOp):
                    return ExpiredClaimRecoveryReceipt(
                        ExpiredClaimRecoveryOutcome(preparation.reason.value),
                        candidate.task_attempt_id,
                        candidate.task_run_id,
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
