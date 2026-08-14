"""Application service for durable retry scheduling transitions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from uuid import UUID, uuid4

from taskforge.retries.domain import (
    InvalidPersistedRetryPolicy,
    RetryCalculationError,
    RetryDecisionKind,
    decide_retry,
    resolve_persisted_retry_policy,
)
from taskforge.retries.persistence_ports import (
    ExistingScheduledRetry,
    NewScheduledRetryAttempt,
    RetryTransitionPersistenceInvariantViolation,
    RetryTransitionPersistenceUnavailable,
    RetryTransitionRepository,
)


class RetryTransitionOutcome(StrEnum):
    SCHEDULED = "scheduled"
    FAILED_NO_POLICY = "failed_no_policy"
    FAILED_EXHAUSTED = "failed_exhausted"
    ALREADY_SCHEDULED = "already_scheduled"
    NOT_ELIGIBLE = "not_eligible"


class RetryTransitionInvariantError(Exception):
    """Durable retry state cannot be interpreted or transitioned safely."""


class RetryTransitionServiceUnavailable(Exception):
    """Retry transition persistence is operationally unavailable."""


@dataclass(frozen=True)
class RetryTransitionReceipt:
    outcome: RetryTransitionOutcome
    task_run_id: UUID
    failed_attempt_id: UUID | None = None
    failed_attempt_number: int | None = None
    scheduled_attempt_id: UUID | None = None
    scheduled_attempt_number: int | None = None
    next_eligible_at: datetime | None = None


class RetryTransitionService:
    def __init__(self, repository: RetryTransitionRepository) -> None:
        self._repository = repository

    async def transition_retry(self, task_run_id: UUID) -> RetryTransitionReceipt:
        try:
            async with self._repository.transition_transaction() as transaction:
                prepared = await transaction.prepare_transition(task_run_id)
                if prepared is None:
                    return RetryTransitionReceipt(
                        RetryTransitionOutcome.NOT_ELIGIBLE, task_run_id
                    )
                if isinstance(prepared, ExistingScheduledRetry):
                    return _existing_receipt(prepared)

                policy = resolve_persisted_retry_policy(
                    prepared.workflow_execution_policy,
                    prepared.step_execution_policy,
                )
                decision = decide_retry(
                    policy=policy,
                    failed_attempt_number=prepared.failed_attempt_number,
                    completed_at=prepared.completed_at,
                )
                if decision.kind is RetryDecisionKind.RETRY_ALLOWED:
                    assert decision.next_attempt_number is not None
                    assert decision.next_eligible_at is not None
                    attempt = NewScheduledRetryAttempt(
                        uuid4(),
                        prepared.task_run_id,
                        decision.next_attempt_number,
                        decision.next_eligible_at,
                    )
                    await transaction.schedule_retry(prepared, attempt)
                    return RetryTransitionReceipt(
                        RetryTransitionOutcome.SCHEDULED,
                        prepared.task_run_id,
                        prepared.failed_attempt_id,
                        prepared.failed_attempt_number,
                        attempt.id,
                        attempt.attempt_number,
                        attempt.next_eligible_at,
                    )

                await transaction.fail_retry(prepared)
                outcome = (
                    RetryTransitionOutcome.FAILED_NO_POLICY
                    if decision.kind is RetryDecisionKind.NO_POLICY
                    else RetryTransitionOutcome.FAILED_EXHAUSTED
                )
                return RetryTransitionReceipt(
                    outcome,
                    prepared.task_run_id,
                    prepared.failed_attempt_id,
                    prepared.failed_attempt_number,
                )
        except (
            InvalidPersistedRetryPolicy,
            RetryCalculationError,
            RetryTransitionPersistenceInvariantViolation,
        ) as error:
            raise RetryTransitionInvariantError from error
        except RetryTransitionPersistenceUnavailable as error:
            raise RetryTransitionServiceUnavailable from error


def _existing_receipt(existing: ExistingScheduledRetry) -> RetryTransitionReceipt:
    return RetryTransitionReceipt(
        RetryTransitionOutcome.ALREADY_SCHEDULED,
        existing.task_run_id,
        existing.failed_attempt_id,
        existing.failed_attempt_number,
        existing.scheduled_attempt_id,
        existing.scheduled_attempt_number,
        existing.next_eligible_at,
    )
