"""Application-service tests for atomic retry transitions."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import TracebackType
from uuid import UUID, uuid4

import pytest

from taskforge.retries.domain import RetryNotScheduledReason
from taskforge.retries.persistence_ports import (
    ExistingScheduledRetry,
    NewScheduledRetryAttempt,
    PreparedRetryTransition,
    RetryTransitionPersistenceInvariantViolation,
    RetryTransitionPersistenceUnavailable,
    RetryTransitionPreparation,
)
from taskforge.retries.service import (
    RetryTransitionInvariantError,
    RetryTransitionOutcome,
    RetryTransitionReceipt,
    RetryTransitionService,
    RetryTransitionServiceUnavailable,
)

COMPLETED_AT = datetime(2026, 8, 14, 12, 30, 0, 123456, tzinfo=UTC)


def retry_policy(
    *,
    maximum_attempts: int = 4,
    initial_delay_seconds: int = 10,
    multiplier: float = 2,
    maximum_delay_seconds: int = 300,
) -> dict[str, object]:
    return {
        "maximum_attempts": maximum_attempts,
        "initial_delay_seconds": initial_delay_seconds,
        "multiplier": multiplier,
        "maximum_delay_seconds": maximum_delay_seconds,
    }


def prepared(
    *,
    attempt_number: int = 1,
    workflow_policy: dict[str, object] | None = None,
    step_policy: dict[str, object] | None = None,
) -> PreparedRetryTransition:
    return PreparedRetryTransition(
        uuid4(),
        uuid4(),
        attempt_number,
        COMPLETED_AT,
        workflow_policy,  # type: ignore[arg-type]
        step_policy,  # type: ignore[arg-type]
    )


@dataclass
class FakeTransaction:
    preparation: RetryTransitionPreparation
    failure: Exception | None = None
    scheduled: list[tuple[PreparedRetryTransition, NewScheduledRetryAttempt]] = field(
        default_factory=list
    )
    failed: list[tuple[PreparedRetryTransition, RetryNotScheduledReason]] = field(
        default_factory=list
    )
    exited_with: type[BaseException] | object | None = field(default="not-exited")

    async def __aenter__(self) -> FakeTransaction:
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exception, traceback
        self.exited_with = exception_type

    async def prepare_transition(self, task_run_id: UUID) -> RetryTransitionPreparation:
        del task_run_id
        if self.failure is not None:
            raise self.failure
        return self.preparation

    async def schedule_retry(
        self,
        transition: PreparedRetryTransition,
        attempt: NewScheduledRetryAttempt,
    ) -> None:
        if self.failure is not None:
            raise self.failure
        self.scheduled.append((transition, attempt))

    async def fail_retry(
        self,
        transition: PreparedRetryTransition,
        reason: RetryNotScheduledReason,
    ) -> bool:
        if self.failure is not None:
            raise self.failure
        self.failed.append((transition, reason))
        return True


@dataclass(frozen=True)
class FakeRepository:
    transaction: FakeTransaction

    def transition_transaction(self) -> FakeTransaction:
        return self.transaction


def run(
    transaction: FakeTransaction, task_run_id: UUID | None = None
) -> RetryTransitionReceipt:
    return asyncio.run(
        RetryTransitionService(FakeRepository(transaction)).transition_retry(
            task_run_id or uuid4()
        )
    )


def test_workflow_policy_schedules_next_attempt_from_durable_completion() -> None:
    transition = prepared(workflow_policy={"retry_policy": retry_policy()})
    transaction = FakeTransaction(transition)

    receipt = run(transaction, transition.task_run_id)

    stored_transition, attempt = transaction.scheduled[0]
    assert transaction.exited_with is None
    assert stored_transition is transition
    assert receipt.outcome is RetryTransitionOutcome.SCHEDULED
    assert receipt.scheduled_attempt_id == attempt.id
    assert attempt.attempt_number == 2
    assert attempt.next_eligible_at == COMPLETED_AT + timedelta(seconds=10)
    assert receipt.next_eligible_at == attempt.next_eligible_at
    assert transaction.failed == []


def test_step_policy_atomically_replaces_workflow_policy() -> None:
    transition = prepared(
        attempt_number=2,
        workflow_policy={
            "retry_policy": retry_policy(initial_delay_seconds=100, multiplier=3)
        },
        step_policy={
            "retry_policy": retry_policy(initial_delay_seconds=7, multiplier=2)
        },
    )
    transaction = FakeTransaction(transition)

    receipt = run(transaction)

    assert receipt.scheduled_attempt_number == 3
    assert receipt.next_eligible_at == COMPLETED_AT + timedelta(seconds=14)


def test_no_policy_fails_without_creating_attempt() -> None:
    transition = prepared()
    transaction = FakeTransaction(transition)

    receipt = run(transaction)

    assert receipt.outcome is RetryTransitionOutcome.FAILED_NO_POLICY
    assert receipt.dead_letter_created is True
    assert transaction.failed == [(transition, RetryNotScheduledReason.NO_POLICY)]
    assert transaction.scheduled == []


def test_exhausted_policy_fails_without_creating_attempt() -> None:
    transition = prepared(
        workflow_policy={"retry_policy": retry_policy(maximum_attempts=1)}
    )
    transaction = FakeTransaction(transition)

    receipt = run(transaction)

    assert receipt.outcome is RetryTransitionOutcome.FAILED_EXHAUSTED
    assert receipt.dead_letter_created is True
    assert transaction.failed == [(transition, RetryNotScheduledReason.EXHAUSTED)]
    assert transaction.scheduled == []


def test_zero_delay_persists_completion_timestamp_unchanged() -> None:
    transition = prepared(
        workflow_policy={
            "retry_policy": retry_policy(
                initial_delay_seconds=0, maximum_delay_seconds=0
            )
        }
    )
    transaction = FakeTransaction(transition)

    receipt = run(transaction)

    assert receipt.next_eligible_at == COMPLETED_AT
    assert transaction.scheduled[0][1].next_eligible_at == COMPLETED_AT


def test_existing_scheduled_retry_is_an_idempotent_noop() -> None:
    task_run_id, failed_id, scheduled_id = uuid4(), uuid4(), uuid4()
    existing = ExistingScheduledRetry(
        task_run_id,
        failed_id,
        1,
        scheduled_id,
        2,
        COMPLETED_AT + timedelta(seconds=10),
    )
    transaction = FakeTransaction(existing)

    receipt = run(transaction, task_run_id)

    assert receipt.outcome is RetryTransitionOutcome.ALREADY_SCHEDULED
    assert receipt.failed_attempt_id == failed_id
    assert receipt.scheduled_attempt_id == scheduled_id
    assert transaction.scheduled == []
    assert transaction.failed == []


def test_noneligible_lifecycle_state_is_a_noop() -> None:
    transaction = FakeTransaction(None)
    receipt = run(transaction)
    assert receipt.outcome is RetryTransitionOutcome.NOT_ELIGIBLE
    assert transaction.scheduled == []
    assert transaction.failed == []


def test_malformed_present_step_policy_fails_without_workflow_fallback() -> None:
    transition = prepared(
        workflow_policy={"retry_policy": retry_policy()},
        step_policy={"retry_policy": {"maximum_attempts": 4}},
    )
    transaction = FakeTransaction(transition)

    with pytest.raises(RetryTransitionInvariantError):
        run(transaction)

    assert transaction.exited_with is not None
    assert transaction.scheduled == []
    assert transaction.failed == []


@pytest.mark.parametrize(
    ("failure", "expected"),
    (
        (
            RetryTransitionPersistenceInvariantViolation(),
            RetryTransitionInvariantError,
        ),
        (
            RetryTransitionPersistenceUnavailable(),
            RetryTransitionServiceUnavailable,
        ),
    ),
)
def test_persistence_errors_are_translated(
    failure: Exception, expected: type[Exception]
) -> None:
    transaction = FakeTransaction(None, failure)
    with pytest.raises(expected):
        run(transaction)
    assert transaction.exited_with is not None
