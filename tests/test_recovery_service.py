"""Focused tests for one-candidate expired-claim recovery."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import TracebackType
from uuid import uuid4

import pytest

from taskforge.recovery.domain import (
    ExpiredClaimCandidate,
    PreparedExpiredClaimRecovery,
)
from taskforge.recovery.persistence_ports import (
    ExpiredClaimRecoveryNoOp,
    ExpiredClaimRecoveryNoOpReason,
    ExpiredClaimRecoveryPersistenceInvariantViolation,
    ExpiredClaimRecoveryPersistenceUnavailable,
    ExpiredClaimRecoveryPreparation,
)
from taskforge.recovery.service import (
    ExpiredClaimRecoveryInvariantError,
    ExpiredClaimRecoveryOutcome,
    ExpiredClaimRecoveryReceipt,
    ExpiredClaimRecoveryService,
    ExpiredClaimRecoveryServiceUnavailable,
)
from taskforge.retries.domain import (
    RetryDecision,
    RetryDecisionKind,
    RetryNotScheduledReason,
)
from taskforge.retries.persistence_ports import NewScheduledRetryAttempt
from taskforge.runs.domain import TaskRunStatus
from taskforge.worker.results import (
    TaskExecutionFailureKind,
    TaskExecutionResult,
    TaskExecutionResultKind,
)

RECOVERED_AT = datetime(2026, 8, 14, 12, tzinfo=UTC)


def candidate() -> ExpiredClaimCandidate:
    return ExpiredClaimCandidate(
        uuid4(),
        uuid4(),
        uuid4(),
        1,
        1,
        uuid4(),
        RECOVERED_AT - timedelta(seconds=1),
        RECOVERED_AT - timedelta(microseconds=1),
    )


def prepared(
    value: ExpiredClaimCandidate,
    *,
    attempt_number: int = 1,
    maximum_attempts: int | None = 3,
) -> PreparedExpiredClaimRecovery:
    policy = None
    if maximum_attempts is not None:
        policy = {
            "retry_policy": {
                "maximum_attempts": maximum_attempts,
                "initial_delay_seconds": 7,
                "multiplier": 2,
                "maximum_delay_seconds": 60,
            }
        }
    return PreparedExpiredClaimRecovery(
        value.task_attempt_id,
        value.task_run_id,
        value.workflow_run_id,
        TaskRunStatus.RUNNING,
        attempt_number,
        value.generation,
        value.worker_session_id,
        uuid4(),
        value.lease_expires_at,
        RECOVERED_AT,
        policy,  # type: ignore[arg-type]
        None,
    )


@dataclass
class FakeRecoveryTransaction:
    preparation: ExpiredClaimRecoveryPreparation
    failure: Exception | None = None
    scheduled: list[tuple[PreparedExpiredClaimRecovery, NewScheduledRetryAttempt]] = (
        field(default_factory=list)
    )
    exhausted: list[tuple[PreparedExpiredClaimRecovery, RetryNotScheduledReason]] = (
        field(default_factory=list)
    )
    exited_with: type[BaseException] | object | None = "not-exited"

    async def __aenter__(self) -> FakeRecoveryTransaction:
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exception, traceback
        self.exited_with = exception_type

    async def prepare_recovery(
        self, value: ExpiredClaimCandidate
    ) -> ExpiredClaimRecoveryPreparation:
        del value
        if self.failure is not None:
            raise self.failure
        return self.preparation

    async def schedule_retry(
        self,
        transition: PreparedExpiredClaimRecovery,
        attempt: NewScheduledRetryAttempt,
    ) -> None:
        if self.failure is not None:
            raise self.failure
        self.scheduled.append((transition, attempt))

    async def exhaust(
        self,
        transition: PreparedExpiredClaimRecovery,
        reason: RetryNotScheduledReason,
    ) -> bool:
        if self.failure is not None:
            raise self.failure
        self.exhausted.append((transition, reason))
        return True


@dataclass(frozen=True)
class FakeRecoveryRepository:
    transaction: FakeRecoveryTransaction

    def recovery_transaction(self) -> FakeRecoveryTransaction:
        return self.transaction


def recover(
    value: ExpiredClaimCandidate, transaction: FakeRecoveryTransaction
) -> ExpiredClaimRecoveryReceipt:
    return asyncio.run(
        ExpiredClaimRecoveryService(
            FakeRecoveryRepository(transaction)
        ).recover_expired_claim(value)
    )


def test_recovery_schedules_exactly_next_attempt_from_database_time() -> None:
    value = candidate()
    transition = prepared(value)
    transaction = FakeRecoveryTransaction(transition)

    receipt = recover(value, transaction)

    assert receipt.outcome is ExpiredClaimRecoveryOutcome.RETRY_SCHEDULED
    assert receipt.recovered_at == RECOVERED_AT
    assert receipt.scheduled_attempt_number == 2
    assert receipt.next_eligible_at == RECOVERED_AT + timedelta(seconds=7)
    assert transaction.scheduled[0][1].attempt_number == 2
    assert transaction.exhausted == []
    assert transaction.exited_with is None


@pytest.mark.parametrize(
    ("maximum_attempts", "outcome", "reason"),
    [
        (None, ExpiredClaimRecoveryOutcome.FAILED_NO_POLICY, "no_policy"),
        (1, ExpiredClaimRecoveryOutcome.FAILED_EXHAUSTED, "exhausted"),
    ],
)
def test_recovery_exhausts_without_replacement(
    maximum_attempts: int | None,
    outcome: ExpiredClaimRecoveryOutcome,
    reason: str,
) -> None:
    value = candidate()
    transaction = FakeRecoveryTransaction(
        prepared(value, maximum_attempts=maximum_attempts)
    )

    receipt = recover(value, transaction)

    assert receipt.outcome is outcome
    assert transaction.exhausted[0][1].value == reason
    assert transaction.scheduled == []


@pytest.mark.parametrize("reason", list(ExpiredClaimRecoveryNoOpReason))
def test_concurrent_invalidation_is_a_typed_noop(
    reason: ExpiredClaimRecoveryNoOpReason,
) -> None:
    value = candidate()
    transaction = FakeRecoveryTransaction(ExpiredClaimRecoveryNoOp(reason))

    receipt = recover(value, transaction)

    assert receipt.outcome.value == reason.value
    assert receipt.recovered_at is None
    assert transaction.scheduled == []
    assert transaction.exhausted == []


@pytest.mark.parametrize(
    ("failure", "expected"),
    [
        (
            ExpiredClaimRecoveryPersistenceInvariantViolation(),
            ExpiredClaimRecoveryInvariantError,
        ),
        (
            ExpiredClaimRecoveryPersistenceUnavailable(),
            ExpiredClaimRecoveryServiceUnavailable,
        ),
    ],
)
def test_persistence_failures_preserve_service_boundary(
    failure: Exception, expected: type[Exception]
) -> None:
    value = candidate()
    transaction = FakeRecoveryTransaction(prepared(value), failure=failure)

    with pytest.raises(expected):
        recover(value, transaction)

    assert transaction.exited_with is type(failure)


def test_claim_expired_is_not_a_worker_producible_failure() -> None:
    with pytest.raises(ValueError, match="supported failure kind"):
        TaskExecutionResult(
            TaskExecutionResultKind.RETRYABLE_FAILURE,
            failure_kind=TaskExecutionFailureKind.CLAIM_EXPIRED,
        )


def test_incomplete_retry_allowed_decision_is_an_explicit_invariant_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = candidate()
    transaction = FakeRecoveryTransaction(prepared(value))
    monkeypatch.setattr(
        "taskforge.recovery.service.decide_retry",
        lambda **_: RetryDecision(RetryDecisionKind.RETRY_ALLOWED, 1),
    )

    with pytest.raises(ExpiredClaimRecoveryInvariantError):
        recover(value, transaction)

    assert transaction.scheduled == []
    assert transaction.exhausted == []
