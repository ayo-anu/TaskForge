"""Persistence contracts for atomic retry-state transitions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from types import TracebackType
from typing import Protocol
from uuid import UUID

from taskforge.retries.domain import JSONMapping


class RetryTransitionPersistenceInvariantViolation(Exception):
    """Durable retry state violates an established lifecycle invariant."""


class RetryTransitionPersistenceUnavailable(Exception):
    """Retry transition persistence is operationally unavailable."""


@dataclass(frozen=True)
class PreparedRetryTransition:
    task_run_id: UUID
    failed_attempt_id: UUID
    failed_attempt_number: int
    completed_at: datetime
    workflow_execution_policy: JSONMapping | None
    step_execution_policy: JSONMapping | None

    def __post_init__(self) -> None:
        if self.failed_attempt_number <= 0:
            raise ValueError("failed attempt number must be positive")
        if self.completed_at.tzinfo is None or self.completed_at.utcoffset() is None:
            raise ValueError("result completion timestamp must be timezone-aware")
        object.__setattr__(self, "completed_at", self.completed_at.astimezone(UTC))


@dataclass(frozen=True)
class ExistingScheduledRetry:
    task_run_id: UUID
    failed_attempt_id: UUID
    failed_attempt_number: int
    scheduled_attempt_id: UUID
    scheduled_attempt_number: int
    next_eligible_at: datetime

    def __post_init__(self) -> None:
        if self.failed_attempt_number <= 0:
            raise ValueError("failed attempt number must be positive")
        if self.scheduled_attempt_number != self.failed_attempt_number + 1:
            raise ValueError("scheduled attempt must immediately follow failed attempt")
        if (
            self.next_eligible_at.tzinfo is None
            or self.next_eligible_at.utcoffset() is None
        ):
            raise ValueError("retry eligibility timestamp must be timezone-aware")
        object.__setattr__(
            self, "next_eligible_at", self.next_eligible_at.astimezone(UTC)
        )


@dataclass(frozen=True)
class NewScheduledRetryAttempt:
    id: UUID
    task_run_id: UUID
    attempt_number: int
    next_eligible_at: datetime

    def __post_init__(self) -> None:
        if self.attempt_number <= 1:
            raise ValueError("scheduled retry attempt number must exceed one")
        if (
            self.next_eligible_at.tzinfo is None
            or self.next_eligible_at.utcoffset() is None
        ):
            raise ValueError("retry eligibility timestamp must be timezone-aware")
        object.__setattr__(
            self, "next_eligible_at", self.next_eligible_at.astimezone(UTC)
        )


RetryTransitionPreparation = PreparedRetryTransition | ExistingScheduledRetry | None


class RetryTransitionTransaction(Protocol):
    async def prepare_transition(
        self, task_run_id: UUID
    ) -> RetryTransitionPreparation: ...

    async def schedule_retry(
        self,
        prepared: PreparedRetryTransition,
        attempt: NewScheduledRetryAttempt,
    ) -> None: ...

    async def fail_retry(self, prepared: PreparedRetryTransition) -> None: ...


class RetryTransitionTransactionContext(Protocol):
    async def __aenter__(self) -> RetryTransitionTransaction: ...

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...


class RetryTransitionRepository(Protocol):
    def transition_transaction(self) -> RetryTransitionTransactionContext: ...
