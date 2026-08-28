"""Persistence contracts for atomic retry-state transitions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from types import TracebackType
from typing import Protocol
from uuid import UUID

from taskforge.retries.domain import JSONMapping, RetryNotScheduledReason


class RetryTransitionPersistenceInvariantViolation(Exception):
    """Durable retry state violates an established lifecycle invariant."""


class RetryTransitionPersistenceUnavailable(Exception):
    """Retry transition persistence is operationally unavailable."""


class DueRetryPersistenceInvariantViolation(Exception):
    """Durable due-retry state violates a lifecycle invariant."""


class DueRetryPersistenceUnavailable(Exception):
    """Due-retry persistence is operationally unavailable."""


@dataclass(frozen=True)
class PreparedRetryTransition:
    task_run_id: UUID
    failed_attempt_id: UUID
    failed_attempt_number: int
    completed_at: datetime
    workflow_execution_policy: JSONMapping | None
    step_execution_policy: JSONMapping | None
    correlation_id: str | None = None

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

    async def fail_retry(
        self,
        prepared: PreparedRetryTransition,
        reason: RetryNotScheduledReason,
    ) -> bool: ...


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


@dataclass(frozen=True, repr=False)
class PreparedDueRetryDispatch:
    workflow_run_id: UUID
    task_run_id: UUID
    workflow_version_id: UUID
    step_identifier: str
    task_attempt_id: UUID
    attempt_number: int
    next_eligible_at: datetime
    task_type: str
    task_parameters: JSONMapping
    deadline_at: datetime | None
    execution_timeout_seconds: int | None
    predecessor_attempt_id: UUID
    predecessor_attempt_number: int
    predecessor_dispatch_id: UUID
    predecessor_route: str
    predecessor_payload: dict[str, object]

    def __post_init__(self) -> None:
        if self.attempt_number <= 1:
            raise ValueError("due retry attempt number must exceed one")
        if self.predecessor_attempt_number != self.attempt_number - 1:
            raise ValueError("retry predecessor number must be consecutive")
        for timestamp in (self.next_eligible_at, self.deadline_at):
            if timestamp is not None and (
                timestamp.tzinfo is None or timestamp.utcoffset() is None
            ):
                raise ValueError("retry dispatch timestamps must be timezone-aware")
        object.__setattr__(
            self, "next_eligible_at", self.next_eligible_at.astimezone(UTC)
        )
        if self.deadline_at is not None:
            object.__setattr__(self, "deadline_at", self.deadline_at.astimezone(UTC))

    def __repr__(self) -> str:
        return (
            "PreparedDueRetryDispatch("
            f"workflow_run_id={self.workflow_run_id!r}, "
            f"task_run_id={self.task_run_id!r}, "
            f"task_attempt_id={self.task_attempt_id!r}, "
            f"attempt_number={self.attempt_number!r}, "
            f"next_eligible_at={self.next_eligible_at!r}, "
            f"task_type={self.task_type!r}, task_parameters=<redacted>, "
            "predecessor_payload=<redacted>)"
        )


@dataclass(frozen=True)
class SkippedDueRetryCandidate:
    task_attempt_id: UUID


DueRetryPreparation = PreparedDueRetryDispatch | SkippedDueRetryCandidate | None


class DueRetryDispatchTransaction(Protocol):
    async def prepare_next_due(self) -> DueRetryPreparation: ...

    async def persist_dispatch(
        self,
        prepared: PreparedDueRetryDispatch,
        outbox_id: UUID,
        route: str,
        payload: dict[str, object],
    ) -> None: ...


class DueRetryDispatchTransactionContext(Protocol):
    async def __aenter__(self) -> DueRetryDispatchTransaction: ...

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...


class DueRetryDispatchRepository(Protocol):
    def due_dispatch_transaction(self) -> DueRetryDispatchTransactionContext: ...
