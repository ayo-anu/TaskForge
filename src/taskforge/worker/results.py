"""In-memory normalized outcomes from one physical handler invocation."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class TaskExecutionResultKind(StrEnum):
    SUCCESS = "success"
    RETRYABLE_FAILURE = "retryable_failure"
    PERMANENT_FAILURE = "permanent_failure"
    CANCELLATION = "cancellation"


class TaskExecutionFailureKind(StrEnum):
    HANDLER_REPORTED = "handler_reported"
    HANDLER_EXCEPTION = "handler_exception"
    EXECUTION_TIMEOUT = "execution_timeout"


@dataclass(frozen=True)
class TaskRetryableFailure:
    """A trusted handler explicitly reported a retryable failure."""


@dataclass(frozen=True)
class TaskPermanentFailure:
    """A trusted handler explicitly reported a permanent failure."""


@dataclass(frozen=True)
class TaskCancellation:
    """A trusted handler explicitly reported business cancellation."""


@dataclass(frozen=True, repr=False)
class TaskExecutionResult:
    kind: TaskExecutionResultKind
    value: object | None = None
    failure_kind: TaskExecutionFailureKind | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.kind, TaskExecutionResultKind):
            raise ValueError("execution result kind must be a supported result kind")
        if self.failure_kind is not None and not isinstance(
            self.failure_kind, TaskExecutionFailureKind
        ):
            raise ValueError("failure kind must be a supported failure kind")
        if self.kind is TaskExecutionResultKind.SUCCESS:
            if self.failure_kind is not None:
                raise ValueError("successful execution cannot have a failure kind")
            return
        if self.kind is TaskExecutionResultKind.RETRYABLE_FAILURE:
            if self.value is not None:
                raise ValueError("retryable failure cannot have a value")
            if self.failure_kind not in (
                TaskExecutionFailureKind.HANDLER_REPORTED,
                TaskExecutionFailureKind.HANDLER_EXCEPTION,
                TaskExecutionFailureKind.EXECUTION_TIMEOUT,
            ):
                raise ValueError("retryable failure requires a supported failure kind")
            return
        if self.kind is TaskExecutionResultKind.PERMANENT_FAILURE:
            if self.value is not None:
                raise ValueError("permanent failure cannot have a value")
            if self.failure_kind is not TaskExecutionFailureKind.HANDLER_REPORTED:
                raise ValueError("permanent failure must be handler-reported")
            return
        if self.value is not None or self.failure_kind is not None:
            raise ValueError("cancelled execution cannot have a value or failure kind")

    @classmethod
    def success(cls, value: object) -> TaskExecutionResult:
        return cls(TaskExecutionResultKind.SUCCESS, value)

    @classmethod
    def retryable_handler_reported(cls) -> TaskExecutionResult:
        return cls(
            TaskExecutionResultKind.RETRYABLE_FAILURE,
            failure_kind=TaskExecutionFailureKind.HANDLER_REPORTED,
        )

    @classmethod
    def retryable_handler_exception(cls) -> TaskExecutionResult:
        return cls(
            TaskExecutionResultKind.RETRYABLE_FAILURE,
            failure_kind=TaskExecutionFailureKind.HANDLER_EXCEPTION,
        )

    @classmethod
    def retryable_execution_timeout(cls) -> TaskExecutionResult:
        return cls(
            TaskExecutionResultKind.RETRYABLE_FAILURE,
            failure_kind=TaskExecutionFailureKind.EXECUTION_TIMEOUT,
        )

    @classmethod
    def permanent_failure(cls) -> TaskExecutionResult:
        return cls(
            TaskExecutionResultKind.PERMANENT_FAILURE,
            failure_kind=TaskExecutionFailureKind.HANDLER_REPORTED,
        )

    @classmethod
    def cancellation(cls) -> TaskExecutionResult:
        return cls(TaskExecutionResultKind.CANCELLATION)

    def __repr__(self) -> str:
        return (
            "TaskExecutionResult("
            f"kind={self.kind!r}, value=<redacted>, "
            f"failure_kind={self.failure_kind!r})"
        )
