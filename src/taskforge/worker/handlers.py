"""Immutable trusted execution-handler registration."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from uuid import UUID

from taskforge.dispatch.envelope import FrozenJSONMapping, TraceContext
from taskforge.workflows.task_types import TaskTypeRegistry


@dataclass(frozen=True, repr=False)
class TaskDeadline:
    expires_at: datetime

    def __post_init__(self) -> None:
        if self.expires_at.tzinfo is None or self.expires_at.utcoffset() is None:
            raise ValueError("task deadline must be timezone-aware")
        object.__setattr__(self, "expires_at", self.expires_at.astimezone(UTC))

    def __repr__(self) -> str:
        return f"TaskDeadline(expires_at={self.expires_at!r})"


@dataclass(frozen=True, repr=False)
class TaskContext:
    idempotency_key: str
    dispatch_id: UUID
    workflow_run_id: UUID
    task_run_id: UUID
    task_attempt_id: UUID
    attempt_number: int
    task_type: str
    parameters: FrozenJSONMapping
    references: FrozenJSONMapping
    correlation_id: str | None
    trace_context: TraceContext | None
    cancellation_requested_at_start: bool
    deadline: TaskDeadline | None

    def __repr__(self) -> str:
        return (
            "TaskContext("
            "idempotency_key=<redacted>, "
            f"dispatch_id={self.dispatch_id!r}, workflow_run_id={self.workflow_run_id!r}, "
            f"task_run_id={self.task_run_id!r}, task_attempt_id={self.task_attempt_id!r}, "
            f"attempt_number={self.attempt_number!r}, task_type={self.task_type!r}, "
            "parameters=<redacted>, references=<redacted>, "
            "correlation_id=<redacted>, trace_context=<redacted>, "
            f"cancellation_requested_at_start={self.cancellation_requested_at_start!r}, "
            f"deadline={self.deadline!r})"
        )


def create_task_context(
    *,
    dispatch_id: UUID,
    workflow_run_id: UUID,
    task_run_id: UUID,
    task_attempt_id: UUID,
    attempt_number: int,
    task_type: str,
    parameters: FrozenJSONMapping,
    references: FrozenJSONMapping,
    correlation_id: str | None,
    trace_context: TraceContext | None,
    cancellation_requested_at_start: bool,
    deadline: TaskDeadline | None,
) -> TaskContext:
    return TaskContext(
        f"taskforge:task-attempt:{task_attempt_id}",
        dispatch_id,
        workflow_run_id,
        task_run_id,
        task_attempt_id,
        attempt_number,
        task_type,
        parameters,
        references,
        correlation_id,
        trace_context,
        cancellation_requested_at_start,
        deadline,
    )


type TaskHandler = Callable[[TaskContext], Awaitable[object]]


@dataclass(frozen=True)
class TaskHandlerDefinition:
    task_type: str
    required_capability: str
    handler: TaskHandler


class TaskHandlerRegistry:
    """Trusted immutable handlers assembled explicitly at composition time."""

    def __init__(
        self,
        definitions: tuple[TaskHandlerDefinition, ...],
        task_types: TaskTypeRegistry,
    ) -> None:
        registered: dict[str, TaskHandlerDefinition] = {}
        for definition in definitions:
            task_type = task_types.definition(definition.task_type)
            if task_type is None:
                raise ValueError("handler task type is not registered")
            if task_type.required_capability != definition.required_capability:
                raise ValueError("handler capability does not match task type")
            if definition.task_type in registered:
                raise ValueError("duplicate registered task handler")
            if not callable(definition.handler):
                raise ValueError("registered task handler is not callable")
            registered[definition.task_type] = definition
        self._registered: Mapping[str, TaskHandlerDefinition] = MappingProxyType(
            registered
        )

    @property
    def task_types(self) -> frozenset[str]:
        return frozenset(self._registered)

    @property
    def required_capabilities(self) -> frozenset[str]:
        return frozenset(
            definition.required_capability for definition in self._registered.values()
        )

    def definition(self, task_type: str) -> TaskHandlerDefinition | None:
        return self._registered.get(task_type)
