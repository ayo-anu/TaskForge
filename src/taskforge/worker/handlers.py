"""Immutable trusted execution-handler registration."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from uuid import UUID

from taskforge.dispatch.envelope import FrozenJSONMapping
from taskforge.workflows.task_types import TaskTypeRegistry


@dataclass(frozen=True, repr=False)
class TaskHandlerInvocation:
    dispatch_id: UUID
    workflow_run_id: UUID
    task_run_id: UUID
    task_attempt_id: UUID
    attempt_number: int
    claim_generation: int
    worker_session_id: UUID
    task_type: str
    parameters: FrozenJSONMapping
    references: FrozenJSONMapping
    correlation_id: str | None

    def __repr__(self) -> str:
        return (
            "TaskHandlerInvocation("
            f"dispatch_id={self.dispatch_id!r}, workflow_run_id={self.workflow_run_id!r}, "
            f"task_run_id={self.task_run_id!r}, task_attempt_id={self.task_attempt_id!r}, "
            f"attempt_number={self.attempt_number!r}, "
            f"claim_generation={self.claim_generation!r}, "
            f"worker_session_id={self.worker_session_id!r}, task_type={self.task_type!r}, "
            "parameters=<redacted>, references=<redacted>, "
            f"correlation_id={self.correlation_id!r})"
        )


type TaskHandler = Callable[[TaskHandlerInvocation], Awaitable[object]]


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
