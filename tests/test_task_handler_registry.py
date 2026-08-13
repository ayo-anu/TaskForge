from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone

import pytest

from taskforge.worker.handlers import (
    TaskContext,
    TaskDeadline,
    TaskHandlerDefinition,
    TaskHandlerRegistry,
)
from taskforge.worker.results import (
    TaskCancellation,
    TaskPermanentFailure,
    TaskRetryableFailure,
)
from taskforge.workflows.task_types import (
    JSONMapping,
    TaskTypeDefinition,
    TaskTypeRegistry,
    WorkflowValidationIssue,
)


@dataclass(frozen=True)
class AcceptParameters:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        del parameters
        return ()


async def handler(invocation: TaskContext) -> object:
    return invocation.task_attempt_id


def task_types() -> TaskTypeRegistry:
    return TaskTypeRegistry(
        (TaskTypeDefinition("test.task", "test-capability", AcceptParameters()),)
    )


def test_registry_resolves_only_explicit_trusted_handler() -> None:
    definition = TaskHandlerDefinition("test.task", "test-capability", handler)
    registry = TaskHandlerRegistry((definition,), task_types())

    assert registry.definition("test.task") is definition
    assert registry.definition("module.callable") is None
    assert registry.task_types == frozenset({"test.task"})
    assert registry.required_capabilities == frozenset({"test-capability"})


def test_task_deadline_normalizes_to_utc_and_has_safe_repr() -> None:
    deadline = TaskDeadline(
        datetime(2030, 1, 1, tzinfo=UTC).astimezone(timezone(timedelta(hours=2)))
    )

    assert deadline.expires_at == datetime(2030, 1, 1, tzinfo=UTC)
    assert repr(deadline) == (
        "TaskDeadline(expires_at=datetime.datetime(2030, 1, 1, 0, 0, "
        "tzinfo=datetime.timezone.utc))"
    )


def test_task_deadline_rejects_naive_time() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        TaskDeadline(datetime(2030, 1, 1))


def test_handler_markers_are_fieldless_immutable_types() -> None:
    for marker in (TaskRetryableFailure(), TaskPermanentFailure(), TaskCancellation()):
        assert marker.__dataclass_fields__ == {}


@pytest.mark.parametrize(
    "definitions",
    (
        (TaskHandlerDefinition("unknown.task", "test-capability", handler),),
        (TaskHandlerDefinition("test.task", "wrong-capability", handler),),
        (
            TaskHandlerDefinition("test.task", "test-capability", handler),
            TaskHandlerDefinition("test.task", "test-capability", handler),
        ),
    ),
)
def test_registry_rejects_configuration_drift(
    definitions: tuple[TaskHandlerDefinition, ...],
) -> None:
    with pytest.raises(ValueError):
        TaskHandlerRegistry(definitions, task_types())
