from __future__ import annotations

from dataclasses import dataclass

import pytest

from taskforge.worker.handlers import (
    TaskHandlerDefinition,
    TaskHandlerInvocation,
    TaskHandlerRegistry,
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


async def handler(invocation: TaskHandlerInvocation) -> object:
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
