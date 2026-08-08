"""Transport- and persistence-neutral workflow run target selection."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID

from taskforge.workflows.domain import WorkflowDefinitionStatus
from taskforge.workflows.task_types import (
    JSONMapping,
    WorkflowValidationIssue,
    validate_parameters,
)


class InvalidWorkflowVersionSelection(ValueError):
    """A workflow version selector is malformed."""


class WorkflowRunTargetUnavailable(Exception):
    """A workflow definition does not currently permit new runs."""

    def __init__(self, status: WorkflowDefinitionStatus) -> None:
        self.status = status
        super().__init__("workflow definition is unavailable for new runs")


class InvalidWorkflowRunInput(ValueError):
    """Accepted run input is not a bounded JSON object snapshot."""

    def __init__(self, issues: tuple[WorkflowValidationIssue, ...]) -> None:
        self.issues = issues
        super().__init__("workflow run input validation failed")


class WorkflowVersionSnapshotInvalid(Exception):
    """A published version cannot be materialized safely."""


class WorkflowRunStatus(StrEnum):
    PENDING = "pending"


class TaskRunStatus(StrEnum):
    BLOCKED = "blocked"
    RUNNABLE = "runnable"


@dataclass(frozen=True)
class ExplicitWorkflowVersion:
    version_number: int

    def __post_init__(self) -> None:
        if (
            isinstance(self.version_number, bool)
            or not isinstance(self.version_number, int)
            or self.version_number <= 0
        ):
            raise InvalidWorkflowVersionSelection(
                "workflow version number must be a positive integer"
            )


@dataclass(frozen=True)
class LatestWorkflowVersion:
    """Select the greatest committed version number visible to the lookup."""


WorkflowVersionSelection = ExplicitWorkflowVersion | LatestWorkflowVersion


@dataclass(frozen=True)
class ResolvedWorkflowVersion:
    workflow_definition_id: UUID
    workflow_version_id: UUID
    version_number: int

    def __post_init__(self) -> None:
        if (
            isinstance(self.version_number, bool)
            or not isinstance(self.version_number, int)
            or self.version_number <= 0
        ):
            raise ValueError("resolved workflow version number must be positive")


@dataclass(frozen=True, repr=False)
class WorkflowRunInput:
    payload: JSONMapping
    input_references: JSONMapping

    def __repr__(self) -> str:
        return "WorkflowRunInput(payload=<redacted>, input_references=<redacted>)"


@dataclass(frozen=True)
class WorkflowRunVersionDependency:
    predecessor_identifier: str
    successor_identifier: str


@dataclass(frozen=True)
class WorkflowRunVersionSnapshot:
    workflow_definition_id: UUID
    workflow_version_id: UUID
    version_number: int
    step_identifiers: tuple[str, ...]
    dependencies: tuple[WorkflowRunVersionDependency, ...]


@dataclass(frozen=True)
class InitialTaskRun:
    step_identifier: str
    status: TaskRunStatus


@dataclass(frozen=True)
class NewTaskRun:
    id: UUID
    step_identifier: str
    status: TaskRunStatus


@dataclass(frozen=True)
class NewWorkflowRun:
    id: UUID
    requested_by_principal_id: UUID
    status: WorkflowRunStatus = WorkflowRunStatus.PENDING


@dataclass(frozen=True)
class CreatedWorkflowRun:
    id: UUID
    workflow_definition_id: UUID
    workflow_version_id: UUID
    version_number: int
    requested_by_principal_id: UUID
    status: WorkflowRunStatus
    created_at: datetime
    task_count: int
    runnable_task_count: int
    blocked_task_count: int

    def __post_init__(self) -> None:
        if self.created_at.tzinfo is None:
            raise ValueError("run creation timestamp must be timezone-aware")
        object.__setattr__(self, "created_at", self.created_at.astimezone(UTC))


def require_run_available(status: WorkflowDefinitionStatus) -> None:
    """Reject every definition state except enabled."""
    if status is not WorkflowDefinitionStatus.ENABLED:
        raise WorkflowRunTargetUnavailable(status)


def create_workflow_run_input(
    payload: object,
    input_references: object,
) -> WorkflowRunInput:
    """Validate and defensively snapshot bounded run input objects."""
    payload_issues, validated_payload = validate_parameters(payload, path=("payload",))
    reference_issues, validated_references = validate_parameters(
        input_references, path=("input_references",)
    )
    issues = (*payload_issues, *reference_issues)
    if issues:
        raise InvalidWorkflowRunInput(issues)
    assert validated_payload is not None
    assert validated_references is not None
    return WorkflowRunInput(
        payload=deepcopy(validated_payload),
        input_references=deepcopy(validated_references),
    )


def materialize_initial_tasks(
    snapshot: WorkflowRunVersionSnapshot,
) -> tuple[InitialTaskRun, ...]:
    """Return one deterministically ordered initial task for every version step."""
    if isinstance(snapshot.version_number, bool) or snapshot.version_number <= 0:
        raise WorkflowVersionSnapshotInvalid
    ordered_steps = tuple(sorted(snapshot.step_identifiers))
    if not ordered_steps or len(set(ordered_steps)) != len(ordered_steps):
        raise WorkflowVersionSnapshotInvalid
    step_set = set(ordered_steps)
    edges: set[tuple[str, str]] = set()
    successors: set[str] = set()
    for dependency in snapshot.dependencies:
        edge = (
            dependency.predecessor_identifier,
            dependency.successor_identifier,
        )
        if (
            edge in edges
            or edge[0] == edge[1]
            or edge[0] not in step_set
            or edge[1] not in step_set
        ):
            raise WorkflowVersionSnapshotInvalid
        edges.add(edge)
        successors.add(edge[1])
    return tuple(
        InitialTaskRun(
            step_identifier=identifier,
            status=(
                TaskRunStatus.BLOCKED
                if identifier in successors
                else TaskRunStatus.RUNNABLE
            ),
        )
        for identifier in ordered_steps
    )
