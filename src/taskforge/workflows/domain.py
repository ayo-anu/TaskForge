"""Transport- and persistence-neutral workflow draft domain types."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID

from taskforge.workflows.dag_validation import (
    DAGEdge,
    DAGValidationResult,
    validate_dag,
)
from taskforge.workflows.task_types import (
    JSONValue,
    TaskTypeRegistry,
    WorkflowValidationError,
    WorkflowValidationIssue,
)

MAX_WORKFLOW_NAME_LENGTH = 128
MAX_WORKFLOW_DESCRIPTION_LENGTH = 4096
MAX_IDENTIFIER_LENGTH = 128

_STEP_IDENTIFIER = re.compile(r"\A[a-z][a-z0-9_-]{0,127}\Z")
_TASK_TYPE_NAME = re.compile(r"\A[a-z][a-z0-9_.-]{0,127}\Z")


class WorkflowDefinitionStatus(StrEnum):
    """Persisted workflow definition states from the PostgreSQL enum."""

    DRAFT = "draft"
    ENABLED = "enabled"
    DISABLED = "disabled"
    ARCHIVED = "archived"


class WorkflowAvailabilityIntent(StrEnum):
    """Requested availability without prescribing lifecycle transition policy."""

    ENABLE = "enable"
    DISABLE = "disable"


@dataclass(frozen=True, repr=False)
class DraftWorkflowStep:
    id: UUID
    identifier: str
    task_type: str
    parameters: JSONValue

    def __repr__(self) -> str:
        return (
            "DraftWorkflowStep("
            f"id={self.id!r}, identifier={self.identifier!r}, "
            f"task_type={self.task_type!r}, parameters=<redacted>)"
        )


@dataclass(frozen=True)
class DraftDependency:
    id: UUID
    predecessor_identifier: str
    successor_identifier: str


@dataclass(frozen=True, repr=False)
class WorkflowDraft:
    id: UUID
    owner_principal_id: UUID
    name: str
    description: str | None
    status: WorkflowDefinitionStatus
    steps: tuple[DraftWorkflowStep, ...]
    dependencies: tuple[DraftDependency, ...]

    def __repr__(self) -> str:
        return (
            "WorkflowDraft("
            f"id={self.id!r}, owner_principal_id={self.owner_principal_id!r}, "
            f"name={self.name!r}, description={self.description!r}, "
            f"status={self.status!r}, steps={len(self.steps)}, "
            f"dependencies={len(self.dependencies)})"
        )


@dataclass(frozen=True)
class PublishedWorkflowVersion:
    """Metadata returned after one complete immutable snapshot commits."""

    id: UUID
    workflow_definition_id: UUID
    version_number: int
    published_at: datetime

    def __post_init__(self) -> None:
        if self.version_number <= 0:
            raise ValueError("published version number must be positive")
        if self.published_at.tzinfo is None:
            raise ValueError("publication timestamp must be timezone-aware")
        object.__setattr__(self, "published_at", self.published_at.astimezone(UTC))


def create_draft_step(
    *,
    step_id: object,
    identifier: object,
    task_type: object,
    parameters: object,
    task_types: TaskTypeRegistry,
) -> DraftWorkflowStep:
    """Validate one draft step and return it only when every invariant holds."""
    issues: list[WorkflowValidationIssue] = []
    if not isinstance(step_id, UUID):
        issues.append(
            WorkflowValidationIssue("invalid_step_id", ("id",), "Invalid step ID.")
        )
    if (
        not isinstance(identifier, str)
        or _STEP_IDENTIFIER.fullmatch(identifier) is None
    ):
        issues.append(
            WorkflowValidationIssue(
                "invalid_step_identifier",
                ("identifier",),
                "Step identifier is invalid.",
            )
        )
    if not isinstance(task_type, str) or _TASK_TYPE_NAME.fullmatch(task_type) is None:
        issues.append(
            WorkflowValidationIssue(
                "invalid_task_type",
                ("task_type",),
                "Task type is invalid.",
            )
        )
    if issues:
        raise WorkflowValidationError(tuple(issues))
    assert isinstance(task_type, str)

    validated_parameters, parameter_issues = task_types.validate(
        task_type,
        parameters,
        path=("parameters",),
    )
    if parameter_issues:
        raise WorkflowValidationError(parameter_issues)
    assert isinstance(step_id, UUID)
    assert isinstance(identifier, str)
    assert isinstance(task_type, str)
    assert validated_parameters is not None
    return DraftWorkflowStep(
        id=step_id,
        identifier=identifier,
        task_type=task_type,
        parameters=validated_parameters,
    )


def create_draft_dependency(
    *,
    dependency_id: object,
    predecessor_identifier: object,
    successor_identifier: object,
) -> DraftDependency:
    """Create a typed edge without performing later DAG validation."""
    issues: list[WorkflowValidationIssue] = []
    if not isinstance(dependency_id, UUID):
        issues.append(
            WorkflowValidationIssue(
                "invalid_dependency_id", ("id",), "Invalid dependency ID."
            )
        )
    for field, value in (
        ("predecessor_identifier", predecessor_identifier),
        ("successor_identifier", successor_identifier),
    ):
        if not isinstance(value, str) or _STEP_IDENTIFIER.fullmatch(value) is None:
            issues.append(
                WorkflowValidationIssue(
                    "invalid_step_identifier",
                    (field,),
                    "Step identifier is invalid.",
                )
            )
    if issues:
        raise WorkflowValidationError(tuple(issues))
    assert isinstance(dependency_id, UUID)
    assert isinstance(predecessor_identifier, str)
    assert isinstance(successor_identifier, str)
    return DraftDependency(
        id=dependency_id,
        predecessor_identifier=predecessor_identifier,
        successor_identifier=successor_identifier,
    )


def create_workflow_draft(
    *,
    workflow_id: object,
    owner_principal_id: object,
    name: object,
    description: object,
    status: object,
    steps: tuple[DraftWorkflowStep, ...],
    dependencies: tuple[DraftDependency, ...] = (),
) -> WorkflowDraft:
    """Validate aggregate and graph invariants without persistence."""
    workflow, _ = _create_workflow_draft_with_validation(
        workflow_id=workflow_id,
        owner_principal_id=owner_principal_id,
        name=name,
        description=description,
        status=status,
        steps=steps,
        dependencies=dependencies,
    )
    return workflow


def _create_workflow_draft_with_validation(
    *,
    workflow_id: object,
    owner_principal_id: object,
    name: object,
    description: object,
    status: object,
    steps: tuple[DraftWorkflowStep, ...],
    dependencies: tuple[DraftDependency, ...] = (),
) -> tuple[WorkflowDraft, DAGValidationResult]:
    """Construct a draft and return its authoritative graph validation result."""
    issues: list[WorkflowValidationIssue] = []
    if not isinstance(workflow_id, UUID):
        issues.append(
            WorkflowValidationIssue(
                "invalid_workflow_id", ("id",), "Invalid workflow ID."
            )
        )
    if not isinstance(owner_principal_id, UUID):
        issues.append(
            WorkflowValidationIssue(
                "invalid_owner_id", ("owner_principal_id",), "Invalid owner ID."
            )
        )
    issues.extend(_validate_name(name))
    issues.extend(_validate_description(description))
    if not isinstance(status, WorkflowDefinitionStatus):
        issues.append(
            WorkflowValidationIssue(
                "invalid_workflow_status", ("status",), "Invalid workflow status."
            )
        )

    seen_step_ids: set[UUID] = set()
    seen_identifiers: set[str] = set()
    for index, step in enumerate(steps):
        if step.id in seen_step_ids:
            issues.append(
                WorkflowValidationIssue(
                    "duplicate_step_id",
                    ("steps", index, "id"),
                    "Step ID is duplicated.",
                )
            )
        else:
            seen_step_ids.add(step.id)
        if step.identifier in seen_identifiers:
            issues.append(
                WorkflowValidationIssue(
                    "duplicate_step_identifier",
                    ("steps", index, "identifier"),
                    "Step identifier is duplicated.",
                )
            )
        else:
            seen_identifiers.add(step.identifier)

    if issues:
        raise WorkflowValidationError(tuple(issues))
    graph_result = validate_dag(
        tuple(step.identifier for step in steps),
        tuple(
            DAGEdge(
                dependency.predecessor_identifier,
                dependency.successor_identifier,
            )
            for dependency in dependencies
        ),
    )
    if not graph_result.is_valid:
        raise WorkflowValidationError.from_graph(graph_result)
    assert isinstance(workflow_id, UUID)
    assert isinstance(owner_principal_id, UUID)
    assert isinstance(name, str)
    assert description is None or isinstance(description, str)
    assert isinstance(status, WorkflowDefinitionStatus)
    return (
        WorkflowDraft(
            id=workflow_id,
            owner_principal_id=owner_principal_id,
            name=name,
            description=description,
            status=status,
            steps=tuple(steps),
            dependencies=tuple(dependencies),
        ),
        graph_result,
    )


def replace_workflow_draft(
    workflow: WorkflowDraft,
    *,
    name: object,
    description: object,
    steps: tuple[DraftWorkflowStep, ...],
    dependencies: tuple[DraftDependency, ...] = (),
) -> WorkflowDraft:
    """Construct a validated draft replacement without defining persistence policy."""
    return create_workflow_draft(
        workflow_id=workflow.id,
        owner_principal_id=workflow.owner_principal_id,
        name=name,
        description=description,
        status=workflow.status,
        steps=steps,
        dependencies=dependencies,
    )


def _validate_name(value: object) -> tuple[WorkflowValidationIssue, ...]:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > MAX_WORKFLOW_NAME_LENGTH
        or _contains_disallowed_control(value)
    ):
        return (
            WorkflowValidationIssue(
                "invalid_workflow_name", ("name",), "Workflow name is invalid."
            ),
        )
    return ()


def _validate_description(value: object) -> tuple[WorkflowValidationIssue, ...]:
    if value is None:
        return ()
    if not isinstance(value, str) or _contains_disallowed_control(value):
        return (
            WorkflowValidationIssue(
                "invalid_workflow_description",
                ("description",),
                "Workflow description is invalid.",
            ),
        )
    if len(value) > MAX_WORKFLOW_DESCRIPTION_LENGTH:
        return (
            WorkflowValidationIssue(
                "description_too_large",
                ("description",),
                "Workflow description is too large.",
            ),
        )
    return ()


def _contains_disallowed_control(value: str) -> bool:
    return any(ord(character) < 32 and character not in "\n\r\t" for character in value)
