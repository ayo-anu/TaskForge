"""Transport- and persistence-neutral workflow draft domain types."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from math import isfinite
from uuid import UUID

from taskforge.workflows.dag_validation import (
    DAGEdge,
    DAGValidationResult,
    validate_dag,
)
from taskforge.workflows.task_types import (
    JSONMapping,
    JSONValue,
    TaskTypeRegistry,
    WorkflowValidationError,
    WorkflowValidationIssue,
    validate_parameters,
)

MAX_WORKFLOW_NAME_LENGTH = 128
MAX_WORKFLOW_DESCRIPTION_LENGTH = 4096
MAX_IDENTIFIER_LENGTH = 128
MAX_TASK_DEADLINE_SECONDS = 31_536_000
MAX_TASK_EXECUTION_TIMEOUT_SECONDS = 31_536_000

# A later effective-policy consumer will select the complete step object over the
# complete workflow object; individual retry fields are intentionally not merged.
RETRY_POLICY_FIELDS = frozenset(
    {
        "maximum_attempts",
        "initial_delay_seconds",
        "multiplier",
        "maximum_delay_seconds",
    }
)

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


class WorkflowAvailabilityTransitionRejected(Exception):
    """The requested availability change is not valid for the current state."""


@dataclass(frozen=True)
class WorkflowAvailabilityResult:
    workflow_id: UUID
    status: WorkflowDefinitionStatus
    changed: bool


def availability_requires_published_version(
    current_status: WorkflowDefinitionStatus,
    intent: WorkflowAvailabilityIntent,
) -> bool:
    """Return whether this availability request needs publication evidence."""
    if not isinstance(intent, WorkflowAvailabilityIntent):
        raise WorkflowAvailabilityTransitionRejected
    return (
        current_status is WorkflowDefinitionStatus.DRAFT
        and intent is WorkflowAvailabilityIntent.ENABLE
    )


def change_workflow_availability(
    *,
    workflow_id: UUID,
    current_status: WorkflowDefinitionStatus,
    intent: WorkflowAvailabilityIntent,
    has_published_version: bool,
) -> WorkflowAvailabilityResult:
    """Apply the enable/disable lifecycle rules owned by the workflow domain."""
    if not isinstance(intent, WorkflowAvailabilityIntent):
        raise WorkflowAvailabilityTransitionRejected
    if current_status is WorkflowDefinitionStatus.DRAFT:
        if intent is not WorkflowAvailabilityIntent.ENABLE or not has_published_version:
            raise WorkflowAvailabilityTransitionRejected
        resulting_status = WorkflowDefinitionStatus.ENABLED
    elif current_status is WorkflowDefinitionStatus.ENABLED:
        resulting_status = (
            WorkflowDefinitionStatus.DISABLED
            if intent is WorkflowAvailabilityIntent.DISABLE
            else WorkflowDefinitionStatus.ENABLED
        )
    elif current_status is WorkflowDefinitionStatus.DISABLED:
        resulting_status = (
            WorkflowDefinitionStatus.ENABLED
            if intent is WorkflowAvailabilityIntent.ENABLE
            else WorkflowDefinitionStatus.DISABLED
        )
    else:
        raise WorkflowAvailabilityTransitionRejected
    return WorkflowAvailabilityResult(
        workflow_id=workflow_id,
        status=resulting_status,
        changed=resulting_status is not current_status,
    )


@dataclass(frozen=True, repr=False)
class DraftWorkflowStep:
    id: UUID
    identifier: str
    task_type: str
    parameters: JSONValue
    execution_policy: JSONMapping | None = None

    def __repr__(self) -> str:
        return (
            "DraftWorkflowStep("
            f"id={self.id!r}, identifier={self.identifier!r}, "
            f"task_type={self.task_type!r}, parameters=<redacted>, "
            "execution_policy=<redacted>)"
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
    execution_policy: JSONMapping | None = None

    def __repr__(self) -> str:
        return (
            "WorkflowDraft("
            f"id={self.id!r}, owner_principal_id={self.owner_principal_id!r}, "
            f"name={self.name!r}, description={self.description!r}, "
            f"status={self.status!r}, steps={len(self.steps)}, "
            f"dependencies={len(self.dependencies)}, execution_policy=<redacted>)"
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


@dataclass(frozen=True, repr=False)
class WorkflowVersionStep:
    identifier: str
    task_type: str
    parameters: JSONMapping
    execution_policy: JSONMapping | None

    def __repr__(self) -> str:
        return (
            "WorkflowVersionStep("
            f"identifier={self.identifier!r}, task_type={self.task_type!r}, "
            "parameters=<redacted>, execution_policy=<redacted>)"
        )


@dataclass(frozen=True)
class WorkflowVersionDependency:
    predecessor_identifier: str
    successor_identifier: str


@dataclass(frozen=True, repr=False)
class WorkflowVersionSnapshot:
    id: UUID
    workflow_definition_id: UUID
    version_number: int
    name: str
    description: str | None
    execution_policy: JSONMapping | None
    published_at: datetime
    steps: tuple[WorkflowVersionStep, ...]
    dependencies: tuple[WorkflowVersionDependency, ...]

    def __post_init__(self) -> None:
        if self.version_number <= 0:
            raise ValueError("workflow version number must be positive")
        if self.published_at.tzinfo is None:
            raise ValueError("publication timestamp must be timezone-aware")
        object.__setattr__(self, "published_at", self.published_at.astimezone(UTC))

    def __repr__(self) -> str:
        return (
            "WorkflowVersionSnapshot("
            f"id={self.id!r}, workflow_definition_id={self.workflow_definition_id!r}, "
            f"version_number={self.version_number!r}, name={self.name!r}, "
            f"description={self.description!r}, execution_policy=<redacted>, "
            f"published_at={self.published_at!r}, steps={len(self.steps)}, "
            f"dependencies={len(self.dependencies)})"
        )


def create_draft_step(
    *,
    step_id: object,
    identifier: object,
    task_type: object,
    parameters: object,
    execution_policy: object = None,
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
    validated_policy, policy_issues = validate_execution_policy(
        execution_policy, path=("execution_policy",)
    )
    if policy_issues:
        raise WorkflowValidationError(policy_issues)
    assert isinstance(step_id, UUID)
    assert isinstance(identifier, str)
    assert isinstance(task_type, str)
    assert validated_parameters is not None
    return DraftWorkflowStep(
        id=step_id,
        identifier=identifier,
        task_type=task_type,
        parameters=validated_parameters,
        execution_policy=validated_policy,
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
    execution_policy: object = None,
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
        execution_policy=execution_policy,
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
    execution_policy: object = None,
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
    validated_policy, policy_issues = validate_execution_policy(
        execution_policy, path=("execution_policy",)
    )
    issues.extend(policy_issues)
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
            execution_policy=validated_policy,
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
    execution_policy: object = None,
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
        execution_policy=execution_policy,
    )


def validate_execution_policy(
    value: object,
    *,
    path: tuple[str | int, ...] = (),
) -> tuple[JSONMapping | None, tuple[WorkflowValidationIssue, ...]]:
    """Validate bounded policy JSON and explicitly supported execution fields."""
    if value is None:
        return None, ()
    structural_issues, validated = validate_parameters(value, path=path)
    if structural_issues:
        return None, structural_issues
    assert validated is not None
    issues: list[WorkflowValidationIssue] = []
    if "deadline_seconds" in validated:
        deadline = validated["deadline_seconds"]
        if type(deadline) is not int or not 1 <= deadline <= MAX_TASK_DEADLINE_SECONDS:
            issues.append(
                WorkflowValidationIssue(
                    "invalid_deadline_seconds",
                    (*path, "deadline_seconds"),
                    "Deadline seconds must be a positive bounded integer.",
                )
            )
    if "execution_timeout_seconds" in validated:
        timeout = validated["execution_timeout_seconds"]
        if (
            type(timeout) is not int
            or not 1 <= timeout <= MAX_TASK_EXECUTION_TIMEOUT_SECONDS
        ):
            issues.append(
                WorkflowValidationIssue(
                    "invalid_execution_timeout_seconds",
                    (*path, "execution_timeout_seconds"),
                    "Execution timeout seconds must be a positive bounded integer.",
                )
            )
    retry_policy = validated.get("retry_policy")
    if "retry_policy" in validated:
        retry_issues = _validate_retry_policy(retry_policy, (*path, "retry_policy"))
        issues.extend(retry_issues)
    return (None, tuple(issues)) if issues else (validated, ())


def _validate_retry_policy(
    value: JSONValue,
    path: tuple[str | int, ...],
) -> tuple[WorkflowValidationIssue, ...]:
    if not isinstance(value, dict):
        return (
            WorkflowValidationIssue(
                "invalid_retry_policy", path, "Retry policy must be an object."
            ),
        )
    issues: list[WorkflowValidationIssue] = []
    keys = set(value)
    if keys != RETRY_POLICY_FIELDS:
        issues.append(
            WorkflowValidationIssue(
                "invalid_retry_policy_fields",
                path,
                "Retry policy must contain exactly the supported fields.",
            )
        )
    maximum_attempts = value.get("maximum_attempts")
    if type(maximum_attempts) is not int or maximum_attempts < 1:
        issues.append(
            WorkflowValidationIssue(
                "invalid_retry_maximum_attempts",
                (*path, "maximum_attempts"),
                "Maximum attempts must be a positive integer including attempt 1.",
            )
        )
    initial_delay = value.get("initial_delay_seconds")
    if type(initial_delay) is not int or initial_delay < 0:
        issues.append(
            WorkflowValidationIssue(
                "invalid_retry_initial_delay_seconds",
                (*path, "initial_delay_seconds"),
                "Initial retry delay must be a non-negative integer.",
            )
        )
    multiplier = value.get("multiplier")
    invalid_multiplier = True
    if isinstance(multiplier, (int, float)) and not isinstance(multiplier, bool):
        invalid_multiplier = not isfinite(multiplier) or multiplier < 1
    if invalid_multiplier:
        issues.append(
            WorkflowValidationIssue(
                "invalid_retry_multiplier",
                (*path, "multiplier"),
                "Retry multiplier must be a finite number of at least one.",
            )
        )
    maximum_delay = value.get("maximum_delay_seconds")
    if type(maximum_delay) is not int or maximum_delay < 0:
        issues.append(
            WorkflowValidationIssue(
                "invalid_retry_maximum_delay_seconds",
                (*path, "maximum_delay_seconds"),
                "Maximum retry delay must be a non-negative integer.",
            )
        )
    if (
        type(initial_delay) is int
        and initial_delay >= 0
        and type(maximum_delay) is int
        and maximum_delay >= 0
        and maximum_delay < initial_delay
    ):
        issues.append(
            WorkflowValidationIssue(
                "invalid_retry_delay_order",
                (*path, "maximum_delay_seconds"),
                "Maximum retry delay cannot be less than the initial delay.",
            )
        )
    return tuple(issues)


def resolve_deadline_seconds(
    workflow_policy: JSONMapping | None,
    step_policy: JSONMapping | None,
) -> int | None:
    """Resolve only deadline_seconds with explicit step-over-workflow precedence."""
    for policy in (step_policy, workflow_policy):
        validated, issues = validate_execution_policy(policy)
        if issues:
            raise WorkflowValidationError(issues)
        if validated is not None and "deadline_seconds" in validated:
            value = validated["deadline_seconds"]
            assert type(value) is int
            return value
    return None


def resolve_execution_timeout_seconds(
    workflow_policy: JSONMapping | None,
    step_policy: JSONMapping | None,
) -> int | None:
    """Resolve only execution_timeout_seconds with step-over-workflow precedence."""
    for policy in (step_policy, workflow_policy):
        validated, issues = validate_execution_policy(policy)
        if issues:
            raise WorkflowValidationError(issues)
        if validated is not None and "execution_timeout_seconds" in validated:
            value = validated["execution_timeout_seconds"]
            assert type(value) is int
            return value
    return None


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
