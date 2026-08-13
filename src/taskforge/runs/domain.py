"""Transport- and persistence-neutral workflow run target selection."""

from __future__ import annotations

import hashlib
import hmac
import json
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID

from taskforge.workflows.domain import (
    MAX_TASK_DEADLINE_SECONDS,
    WorkflowDefinitionStatus,
)
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


class InvalidWorkflowRunIdempotencyKey(ValueError):
    """An idempotency key is not a bounded printable ASCII token."""


class WorkflowRunIdempotencyConflict(Exception):
    """A scoped idempotency key was reused for a different request."""


class WorkflowRunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    CANCELLING = "cancelling"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


MAX_WORKFLOW_RECONCILIATION_ITERATIONS = 8


class TaskRunStatus(StrEnum):
    BLOCKED = "blocked"
    RUNNABLE = "runnable"
    DISPATCHED = "dispatched"
    CLAIMED = "claimed"
    RUNNING = "running"
    RETRY_SCHEDULED = "retry_scheduled"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class RunFailureReason(StrEnum):
    """Stable failure categories exposed by run inspection responses."""

    TASK_FAILED = "task_failed"
    DEPENDENCY_FAILED = "dependency_failed"


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
class WorkflowRunVersionStep:
    step_identifier: str
    deadline_seconds: int | None = None

    def __post_init__(self) -> None:
        if self.deadline_seconds is not None and (
            type(self.deadline_seconds) is not int
            or not 1 <= self.deadline_seconds <= MAX_TASK_DEADLINE_SECONDS
        ):
            raise ValueError("step deadline seconds must be within the supported range")


@dataclass(frozen=True)
class WorkflowRunVersionSnapshot:
    workflow_definition_id: UUID
    workflow_version_id: UUID
    version_number: int
    steps: tuple[WorkflowRunVersionStep, ...]
    dependencies: tuple[WorkflowRunVersionDependency, ...]

    @property
    def step_identifiers(self) -> tuple[str, ...]:
        return tuple(step.step_identifier for step in self.steps)


@dataclass(frozen=True)
class InitialTaskRun:
    step_identifier: str
    status: TaskRunStatus
    deadline_seconds: int | None = None


@dataclass(frozen=True)
class NewTaskRun:
    id: UUID
    step_identifier: str
    status: TaskRunStatus
    deadline_seconds: int | None = None


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


@dataclass(frozen=True)
class InspectedWorkflowRun:
    id: UUID
    workflow_definition_id: UUID
    workflow_version_id: UUID
    version_number: int
    requested_by_principal_id: UUID
    status: WorkflowRunStatus
    created_at: datetime
    updated_at: datetime
    failure_reason: RunFailureReason | None = None

    def __post_init__(self) -> None:
        if self.created_at.tzinfo is None or self.updated_at.tzinfo is None:
            raise ValueError("workflow run timestamps must be timezone-aware")
        object.__setattr__(self, "created_at", self.created_at.astimezone(UTC))
        object.__setattr__(self, "updated_at", self.updated_at.astimezone(UTC))


@dataclass(frozen=True)
class InspectedTaskRun:
    id: UUID
    workflow_run_id: UUID
    workflow_version_id: UUID
    step_identifier: str
    status: TaskRunStatus
    created_at: datetime
    updated_at: datetime
    failure_reason: RunFailureReason | None = None

    def __post_init__(self) -> None:
        if self.created_at.tzinfo is None or self.updated_at.tzinfo is None:
            raise ValueError("task run timestamps must be timezone-aware")
        object.__setattr__(self, "created_at", self.created_at.astimezone(UTC))
        object.__setattr__(self, "updated_at", self.updated_at.astimezone(UTC))


@dataclass(frozen=True)
class RunnableTransitionResult:
    """The immutable outcome of one persisted runnable-transition evaluation."""

    workflow_run_id: UUID
    transitioned_task_ids: tuple[UUID, ...]
    transitioned_step_identifiers: tuple[str, ...]

    def __post_init__(self) -> None:
        if len(self.transitioned_task_ids) != len(self.transitioned_step_identifiers):
            raise ValueError("runnable transition identities must remain paired")
        if len(set(self.transitioned_task_ids)) != len(self.transitioned_task_ids):
            raise ValueError("runnable transition task identifiers must be unique")
        if len(set(self.transitioned_step_identifiers)) != len(
            self.transitioned_step_identifiers
        ):
            raise ValueError("runnable transition step identifiers must be unique")

    @property
    def transitioned_count(self) -> int:
        return len(self.transitioned_task_ids)

    @property
    def made_progress(self) -> bool:
        return self.transitioned_count > 0


@dataclass(frozen=True)
class DependencyFailurePropagationResult:
    """The immutable outcome of one persisted dependency-failure propagation."""

    workflow_run_id: UUID
    skipped_task_ids: tuple[UUID, ...]
    skipped_step_identifiers: tuple[str, ...]

    def __post_init__(self) -> None:
        if len(self.skipped_task_ids) != len(self.skipped_step_identifiers):
            raise ValueError(
                "dependency-failure propagation identities must remain paired"
            )
        if len(set(self.skipped_task_ids)) != len(self.skipped_task_ids):
            raise ValueError("propagated task identifiers must be unique")
        if len(set(self.skipped_step_identifiers)) != len(
            self.skipped_step_identifiers
        ):
            raise ValueError("propagated step identifiers must be unique")

    @property
    def skipped_count(self) -> int:
        return len(self.skipped_task_ids)

    @property
    def made_progress(self) -> bool:
        return self.skipped_count > 0


@dataclass(frozen=True)
class WorkflowRunEvaluationResult:
    """The immutable outcome of one workflow-run state evaluation."""

    workflow_run_id: UUID
    found: bool
    previous_status: WorkflowRunStatus | None
    resulting_status: WorkflowRunStatus | None

    def __post_init__(self) -> None:
        if self.found and (
            self.previous_status is None or self.resulting_status is None
        ):
            raise ValueError("workflow run presence and evaluation statuses disagree")
        if not self.found and (
            self.previous_status is not None or self.resulting_status is not None
        ):
            raise ValueError("workflow run presence and evaluation statuses disagree")

    @property
    def transitioned(self) -> bool:
        return (
            self.found
            and self.previous_status is not None
            and self.previous_status is not self.resulting_status
        )

    @property
    def made_progress(self) -> bool:
        return self.transitioned


@dataclass(frozen=True)
class WorkflowRunReconciliationResult:
    """The bounded outcome of reconciling one workflow run."""

    workflow_run_id: UUID
    found: bool
    iterations: int
    runnable_transition_count: int
    skipped_transition_count: int
    workflow_transition_count: int
    final_status: WorkflowRunStatus | None
    quiescent: bool
    bound_reached: bool

    def __post_init__(self) -> None:
        if not 1 <= self.iterations <= MAX_WORKFLOW_RECONCILIATION_ITERATIONS:
            raise ValueError("reconciliation iteration count is outside its bound")
        counts = (
            self.runnable_transition_count,
            self.skipped_transition_count,
            self.workflow_transition_count,
        )
        if any(count < 0 for count in counts):
            raise ValueError("reconciliation transition counts cannot be negative")
        if self.workflow_transition_count > self.iterations:
            raise ValueError(
                "workflow transitions cannot exceed reconciliation iterations"
            )
        if self.found is (self.final_status is None):
            raise ValueError("reconciliation presence and final status disagree")
        if self.quiescent and self.bound_reached:
            raise ValueError("quiescent reconciliation cannot exhaust its bound")
        if (
            self.bound_reached
            and self.iterations != MAX_WORKFLOW_RECONCILIATION_ITERATIONS
        ):
            raise ValueError("reconciliation bound was not fully consumed")
        if not self.found and (self.quiescent or self.bound_reached):
            raise ValueError("missing workflow run cannot be reconciled")
        if self.final_status in (
            WorkflowRunStatus.CANCELLING,
            WorkflowRunStatus.SUCCEEDED,
            WorkflowRunStatus.FAILED,
            WorkflowRunStatus.CANCELLED,
        ) and (not self.quiescent or self.bound_reached):
            raise ValueError("inactive workflow status must stop reconciliation")


@dataclass(frozen=True, repr=False)
class WorkflowRunIdempotency:
    key_digest: str
    request_fingerprint: str

    def __repr__(self) -> str:
        return (
            "WorkflowRunIdempotency(key_digest=<redacted>, "
            "request_fingerprint=<redacted>)"
        )


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


def create_workflow_run_idempotency(
    key: object,
    *,
    workflow_definition_id: UUID,
    requested_by_principal_id: UUID,
    selection: WorkflowVersionSelection,
    input_snapshot: WorkflowRunInput,
) -> WorkflowRunIdempotency:
    """Validate an opaque key and fingerprint one normalized start request."""
    if (
        not isinstance(key, str)
        or not 16 <= len(key) <= 128
        or any(not 0x21 <= ord(character) <= 0x7E for character in key)
    ):
        raise InvalidWorkflowRunIdempotencyKey("idempotency key is invalid")
    key_digest = _versioned_sha256(
        b"taskforge:workflow-run-idempotency-key:v1\0" + key.encode("ascii")
    )
    selector: dict[str, object]
    if isinstance(selection, ExplicitWorkflowVersion):
        selector = {
            "kind": "explicit",
            "version_number": selection.version_number,
        }
    else:
        selector = {"kind": "latest"}
    normalized_request = {
        "operation": "workflow_run_start",
        "requested_by_principal_id": str(requested_by_principal_id),
        "schema_version": 1,
        "selection": selector,
        "workflow_definition_id": str(workflow_definition_id),
        "input": {
            "payload": input_snapshot.payload,
            "input_references": input_snapshot.input_references,
        },
    }
    encoded = json.dumps(
        normalized_request,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return WorkflowRunIdempotency(
        key_digest=key_digest,
        request_fingerprint=_versioned_sha256(
            b"taskforge:workflow-run-request-fingerprint:v1\0" + encoded
        ),
    )


def idempotency_fingerprints_match(left: str, right: str) -> bool:
    """Compare stored request fingerprints without early mismatch behavior."""
    return hmac.compare_digest(left, right)


def _versioned_sha256(value: bytes) -> str:
    return f"sha256:v1:{hashlib.sha256(value).hexdigest()}"


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
            deadline_seconds=next(
                step.deadline_seconds
                for step in snapshot.steps
                if step.step_identifier == identifier
            ),
        )
        for identifier in ordered_steps
    )
