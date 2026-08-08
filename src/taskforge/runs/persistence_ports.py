"""Persistence contracts for workflow run target resolution."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from types import TracebackType
from typing import Protocol
from uuid import UUID

from taskforge.runs.domain import (
    CreatedWorkflowRun,
    DependencyFailurePropagationResult,
    InspectedTaskRun,
    InspectedWorkflowRun,
    NewTaskRun,
    NewWorkflowRun,
    RunnableTransitionResult,
    WorkflowRunEvaluationResult,
    WorkflowRunIdempotency,
    WorkflowRunInput,
    WorkflowRunVersionSnapshot,
    WorkflowVersionSelection,
)
from taskforge.workflows.domain import WorkflowDefinitionStatus


class WorkflowRunPersistenceUnavailable(Exception):
    """Workflow run target persistence is operationally unavailable."""


class WorkflowRunRecordConflict(Exception):
    """A database constraint rejected complete run creation."""


class WorkflowRunIdempotencyRecordConflict(Exception):
    """The scoped idempotency row already exists."""


@dataclass(frozen=True)
class WorkflowVersionResolutionRecord:
    """One owner-scoped, statement-consistent resolution result."""

    workflow_definition_id: UUID
    status: WorkflowDefinitionStatus
    workflow_version_id: UUID | None
    version_number: int | None

    def __post_init__(self) -> None:
        if (self.workflow_version_id is None) is not (self.version_number is None):
            raise ValueError(
                "resolved version identity must be wholly present or absent"
            )
        if self.version_number is not None and (
            isinstance(self.version_number, bool) or self.version_number <= 0
        ):
            raise ValueError("resolved version number must be positive")


@dataclass(frozen=True)
class PreparedWorkflowRunCreation:
    workflow_definition_id: UUID
    status: WorkflowDefinitionStatus
    snapshot: WorkflowRunVersionSnapshot | None


@dataclass(frozen=True)
class ExistingIdempotentWorkflowRun:
    request_fingerprint: str
    run: CreatedWorkflowRun


IdempotentCreationPreparation = (
    PreparedWorkflowRunCreation | ExistingIdempotentWorkflowRun
)


@dataclass(frozen=True)
class WorkflowRunTimestamps:
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        if self.created_at.tzinfo is None or self.updated_at.tzinfo is None:
            raise ValueError("workflow run timestamps must be timezone-aware")
        object.__setattr__(self, "created_at", self.created_at.astimezone(UTC))
        object.__setattr__(self, "updated_at", self.updated_at.astimezone(UTC))


class WorkflowRunCreationTransaction(Protocol):
    async def prepare_creation_target(
        self,
        workflow_id: UUID,
        owner_principal_id: UUID,
        selection: WorkflowVersionSelection,
    ) -> PreparedWorkflowRunCreation | None: ...

    async def prepare_idempotent_creation(
        self,
        workflow_id: UUID,
        owner_principal_id: UUID,
        principal_id: UUID,
        selection: WorkflowVersionSelection,
        key_digest: str,
    ) -> IdempotentCreationPreparation | None: ...

    async def insert_complete_run(
        self,
        prepared: PreparedWorkflowRunCreation,
        run: NewWorkflowRun,
        input_snapshot: WorkflowRunInput,
        task_run_values: tuple[NewTaskRun, ...],
        idempotency: WorkflowRunIdempotency | None = None,
    ) -> WorkflowRunTimestamps: ...

    async def commit(self) -> None: ...


class WorkflowRunCreationTransactionContext(Protocol):
    async def __aenter__(self) -> WorkflowRunCreationTransaction: ...

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...


class WorkflowRunRepository(Protocol):
    def creation_transaction(self) -> WorkflowRunCreationTransactionContext: ...

    async def find_idempotent_run(
        self,
        principal_id: UUID,
        workflow_id: UUID,
        key_digest: str,
    ) -> ExistingIdempotentWorkflowRun | None: ...

    async def get_run(
        self,
        run_id: UUID,
        owner_principal_id: UUID,
    ) -> InspectedWorkflowRun | None: ...

    async def list_task_runs(
        self,
        run_id: UUID,
        owner_principal_id: UUID,
    ) -> tuple[InspectedTaskRun, ...] | None: ...

    async def get_task_run(
        self,
        task_run_id: UUID,
        owner_principal_id: UUID,
    ) -> InspectedTaskRun | None: ...

    async def transition_runnable_tasks(
        self,
        workflow_run_id: UUID,
    ) -> RunnableTransitionResult:
        """Evaluate immutable dependencies and persist blocked-to-runnable moves."""

    async def propagate_dependency_failures(
        self,
        workflow_run_id: UUID,
    ) -> DependencyFailurePropagationResult:
        """Persist blocked-to-skipped transitions through immutable dependencies."""

    async def evaluate_workflow_run_state(
        self,
        workflow_run_id: UUID,
    ) -> WorkflowRunEvaluationResult:
        """Derive and persist at most one guarded workflow-run transition."""

    async def resolve_workflow_version(
        self,
        workflow_id: UUID,
        owner_principal_id: UUID,
        selection: WorkflowVersionSelection,
    ) -> WorkflowVersionResolutionRecord | None:
        """Resolve availability and version without locking the definition row."""
