"""Persistence contracts for workflow run target resolution."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from types import TracebackType
from typing import Protocol
from uuid import UUID

from taskforge.runs.domain import (
    NewTaskRun,
    NewWorkflowRun,
    WorkflowRunInput,
    WorkflowRunVersionSnapshot,
    WorkflowVersionSelection,
)
from taskforge.workflows.domain import WorkflowDefinitionStatus


class WorkflowRunPersistenceUnavailable(Exception):
    """Workflow run target persistence is operationally unavailable."""


class WorkflowRunRecordConflict(Exception):
    """A database constraint rejected complete run creation."""


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

    async def insert_complete_run(
        self,
        prepared: PreparedWorkflowRunCreation,
        run: NewWorkflowRun,
        input_snapshot: WorkflowRunInput,
        task_run_values: tuple[NewTaskRun, ...],
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

    async def resolve_workflow_version(
        self,
        workflow_id: UUID,
        owner_principal_id: UUID,
        selection: WorkflowVersionSelection,
    ) -> WorkflowVersionResolutionRecord | None:
        """Resolve availability and version without locking the definition row."""
