"""Persistence contracts for transactional workflow draft operations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from types import TracebackType
from typing import Protocol
from uuid import UUID

from taskforge.workflows.domain import (
    DraftWorkflowStep,
    WorkflowDefinitionStatus,
    WorkflowDraft,
)


class WorkflowOwnerRecordNotFound(Exception):
    """The requested workflow owner does not exist."""


class WorkflowOwnerRecordDisabled(Exception):
    """The requested workflow owner is disabled."""


class WorkflowRecordConflict(Exception):
    """A database constraint rejected workflow persistence."""


class WorkflowPersistenceUnavailable(Exception):
    """Workflow persistence was operationally unavailable."""


@dataclass(frozen=True)
class WorkflowTimestamps:
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class ResolvedDependency:
    id: UUID
    predecessor_step_id: UUID
    successor_step_id: UUID


@dataclass(frozen=True, repr=False)
class StoredWorkflowDraft:
    draft: WorkflowDraft
    created_at: datetime
    updated_at: datetime

    def __repr__(self) -> str:
        return (
            "StoredWorkflowDraft("
            f"draft={self.draft!r}, created_at={self.created_at!r}, "
            f"updated_at={self.updated_at!r})"
        )


@dataclass(frozen=True)
class WorkflowSummary:
    id: UUID
    owner_principal_id: UUID
    name: str
    description: str | None
    status: WorkflowDefinitionStatus
    created_at: datetime
    updated_at: datetime


class WorkflowTransaction(Protocol):
    async def require_enabled_owner(self, owner_principal_id: UUID) -> None: ...

    async def insert_definition(
        self, workflow: WorkflowDraft
    ) -> WorkflowTimestamps: ...

    async def insert_steps(
        self,
        workflow_id: UUID,
        steps: tuple[DraftWorkflowStep, ...],
    ) -> None: ...

    async def insert_dependencies(
        self,
        workflow_id: UUID,
        dependencies: tuple[ResolvedDependency, ...],
    ) -> None: ...

    async def commit(self) -> None: ...


class WorkflowTransactionContext(Protocol):
    async def __aenter__(self) -> WorkflowTransaction: ...

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...


class WorkflowRepository(Protocol):
    def transaction(self) -> WorkflowTransactionContext: ...

    async def find_draft(
        self,
        workflow_id: UUID,
        owner_principal_id: UUID,
    ) -> StoredWorkflowDraft | None: ...

    async def list_summaries(
        self,
        owner_principal_id: UUID,
        *,
        limit: int,
    ) -> tuple[WorkflowSummary, ...]: ...
