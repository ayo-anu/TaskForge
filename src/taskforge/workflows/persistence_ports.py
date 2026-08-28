"""Persistence contracts for transactional workflow draft operations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from types import TracebackType
from typing import Protocol
from uuid import UUID

from taskforge.identity.authorization import OwnerFilter
from taskforge.workflows.domain import (
    DraftDependency,
    DraftWorkflowStep,
    WorkflowDefinitionStatus,
    WorkflowDraft,
    WorkflowVersionSnapshot,
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
class LockedWorkflowDefinition:
    id: UUID
    owner_principal_id: UUID
    status: WorkflowDefinitionStatus


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


@dataclass(frozen=True)
class WorkflowPageCursor:
    """Immutable keyset position in the stable workflow list order."""

    created_at: datetime
    workflow_id: UUID

    def __post_init__(self) -> None:
        if self.created_at.tzinfo is None:
            raise ValueError("workflow page cursor timestamp must be timezone-aware")
        object.__setattr__(self, "created_at", self.created_at.astimezone(UTC))


@dataclass(frozen=True)
class WorkflowPage:
    items: tuple[WorkflowSummary, ...]
    next_cursor: WorkflowPageCursor | None


@dataclass(frozen=True)
class WorkflowVersionSummary:
    id: UUID
    version_number: int
    published_at: datetime

    def __post_init__(self) -> None:
        if isinstance(self.version_number, bool) or self.version_number <= 0:
            raise ValueError("workflow version number must be positive")
        if self.published_at.tzinfo is None:
            raise ValueError("publication timestamp must be timezone-aware")
        object.__setattr__(self, "published_at", self.published_at.astimezone(UTC))


@dataclass(frozen=True)
class WorkflowVersionPageCursor:
    version_number: int

    def __post_init__(self) -> None:
        if isinstance(self.version_number, bool) or self.version_number <= 0:
            raise ValueError("workflow version cursor must be positive")


@dataclass(frozen=True)
class WorkflowVersionPage:
    items: tuple[WorkflowVersionSummary, ...]
    next_cursor: WorkflowVersionPageCursor | None


class WorkflowTransaction(Protocol):
    async def require_enabled_owner(self, owner_principal_id: UUID) -> None: ...

    async def insert_definition(
        self, workflow: WorkflowDraft, correlation_id: str | None = None
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

    async def lock_definition_for_availability(
        self,
        workflow_id: UUID,
        owner_filter: OwnerFilter,
    ) -> LockedWorkflowDefinition | None: ...

    async def has_published_version(self, workflow_id: UUID) -> bool: ...

    async def update_availability(
        self,
        workflow_id: UUID,
        status: WorkflowDefinitionStatus,
        actor_principal_id: UUID,
        correlation_id: str | None = None,
    ) -> None: ...

    async def lock_draft_for_publication(
        self,
        workflow_id: UUID,
        owner_filter: OwnerFilter,
    ) -> StoredWorkflowDraft | None:
        """Lock the definition before any version allocation or insertion."""

    async def next_version_number(self, workflow_id: UUID) -> int:
        """Allocate under the definition lock held by this transaction."""

    async def insert_version(
        self,
        version_id: UUID,
        version_number: int,
        workflow: WorkflowDraft,
        actor_principal_id: UUID,
        correlation_id: str | None = None,
    ) -> datetime: ...

    async def insert_version_steps(
        self,
        version_id: UUID,
        steps: tuple[DraftWorkflowStep, ...],
    ) -> None: ...

    async def insert_version_dependencies(
        self,
        version_id: UUID,
        dependencies: tuple[DraftDependency, ...],
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
        owner_filter: OwnerFilter,
    ) -> StoredWorkflowDraft | None: ...

    async def list_summaries(
        self,
        owner_filter: OwnerFilter,
        *,
        limit: int,
        cursor: WorkflowPageCursor | None,
    ) -> WorkflowPage: ...

    async def list_versions(
        self,
        workflow_id: UUID,
        owner_filter: OwnerFilter,
        *,
        limit: int,
        cursor: WorkflowVersionPageCursor | None,
    ) -> WorkflowVersionPage | None: ...

    async def find_version(
        self,
        workflow_id: UUID,
        version_number: int,
        owner_filter: OwnerFilter,
    ) -> WorkflowVersionSnapshot | None: ...
