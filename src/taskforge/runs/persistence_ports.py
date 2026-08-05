"""Persistence contracts for workflow run target resolution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from taskforge.runs.domain import WorkflowVersionSelection
from taskforge.workflows.domain import WorkflowDefinitionStatus


class WorkflowRunPersistenceUnavailable(Exception):
    """Workflow run target persistence is operationally unavailable."""


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


class WorkflowRunRepository(Protocol):
    async def resolve_workflow_version(
        self,
        workflow_id: UUID,
        owner_principal_id: UUID,
        selection: WorkflowVersionSelection,
    ) -> WorkflowVersionResolutionRecord | None:
        """Resolve availability and version without locking the definition row."""
