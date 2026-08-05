"""Application service for workflow run target resolution."""

from __future__ import annotations

from uuid import UUID

from taskforge.runs.domain import (
    ResolvedWorkflowVersion,
    WorkflowVersionSelection,
    require_run_available,
)
from taskforge.runs.persistence_ports import (
    WorkflowRunPersistenceUnavailable,
    WorkflowRunRepository,
)


class WorkflowRunTargetNotFound(Exception):
    """A workflow is absent from the requested owner's scope."""


class WorkflowVersionUnavailable(Exception):
    """The selected version is not published for the workflow."""


class WorkflowRunServiceUnavailable(Exception):
    """Workflow run target persistence was operationally unavailable."""


class WorkflowRunService:
    def __init__(self, repository: WorkflowRunRepository) -> None:
        self._repository = repository

    async def resolve_version(
        self,
        workflow_id: UUID,
        *,
        owner_principal_id: UUID,
        selection: WorkflowVersionSelection,
    ) -> ResolvedWorkflowVersion:
        """Resolve a target valid at lookup time, without admitting a run."""
        try:
            record = await self._repository.resolve_workflow_version(
                workflow_id,
                owner_principal_id,
                selection,
            )
        except WorkflowRunPersistenceUnavailable as error:
            raise WorkflowRunServiceUnavailable from error
        if record is None:
            raise WorkflowRunTargetNotFound
        require_run_available(record.status)
        if record.workflow_version_id is None or record.version_number is None:
            raise WorkflowVersionUnavailable
        return ResolvedWorkflowVersion(
            workflow_definition_id=record.workflow_definition_id,
            workflow_version_id=record.workflow_version_id,
            version_number=record.version_number,
        )
