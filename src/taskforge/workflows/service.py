"""Application services for transactional workflow draft persistence."""

from __future__ import annotations

from uuid import UUID

from taskforge.workflows.domain import WorkflowDraft
from taskforge.workflows.persistence_ports import (
    ResolvedDependency,
    StoredWorkflowDraft,
    WorkflowOwnerRecordDisabled,
    WorkflowOwnerRecordNotFound,
    WorkflowPage,
    WorkflowPageCursor,
    WorkflowPersistenceUnavailable,
    WorkflowRecordConflict,
    WorkflowRepository,
)


class WorkflowNotFound(Exception):
    """A workflow is absent from the requested owner's scope."""


class WorkflowOwnerNotFound(Exception):
    """A workflow owner does not exist."""


class WorkflowOwnerDisabled(Exception):
    """A workflow owner is disabled."""


class WorkflowPersistenceConflict(Exception):
    """A workflow could not be persisted without violating an invariant."""


class WorkflowServiceUnavailable(Exception):
    """Workflow persistence was operationally unavailable."""


class InvalidWorkflowListQuery(ValueError):
    """A workflow list request is not valid."""


class WorkflowService:
    def __init__(self, repository: WorkflowRepository) -> None:
        self._repository = repository

    async def create(self, workflow: WorkflowDraft) -> StoredWorkflowDraft:
        dependencies = _resolve_dependencies(workflow)
        try:
            async with self._repository.transaction() as transaction:
                await transaction.require_enabled_owner(workflow.owner_principal_id)
                timestamps = await transaction.insert_definition(workflow)
                await transaction.insert_steps(workflow.id, workflow.steps)
                await transaction.insert_dependencies(workflow.id, dependencies)
                await transaction.commit()
        except WorkflowOwnerRecordNotFound as error:
            raise WorkflowOwnerNotFound from error
        except WorkflowOwnerRecordDisabled as error:
            raise WorkflowOwnerDisabled from error
        except WorkflowRecordConflict as error:
            raise WorkflowPersistenceConflict from error
        except WorkflowPersistenceUnavailable as error:
            raise WorkflowServiceUnavailable from error
        return StoredWorkflowDraft(
            draft=workflow,
            created_at=timestamps.created_at,
            updated_at=timestamps.updated_at,
        )

    async def get(
        self,
        workflow_id: UUID,
        *,
        owner_principal_id: UUID,
    ) -> StoredWorkflowDraft:
        try:
            stored = await self._repository.find_draft(
                workflow_id,
                owner_principal_id,
            )
        except WorkflowPersistenceUnavailable as error:
            raise WorkflowServiceUnavailable from error
        if stored is None:
            raise WorkflowNotFound
        return stored

    async def list(
        self,
        *,
        owner_principal_id: UUID,
        limit: int,
        cursor: WorkflowPageCursor | None = None,
    ) -> WorkflowPage:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise InvalidWorkflowListQuery("limit must be a positive integer")
        try:
            return await self._repository.list_summaries(
                owner_principal_id,
                limit=limit,
                cursor=cursor,
            )
        except WorkflowPersistenceUnavailable as error:
            raise WorkflowServiceUnavailable from error


def _resolve_dependencies(
    workflow: WorkflowDraft,
) -> tuple[ResolvedDependency, ...]:
    identifiers = {step.identifier: step.id for step in workflow.steps}
    resolved: list[ResolvedDependency] = []
    for dependency in workflow.dependencies:
        try:
            predecessor_id = identifiers[dependency.predecessor_identifier]
            successor_id = identifiers[dependency.successor_identifier]
        except KeyError as error:
            raise WorkflowPersistenceConflict(
                "dependency references an unknown step"
            ) from error
        resolved.append(
            ResolvedDependency(
                id=dependency.id,
                predecessor_step_id=predecessor_id,
                successor_step_id=successor_id,
            )
        )
    return tuple(resolved)
