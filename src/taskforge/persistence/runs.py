"""SQLAlchemy persistence for workflow run target resolution."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy import Select, and_, select, true
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.runs.domain import ExplicitWorkflowVersion, WorkflowVersionSelection
from taskforge.runs.persistence_ports import (
    WorkflowRunPersistenceUnavailable,
    WorkflowVersionResolutionRecord,
)
from taskforge.workflows.domain import WorkflowDefinitionStatus
from taskforge.workflows.schema import workflow_definitions, workflow_versions


class SQLAlchemyWorkflowRunRepository:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def resolve_workflow_version(
        self,
        workflow_id: UUID,
        owner_principal_id: UUID,
        selection: WorkflowVersionSelection,
    ) -> WorkflowVersionResolutionRecord | None:
        statement = _version_resolution_statement(
            workflow_id, owner_principal_id, selection
        )
        try:
            async with self._sessions() as session, session.begin():
                row = (await session.execute(statement)).one_or_none()
        except DBAPIError as error:
            raise WorkflowRunPersistenceUnavailable from error
        if row is None:
            return None
        return WorkflowVersionResolutionRecord(
            workflow_definition_id=row.workflow_definition_id,
            status=WorkflowDefinitionStatus(row.status),
            workflow_version_id=row.workflow_version_id,
            version_number=row.version_number,
        )


def _version_resolution_statement(
    workflow_id: UUID,
    owner_principal_id: UUID,
    selection: WorkflowVersionSelection,
) -> Select[Any]:
    version_query = select(
        workflow_versions.c.id.label("workflow_version_id"),
        workflow_versions.c.version_number,
    ).where(workflow_versions.c.workflow_definition_id == workflow_id)
    if isinstance(selection, ExplicitWorkflowVersion):
        version_query = version_query.where(
            workflow_versions.c.version_number == selection.version_number
        )
    else:
        version_query = version_query.order_by(
            workflow_versions.c.version_number.desc()
        )
    selected_version = version_query.limit(1).lateral("selected_version")
    return (
        select(
            workflow_definitions.c.id.label("workflow_definition_id"),
            workflow_definitions.c.status,
            selected_version.c.workflow_version_id,
            selected_version.c.version_number,
        )
        .select_from(workflow_definitions.outerjoin(selected_version, true()))
        .where(
            and_(
                workflow_definitions.c.id == workflow_id,
                workflow_definitions.c.owner_principal_id == owner_principal_id,
            )
        )
    )
