"""SQLAlchemy persistence for workflow draft create and owner-scoped reads."""

from __future__ import annotations

from collections.abc import Sequence
from types import TracebackType
from typing import Any
from uuid import UUID

from sqlalchemy import and_, insert, or_, select, text
from sqlalchemy.engine import Row
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.identity.schema import api_principals
from taskforge.workflows.domain import (
    DraftDependency,
    DraftWorkflowStep,
    WorkflowDefinitionStatus,
    WorkflowDraft,
)
from taskforge.workflows.persistence_ports import (
    ResolvedDependency,
    StoredWorkflowDraft,
    WorkflowOwnerRecordDisabled,
    WorkflowOwnerRecordNotFound,
    WorkflowPage,
    WorkflowPageCursor,
    WorkflowPersistenceUnavailable,
    WorkflowRecordConflict,
    WorkflowSummary,
    WorkflowTimestamps,
)
from taskforge.workflows.schema import (
    workflow_definitions,
    workflow_draft_dependencies,
    workflow_draft_steps,
)


class SQLAlchemyWorkflowRepository:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    def transaction(self) -> SQLAlchemyWorkflowUnitOfWork:
        return SQLAlchemyWorkflowUnitOfWork(self._sessions)

    async def find_draft(
        self,
        workflow_id: UUID,
        owner_principal_id: UUID,
    ) -> StoredWorkflowDraft | None:
        try:
            async with self._sessions() as session, session.begin():
                await session.execute(
                    text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                )
                definition = (
                    await session.execute(
                        select(workflow_definitions).where(
                            workflow_definitions.c.id == workflow_id,
                            workflow_definitions.c.owner_principal_id
                            == owner_principal_id,
                        )
                    )
                ).one_or_none()
                if definition is None:
                    return None
                step_rows = (
                    await session.execute(
                        select(workflow_draft_steps)
                        .where(
                            workflow_draft_steps.c.workflow_definition_id == workflow_id
                        )
                        .order_by(
                            workflow_draft_steps.c.step_identifier,
                            workflow_draft_steps.c.id,
                        )
                    )
                ).all()
                dependency_rows = (
                    await session.execute(
                        select(workflow_draft_dependencies)
                        .where(
                            workflow_draft_dependencies.c.workflow_definition_id
                            == workflow_id
                        )
                        .order_by(
                            workflow_draft_dependencies.c.predecessor_step_id,
                            workflow_draft_dependencies.c.successor_step_id,
                            workflow_draft_dependencies.c.id,
                        )
                    )
                ).all()
        except DBAPIError as error:
            raise WorkflowPersistenceUnavailable from error
        return _stored_draft(definition, step_rows, dependency_rows)

    async def list_summaries(
        self,
        owner_principal_id: UUID,
        *,
        limit: int,
        cursor: WorkflowPageCursor | None,
    ) -> WorkflowPage:
        statement = _workflow_list_statement(owner_principal_id, limit, cursor)
        try:
            async with self._sessions() as session:
                rows = (await session.execute(statement)).all()
        except DBAPIError as error:
            raise WorkflowPersistenceUnavailable from error
        return _workflow_page(rows, limit)


def _workflow_list_statement(
    owner_principal_id: UUID,
    limit: int,
    cursor: WorkflowPageCursor | None,
) -> Any:
    statement = (
        select(
            workflow_definitions.c.id,
            workflow_definitions.c.owner_principal_id,
            workflow_definitions.c.name,
            workflow_definitions.c.description,
            workflow_definitions.c.status,
            workflow_definitions.c.created_at,
            workflow_definitions.c.updated_at,
        )
        .where(workflow_definitions.c.owner_principal_id == owner_principal_id)
        .order_by(
            workflow_definitions.c.created_at.desc(),
            workflow_definitions.c.id.desc(),
        )
        .limit(limit + 1)
    )
    if cursor is not None:
        statement = statement.where(
            or_(
                workflow_definitions.c.created_at < cursor.created_at,
                and_(
                    workflow_definitions.c.created_at == cursor.created_at,
                    workflow_definitions.c.id < cursor.workflow_id,
                ),
            )
        )
    return statement


def _workflow_page(rows: Sequence[Row[Any]], limit: int) -> WorkflowPage:
    summaries = tuple(
        WorkflowSummary(
            id=row.id,
            owner_principal_id=row.owner_principal_id,
            name=row.name,
            description=row.description,
            status=WorkflowDefinitionStatus(row.status),
            created_at=row.created_at,
            updated_at=row.updated_at,
        )
        for row in rows
    )
    has_more = len(summaries) > limit
    items = summaries[:limit]
    next_cursor = None
    if has_more:
        last = items[-1]
        next_cursor = WorkflowPageCursor(last.created_at, last.id)
    return WorkflowPage(items=items, next_cursor=next_cursor)


class SQLAlchemyWorkflowUnitOfWork:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions
        self._session: AsyncSession | None = None
        self._committed = False

    async def __aenter__(self) -> SQLAlchemyWorkflowUnitOfWork:
        self._session = self._sessions()
        await self._session.begin()
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exception_type, exception, traceback
        session, self._session = self._required_session(), None
        try:
            if not self._committed:
                await session.rollback()
        finally:
            await session.close()

    async def require_enabled_owner(self, owner_principal_id: UUID) -> None:
        try:
            row = (
                await self._required_session().execute(
                    select(api_principals.c.id, api_principals.c.disabled_at)
                    .where(api_principals.c.id == owner_principal_id)
                    .with_for_update(key_share=True)
                )
            ).one_or_none()
        except DBAPIError as error:
            raise WorkflowPersistenceUnavailable from error
        if row is None:
            raise WorkflowOwnerRecordNotFound
        if row.disabled_at is not None:
            raise WorkflowOwnerRecordDisabled

    async def insert_definition(self, workflow: WorkflowDraft) -> WorkflowTimestamps:
        try:
            row = (
                await self._required_session().execute(
                    insert(workflow_definitions)
                    .values(
                        id=workflow.id,
                        owner_principal_id=workflow.owner_principal_id,
                        name=workflow.name,
                        description=workflow.description,
                        status=workflow.status.value,
                    )
                    .returning(
                        workflow_definitions.c.created_at,
                        workflow_definitions.c.updated_at,
                    )
                )
            ).one()
        except IntegrityError as error:
            raise WorkflowRecordConflict from error
        except DBAPIError as error:
            raise WorkflowPersistenceUnavailable from error
        return WorkflowTimestamps(row.created_at, row.updated_at)

    async def insert_steps(
        self,
        workflow_id: UUID,
        steps: tuple[DraftWorkflowStep, ...],
    ) -> None:
        if not steps:
            return
        try:
            await self._required_session().execute(
                insert(workflow_draft_steps),
                [
                    {
                        "id": step.id,
                        "workflow_definition_id": workflow_id,
                        "step_identifier": step.identifier,
                        "task_type": step.task_type,
                        "parameters": step.parameters,
                    }
                    for step in steps
                ],
            )
        except IntegrityError as error:
            raise WorkflowRecordConflict from error
        except DBAPIError as error:
            raise WorkflowPersistenceUnavailable from error

    async def insert_dependencies(
        self,
        workflow_id: UUID,
        dependencies: tuple[ResolvedDependency, ...],
    ) -> None:
        if not dependencies:
            return
        try:
            await self._required_session().execute(
                insert(workflow_draft_dependencies),
                [
                    {
                        "id": dependency.id,
                        "workflow_definition_id": workflow_id,
                        "predecessor_step_id": dependency.predecessor_step_id,
                        "successor_step_id": dependency.successor_step_id,
                    }
                    for dependency in dependencies
                ],
            )
        except IntegrityError as error:
            raise WorkflowRecordConflict from error
        except DBAPIError as error:
            raise WorkflowPersistenceUnavailable from error

    async def commit(self) -> None:
        try:
            await self._required_session().commit()
        except IntegrityError as error:
            raise WorkflowRecordConflict from error
        except DBAPIError as error:
            raise WorkflowPersistenceUnavailable from error
        self._committed = True

    def _required_session(self) -> AsyncSession:
        if self._session is None:
            raise RuntimeError("workflow transaction is not active")
        return self._session


def _stored_draft(
    definition: Row[Any],
    step_rows: Sequence[Row[Any]],
    dependency_rows: Sequence[Row[Any]],
) -> StoredWorkflowDraft:
    definition_row = definition
    steps = tuple(
        DraftWorkflowStep(
            id=row.id,
            identifier=row.step_identifier,
            task_type=row.task_type,
            parameters=row.parameters,
        )
        for row in step_rows
    )
    identifiers = {step.id: step.identifier for step in steps}
    dependencies = tuple(
        DraftDependency(
            id=row.id,
            predecessor_identifier=identifiers[row.predecessor_step_id],
            successor_identifier=identifiers[row.successor_step_id],
        )
        for row in dependency_rows
    )
    draft = WorkflowDraft(
        id=definition_row.id,
        owner_principal_id=definition_row.owner_principal_id,
        name=definition_row.name,
        description=definition_row.description,
        status=WorkflowDefinitionStatus(definition_row.status),
        steps=steps,
        dependencies=dependencies,
    )
    return StoredWorkflowDraft(
        draft=draft,
        created_at=definition_row.created_at,
        updated_at=definition_row.updated_at,
    )
