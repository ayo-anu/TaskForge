"""SQLAlchemy persistence for workflow draft create and owner-scoped reads."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from types import TracebackType
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import and_, func, insert, or_, select, text, update
from sqlalchemy.engine import Row
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.audit.domain import (
    AuditActor,
    AuditActorKind,
    AuditOutcome,
    AuditRecord,
    bounded_string_set,
)
from taskforge.identity.schema import api_principals
from taskforge.persistence.audit import append_audit_record
from taskforge.workflows.domain import (
    DraftDependency,
    DraftWorkflowStep,
    WorkflowDefinitionStatus,
    WorkflowDraft,
    WorkflowVersionDependency,
    WorkflowVersionSnapshot,
    WorkflowVersionStep,
)
from taskforge.workflows.persistence_ports import (
    LockedWorkflowDefinition,
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
    WorkflowVersionPage,
    WorkflowVersionPageCursor,
    WorkflowVersionSummary,
)
from taskforge.workflows.schema import (
    workflow_definitions,
    workflow_draft_dependencies,
    workflow_draft_steps,
    workflow_version_dependencies,
    workflow_version_steps,
    workflow_versions,
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

    async def list_versions(
        self,
        workflow_id: UUID,
        owner_principal_id: UUID,
        *,
        limit: int,
        cursor: WorkflowVersionPageCursor | None,
    ) -> WorkflowVersionPage | None:
        try:
            async with self._sessions() as session, session.begin():
                rows = (
                    await session.execute(
                        _workflow_version_list_statement(
                            workflow_id,
                            owner_principal_id,
                            limit,
                            cursor,
                        )
                    )
                ).all()
                if not rows:
                    return None
                if rows[0].id is None:
                    return WorkflowVersionPage((), None)
        except DBAPIError as error:
            raise WorkflowPersistenceUnavailable from error
        return _workflow_version_page(rows, limit)

    async def find_version(
        self,
        workflow_id: UUID,
        version_number: int,
        owner_principal_id: UUID,
    ) -> WorkflowVersionSnapshot | None:
        try:
            async with self._sessions() as session, session.begin():
                version = (
                    await session.execute(
                        select(workflow_versions)
                        .join(
                            workflow_definitions,
                            workflow_definitions.c.id
                            == workflow_versions.c.workflow_definition_id,
                        )
                        .where(
                            workflow_versions.c.workflow_definition_id == workflow_id,
                            workflow_versions.c.version_number == version_number,
                            workflow_definitions.c.owner_principal_id
                            == owner_principal_id,
                        )
                    )
                ).one_or_none()
                if version is None:
                    return None
                steps = (
                    await session.execute(
                        select(workflow_version_steps)
                        .where(
                            workflow_version_steps.c.workflow_version_id == version.id
                        )
                        .order_by(workflow_version_steps.c.step_identifier)
                    )
                ).all()
                dependencies = (
                    await session.execute(
                        select(workflow_version_dependencies)
                        .where(
                            workflow_version_dependencies.c.workflow_version_id
                            == version.id
                        )
                        .order_by(
                            workflow_version_dependencies.c.predecessor_step_identifier,
                            workflow_version_dependencies.c.successor_step_identifier,
                        )
                    )
                ).all()
        except DBAPIError as error:
            raise WorkflowPersistenceUnavailable from error
        return _stored_version(version, steps, dependencies)


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


def _workflow_version_list_statement(
    workflow_id: UUID,
    owner_principal_id: UUID,
    limit: int,
    cursor: WorkflowVersionPageCursor | None,
) -> Any:
    join_condition = workflow_versions.c.workflow_definition_id == workflow_id
    if cursor is not None:
        join_condition = and_(
            join_condition,
            workflow_versions.c.version_number < cursor.version_number,
        )
    statement = (
        select(
            workflow_versions.c.id,
            workflow_versions.c.version_number,
            workflow_versions.c.published_at,
        )
        .select_from(workflow_definitions.outerjoin(workflow_versions, join_condition))
        .where(
            workflow_definitions.c.id == workflow_id,
            workflow_definitions.c.owner_principal_id == owner_principal_id,
        )
        .order_by(workflow_versions.c.version_number.desc())
        .limit(limit + 1)
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


def _workflow_version_page(rows: Sequence[Row[Any]], limit: int) -> WorkflowVersionPage:
    summaries = tuple(
        WorkflowVersionSummary(row.id, row.version_number, row.published_at)
        for row in rows
    )
    has_more = len(summaries) > limit
    items = summaries[:limit]
    next_cursor = (
        WorkflowVersionPageCursor(items[-1].version_number)
        if has_more and items
        else None
    )
    return WorkflowVersionPage(items, next_cursor)


def _stored_version(
    version: Row[Any],
    step_rows: Sequence[Row[Any]],
    dependency_rows: Sequence[Row[Any]],
) -> WorkflowVersionSnapshot:
    return WorkflowVersionSnapshot(
        id=version.id,
        workflow_definition_id=version.workflow_definition_id,
        version_number=version.version_number,
        name=version.name,
        description=version.description,
        execution_policy=version.execution_policy,
        published_at=version.published_at,
        steps=tuple(
            WorkflowVersionStep(
                identifier=row.step_identifier,
                task_type=row.task_type,
                parameters=row.parameters,
                execution_policy=row.execution_policy,
            )
            for row in step_rows
        ),
        dependencies=tuple(
            WorkflowVersionDependency(
                predecessor_identifier=row.predecessor_step_identifier,
                successor_identifier=row.successor_step_identifier,
            )
            for row in dependency_rows
        ),
    )


class SQLAlchemyWorkflowUnitOfWork:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions
        self._session: AsyncSession | None = None
        self._committed = False
        self._publication_locks: set[UUID] = set()
        self._availability_locks: set[UUID] = set()

    async def __aenter__(self) -> SQLAlchemyWorkflowUnitOfWork:
        self._publication_locks.clear()
        self._availability_locks.clear()
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
            self._publication_locks.clear()
            self._availability_locks.clear()
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

    async def insert_definition(
        self, workflow: WorkflowDraft, correlation_id: str | None = None
    ) -> WorkflowTimestamps:
        try:
            row = (
                await self._required_session().execute(
                    insert(workflow_definitions)
                    .values(
                        id=workflow.id,
                        owner_principal_id=workflow.owner_principal_id,
                        name=workflow.name,
                        description=workflow.description,
                        execution_policy=workflow.execution_policy,
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
        await append_audit_record(
            self._required_session(),
            AuditRecord(
                uuid4(),
                AuditActor(
                    AuditActorKind.API_PRINCIPAL,
                    api_principal_id=workflow.owner_principal_id,
                ),
                "workflow.create",
                AuditOutcome.ACCEPTED,
                "workflow",
                workflow.id,
                correlation_id,
                {
                    "step_count": len(workflow.steps),
                    "dependency_count": len(workflow.dependencies),
                },
            ),
        )
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
                        "execution_policy": step.execution_policy,
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

    async def lock_draft_for_publication(
        self,
        workflow_id: UUID,
        owner_principal_id: UUID,
    ) -> StoredWorkflowDraft | None:
        """Lock the definition that serializes all version allocation."""
        session = self._required_session()
        try:
            definition = (
                await session.execute(
                    select(workflow_definitions)
                    .where(
                        workflow_definitions.c.id == workflow_id,
                        workflow_definitions.c.owner_principal_id == owner_principal_id,
                    )
                    .with_for_update()
                )
            ).one_or_none()
            if definition is None:
                return None
            self._publication_locks.add(workflow_id)
            step_rows = (
                await session.execute(
                    select(workflow_draft_steps)
                    .where(workflow_draft_steps.c.workflow_definition_id == workflow_id)
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

    async def lock_definition_for_availability(
        self,
        workflow_id: UUID,
        owner_principal_id: UUID,
    ) -> LockedWorkflowDefinition | None:
        try:
            row = (
                await self._required_session().execute(
                    select(
                        workflow_definitions.c.id,
                        workflow_definitions.c.status,
                    )
                    .where(
                        workflow_definitions.c.id == workflow_id,
                        workflow_definitions.c.owner_principal_id == owner_principal_id,
                    )
                    .with_for_update()
                )
            ).one_or_none()
        except DBAPIError as error:
            raise WorkflowPersistenceUnavailable from error
        if row is None:
            return None
        self._availability_locks.add(workflow_id)
        return LockedWorkflowDefinition(
            id=row.id,
            status=WorkflowDefinitionStatus(row.status),
        )

    async def has_published_version(self, workflow_id: UUID) -> bool:
        self._require_availability_lock(workflow_id)
        try:
            return bool(
                await self._required_session().scalar(
                    select(
                        select(workflow_versions.c.id)
                        .where(
                            workflow_versions.c.workflow_definition_id == workflow_id
                        )
                        .exists()
                    )
                )
            )
        except DBAPIError as error:
            raise WorkflowPersistenceUnavailable from error

    async def update_availability(
        self,
        workflow_id: UUID,
        status: WorkflowDefinitionStatus,
        correlation_id: str | None = None,
    ) -> None:
        self._require_availability_lock(workflow_id)
        try:
            updated_id = await self._required_session().scalar(
                update(workflow_definitions)
                .where(workflow_definitions.c.id == workflow_id)
                .values(status=status.value, updated_at=func.current_timestamp())
                .returning(workflow_definitions.c.id)
            )
        except IntegrityError as error:
            raise WorkflowRecordConflict from error
        except DBAPIError as error:
            raise WorkflowPersistenceUnavailable from error
        if updated_id != workflow_id:
            raise WorkflowRecordConflict
        owner_id = await self._required_session().scalar(
            select(workflow_definitions.c.owner_principal_id).where(
                workflow_definitions.c.id == workflow_id
            )
        )
        if not isinstance(owner_id, UUID):
            raise WorkflowRecordConflict
        await append_audit_record(
            self._required_session(),
            AuditRecord(
                uuid4(),
                AuditActor(AuditActorKind.API_PRINCIPAL, api_principal_id=owner_id),
                "workflow.availability_change",
                AuditOutcome.ACCEPTED,
                "workflow",
                workflow_id,
                correlation_id,
                {"new_status": status.value},
            ),
        )

    async def next_version_number(self, workflow_id: UUID) -> int:
        """Read MAX only after this transaction holds the definition lock."""
        self._require_publication_lock(workflow_id)
        try:
            value = await self._required_session().scalar(
                select(
                    func.coalesce(func.max(workflow_versions.c.version_number), 0) + 1
                ).where(workflow_versions.c.workflow_definition_id == workflow_id)
            )
        except DBAPIError as error:
            raise WorkflowPersistenceUnavailable from error
        if not isinstance(value, int) or value <= 0:
            raise RuntimeError("database returned an invalid workflow version number")
        return value

    async def insert_version(
        self,
        version_id: UUID,
        version_number: int,
        workflow: WorkflowDraft,
        correlation_id: str | None = None,
    ) -> datetime:
        self._require_publication_lock(workflow.id)
        try:
            published_at = await self._required_session().scalar(
                insert(workflow_versions)
                .values(
                    id=version_id,
                    workflow_definition_id=workflow.id,
                    version_number=version_number,
                    name=workflow.name,
                    description=workflow.description,
                    execution_policy=workflow.execution_policy,
                )
                .returning(workflow_versions.c.published_at)
            )
        except IntegrityError as error:
            raise WorkflowRecordConflict from error
        except DBAPIError as error:
            raise WorkflowPersistenceUnavailable from error
        if not isinstance(published_at, datetime):
            raise RuntimeError("database did not return a publication timestamp")
        await append_audit_record(
            self._required_session(),
            AuditRecord(
                uuid4(),
                AuditActor(
                    AuditActorKind.API_PRINCIPAL,
                    api_principal_id=workflow.owner_principal_id,
                ),
                "workflow.publish",
                AuditOutcome.ACCEPTED,
                "workflow",
                workflow.id,
                correlation_id,
                {
                    "workflow_version_id": str(version_id),
                    "version_number": version_number,
                    "steps": bounded_string_set(
                        tuple(step.identifier for step in workflow.steps)
                    ),
                },
            ),
        )
        return published_at

    async def insert_version_steps(
        self,
        version_id: UUID,
        steps: tuple[DraftWorkflowStep, ...],
    ) -> None:
        if not steps:
            return
        try:
            await self._required_session().execute(
                insert(workflow_version_steps),
                [
                    {
                        "workflow_version_id": version_id,
                        "step_identifier": step.identifier,
                        "task_type": step.task_type,
                        "parameters": step.parameters,
                        "execution_policy": step.execution_policy,
                    }
                    for step in sorted(steps, key=lambda item: item.identifier)
                ],
            )
        except IntegrityError as error:
            raise WorkflowRecordConflict from error
        except DBAPIError as error:
            raise WorkflowPersistenceUnavailable from error

    async def insert_version_dependencies(
        self,
        version_id: UUID,
        dependencies: tuple[DraftDependency, ...],
    ) -> None:
        if not dependencies:
            return
        ordered = sorted(
            dependencies,
            key=lambda dependency: (
                dependency.predecessor_identifier,
                dependency.successor_identifier,
            ),
        )
        try:
            await self._required_session().execute(
                insert(workflow_version_dependencies),
                [
                    {
                        "workflow_version_id": version_id,
                        "predecessor_step_identifier": (
                            dependency.predecessor_identifier
                        ),
                        "successor_step_identifier": dependency.successor_identifier,
                    }
                    for dependency in ordered
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

    def _require_publication_lock(self, workflow_id: UUID) -> None:
        if self._committed or workflow_id not in self._publication_locks:
            raise RuntimeError(
                "workflow definition lock is required before version allocation"
            )

    def _require_availability_lock(self, workflow_id: UUID) -> None:
        if self._committed or workflow_id not in self._availability_locks:
            raise RuntimeError(
                "workflow definition lock is required before availability changes"
            )


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
            execution_policy=getattr(row, "execution_policy", None),
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
        execution_policy=getattr(definition_row, "execution_policy", None),
    )
    return StoredWorkflowDraft(
        draft=draft,
        created_at=definition_row.created_at,
        updated_at=definition_row.updated_at,
    )
