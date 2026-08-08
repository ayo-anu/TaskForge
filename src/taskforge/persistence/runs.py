"""SQLAlchemy persistence for workflow run target resolution."""

from __future__ import annotations

from collections.abc import Sequence
from types import TracebackType
from typing import Any
from uuid import UUID

from sqlalchemy import Select, and_, insert, select, true
from sqlalchemy.engine import Row
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.runs.domain import (
    ExplicitWorkflowVersion,
    NewTaskRun,
    NewWorkflowRun,
    WorkflowRunInput,
    WorkflowRunVersionDependency,
    WorkflowRunVersionSnapshot,
    WorkflowVersionSelection,
)
from taskforge.runs.persistence_ports import (
    PreparedWorkflowRunCreation,
    WorkflowRunPersistenceUnavailable,
    WorkflowRunRecordConflict,
    WorkflowRunTimestamps,
    WorkflowVersionResolutionRecord,
)
from taskforge.runs.schema import task_runs, workflow_run_inputs, workflow_runs
from taskforge.workflows.domain import WorkflowDefinitionStatus
from taskforge.workflows.schema import (
    workflow_definitions,
    workflow_version_dependencies,
    workflow_version_steps,
    workflow_versions,
)


class SQLAlchemyWorkflowRunRepository:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    def creation_transaction(self) -> SQLAlchemyWorkflowRunCreationTransaction:
        return SQLAlchemyWorkflowRunCreationTransaction(self._sessions)

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


class SQLAlchemyWorkflowRunCreationTransaction:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions
        self._session: AsyncSession | None = None
        self._committed = False

    async def __aenter__(self) -> SQLAlchemyWorkflowRunCreationTransaction:
        self._committed = False
        session = self._sessions()
        self._session = session
        try:
            await session.begin()
        except DBAPIError as error:
            self._session = None
            try:
                await session.close()
            except BaseException:
                pass
            raise WorkflowRunPersistenceUnavailable from error
        except BaseException:
            self._session = None
            try:
                await session.close()
            except BaseException:
                pass
            raise
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

    async def prepare_creation_target(
        self,
        workflow_id: UUID,
        owner_principal_id: UUID,
        selection: WorkflowVersionSelection,
    ) -> PreparedWorkflowRunCreation | None:
        session = self._required_session()
        try:
            definition = (
                await session.execute(
                    _definition_lock_statement(workflow_id, owner_principal_id)
                )
            ).one_or_none()
            if definition is None:
                return None
            status = WorkflowDefinitionStatus(definition.status)
            if status is not WorkflowDefinitionStatus.ENABLED:
                return PreparedWorkflowRunCreation(workflow_id, status, None)
            version = (
                await session.execute(_locked_version_statement(workflow_id, selection))
            ).one_or_none()
            if version is None:
                return PreparedWorkflowRunCreation(workflow_id, status, None)
            step_rows = (
                await session.execute(
                    select(workflow_version_steps.c.step_identifier)
                    .where(workflow_version_steps.c.workflow_version_id == version.id)
                    .order_by(workflow_version_steps.c.step_identifier)
                )
            ).all()
            dependency_rows = (
                await session.execute(
                    select(
                        workflow_version_dependencies.c.predecessor_step_identifier,
                        workflow_version_dependencies.c.successor_step_identifier,
                    )
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
            raise WorkflowRunPersistenceUnavailable from error
        snapshot = _creation_snapshot(version, step_rows, dependency_rows)
        return PreparedWorkflowRunCreation(workflow_id, status, snapshot)

    async def insert_complete_run(
        self,
        prepared: PreparedWorkflowRunCreation,
        run: NewWorkflowRun,
        input_snapshot: WorkflowRunInput,
        task_run_values: tuple[NewTaskRun, ...],
    ) -> WorkflowRunTimestamps:
        snapshot = prepared.snapshot
        if snapshot is None:
            raise ValueError("prepared creation has no workflow version snapshot")
        session = self._required_session()
        try:
            row = (
                await session.execute(
                    insert(workflow_runs)
                    .values(
                        id=run.id,
                        workflow_definition_id=prepared.workflow_definition_id,
                        workflow_version_id=snapshot.workflow_version_id,
                        requested_by_principal_id=run.requested_by_principal_id,
                        status=run.status.value,
                    )
                    .returning(
                        workflow_runs.c.created_at,
                        workflow_runs.c.updated_at,
                    )
                )
            ).one()
            await session.execute(
                insert(workflow_run_inputs).values(
                    workflow_run_id=run.id,
                    payload=input_snapshot.payload,
                    input_references=input_snapshot.input_references,
                )
            )
            if task_run_values:
                await session.execute(
                    insert(task_runs),
                    [
                        {
                            "id": task.id,
                            "workflow_run_id": run.id,
                            "workflow_version_id": snapshot.workflow_version_id,
                            "step_identifier": task.step_identifier,
                            "status": task.status.value,
                        }
                        for task in task_run_values
                    ],
                )
        except IntegrityError as error:
            raise WorkflowRunRecordConflict from error
        except DBAPIError as error:
            raise WorkflowRunPersistenceUnavailable from error
        return WorkflowRunTimestamps(row.created_at, row.updated_at)

    async def commit(self) -> None:
        try:
            await self._required_session().commit()
        except IntegrityError as error:
            raise WorkflowRunRecordConflict from error
        except DBAPIError as error:
            raise WorkflowRunPersistenceUnavailable from error
        self._committed = True

    def _required_session(self) -> AsyncSession:
        if self._session is None:
            raise RuntimeError("workflow run creation transaction is not active")
        return self._session


def _locked_version_statement(
    workflow_id: UUID,
    selection: WorkflowVersionSelection,
) -> Select[Any]:
    statement = select(
        workflow_versions.c.id,
        workflow_versions.c.workflow_definition_id,
        workflow_versions.c.version_number,
    ).where(workflow_versions.c.workflow_definition_id == workflow_id)
    if isinstance(selection, ExplicitWorkflowVersion):
        return statement.where(
            workflow_versions.c.version_number == selection.version_number
        )
    return statement.order_by(workflow_versions.c.version_number.desc()).limit(1)


def _definition_lock_statement(
    workflow_id: UUID,
    owner_principal_id: UUID,
) -> Select[Any]:
    return (
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


def _creation_snapshot(
    version: Row[Any],
    step_rows: Sequence[Row[Any]],
    dependency_rows: Sequence[Row[Any]],
) -> WorkflowRunVersionSnapshot:
    return WorkflowRunVersionSnapshot(
        workflow_definition_id=version.workflow_definition_id,
        workflow_version_id=version.id,
        version_number=version.version_number,
        step_identifiers=tuple(row.step_identifier for row in step_rows),
        dependencies=tuple(
            WorkflowRunVersionDependency(
                row.predecessor_step_identifier,
                row.successor_step_identifier,
            )
            for row in dependency_rows
        ),
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
