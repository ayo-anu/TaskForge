"""SQLAlchemy persistence for atomic durable task dispatch creation."""

from __future__ import annotations

from copy import deepcopy
from types import TracebackType
from typing import Any
from uuid import UUID

from sqlalchemy import func, insert, select, tuple_, update
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.sql import Select, Update

from taskforge.dispatch.persistence_ports import (
    NewTaskAttempt,
    NewTaskDispatchOutbox,
    PreparedTaskDispatch,
    TaskDispatchPersistenceConflict,
    TaskDispatchPersistenceUnavailable,
    TaskDispatchStateConflict,
)
from taskforge.dispatch.publisher_ports import (
    DispatchAcknowledgementPersistenceFailure,
    DispatchOutboxPersistenceUnavailable,
    DispatchPublicationInvariantConflict,
    OutboxBacklogObservation,
    PublicationAcknowledgement,
    StoredDispatch,
    UnpublishedDispatchCursor,
)
from taskforge.persistence.execution_events import append_status_changed_execution_event
from taskforge.runs.domain import TaskRunStatus, WorkflowRunStatus
from taskforge.runs.persistence_ports import (
    WorkflowRunExecutionEventInvariantViolation,
    WorkflowRunExecutionEventPersistenceUnavailable,
)
from taskforge.runs.schema import (
    task_attempts,
    task_dispatch_outbox,
    task_runs,
    workflow_runs,
)
from taskforge.workflows.schema import workflow_version_steps


class SQLAlchemyTaskDispatchRepository:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    def dispatch_transaction(self) -> SQLAlchemyTaskDispatchTransaction:
        return SQLAlchemyTaskDispatchTransaction(self._sessions)


class SQLAlchemyDispatchOutboxRepository:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def list_unpublished_page(
        self,
        *,
        after: UnpublishedDispatchCursor | None,
        limit: int,
    ) -> tuple[StoredDispatch, ...]:
        try:
            async with self._sessions() as session:
                rows = (
                    await session.execute(
                        _unpublished_dispatch_page_statement(after, limit)
                    )
                ).all()
        except DBAPIError as error:
            raise DispatchOutboxPersistenceUnavailable from error
        return tuple(
            StoredDispatch(
                row.id,
                row.task_attempt_id,
                row.route,
                deepcopy(row.payload),
                row.created_at,
            )
            for row in rows
        )

    async def record_accepted_publication(
        self, expected: StoredDispatch
    ) -> PublicationAcknowledgement:
        try:
            async with self._sessions.begin() as session:
                published_at = await session.scalar(
                    _record_publication_acknowledgement_statement(expected)
                )
                if published_at is not None:
                    return PublicationAcknowledgement.RECORDED
                current = (
                    await session.execute(
                        _dispatch_acknowledgement_snapshot_statement(
                            expected.dispatch_id
                        )
                    )
                ).one_or_none()
                if current is None:
                    raise DispatchPublicationInvariantConflict
                if (
                    current.task_attempt_id != expected.task_attempt_id
                    or current.route != expected.route
                    or current.payload != expected.payload
                ):
                    raise DispatchPublicationInvariantConflict
                if current.published_at is None:
                    raise DispatchAcknowledgementPersistenceFailure
                return PublicationAcknowledgement.ALREADY_RECORDED
        except DBAPIError as error:
            raise DispatchOutboxPersistenceUnavailable from error

    async def observe_unpublished_backlog(
        self, *, limit: int
    ) -> OutboxBacklogObservation:
        if type(limit) is not int or limit <= 0:
            raise ValueError("outbox observation limit must be positive")
        bounded = (
            select(task_dispatch_outbox.c.created_at)
            .where(task_dispatch_outbox.c.published_at.is_(None))
            .order_by(
                task_dispatch_outbox.c.created_at,
                task_dispatch_outbox.c.id,
            )
            .limit(limit + 1)
            .subquery()
        )
        statement = select(
            func.count().label("pending"),
            func.min(bounded.c.created_at).label("oldest_created_at"),
            func.statement_timestamp().label("observed_at"),
        ).select_from(bounded)
        try:
            async with self._sessions() as session:
                row = (await session.execute(statement)).one()
        except DBAPIError as error:
            raise DispatchOutboxPersistenceUnavailable from error
        pending = min(row.pending, limit)
        return OutboxBacklogObservation(
            pending,
            row.pending > limit,
            row.oldest_created_at,
            row.observed_at,
        )


class SQLAlchemyTaskDispatchTransaction:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions
        self._session: AsyncSession | None = None
        self._committed = False

    async def __aenter__(self) -> SQLAlchemyTaskDispatchTransaction:
        self._committed = False
        session = self._sessions()
        self._session = session
        try:
            await session.begin()
        except DBAPIError as error:
            self._session = None
            await session.close()
            raise TaskDispatchPersistenceUnavailable from error
        except BaseException:
            self._session = None
            await session.close()
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

    async def prepare_dispatch(
        self, workflow_run_id: UUID, task_run_id: UUID
    ) -> PreparedTaskDispatch | None:
        session = self._required_session()
        try:
            locked_run = (
                await session.execute(
                    _workflow_run_dispatch_lock_statement(workflow_run_id, task_run_id)
                )
            ).one_or_none()
            if locked_run is None or locked_run.status not in (
                WorkflowRunStatus.PENDING.value,
                WorkflowRunStatus.RUNNING.value,
            ):
                return None
            task = (
                await session.execute(
                    _runnable_task_dispatch_snapshot_statement(
                        workflow_run_id,
                        task_run_id,
                        locked_run.workflow_version_id,
                    )
                )
            ).one_or_none()
            if task is None:
                return None
            attempt_number = await session.scalar(
                _next_attempt_number_statement(task_run_id)
            )
        except DBAPIError as error:
            raise TaskDispatchPersistenceUnavailable from error
        if not isinstance(attempt_number, int) or attempt_number <= 0:
            raise TaskDispatchPersistenceConflict
        return PreparedTaskDispatch(
            workflow_run_id=workflow_run_id,
            task_run_id=task_run_id,
            workflow_version_id=locked_run.workflow_version_id,
            step_identifier=task.step_identifier,
            task_type=task.task_type,
            task_parameters=deepcopy(task.parameters),
            attempt_number=attempt_number,
            deadline_at=task.deadline_at,
            execution_timeout_seconds=task.execution_timeout_seconds,
        )

    async def persist_dispatch(
        self,
        prepared: PreparedTaskDispatch,
        attempt: NewTaskAttempt,
        outbox: NewTaskDispatchOutbox,
    ) -> None:
        if (
            attempt.task_run_id != prepared.task_run_id
            or attempt.attempt_number != prepared.attempt_number
            or outbox.task_attempt_id != attempt.id
        ):
            raise TaskDispatchPersistenceConflict
        session = self._required_session()
        try:
            await session.execute(
                insert(task_attempts).values(
                    id=attempt.id,
                    task_run_id=attempt.task_run_id,
                    attempt_number=attempt.attempt_number,
                )
            )
            await session.execute(
                insert(task_dispatch_outbox).values(
                    id=outbox.id,
                    task_attempt_id=outbox.task_attempt_id,
                    route=outbox.route,
                    payload=outbox.payload,
                )
            )
            transitioned = (
                await session.execute(
                    _runnable_to_dispatched_statement(
                        prepared.workflow_run_id, prepared.task_run_id
                    )
                )
            ).one_or_none()
        except IntegrityError as error:
            raise TaskDispatchPersistenceConflict from error
        except DBAPIError as error:
            raise TaskDispatchPersistenceUnavailable from error
        if transitioned is None:
            raise TaskDispatchStateConflict
        try:
            await append_status_changed_execution_event(
                session,
                workflow_run_id=prepared.workflow_run_id,
                task_run_id=prepared.task_run_id,
                previous_status=TaskRunStatus.RUNNABLE,
                status=TaskRunStatus.DISPATCHED,
            )
        except WorkflowRunExecutionEventInvariantViolation as error:
            raise TaskDispatchPersistenceConflict from error
        except WorkflowRunExecutionEventPersistenceUnavailable as error:
            raise TaskDispatchPersistenceUnavailable from error

    async def commit(self) -> None:
        session = self._required_session()
        try:
            await session.commit()
        except IntegrityError as error:
            raise TaskDispatchPersistenceConflict from error
        except DBAPIError as error:
            raise TaskDispatchPersistenceUnavailable from error
        self._committed = True

    def _required_session(self) -> AsyncSession:
        if self._session is None:
            raise RuntimeError("task dispatch transaction is not active")
        return self._session


def _workflow_run_dispatch_lock_statement(
    workflow_run_id: UUID, task_run_id: UUID
) -> Select[Any]:
    return (
        select(
            workflow_runs.c.id,
            workflow_runs.c.workflow_version_id,
            workflow_runs.c.status,
        )
        .select_from(
            workflow_runs.join(
                task_runs,
                (task_runs.c.workflow_run_id == workflow_runs.c.id)
                & (
                    task_runs.c.workflow_version_id
                    == workflow_runs.c.workflow_version_id
                ),
            )
        )
        .where(
            workflow_runs.c.id == workflow_run_id,
            task_runs.c.id == task_run_id,
        )
        .with_for_update(of=workflow_runs)
    )


def _runnable_task_dispatch_snapshot_statement(
    workflow_run_id: UUID,
    task_run_id: UUID,
    workflow_version_id: UUID,
) -> Select[Any]:
    return (
        select(
            task_runs.c.step_identifier,
            task_runs.c.deadline_at,
            task_runs.c.execution_timeout_seconds,
            workflow_version_steps.c.task_type,
            workflow_version_steps.c.parameters,
        )
        .select_from(
            task_runs.join(
                workflow_version_steps,
                (
                    workflow_version_steps.c.workflow_version_id
                    == task_runs.c.workflow_version_id
                )
                & (
                    workflow_version_steps.c.step_identifier
                    == task_runs.c.step_identifier
                ),
            )
        )
        .where(
            task_runs.c.id == task_run_id,
            task_runs.c.workflow_run_id == workflow_run_id,
            task_runs.c.workflow_version_id == workflow_version_id,
            task_runs.c.status == TaskRunStatus.RUNNABLE.value,
        )
    )


def _next_attempt_number_statement(task_run_id: UUID) -> Select[Any]:
    return select(func.coalesce(func.max(task_attempts.c.attempt_number), 0) + 1).where(
        task_attempts.c.task_run_id == task_run_id
    )


def _runnable_to_dispatched_statement(
    workflow_run_id: UUID, task_run_id: UUID
) -> Update:
    return (
        update(task_runs)
        .where(
            task_runs.c.id == task_run_id,
            task_runs.c.workflow_run_id == workflow_run_id,
            task_runs.c.status == TaskRunStatus.RUNNABLE.value,
        )
        .values(
            status=TaskRunStatus.DISPATCHED.value,
            updated_at=func.current_timestamp(),
        )
        .returning(task_runs.c.id)
    )


def _unpublished_dispatch_page_statement(
    after: UnpublishedDispatchCursor | None,
    limit: int,
) -> Select[Any]:
    statement = (
        select(
            task_dispatch_outbox.c.id,
            task_dispatch_outbox.c.task_attempt_id,
            task_dispatch_outbox.c.route,
            task_dispatch_outbox.c.payload,
            task_dispatch_outbox.c.created_at,
        )
        .where(task_dispatch_outbox.c.published_at.is_(None))
        .order_by(
            task_dispatch_outbox.c.created_at,
            task_dispatch_outbox.c.id,
        )
        .limit(limit)
    )
    if after is not None:
        statement = statement.where(
            tuple_(task_dispatch_outbox.c.created_at, task_dispatch_outbox.c.id)
            > (after.created_at, after.dispatch_id)
        )
    return statement


def _record_publication_acknowledgement_statement(
    expected: StoredDispatch,
) -> Update:
    return (
        update(task_dispatch_outbox)
        .where(
            task_dispatch_outbox.c.id == expected.dispatch_id,
            task_dispatch_outbox.c.task_attempt_id == expected.task_attempt_id,
            task_dispatch_outbox.c.route == expected.route,
            task_dispatch_outbox.c.payload == expected.payload,
            task_dispatch_outbox.c.published_at.is_(None),
        )
        .values(published_at=func.current_timestamp())
        .returning(task_dispatch_outbox.c.published_at)
    )


def _dispatch_acknowledgement_snapshot_statement(
    dispatch_id: UUID,
) -> Select[Any]:
    return select(
        task_dispatch_outbox.c.task_attempt_id,
        task_dispatch_outbox.c.route,
        task_dispatch_outbox.c.payload,
        task_dispatch_outbox.c.published_at,
    ).where(task_dispatch_outbox.c.id == dispatch_id)
