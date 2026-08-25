"""Ordered, append-only workflow-run execution-event persistence."""

from __future__ import annotations

from enum import StrEnum
from typing import Any, cast
from uuid import UUID, uuid4

from sqlalchemy import Boolean, func, insert, literal, select
from sqlalchemy.engine import Row
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.runs.domain import (
    InvalidWorkflowRunExecutionEvent,
    NewWorkflowRunExecutionEvent,
    StoredWorkflowRunExecutionEvent,
    WorkflowReplayMode,
    WorkflowRunExecutionEventResumeState,
)
from taskforge.runs.persistence_ports import (
    WorkflowRunExecutionEventInvariantViolation,
    WorkflowRunExecutionEventPersistenceUnavailable,
)
from taskforge.runs.schema import workflow_run_execution_events, workflow_runs
from taskforge.workflows.task_types import JSONMapping

MAX_EXECUTION_EVENT_PAGE_SIZE = 1000
WORKFLOW_RUN_STATUS_CHANGED = "workflow_run.status_changed"
TASK_RUN_STATUS_CHANGED = "task_run.status_changed"
WORKFLOW_RUN_REPLAY_CREATED = "workflow_run.replay_created"
WORKFLOW_RUN_CREATED = "workflow_run.created"
WORKFLOW_RUN_REDRIVE_CREATED = "workflow_run.redrive_created"


async def append_workflow_created_execution_event(
    session: AsyncSession,
    *,
    workflow_run_id: UUID,
    workflow_definition_id: UUID,
    workflow_version_id: UUID,
    requested_by_principal_id: UUID,
    correlation_id: UUID | None,
) -> StoredWorkflowRunExecutionEvent:
    payload: dict[str, Any] = {
        "workflow_definition_id": str(workflow_definition_id),
        "workflow_version_id": str(workflow_version_id),
        "requested_by_principal_id": str(requested_by_principal_id),
        "creation_kind": "ordinary",
    }
    if correlation_id is not None:
        payload["correlation_id"] = str(correlation_id)
    return await append_workflow_run_execution_event(
        session,
        NewWorkflowRunExecutionEvent(
            uuid4(),
            workflow_run_id,
            None,
            WORKFLOW_RUN_CREATED,
            cast(JSONMapping, payload),
        ),
    )


async def append_workflow_redrive_created_execution_event(
    session: AsyncSession,
    *,
    workflow_run_id: UUID,
    dead_letter_item_id: UUID,
    source_workflow_run_id: UUID,
    source_task_run_id: UUID,
    source_task_attempt_id: UUID,
    requested_by_principal_id: UUID,
    correlation_id: UUID,
) -> StoredWorkflowRunExecutionEvent:
    return await append_workflow_run_execution_event(
        session,
        NewWorkflowRunExecutionEvent(
            uuid4(),
            workflow_run_id,
            None,
            WORKFLOW_RUN_REDRIVE_CREATED,
            {
                "dead_letter_item_id": str(dead_letter_item_id),
                "source_workflow_run_id": str(source_workflow_run_id),
                "source_task_run_id": str(source_task_run_id),
                "source_task_attempt_id": str(source_task_attempt_id),
                "requested_by_principal_id": str(requested_by_principal_id),
                "correlation_id": str(correlation_id),
            },
        ),
    )


async def append_workflow_replay_created_execution_event(
    session: AsyncSession,
    *,
    workflow_run_id: UUID,
    source_workflow_run_id: UUID,
    mode: WorkflowReplayMode,
    requested_scope: dict[str, object],
    requested_by_principal_id: UUID,
    correlation_id: UUID,
) -> StoredWorkflowRunExecutionEvent:
    """Append immutable replay provenance through the caller's transaction."""
    try:
        event = NewWorkflowRunExecutionEvent(
            uuid4(),
            workflow_run_id,
            None,
            WORKFLOW_RUN_REPLAY_CREATED,
            {
                "source_workflow_run_id": str(source_workflow_run_id),
                "replay_mode": mode.value,
                "requested_scope": cast(JSONMapping, requested_scope),
                "requested_by_principal_id": str(requested_by_principal_id),
                "correlation_id": str(correlation_id),
            },
        )
    except InvalidWorkflowRunExecutionEvent as error:
        raise WorkflowRunExecutionEventInvariantViolation from error
    return await append_workflow_run_execution_event(session, event)


async def append_status_changed_execution_event(
    session: AsyncSession,
    *,
    workflow_run_id: UUID,
    task_run_id: UUID | None,
    previous_status: StrEnum,
    status: StrEnum,
) -> StoredWorkflowRunExecutionEvent:
    """Append one controlled workflow/task status transition event."""
    event_type = (
        WORKFLOW_RUN_STATUS_CHANGED if task_run_id is None else TASK_RUN_STATUS_CHANGED
    )
    try:
        event = NewWorkflowRunExecutionEvent(
            uuid4(),
            workflow_run_id,
            task_run_id,
            event_type,
            {"previous_status": previous_status.value, "status": status.value},
        )
    except InvalidWorkflowRunExecutionEvent as error:
        raise WorkflowRunExecutionEventInvariantViolation from error
    return await append_workflow_run_execution_event(session, event)


async def append_workflow_run_execution_event(
    session: AsyncSession,
    event: NewWorkflowRunExecutionEvent,
) -> StoredWorkflowRunExecutionEvent:
    """Append through the caller's transaction without committing it."""
    try:
        row = (
            await session.execute(
                insert(workflow_run_execution_events)
                .values(
                    id=event.id,
                    workflow_run_id=event.workflow_run_id,
                    task_run_id=event.task_run_id,
                    event_type=event.event_type,
                    payload=event.payload,
                )
                .returning(workflow_run_execution_events)
            )
        ).one()
    except IntegrityError as error:
        raise WorkflowRunExecutionEventInvariantViolation from error
    except DBAPIError as error:
        raise WorkflowRunExecutionEventPersistenceUnavailable from error
    return _stored_event(row)


class SQLAlchemyWorkflowRunExecutionEventRepository:
    """Read committed execution events in workflow-run cursor order."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def inspect_resume_cursor(
        self,
        workflow_run_id: UUID,
        requested_cursor: int | None,
    ) -> WorkflowRunExecutionEventResumeState:
        if requested_cursor is not None and (
            isinstance(requested_cursor, bool)
            or not isinstance(requested_cursor, int)
            or requested_cursor < 0
        ):
            raise ValueError("requested cursor must be a non-negative integer")
        earliest = (
            select(func.min(workflow_run_execution_events.c.cursor))
            .where(workflow_run_execution_events.c.workflow_run_id == workflow_run_id)
            .scalar_subquery()
        )
        requested_exists = (
            literal(None, Boolean)
            if requested_cursor is None
            else select(workflow_run_execution_events.c.id)
            .where(
                workflow_run_execution_events.c.workflow_run_id == workflow_run_id,
                workflow_run_execution_events.c.cursor == requested_cursor,
            )
            .exists()
        ).label("requested_cursor_exists")
        try:
            async with self._sessions() as session, session.begin():
                row = (
                    await session.execute(
                        select(
                            earliest.label("earliest_retained_cursor"),
                            workflow_runs.c.last_execution_event_cursor.label(
                                "latest_cursor"
                            ),
                            requested_exists,
                        ).where(workflow_runs.c.id == workflow_run_id)
                    )
                ).one_or_none()
        except DBAPIError as error:
            raise WorkflowRunExecutionEventPersistenceUnavailable from error
        if row is None:
            raise WorkflowRunExecutionEventInvariantViolation
        try:
            return WorkflowRunExecutionEventResumeState(
                row.earliest_retained_cursor,
                row.latest_cursor,
                requested_cursor,
                row.requested_cursor_exists,
            )
        except (TypeError, ValueError) as error:
            raise WorkflowRunExecutionEventInvariantViolation from error

    async def list_after(
        self,
        workflow_run_id: UUID,
        after_cursor: int,
        limit: int,
    ) -> tuple[StoredWorkflowRunExecutionEvent, ...]:
        if (
            isinstance(after_cursor, bool)
            or not isinstance(after_cursor, int)
            or after_cursor < 0
        ):
            raise ValueError("after cursor must be a non-negative integer")
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= MAX_EXECUTION_EVENT_PAGE_SIZE
        ):
            raise ValueError("execution event page limit is out of range")
        try:
            async with self._sessions() as session, session.begin():
                rows = (
                    await session.execute(
                        select(workflow_run_execution_events)
                        .where(
                            workflow_run_execution_events.c.workflow_run_id
                            == workflow_run_id,
                            workflow_run_execution_events.c.cursor > after_cursor,
                        )
                        .order_by(workflow_run_execution_events.c.cursor.asc())
                        .limit(limit)
                    )
                ).all()
        except DBAPIError as error:
            raise WorkflowRunExecutionEventPersistenceUnavailable from error
        try:
            return tuple(_stored_event(row) for row in rows)
        except (TypeError, ValueError) as error:
            raise WorkflowRunExecutionEventInvariantViolation from error


def _stored_event(row: Row[Any]) -> StoredWorkflowRunExecutionEvent:
    return StoredWorkflowRunExecutionEvent(
        id=row.id,
        workflow_run_id=row.workflow_run_id,
        cursor=row.cursor,
        task_run_id=row.task_run_id,
        event_type=row.event_type,
        payload=row.payload,
        occurred_at=row.occurred_at,
    )
