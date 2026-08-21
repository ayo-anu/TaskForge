"""Ordered, append-only workflow-run execution-event persistence."""

from __future__ import annotations

from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import insert, select
from sqlalchemy.engine import Row
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.runs.domain import (
    InvalidWorkflowRunExecutionEvent,
    NewWorkflowRunExecutionEvent,
    StoredWorkflowRunExecutionEvent,
)
from taskforge.runs.persistence_ports import (
    WorkflowRunExecutionEventInvariantViolation,
    WorkflowRunExecutionEventPersistenceUnavailable,
)
from taskforge.runs.schema import workflow_run_execution_events

MAX_EXECUTION_EVENT_PAGE_SIZE = 1000
WORKFLOW_RUN_STATUS_CHANGED = "workflow_run.status_changed"
TASK_RUN_STATUS_CHANGED = "task_run.status_changed"


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
