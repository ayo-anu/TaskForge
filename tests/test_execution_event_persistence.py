"""Focused execution-event domain and adapter boundary tests."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any, cast
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.persistence.execution_events import (
    MAX_EXECUTION_EVENT_PAGE_SIZE,
    SQLAlchemyWorkflowRunExecutionEventRepository,
)
from taskforge.runs.domain import (
    InvalidWorkflowRunExecutionEvent,
    NewWorkflowRunExecutionEvent,
    StoredWorkflowRunExecutionEvent,
)
from taskforge.workflows.task_types import JSONMapping


def test_new_execution_event_validates_and_snapshots_payload() -> None:
    payload: JSONMapping = {"previous_status": "pending", "status": "running"}
    event = NewWorkflowRunExecutionEvent(
        uuid4(), uuid4(), None, "workflow_run.status_changed", payload
    )
    payload["status"] = "failed"

    assert event.payload == {"previous_status": "pending", "status": "running"}
    assert "payload=<redacted>" in repr(event)

    for event_type in ("", "   ", "x" * 129):
        with pytest.raises(InvalidWorkflowRunExecutionEvent):
            NewWorkflowRunExecutionEvent(uuid4(), uuid4(), None, event_type, {})
    with pytest.raises(InvalidWorkflowRunExecutionEvent):
        NewWorkflowRunExecutionEvent(
            uuid4(), uuid4(), None, "task_run.status_changed", cast(Any, [])
        )


def test_stored_execution_event_requires_cursor_and_server_time_shape() -> None:
    event_id, run_id, task_id = uuid4(), uuid4(), uuid4()
    occurred_at = datetime.now(UTC)
    event = StoredWorkflowRunExecutionEvent(
        event_id,
        run_id,
        1,
        task_id,
        "task_run.status_changed",
        {"status": "running"},
        occurred_at,
    )
    assert event.cursor == 1
    assert event.occurred_at.tzinfo is UTC

    for invalid_cursor in (0, -1, True):
        with pytest.raises(ValueError):
            StoredWorkflowRunExecutionEvent(
                event_id,
                run_id,
                invalid_cursor,
                task_id,
                "task_run.status_changed",
                {"status": "running"},
                occurred_at,
            )
    with pytest.raises(ValueError):
        StoredWorkflowRunExecutionEvent(
            event_id,
            run_id,
            1,
            task_id,
            "task_run.status_changed",
            {"status": "running"},
            datetime.now(),
        )


def test_list_after_rejects_invalid_cursor_and_page_limit_before_database_use() -> None:
    repository = SQLAlchemyWorkflowRunExecutionEventRepository(
        cast(async_sessionmaker[AsyncSession], None)
    )

    for cursor in (-1, True, cast(Any, "1")):
        with pytest.raises(ValueError):
            asyncio.run(repository.list_after(uuid4(), cursor, 1))
    for limit in (0, MAX_EXECUTION_EVENT_PAGE_SIZE + 1, True, cast(Any, "1")):
        with pytest.raises(ValueError):
            asyncio.run(repository.list_after(uuid4(), 0, limit))
