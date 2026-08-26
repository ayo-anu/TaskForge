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
    WORKFLOW_RUN_REPLAY_CREATED,
    SQLAlchemyWorkflowRunExecutionEventRepository,
    append_workflow_replay_created_execution_event,
)
from taskforge.runs.domain import (
    InvalidWorkflowRunExecutionEvent,
    NewWorkflowRunExecutionEvent,
    StoredWorkflowRunExecutionEvent,
    WorkflowReplayMode,
    WorkflowRunExecutionEventResumeState,
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


def test_replay_created_event_uses_bounded_redacted_structural_provenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[NewWorkflowRunExecutionEvent] = []
    target_id, source_id, principal_id, correlation_id = (
        uuid4(),
        uuid4(),
        uuid4(),
        uuid4(),
    )

    async def capture(
        session: AsyncSession, event: NewWorkflowRunExecutionEvent
    ) -> StoredWorkflowRunExecutionEvent:
        del session
        captured.append(event)
        return StoredWorkflowRunExecutionEvent(
            event.id,
            event.workflow_run_id,
            1,
            event.task_run_id,
            event.event_type,
            event.payload,
            datetime.now(UTC),
        )

    monkeypatch.setattr(
        "taskforge.persistence.execution_events.append_workflow_run_execution_event",
        capture,
    )
    scope = {"failed_step_identifiers": ["alpha"]}
    stored = asyncio.run(
        append_workflow_replay_created_execution_event(
            cast(AsyncSession, object()),
            workflow_run_id=target_id,
            source_workflow_run_id=source_id,
            mode=WorkflowReplayMode.FAILED_SUBGRAPH,
            requested_scope=scope,
            requested_by_principal_id=principal_id,
            correlation_id=correlation_id,
        )
    )
    scope["secret"] = "must-not-be-copied"

    assert stored.workflow_run_id == target_id
    assert stored.task_run_id is None
    assert stored.event_type == WORKFLOW_RUN_REPLAY_CREATED
    assert captured[0].payload == {
        "source_workflow_run_id": str(source_id),
        "replay_mode": "failed_subgraph",
        "requested_scope": {"failed_step_identifiers": ["alpha"]},
        "requested_by_principal_id": str(principal_id),
        "correlation_id": str(correlation_id),
    }
    assert "payload=<redacted>" in repr(captured[0])


def test_stored_execution_event_requires_cursor_and_server_time_shape() -> None:
    event_id, run_id, task_id = uuid4(), uuid4(), uuid4()
    occurred_at = datetime.now(UTC)
    event = StoredWorkflowRunExecutionEvent(
        event_id,
        run_id,
        1,
        task_id,
        "task_run.status_changed",
        {"previous_status": "claimed", "status": "running"},
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
                {"previous_status": "claimed", "status": "running"},
                occurred_at,
            )


def test_execution_event_payload_contract_is_closed_by_event_type() -> None:
    run_id, task_id = uuid4(), uuid4()
    valid = (
        (
            None,
            "workflow_run.created",
            {
                "workflow_definition_id": str(uuid4()),
                "workflow_version_id": str(uuid4()),
                "requested_by_principal_id": str(uuid4()),
                "creation_kind": "ordinary",
            },
        ),
        (
            None,
            "workflow_run.replay_created",
            {
                "source_workflow_run_id": str(uuid4()),
                "replay_mode": "full",
                "requested_scope": {},
                "requested_by_principal_id": str(uuid4()),
                "correlation_id": str(uuid4()),
            },
        ),
        (
            None,
            "workflow_run.redrive_created",
            {
                "dead_letter_item_id": str(uuid4()),
                "source_workflow_run_id": str(uuid4()),
                "source_task_run_id": str(uuid4()),
                "source_task_attempt_id": str(uuid4()),
                "requested_by_principal_id": str(uuid4()),
                "correlation_id": str(uuid4()),
            },
        ),
        (
            None,
            "workflow_run.status_changed",
            {"previous_status": "pending", "status": "running"},
        ),
        (
            task_id,
            "task_run.status_changed",
            {"previous_status": "runnable", "status": "dispatched"},
        ),
    )
    for target, event_type, payload in valid:
        assert (
            NewWorkflowRunExecutionEvent(
                uuid4(), run_id, target, event_type, payload
            ).event_type
            == event_type
        )

    invalid_payloads = (
        (None, "workflow_run.created", {"secret": "value"}),
        (None, "workflow_run.status_changed", {"status": "running"}),
        (
            task_id,
            "workflow_run.status_changed",
            {"previous_status": "pending", "status": "running"},
        ),
        (
            None,
            "task_run.status_changed",
            {"previous_status": "runnable", "status": "dispatched"},
        ),
        (
            None,
            "workflow_run.status_changed",
            {"previous_status": "running", "status": "runnable"},
        ),
        (
            task_id,
            "task_run.status_changed",
            {"previous_status": "running", "status": "cancelling"},
        ),
    )
    for target, event_type, payload in invalid_payloads:
        with pytest.raises(InvalidWorkflowRunExecutionEvent):
            NewWorkflowRunExecutionEvent(uuid4(), run_id, target, event_type, payload)


def test_maximum_valid_failed_subgraph_scope_is_preserved_exactly() -> None:
    identifiers = [f"step-{index:03d}-" + "x" * 119 for index in range(256)]
    payload: JSONMapping = {
        "source_workflow_run_id": str(uuid4()),
        "replay_mode": "failed_subgraph",
        "requested_scope": {"failed_step_identifiers": identifiers},
        "requested_by_principal_id": str(uuid4()),
        "correlation_id": str(uuid4()),
    }
    event = NewWorkflowRunExecutionEvent(
        uuid4(), uuid4(), None, "workflow_run.replay_created", payload
    )
    assert event.payload["requested_scope"] == payload["requested_scope"]
    with pytest.raises(ValueError):
        StoredWorkflowRunExecutionEvent(
            uuid4(),
            uuid4(),
            1,
            uuid4(),
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


def test_resume_state_validates_empty_nonempty_and_requested_cursor_shape() -> None:
    assert WorkflowRunExecutionEventResumeState(None, 0, None, None).latest_cursor == 0
    assert WorkflowRunExecutionEventResumeState(1, 3, 2, True) == (
        WorkflowRunExecutionEventResumeState(1, 3, 2, True)
    )

    for values in (
        (1, 0, None, None),
        (None, 1, None, None),
        (0, 1, None, None),
        (2, 1, None, None),
        (None, -1, None, None),
        (None, 0, 0, None),
        (None, 0, None, False),
    ):
        with pytest.raises(ValueError):
            WorkflowRunExecutionEventResumeState(*values)

    for contradictory in (
        (3, 8, 0, True),
        (3, 8, 2, True),
        (3, 8, 4, False),
        (3, 8, 9, True),
    ):
        with pytest.raises(ValueError):
            WorkflowRunExecutionEventResumeState(*contradictory)

    assert WorkflowRunExecutionEventResumeState(3, 8, 2, False).requested_cursor == 2
    assert WorkflowRunExecutionEventResumeState(None, 0, 0, False).latest_cursor == 0


def test_resume_cursor_inspection_rejects_invalid_values_before_database_use() -> None:
    repository = SQLAlchemyWorkflowRunExecutionEventRepository(
        cast(async_sessionmaker[AsyncSession], None)
    )

    for cursor in (-1, True, cast(Any, "1")):
        with pytest.raises(ValueError):
            asyncio.run(repository.inspect_resume_cursor(uuid4(), cursor))
