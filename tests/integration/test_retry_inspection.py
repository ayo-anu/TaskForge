"""Real PostgreSQL retry projection, history, and ownership tests."""

from __future__ import annotations

import asyncio
import os
from datetime import timedelta
from uuid import uuid4

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from taskforge.persistence.database import build_session_factory
from taskforge.persistence.retries import SQLAlchemyRetryTransitionRepository
from taskforge.persistence.runs import SQLAlchemyWorkflowRunRepository
from taskforge.retries.domain import RetryEventType
from taskforge.retries.service import RetryTransitionService
from taskforge.worker.results import TaskExecutionFailureKind
from tests.integration.postgresql import (
    asyncpg_dsn,
    migration_database_url,
    temporary_database,
)
from tests.integration.test_retry_transition import add_retry_pending_task, retry_policy

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_RETRY_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_RETRY_INTEGRATION=1 explicitly",
    ),
]


async def exercise_inspection(database_url: URL) -> None:
    setup = await asyncpg.connect(asyncpg_dsn(database_url))
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
    )
    sessions = build_session_factory(engine)
    runs = SQLAlchemyWorkflowRunRepository(sessions)
    transitions = RetryTransitionService(SQLAlchemyRetryTransitionRepository(sessions))
    try:
        facts = await add_retry_pending_task(
            setup,
            workflow_policy={"retry_policy": retry_policy(maximum_attempts=4)},
            failure_kind="handler_exception",
        )
        owner_id = await setup.fetchval(
            "SELECT requested_by_principal_id FROM workflow_runs WHERE id = $1",
            facts.workflow_run_id,
        )
        scheduled = await transitions.transition_retry(facts.task_run_id)
        task = await runs.get_task_run(facts.task_run_id, owner_id)
        assert task is not None
        assert task.attempt_count == 2
        assert task.retry_attempt_count == 1
        assert task.maximum_attempts == 4
        assert task.retry_eligible_at == scheduled.next_eligible_at
        assert task.latest_failure_kind is TaskExecutionFailureKind.HANDLER_EXCEPTION

        scheduled_event = await setup.fetchrow(
            "SELECT id, occurred_at FROM task_retry_events WHERE task_run_id = $1",
            facts.task_run_id,
        )
        await setup.execute(
            "INSERT INTO task_retry_events (id, task_run_id, event_type, "
            "retry_attempt_number, occurred_at) VALUES "
            "($1, $2, 'retry_dispatched', 2, $3)",
            uuid4(),
            facts.task_run_id,
            scheduled_event["occurred_at"] + timedelta(microseconds=1),
        )
        first = await runs.list_retry_events(
            facts.task_run_id, owner_id, limit=1, cursor=None
        )
        assert first is not None
        assert first.items[0].event_type is RetryEventType.RETRY_DISPATCHED
        assert first.items[0].failed_attempt_id is None
        assert first.items[0].retry_attempt_id == scheduled.scheduled_attempt_id
        assert first.next_cursor is not None
        second = await runs.list_retry_events(
            facts.task_run_id,
            owner_id,
            limit=1,
            cursor=first.next_cursor,
        )
        assert second is not None
        assert second.items[0].event_type is RetryEventType.RETRY_SCHEDULED
        assert second.items[0].failed_attempt_id == facts.failed_attempt_id
        assert (
            second.items[0].failure_kind is TaskExecutionFailureKind.HANDLER_EXCEPTION
        )
        assert second.next_cursor is None
        assert (
            await runs.list_retry_events(
                facts.task_run_id, uuid4(), limit=50, cursor=None
            )
            is None
        )

        empty = await add_retry_pending_task(setup)
        empty_owner = await setup.fetchval(
            "SELECT requested_by_principal_id FROM workflow_runs WHERE id = $1",
            empty.workflow_run_id,
        )
        old_page = await runs.list_retry_events(
            empty.task_run_id, empty_owner, limit=50, cursor=None
        )
        assert old_page is not None and old_page.items == ()
    finally:
        await setup.close()
        await engine.dispose()


def test_real_postgresql_retry_inspection() -> None:
    with temporary_database(
        "TASKFORGE_RETRY_TEST_DATABASE_URL", "taskforge_retry_inspection"
    ) as database_url:
        configuration = Config("alembic.ini")
        with migration_database_url(database_url.render_as_string(hide_password=False)):
            command.upgrade(configuration, "head")
        asyncio.run(exercise_inspection(database_url))
