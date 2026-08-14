"""Real PostgreSQL validation for immutable task retry events."""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime
from uuid import uuid4

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL

from tests.integration.postgresql import (
    asyncpg_dsn,
    migration_database_url,
    temporary_database,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_MIGRATION_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_MIGRATION_INTEGRATION=1 explicitly",
    ),
]

REVISION = "0015_task_retry_events"
PREVIOUS_REVISION = "0014_retry_eligibility"


async def exercise_schema(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        columns = await connection.fetch(
            "SELECT column_name, data_type, is_nullable, column_default "
            "FROM information_schema.columns WHERE table_name = 'task_retry_events' "
            "ORDER BY ordinal_position"
        )
        assert [row["column_name"] for row in columns] == [
            "id",
            "task_run_id",
            "event_type",
            "failed_attempt_number",
            "retry_attempt_number",
            "next_eligible_at",
            "decision_reason",
            "occurred_at",
        ]
        assert columns[-1]["data_type"] == "timestamp with time zone"
        assert columns[-1]["column_default"] == "statement_timestamp()"
        constraints = {
            row["conname"]
            for row in await connection.fetch(
                "SELECT conname FROM pg_constraint WHERE "
                "conrelid = 'task_retry_events'::regclass"
            )
        }
        assert constraints == {
            "pk_task_retry_events",
            "ck_task_retry_events_event_type_valid",
            "ck_task_retry_events_event_shape_valid",
            "fk_task_retry_events_task_run_id_task_runs",
            "fk_task_retry_events_failed_attempt",
            "fk_task_retry_events_retry_attempt",
        }
        indexes = {
            row["indexname"]
            for row in await connection.fetch(
                "SELECT indexname FROM pg_indexes WHERE tablename = 'task_retry_events'"
            )
        }
        assert indexes == {
            "pk_task_retry_events",
            "uq_task_retry_events_scheduled_attempt",
            "uq_task_retry_events_dispatched_attempt",
            "uq_task_retry_events_not_scheduled_attempt",
            "ix_task_retry_events_task_run_id_occurred_at_id",
        }

        principal, workflow, version, run, task, other_task = (
            uuid4() for _ in range(6)
        )
        await connection.execute(
            "INSERT INTO api_principals (id, name) VALUES ($1, $2)",
            principal,
            f"retry-events-{uuid4().hex}",
        )
        await connection.execute(
            "INSERT INTO workflow_definitions (id, owner_principal_id, name) "
            "VALUES ($1, $2, 'events')",
            workflow,
            principal,
        )
        await connection.execute(
            "INSERT INTO workflow_versions "
            "(id, workflow_definition_id, version_number, name) "
            "VALUES ($1, $2, 1, 'events')",
            version,
            workflow,
        )
        await connection.execute(
            "INSERT INTO workflow_version_steps "
            "(workflow_version_id, step_identifier, task_type, parameters) VALUES "
            "($1, 'one', 'test.task', '{}'::jsonb), "
            "($1, 'two', 'test.task', '{}'::jsonb)",
            version,
        )
        await connection.execute(
            "INSERT INTO workflow_runs (id, workflow_definition_id, "
            "workflow_version_id, requested_by_principal_id, status) "
            "VALUES ($1, $2, $3, $4, 'running')",
            run,
            workflow,
            version,
            principal,
        )
        await connection.execute(
            "INSERT INTO task_runs (id, workflow_run_id, workflow_version_id, "
            "step_identifier, status) VALUES ($1, $3, $4, 'one', 'failed'), "
            "($2, $3, $4, 'two', 'failed')",
            task,
            other_task,
            run,
            version,
        )
        await connection.execute(
            "INSERT INTO task_attempts (id, task_run_id, attempt_number, "
            "next_eligible_at) VALUES ($1, $3, 1, NULL), ($2, $3, 2, $5), "
            "($4, $6, 1, NULL)",
            uuid4(),
            uuid4(),
            task,
            uuid4(),
            datetime(2030, 1, 1, tzinfo=UTC),
            other_task,
        )
        scheduled_id = uuid4()
        await connection.execute(
            "INSERT INTO task_retry_events (id, task_run_id, event_type, "
            "failed_attempt_number, retry_attempt_number, next_eligible_at) "
            "VALUES ($1, $2, 'retry_scheduled', 1, 2, $3)",
            scheduled_id,
            task,
            datetime(2030, 1, 1, tzinfo=UTC),
        )
        await connection.execute(
            "INSERT INTO task_retry_events (id, task_run_id, event_type, "
            "retry_attempt_number) VALUES ($1, $2, 'retry_dispatched', 2)",
            uuid4(),
            task,
        )
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                "INSERT INTO task_retry_events (id, task_run_id, event_type, "
                "failed_attempt_number, retry_attempt_number, next_eligible_at) "
                "VALUES ($1, $2, 'retry_scheduled', 1, 3, $3)",
                uuid4(),
                task,
                datetime(2030, 1, 1, tzinfo=UTC),
            )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await connection.execute(
                "INSERT INTO task_retry_events (id, task_run_id, event_type, "
                "failed_attempt_number, decision_reason) "
                "VALUES ($1, $2, 'retry_not_scheduled', 2, 'exhausted')",
                uuid4(),
                other_task,
            )
        with pytest.raises(asyncpg.UniqueViolationError):
            await connection.execute(
                "INSERT INTO task_retry_events (id, task_run_id, event_type, "
                "retry_attempt_number) VALUES ($1, $2, 'retry_dispatched', 2)",
                uuid4(),
                task,
            )
        occurred_at = await connection.fetchval(
            "SELECT occurred_at FROM task_retry_events WHERE id = $1", scheduled_id
        )
        assert occurred_at.tzinfo is not None
        for statement in (
            "UPDATE task_retry_events SET event_type = event_type WHERE id = $1",
            "DELETE FROM task_retry_events WHERE id = $1",
        ):
            with pytest.raises(asyncpg.PostgresError) as raised:
                await connection.execute(statement, scheduled_id)
            assert raised.value.sqlstate == "TF005"
        with pytest.raises(asyncpg.PostgresError) as raised:
            await connection.execute("TRUNCATE task_retry_events")
        assert raised.value.sqlstate == "TF005"
    finally:
        await connection.close()


async def assert_downgraded(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        assert not await connection.fetchval(
            "SELECT to_regclass('public.task_retry_events') IS NOT NULL"
        )
    finally:
        await connection.close()


def test_retry_event_upgrade_downgrade_reupgrade() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_retry_event_mig"
    ) as database_url:
        configuration = Config("alembic.ini")
        rendered = database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        with migration_database_url(rendered):
            command.upgrade(configuration, PREVIOUS_REVISION)
            command.upgrade(configuration, REVISION)
            asyncio.run(exercise_schema(database_url))
            command.downgrade(configuration, PREVIOUS_REVISION)
            asyncio.run(assert_downgraded(database_url))
            command.upgrade(configuration, REVISION)
            asyncio.run(exercise_schema(database_url))
