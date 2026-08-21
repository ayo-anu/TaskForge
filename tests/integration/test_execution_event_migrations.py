"""Real PostgreSQL migration checks for ordered execution events."""

from __future__ import annotations

import asyncio
import os

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

REVISION = "0022_run_execution_events"
WAKEUP_REVISION = "0023_execution_event_wakeups"
PREVIOUS_REVISION = "0021_recovered_cancellation"


async def assert_absent(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        assert await connection.fetchval(
            "SELECT to_regclass('public.workflow_run_execution_events') IS NULL"
        )
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT FROM information_schema.columns WHERE "
            "table_name = 'workflow_runs' AND "
            "column_name = 'last_execution_event_cursor')"
        )
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT FROM pg_constraint WHERE "
            "conname = 'uq_task_runs_workflow_run_id_id')"
        )
    finally:
        await connection.close()


async def assert_catalog(database_url: URL, *, wakeup: bool) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        columns = await connection.fetch(
            "SELECT column_name, data_type, is_nullable, column_default "
            "FROM information_schema.columns WHERE table_schema = 'public' "
            "AND table_name = 'workflow_run_execution_events' "
            "ORDER BY ordinal_position"
        )
        assert [row["column_name"] for row in columns] == [
            "id",
            "workflow_run_id",
            "cursor",
            "task_run_id",
            "event_type",
            "payload",
            "occurred_at",
        ]
        assert columns[2]["column_default"] is None
        assert columns[3]["is_nullable"] == "YES"
        assert columns[5]["column_default"] == "'{}'::jsonb"
        assert columns[6]["column_default"] == "statement_timestamp()"

        constraints = await connection.fetch(
            "SELECT conname, contype::text, pg_get_constraintdef(oid) AS definition, "
            "confupdtype::text, confdeltype::text FROM pg_constraint WHERE "
            "conrelid = 'workflow_run_execution_events'::regclass"
        )
        assert {row["conname"] for row in constraints} == {
            "pk_workflow_run_execution_events",
            "uq_workflow_run_execution_events_run_cursor",
            "ck_workflow_run_execution_events_cursor_positive",
            "ck_workflow_run_execution_events_event_type_valid",
            "ck_workflow_run_execution_events_payload_object",
            "fk_workflow_run_execution_events_run",
            "fk_workflow_run_execution_events_task_ownership",
        }
        ownership = next(
            row for row in constraints if row["conname"].endswith("task_ownership")
        )
        assert ownership["confupdtype"] == ownership["confdeltype"] == "r"
        assert "FOREIGN KEY (workflow_run_id, task_run_id)" in ownership["definition"]
        assert "REFERENCES task_runs(workflow_run_id, id)" in ownership["definition"]

        indexes = await connection.fetch(
            "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'public' "
            "AND tablename = 'workflow_run_execution_events'"
        )
        assert {row["indexname"] for row in indexes} == {
            "pk_workflow_run_execution_events",
            "uq_workflow_run_execution_events_run_cursor",
        }
        task_index = await connection.fetchval(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = 'public' AND "
            "tablename = 'task_runs' AND "
            "indexname = 'uq_task_runs_workflow_run_id_id'"
        )
        assert task_index is not None
        assert "UNIQUE" in task_index
        assert "(workflow_run_id, id)" in task_index

        triggers = await connection.fetch(
            "SELECT tgname FROM pg_trigger WHERE tgrelid = "
            "'workflow_run_execution_events'::regclass AND NOT tgisinternal"
        )
        expected_triggers = {
            "trg_workflow_run_execution_events_allocate_cursor",
            "trg_workflow_run_execution_events_reject_mutation",
            "trg_workflow_run_execution_events_reject_truncate",
        }
        if wakeup:
            expected_triggers.add("trg_workflow_run_execution_events_publish_wakeup")
        assert {row["tgname"] for row in triggers} == expected_triggers
        counter = await connection.fetchrow(
            "SELECT column_default, is_nullable FROM information_schema.columns "
            "WHERE table_name = 'workflow_runs' AND "
            "column_name = 'last_execution_event_cursor'"
        )
        assert counter is not None
        assert tuple(counter.values()) == ("0", "NO")
    finally:
        await connection.close()


def test_execution_event_migration_is_exact_and_reversible() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_execution_event_mig"
    ) as database_url:
        config = Config("alembic.ini")
        rendered = database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        with migration_database_url(rendered):
            command.upgrade(config, PREVIOUS_REVISION)
            asyncio.run(assert_absent(database_url))
            command.upgrade(config, REVISION)
            asyncio.run(assert_catalog(database_url, wakeup=False))
            command.upgrade(config, WAKEUP_REVISION)
            asyncio.run(assert_catalog(database_url, wakeup=True))
            command.downgrade(config, REVISION)
            asyncio.run(assert_catalog(database_url, wakeup=False))
            command.downgrade(config, PREVIOUS_REVISION)
            asyncio.run(assert_absent(database_url))
            command.upgrade(config, "head")
            asyncio.run(assert_catalog(database_url, wakeup=True))
