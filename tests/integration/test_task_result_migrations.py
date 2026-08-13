"""Real PostgreSQL migration validation for task result history."""

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


async def assert_upgrade(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        assert await connection.fetchval(
            "SELECT EXISTS (SELECT FROM pg_enum e JOIN pg_type t ON t.oid = "
            "e.enumtypid WHERE t.typname = 'task_run_status' "
            "AND e.enumlabel = 'retry_pending')"
        )
        assert await connection.fetchval(
            "SELECT to_regclass('public.task_attempt_results') IS NOT NULL"
        )
        assert await connection.fetchval(
            "SELECT to_regclass('public.task_result_events') IS NOT NULL"
        )
        indexes = await connection.fetch(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'public' "
            "AND tablename = 'task_result_events' ORDER BY indexname"
        )
        assert [row["indexname"] for row in indexes] == [
            "ix_task_result_events_task_attempt_id_occurred_at_id",
            "pk_task_result_events",
        ]
        trigger_count = await connection.fetchval(
            "SELECT count(*) FROM pg_trigger WHERE tgrelid IN "
            "('task_attempt_results'::regclass, 'task_result_events'::regclass) "
            "AND NOT tgisinternal"
        )
        assert trigger_count == 4
    finally:
        await connection.close()


async def assert_downgrade(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT FROM pg_enum e JOIN pg_type t ON "
            "t.oid = e.enumtypid WHERE t.typname = 'task_run_status' "
            "AND e.enumlabel = 'retry_pending')"
        )
        assert not await connection.fetchval(
            "SELECT to_regclass('public.task_attempt_results') IS NOT NULL"
        )
    finally:
        await connection.close()


def test_result_migration_upgrade_downgrade_reupgrade() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_result_migration"
    ) as database_url:
        configuration = Config("alembic.ini")
        rendered = database_url.render_as_string(hide_password=False)
        with migration_database_url(rendered):
            command.upgrade(configuration, "head")
            asyncio.run(assert_upgrade(database_url))
            command.downgrade(configuration, "0012_task_execution_timeout")
            asyncio.run(assert_downgrade(database_url))
            command.upgrade(configuration, "head")
            asyncio.run(assert_upgrade(database_url))
