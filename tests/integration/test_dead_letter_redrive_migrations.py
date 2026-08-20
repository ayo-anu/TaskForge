"""Migration lifecycle checks for dead-letter redrive lineage."""

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


async def assert_at_0019(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        columns = {
            row["column_name"]: row["is_nullable"]
            for row in await connection.fetch(
                "SELECT column_name, is_nullable FROM information_schema.columns "
                "WHERE table_name = 'dead_letter_redrive_requests'"
            )
        }
        assert columns["target_workflow_run_id"] == "NO"
        assert columns["reason"] == "YES"
        constraints = {
            row["conname"]
            for row in await connection.fetch(
                "SELECT conname FROM pg_constraint WHERE "
                "conrelid = 'dead_letter_redrive_requests'::regclass"
            )
        }
        assert {
            "fk_dead_letter_redrive_requests_target_run",
            "uq_dead_letter_redrive_requests_item",
            "uq_dead_letter_redrive_requests_target_run",
            "uq_dead_letter_redrive_requests_item_requester_key",
            "ck_dead_letter_redrive_requests_reason_valid",
        } <= constraints
    finally:
        await connection.close()


async def assert_at_0018(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        columns = {
            row["column_name"]
            for row in await connection.fetch(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'dead_letter_redrive_requests'"
            )
        }
        assert "target_workflow_run_id" not in columns
        assert "reason" not in columns
        assert await connection.fetchval(
            "SELECT to_regclass('public.dead_letter_redrive_requests') IS NOT NULL"
        )
    finally:
        await connection.close()


def test_redrive_lineage_upgrade_downgrade_reupgrade() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_dead_letter_redrive"
    ) as database_url:
        config = Config("alembic.ini")
        rendered = database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        with migration_database_url(rendered):
            command.upgrade(config, "0018_dead_letter_persistence")
            command.upgrade(config, "0019_dead_letter_redrive")
            asyncio.run(assert_at_0019(database_url))
            command.downgrade(config, "0018_dead_letter_persistence")
            asyncio.run(assert_at_0018(database_url))
            command.upgrade(config, "0019_dead_letter_redrive")
            asyncio.run(assert_at_0019(database_url))
