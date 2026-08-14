"""Real PostgreSQL validation for the claim-expired result kind."""

from __future__ import annotations

import asyncio
import os

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL

from tests.integration.postgresql import migration_database_url, temporary_database

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_MIGRATION_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_MIGRATION_INTEGRATION=1 explicitly",
    ),
]

REVISION = "0016_claim_expired_result"
PREVIOUS_REVISION = "0015_task_retry_events"


async def constraint_definitions(database_url: URL) -> tuple[str, str]:
    connection = await asyncpg.connect(
        database_url.set(drivername="postgresql").render_as_string(hide_password=False)
    )
    try:
        result = await connection.fetchval(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE "
            "conname = 'ck_task_attempt_results_result_shape_valid'"
        )
        event = await connection.fetchval(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE "
            "conname = 'ck_task_result_events_result_shape_valid'"
        )
        assert isinstance(result, str) and isinstance(event, str)
        return result, event
    finally:
        await connection.close()


def test_recovery_migration_changes_only_attempt_result_vocabulary() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_recovery_migration"
    ) as database_url:
        config = Config("alembic.ini")
        rendered = database_url.render_as_string(hide_password=False)
        with migration_database_url(rendered):
            command.upgrade(config, PREVIOUS_REVISION)
            previous_result, previous_event = asyncio.run(
                constraint_definitions(database_url)
            )
            assert "claim_expired" not in previous_result
            assert "claim_expired" not in previous_event

            command.upgrade(config, REVISION)
            upgraded_result, upgraded_event = asyncio.run(
                constraint_definitions(database_url)
            )
            assert "claim_expired" in upgraded_result
            assert upgraded_event == previous_event

            command.downgrade(config, PREVIOUS_REVISION)
            downgraded_result, downgraded_event = asyncio.run(
                constraint_definitions(database_url)
            )
            assert "claim_expired" not in downgraded_result
            assert downgraded_event == previous_event
