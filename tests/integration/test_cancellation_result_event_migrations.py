"""Real PostgreSQL lifecycle checks for recovered cancellation result events."""

from __future__ import annotations

import asyncio
import os
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
from tests.integration.test_recovery_transition import recoverable_candidate

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_MIGRATION_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_MIGRATION_INTEGRATION=1 explicitly",
    ),
]

REVISION = "0021_recovered_cancellation"
PREVIOUS_REVISION = "0020_run_cancellation"


async def exercise_constraint(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        candidate = await recoverable_candidate(connection)
        dispatch_id = await connection.fetchval(
            "SELECT id FROM task_dispatch_outbox WHERE task_attempt_id = $1",
            candidate.task_attempt_id,
        )
        await connection.execute(
            "INSERT INTO task_result_events (id, task_attempt_id, claim_generation, "
            "worker_session_id, dispatch_id, event_type, result_kind, failure_kind, "
            "result_fingerprint) VALUES ($1, $2, $3, $4, $5, "
            "'result_recovered', 'cancellation', NULL, $6)",
            uuid4(),
            candidate.task_attempt_id,
            candidate.generation,
            candidate.worker_session_id,
            dispatch_id,
            "c" * 64,
        )
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                "INSERT INTO task_result_events (id, task_attempt_id, "
                "claim_generation, worker_session_id, dispatch_id, event_type, "
                "result_kind, failure_kind, result_fingerprint) VALUES "
                "($1, $2, $3, $4, $5, 'result_recovered', 'cancellation', "
                "'claim_expired', $6)",
                uuid4(),
                candidate.task_attempt_id,
                candidate.generation + 1,
                candidate.worker_session_id,
                dispatch_id,
                "d" * 64,
            )
    finally:
        await connection.close()


async def assert_recovered_cancellation_removed(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT FROM task_result_events WHERE "
            "event_type = 'result_recovered' AND result_kind = 'cancellation')"
        )
        shape = await connection.fetchval(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE "
            "conname = 'ck_task_result_events_result_shape_valid'"
        )
        assert "claim_expired" in shape
        assert shape.count("result_recovered") == 1
        assert "retryable_failure" in shape
    finally:
        await connection.close()


def test_upgrade_downgrade_reupgrade_recovered_cancellation_constraint() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_recovery_event_mig"
    ) as database_url:
        config = Config("alembic.ini")
        with migration_database_url(database_url.render_as_string(hide_password=False)):
            command.upgrade(config, REVISION)
            asyncio.run(exercise_constraint(database_url))
            command.downgrade(config, PREVIOUS_REVISION)
            asyncio.run(assert_recovered_cancellation_removed(database_url))
            command.upgrade(config, REVISION)
            asyncio.run(exercise_constraint(database_url))
