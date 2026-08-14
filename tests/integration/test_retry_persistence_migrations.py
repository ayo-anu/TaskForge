"""Real PostgreSQL validation for retry-policy and eligibility persistence."""

from __future__ import annotations

import asyncio
import json
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

REVISION = "0014_retry_eligibility"
PREVIOUS_REVISION = "0013_task_attempt_results"
INDEX_NAME = "ix_task_attempts_scheduled_next_eligible_at_id"
POLICY_TABLES = (
    "workflow_definitions",
    "workflow_draft_steps",
    "workflow_versions",
    "workflow_version_steps",
)


async def seed_previous_revision(database_url: URL) -> tuple[object, ...]:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        principal_id, workflow_id, version_id, task_run_id, attempt_id = (
            uuid4(),
            uuid4(),
            uuid4(),
            uuid4(),
            uuid4(),
        )
        await connection.execute(
            "INSERT INTO api_principals (id, name) VALUES ($1, $2)",
            principal_id,
            f"retry-owner-{uuid4().hex}",
        )
        await connection.execute(
            "INSERT INTO workflow_definitions "
            "(id, owner_principal_id, name, execution_policy) "
            "VALUES ($1, $2, $3, $4::jsonb)",
            workflow_id,
            principal_id,
            "Retry workflow",
            '{"future": true}',
        )
        await connection.execute(
            "INSERT INTO workflow_versions "
            "(id, workflow_definition_id, version_number, name, execution_policy) "
            "VALUES ($1, $2, 1, $3, $4::jsonb)",
            version_id,
            workflow_id,
            "Retry snapshot",
            '{"future": true}',
        )
        await connection.execute(
            "INSERT INTO workflow_version_steps "
            "(workflow_version_id, step_identifier, task_type, parameters) "
            "VALUES ($1, 'step', 'test.task', '{}'::jsonb)",
            version_id,
        )
        run_id = uuid4()
        await connection.execute(
            "INSERT INTO workflow_runs "
            "(id, workflow_definition_id, workflow_version_id, "
            "requested_by_principal_id, status) VALUES ($1, $2, $3, $4, 'running')",
            run_id,
            workflow_id,
            version_id,
            principal_id,
        )
        await connection.execute(
            "INSERT INTO task_runs "
            "(id, workflow_run_id, workflow_version_id, step_identifier, status) "
            "VALUES ($1, $2, $3, 'step', 'runnable')",
            task_run_id,
            run_id,
            version_id,
        )
        await connection.execute(
            "INSERT INTO task_attempts (id, task_run_id, attempt_number) "
            "VALUES ($1, $2, 1)",
            attempt_id,
            task_run_id,
        )
        return workflow_id, version_id, task_run_id, attempt_id
    finally:
        await connection.close()


async def assert_upgraded(
    database_url: URL, seeded: tuple[object, ...], *, exercise_writes: bool = True
) -> None:
    workflow_id, version_id, task_run_id, attempt_id = seeded
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        column = await connection.fetchrow(
            "SELECT data_type, udt_name, is_nullable, column_default "
            "FROM information_schema.columns WHERE table_schema = 'public' "
            "AND table_name = 'task_attempts' AND column_name = 'next_eligible_at'"
        )
        assert column is not None
        assert dict(column) == {
            "data_type": "timestamp with time zone",
            "udt_name": "timestamptz",
            "is_nullable": "YES",
            "column_default": None,
        }
        assert await connection.fetchval(
            "SELECT next_eligible_at IS NULL FROM task_attempts WHERE id = $1",
            attempt_id,
        )
        index_definition = await connection.fetchval(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = 'public' "
            "AND indexname = $1",
            INDEX_NAME,
        )
        assert index_definition is not None
        assert "(next_eligible_at, id) WHERE (next_eligible_at IS NOT NULL)" in (
            index_definition
        )
        constraints = await connection.fetch(
            "SELECT c.relname, con.conname, con.convalidated "
            "FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid "
            "WHERE c.relname = ANY($1::text[]) AND con.conname LIKE '%retry_policy%' "
            "ORDER BY c.relname",
            list(POLICY_TABLES),
        )
        assert [(row["relname"], row["convalidated"]) for row in constraints] == [
            ("workflow_definitions", True),
            ("workflow_draft_steps", True),
            ("workflow_version_steps", False),
            ("workflow_versions", False),
        ]

        if not exercise_writes:
            return

        workflow_policy = {
            "retry_policy": {
                "maximum_attempts": 3,
                "initial_delay_seconds": 5,
                "multiplier": 2,
                "maximum_delay_seconds": 60,
            }
        }
        step_policy = {
            "retry_policy": {
                "maximum_attempts": 1,
                "initial_delay_seconds": 0,
                "multiplier": 1.5,
                "maximum_delay_seconds": 0,
            }
        }
        await connection.execute(
            "UPDATE workflow_definitions SET execution_policy = $2::jsonb "
            "WHERE id = $1",
            workflow_id,
            json.dumps(workflow_policy),
        )
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                "UPDATE workflow_definitions SET execution_policy = $2::jsonb "
                "WHERE id = $1",
                workflow_id,
                '{"retry_policy":{"maximum_attempts":0}}',
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                "UPDATE workflow_definitions SET execution_policy = $2::jsonb "
                "WHERE id = $1",
                workflow_id,
                '{"retry_policy":[]}',
            )
        await connection.execute(
            "INSERT INTO workflow_version_steps "
            "(workflow_version_id, step_identifier, task_type, parameters, "
            "execution_policy) VALUES ($1, 'override', 'test.task', '{}'::jsonb, "
            "$2::jsonb)",
            version_id,
            json.dumps(step_policy),
        )
        stored = await connection.fetchval(
            "SELECT execution_policy FROM workflow_version_steps "
            "WHERE workflow_version_id = $1 AND step_identifier = 'override'",
            version_id,
        )
        assert json.loads(stored) == step_policy

        due_one = datetime(2030, 1, 1, 0, 0, 0, 123456, tzinfo=UTC)
        due_two = datetime(2030, 1, 2, 0, 0, 0, 654321, tzinfo=UTC)
        retry_attempt_id = uuid4()
        await connection.execute(
            "INSERT INTO task_attempts "
            "(id, task_run_id, attempt_number, next_eligible_at) "
            "VALUES ($1, $2, 2, $3)",
            retry_attempt_id,
            task_run_id,
            due_two,
        )
        await connection.execute(
            "UPDATE task_attempts SET next_eligible_at = $2 WHERE id = $1",
            attempt_id,
            due_one,
        )
        rows = await connection.fetch(
            "SELECT attempt_number, next_eligible_at FROM task_attempts "
            "WHERE task_run_id = $1 ORDER BY attempt_number",
            task_run_id,
        )
        assert [(row["attempt_number"], row["next_eligible_at"]) for row in rows] == [
            (1, due_one),
            (2, due_two),
        ]
    finally:
        await connection.close()


async def assert_downgraded(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'task_attempts' "
            "AND column_name = 'next_eligible_at')"
        )
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT FROM pg_indexes WHERE schemaname = 'public' "
            "AND indexname = $1)",
            INDEX_NAME,
        )
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT FROM pg_constraint "
            "WHERE conname LIKE '%retry_policy%')"
        )
    finally:
        await connection.close()


def test_retry_persistence_upgrade_downgrade_reupgrade() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_retry_migration"
    ) as database_url:
        configuration = Config("alembic.ini")
        rendered = database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        with migration_database_url(rendered):
            command.upgrade(configuration, PREVIOUS_REVISION)
            seeded = asyncio.run(seed_previous_revision(database_url))
            command.upgrade(configuration, REVISION)
            asyncio.run(assert_upgraded(database_url, seeded))
            command.downgrade(configuration, PREVIOUS_REVISION)
            asyncio.run(assert_downgraded(database_url))
            command.upgrade(configuration, REVISION)
            asyncio.run(assert_upgraded(database_url, seeded, exercise_writes=False))
