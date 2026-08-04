"""Opt-in workflow definition migration verification against real PostgreSQL."""

from __future__ import annotations

import asyncio
import os
from uuid import UUID, uuid4

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

WORKFLOW_TABLES = {
    "workflow_definitions",
    "workflow_draft_dependencies",
    "workflow_draft_steps",
}
IDENTITY_TABLES = {
    "api_credentials",
    "api_principal_roles",
    "api_principals",
    "worker_credentials",
    "worker_identities",
}


async def insert_principal(connection: asyncpg.Connection[asyncpg.Record]) -> UUID:
    principal_id = uuid4()
    await connection.execute(
        "INSERT INTO api_principals (id, name) VALUES ($1, $2)",
        principal_id,
        f"workflow-owner-{uuid4().hex}",
    )
    return principal_id


async def insert_workflow(
    connection: asyncpg.Connection[asyncpg.Record], principal_id: UUID
) -> UUID:
    workflow_id = uuid4()
    await connection.execute(
        "INSERT INTO workflow_definitions (id, owner_principal_id, name) "
        "VALUES ($1, $2, $3)",
        workflow_id,
        principal_id,
        f"workflow-{uuid4().hex}",
    )
    return workflow_id


async def insert_step(
    connection: asyncpg.Connection[asyncpg.Record],
    workflow_id: UUID,
    identifier: str,
) -> UUID:
    step_id = uuid4()
    await connection.execute(
        "INSERT INTO workflow_draft_steps "
        "(id, workflow_definition_id, step_identifier, task_type, parameters) "
        "VALUES ($1, $2, $3, $4, $5::jsonb)",
        step_id,
        workflow_id,
        identifier,
        "test.task",
        '{"value": 1}',
    )
    return step_id


async def inspect_upgraded_workflow_schema(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        tables = set(
            await connection.fetchval(
                "SELECT array_agg(tablename ORDER BY tablename) "
                "FROM pg_tables WHERE schemaname = 'public' "
                "AND tablename <> 'alembic_version'"
            )
            or []
        )
        assert tables == IDENTITY_TABLES | WORKFLOW_TABLES
        assert await connection.fetchval(
            "SELECT enum_range(NULL::workflow_definition_status)::text[]"
        ) == ["draft", "enabled", "disabled", "archived"]

        principal_id = await insert_principal(connection)
        workflow_id = await insert_workflow(connection, principal_id)
        predecessor_id = await insert_step(connection, workflow_id, "first")
        successor_id = await insert_step(connection, workflow_id, "second")
        await connection.execute(
            "INSERT INTO workflow_draft_dependencies "
            "(id, workflow_definition_id, predecessor_step_id, successor_step_id) "
            "VALUES ($1, $2, $3, $4)",
            uuid4(),
            workflow_id,
            predecessor_id,
            successor_id,
        )
        row = await connection.fetchrow(
            "SELECT status::text, created_at, updated_at "
            "FROM workflow_definitions WHERE id = $1",
            workflow_id,
        )
        assert row is not None
        assert row["status"] == "draft"
        assert row["created_at"] is not None
        assert row["updated_at"] is not None

        with pytest.raises(asyncpg.InvalidTextRepresentationError):
            await connection.execute(
                "UPDATE workflow_definitions SET status = $1 WHERE id = $2",
                "unsupported",
                workflow_id,
            )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await insert_workflow(connection, uuid4())
        with pytest.raises(asyncpg.UniqueViolationError):
            await insert_step(connection, workflow_id, "first")
        with pytest.raises(asyncpg.NotNullViolationError):
            await connection.execute(
                "INSERT INTO workflow_draft_steps "
                "(id, workflow_definition_id, step_identifier, task_type) "
                "VALUES ($1, $2, $3, $4)",
                uuid4(),
                workflow_id,
                "missing-parameters",
                "test.task",
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                "INSERT INTO workflow_draft_steps "
                "(id, workflow_definition_id, step_identifier, task_type, parameters) "
                "VALUES ($1, $2, $3, $4, $5::jsonb)",
                uuid4(),
                workflow_id,
                "array-parameters",
                "test.task",
                "[]",
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                "INSERT INTO workflow_draft_dependencies "
                "(id, workflow_definition_id, predecessor_step_id, successor_step_id) "
                "VALUES ($1, $2, $3, $3)",
                uuid4(),
                workflow_id,
                predecessor_id,
            )
        with pytest.raises(asyncpg.UniqueViolationError):
            await connection.execute(
                "INSERT INTO workflow_draft_dependencies "
                "(id, workflow_definition_id, predecessor_step_id, successor_step_id) "
                "VALUES ($1, $2, $3, $4)",
                uuid4(),
                workflow_id,
                predecessor_id,
                successor_id,
            )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await connection.execute(
                "INSERT INTO workflow_draft_dependencies "
                "(id, workflow_definition_id, predecessor_step_id, successor_step_id) "
                "VALUES ($1, $2, $3, $4)",
                uuid4(),
                workflow_id,
                predecessor_id,
                uuid4(),
            )

        other_workflow_id = await insert_workflow(connection, principal_id)
        other_step_id = await insert_step(connection, other_workflow_id, "other")
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await connection.execute(
                "INSERT INTO workflow_draft_dependencies "
                "(id, workflow_definition_id, predecessor_step_id, successor_step_id) "
                "VALUES ($1, $2, $3, $4)",
                uuid4(),
                workflow_id,
                predecessor_id,
                other_step_id,
            )
    finally:
        await connection.close()


async def inspect_workflow_downgrade(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        remaining_workflow_tables = await connection.fetchval(
            "SELECT count(*) FROM pg_tables WHERE schemaname = 'public' "
            "AND tablename = ANY($1::text[])",
            list(WORKFLOW_TABLES),
        )
        remaining_identity_tables = set(
            await connection.fetchval(
                "SELECT array_agg(tablename ORDER BY tablename) FROM pg_tables "
                "WHERE schemaname = 'public' AND tablename = ANY($1::text[])",
                list(IDENTITY_TABLES),
            )
            or []
        )
        enum_exists = await connection.fetchval(
            "SELECT EXISTS (SELECT FROM pg_type WHERE typname = $1)",
            "workflow_definition_status",
        )
        assert remaining_workflow_tables == 0
        assert remaining_identity_tables == IDENTITY_TABLES
        assert enum_exists is False
    finally:
        await connection.close()


def test_workflow_migration_constraints_and_reversible_boundary() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL",
        "taskforge_migration_test",
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        configuration = Config("alembic.ini")
        with migration_database_url(alembic_url):
            command.upgrade(configuration, "head")
            asyncio.run(inspect_upgraded_workflow_schema(database_url))
            command.downgrade(configuration, "0001_identity")
            asyncio.run(inspect_workflow_downgrade(database_url))
