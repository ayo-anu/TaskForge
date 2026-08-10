"""Opt-in task-attempt and dispatch-outbox migration verification."""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime
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

DISPATCH_TABLES = {"task_attempts", "task_dispatch_outbox"}
UNPUBLISHED_INDEX = "ix_task_dispatch_outbox_unpublished_created_at_id"


async def assert_dispatch_tables_absent(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM pg_tables WHERE schemaname = 'public' "
                "AND tablename = ANY($1::text[])",
                list(DISPATCH_TABLES),
            )
            == 0
        )
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT FROM pg_indexes WHERE schemaname = 'public' "
            "AND indexname = $1)",
            UNPUBLISHED_INDEX,
        )
    finally:
        await connection.close()


async def assert_dispatch_schema_catalog(
    connection: asyncpg.Connection[asyncpg.Record],
) -> None:
    assert (
        await connection.fetchval(
            "SELECT count(*) FROM pg_tables WHERE schemaname = 'public' "
            "AND tablename = ANY($1::text[])",
            list(DISPATCH_TABLES),
        )
        == 2
    )
    constraints = await connection.fetch(
        "SELECT c.relname AS table_name, con.conname, con.contype::text, "
        "con.confupdtype::text, con.confdeltype::text "
        "FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'public' AND c.relname = ANY($1::text[]) "
        "ORDER BY c.relname, con.conname",
        list(DISPATCH_TABLES),
    )
    assert {(row["table_name"], row["conname"]) for row in constraints} == {
        ("task_attempts", "ck_task_attempts_attempt_number_positive"),
        ("task_attempts", "fk_task_attempts_task_run_id_task_runs"),
        ("task_attempts", "pk_task_attempts"),
        ("task_attempts", "uq_task_attempts_task_run_id_attempt_number"),
        ("task_dispatch_outbox", "ck_task_dispatch_outbox_payload_object"),
        ("task_dispatch_outbox", "ck_task_dispatch_outbox_route_not_blank"),
        (
            "task_dispatch_outbox",
            "fk_task_dispatch_outbox_task_attempt_id_task_attempts",
        ),
        ("task_dispatch_outbox", "pk_task_dispatch_outbox"),
        ("task_dispatch_outbox", "uq_task_dispatch_outbox_task_attempt_id"),
    }
    foreign_keys = [row for row in constraints if row["contype"] == "f"]
    assert len(foreign_keys) == 2
    assert all(row["confupdtype"] == "r" for row in foreign_keys)
    assert all(row["confdeltype"] == "r" for row in foreign_keys)
    index = await connection.fetchrow(
        "SELECT indexdef FROM pg_indexes WHERE schemaname = 'public' "
        "AND indexname = $1",
        UNPUBLISHED_INDEX,
    )
    assert index is not None
    assert "(created_at, id) WHERE (published_at IS NULL)" in index["indexdef"]


async def insert_run_graph(
    connection: asyncpg.Connection[asyncpg.Record],
) -> tuple[UUID, UUID]:
    principal_id, workflow_id, version_id, run_id = (
        uuid4(),
        uuid4(),
        uuid4(),
        uuid4(),
    )
    await connection.execute(
        "INSERT INTO api_principals (id, name) VALUES ($1, $2)",
        principal_id,
        f"dispatch-owner-{uuid4().hex}",
    )
    await connection.execute(
        "INSERT INTO workflow_definitions (id, owner_principal_id, name) "
        "VALUES ($1, $2, $3)",
        workflow_id,
        principal_id,
        f"dispatch-workflow-{uuid4().hex}",
    )
    await connection.execute(
        "INSERT INTO workflow_versions "
        "(id, workflow_definition_id, version_number, name) "
        "VALUES ($1, $2, 1, $3)",
        version_id,
        workflow_id,
        "Dispatch version",
    )
    for step_identifier in ("first", "second"):
        await connection.execute(
            "INSERT INTO workflow_version_steps "
            "(workflow_version_id, step_identifier, task_type, parameters) "
            "VALUES ($1, $2, $3, $4::jsonb)",
            version_id,
            step_identifier,
            "test.task",
            "{}",
        )
    await connection.execute(
        "INSERT INTO workflow_runs "
        "(id, workflow_definition_id, workflow_version_id, "
        "requested_by_principal_id, status) VALUES ($1, $2, $3, $4, $5)",
        run_id,
        workflow_id,
        version_id,
        principal_id,
        "running",
    )
    task_ids = (uuid4(), uuid4())
    for task_id, step_identifier in zip(task_ids, ("first", "second"), strict=True):
        await connection.execute(
            "INSERT INTO task_runs "
            "(id, workflow_run_id, workflow_version_id, step_identifier, status) "
            "VALUES ($1, $2, $3, $4, $5)",
            task_id,
            run_id,
            version_id,
            step_identifier,
            "runnable",
        )
    return task_ids


async def inspect_upgraded_dispatch_schema(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        await assert_dispatch_schema_catalog(connection)
        first_task_id, second_task_id = await insert_run_graph(connection)
        first_attempt_id, second_attempt_id, other_task_attempt_id = (
            uuid4(),
            uuid4(),
            uuid4(),
        )
        await connection.execute(
            "INSERT INTO task_attempts (id, task_run_id, attempt_number) "
            "VALUES ($1, $2, 1), ($3, $2, 2), ($4, $5, 1)",
            first_attempt_id,
            first_task_id,
            second_attempt_id,
            other_task_attempt_id,
            second_task_id,
        )
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM task_attempts WHERE task_run_id = $1",
                first_task_id,
            )
            == 2
        )

        with pytest.raises(asyncpg.UniqueViolationError):
            await connection.execute(
                "INSERT INTO task_attempts (id, task_run_id, attempt_number) "
                "VALUES ($1, $2, 1)",
                uuid4(),
                first_task_id,
            )
        for invalid_number in (0, -1):
            with pytest.raises(asyncpg.CheckViolationError):
                await connection.execute(
                    "INSERT INTO task_attempts (id, task_run_id, attempt_number) "
                    "VALUES ($1, $2, $3)",
                    uuid4(),
                    second_task_id,
                    invalid_number,
                )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await connection.execute(
                "INSERT INTO task_attempts (id, task_run_id, attempt_number) "
                "VALUES ($1, $2, 1)",
                uuid4(),
                uuid4(),
            )

        dispatch_id = uuid4()
        await connection.execute(
            "INSERT INTO task_dispatch_outbox "
            "(id, task_attempt_id, route, payload) "
            "VALUES ($1, $2, $3, $4::jsonb)",
            dispatch_id,
            first_attempt_id,
            "tasks.test",
            '{"version": 1}',
        )
        assert await connection.fetchval(
            "SELECT published_at IS NULL FROM task_dispatch_outbox WHERE id = $1",
            dispatch_id,
        )
        with pytest.raises(asyncpg.UniqueViolationError):
            await connection.execute(
                "INSERT INTO task_dispatch_outbox "
                "(id, task_attempt_id, route, payload) "
                "VALUES ($1, $2, $3, $4::jsonb)",
                dispatch_id,
                second_attempt_id,
                "tasks.test",
                "{}",
            )
        with pytest.raises(asyncpg.UniqueViolationError):
            await connection.execute(
                "INSERT INTO task_dispatch_outbox "
                "(id, task_attempt_id, route, payload) "
                "VALUES ($1, $2, $3, $4::jsonb)",
                uuid4(),
                first_attempt_id,
                "tasks.test",
                "{}",
            )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await connection.execute(
                "INSERT INTO task_dispatch_outbox "
                "(id, task_attempt_id, route, payload) "
                "VALUES ($1, $2, $3, $4::jsonb)",
                uuid4(),
                uuid4(),
                "tasks.test",
                "{}",
            )
        for invalid_route in ("", "   "):
            with pytest.raises(asyncpg.CheckViolationError):
                await connection.execute(
                    "INSERT INTO task_dispatch_outbox "
                    "(id, task_attempt_id, route, payload) "
                    "VALUES ($1, $2, $3, $4::jsonb)",
                    uuid4(),
                    second_attempt_id,
                    invalid_route,
                    "{}",
                )
        for invalid_payload in ("[]", '"value"', "null"):
            with pytest.raises(asyncpg.CheckViolationError):
                await connection.execute(
                    "INSERT INTO task_dispatch_outbox "
                    "(id, task_attempt_id, route, payload) "
                    "VALUES ($1, $2, $3, $4::jsonb)",
                    uuid4(),
                    second_attempt_id,
                    "tasks.test",
                    invalid_payload,
                )

        # Task 1 records only whether an acknowledgement exists. It deliberately
        # imposes no ordering semantics between publication and creation times.
        acknowledged_dispatch_id = uuid4()
        early_acknowledgement = datetime(2000, 1, 1, tzinfo=UTC)
        await connection.execute(
            "INSERT INTO task_dispatch_outbox "
            "(id, task_attempt_id, route, payload, published_at) "
            "VALUES ($1, $2, $3, $4::jsonb, $5)",
            acknowledged_dispatch_id,
            second_attempt_id,
            "tasks.test",
            "{}",
            early_acknowledgement,
        )
        assert await connection.fetchval(
            "SELECT published_at = $2 FROM task_dispatch_outbox WHERE id = $1",
            acknowledged_dispatch_id,
            early_acknowledgement,
        )

        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await connection.execute(
                "DELETE FROM task_runs WHERE id = $1",
                first_task_id,
            )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await connection.execute(
                "DELETE FROM task_attempts WHERE id = $1",
                first_attempt_id,
            )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await connection.execute(
                "UPDATE task_runs SET id = $1 WHERE id = $2",
                uuid4(),
                first_task_id,
            )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await connection.execute(
                "UPDATE task_attempts SET id = $1 WHERE id = $2",
                uuid4(),
                first_attempt_id,
            )
    finally:
        await connection.close()


async def assert_dispatch_schema_recreated(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        await assert_dispatch_schema_catalog(connection)
    finally:
        await connection.close()


def test_task_dispatch_migration_constraints_and_reversible_boundary() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL",
        "taskforge_run_migration",
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        configuration = Config("alembic.ini")
        with migration_database_url(alembic_url):
            command.upgrade(configuration, "0006_run_foundation")
            asyncio.run(assert_dispatch_tables_absent(database_url))
            command.upgrade(configuration, "0007_attempt_dispatch_outbox")
            asyncio.run(inspect_upgraded_dispatch_schema(database_url))
            command.downgrade(configuration, "0006_run_foundation")
            asyncio.run(assert_dispatch_tables_absent(database_url))
            command.upgrade(configuration, "head")
            asyncio.run(assert_dispatch_schema_recreated(database_url))
