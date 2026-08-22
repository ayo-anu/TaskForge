"""Migration lifecycle checks for immutable workflow replay lineage."""

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

IMMUTABILITY_FUNCTION = "reject_workflow_run_replay_mutation"
IMMUTABILITY_SQLSTATE = "TF008"
IMMUTABILITY_MESSAGE = "workflow run replay lineage is immutable"
ROW_TRIGGER = "trg_workflow_run_replays_reject_mutation"
TRUNCATE_TRIGGER = "trg_workflow_run_replays_reject_truncate"


async def insert_foundation(
    connection: asyncpg.Connection[asyncpg.Record],
) -> tuple[UUID, UUID, list[UUID]]:
    principal_id, workflow_id, version_id = uuid4(), uuid4(), uuid4()
    await connection.execute(
        "INSERT INTO api_principals (id, name) VALUES ($1, $2)",
        principal_id,
        f"replay-owner-{uuid4().hex}",
    )
    await connection.execute(
        "INSERT INTO workflow_definitions (id, owner_principal_id, name) "
        "VALUES ($1, $2, $3)",
        workflow_id,
        principal_id,
        f"replay-workflow-{uuid4().hex}",
    )
    await connection.execute(
        "INSERT INTO workflow_versions "
        "(id, workflow_definition_id, version_number, name) "
        "VALUES ($1, $2, 1, $3)",
        version_id,
        workflow_id,
        "Replay version",
    )
    run_ids = [uuid4() for _ in range(12)]
    await connection.executemany(
        "INSERT INTO workflow_runs "
        "(id, workflow_definition_id, workflow_version_id, "
        "requested_by_principal_id, status) VALUES ($1, $2, $3, $4, 'pending')",
        [(run_id, workflow_id, version_id, principal_id) for run_id in run_ids],
    )
    return workflow_id, version_id, run_ids


async def assert_catalog(connection: asyncpg.Connection[asyncpg.Record]) -> None:
    assert await connection.fetchval(
        "SELECT enum_range(NULL::workflow_replay_mode)::text[]"
    ) == ["full", "failed_subgraph"]
    columns = await connection.fetch(
        "SELECT column_name, data_type, is_nullable, column_default "
        "FROM information_schema.columns WHERE table_schema = 'public' "
        "AND table_name = 'workflow_run_replays' ORDER BY ordinal_position"
    )
    assert [row["column_name"] for row in columns] == [
        "workflow_run_id",
        "source_workflow_run_id",
        "mode",
        "requested_scope",
        "created_at",
    ]
    assert [row["data_type"] for row in columns] == [
        "uuid",
        "uuid",
        "USER-DEFINED",
        "jsonb",
        "timestamp with time zone",
    ]
    assert all(row["is_nullable"] == "NO" for row in columns)
    assert columns[0]["column_default"] is None
    assert columns[1]["column_default"] is None
    assert columns[2]["column_default"] is None
    assert columns[3]["column_default"] is None
    assert "statement_timestamp()" in columns[4]["column_default"]

    constraints = {
        row["conname"]: row["contype"]
        for row in await connection.fetch(
            "SELECT conname, contype::text FROM pg_constraint "
            "WHERE conrelid = 'workflow_run_replays'::regclass"
        )
    }
    assert constraints == {
        "pk_workflow_run_replays": "p",
        "fk_workflow_run_replays_run": "f",
        "fk_workflow_run_replays_source_run": "f",
        "ck_workflow_run_replays_source_not_self": "c",
        "ck_workflow_run_replays_requested_scope_object": "c",
    }
    foreign_keys = await connection.fetch(
        "SELECT conname, confupdtype::text, confdeltype::text, "
        "confrelid::regclass::text AS target FROM pg_constraint "
        "WHERE conrelid = 'workflow_run_replays'::regclass AND contype = 'f' "
        "ORDER BY conname"
    )
    assert [
        (row["conname"], row["confupdtype"], row["confdeltype"], row["target"])
        for row in foreign_keys
    ] == [
        ("fk_workflow_run_replays_run", "r", "r", "workflow_runs"),
        ("fk_workflow_run_replays_source_run", "r", "r", "workflow_runs"),
    ]
    indexes = {
        row["indexname"]
        for row in await connection.fetch(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'public' "
            "AND tablename = 'workflow_run_replays'"
        )
    }
    assert indexes == {
        "pk_workflow_run_replays",
        "ix_workflow_run_replays_source_workflow_run_id",
    }
    assert await connection.fetchval(
        "SELECT EXISTS (SELECT FROM pg_proc WHERE proname = $1 AND pronargs = 0)",
        IMMUTABILITY_FUNCTION,
    )
    triggers = {
        row["tgname"]: row["tgtype"]
        for row in await connection.fetch(
            "SELECT tgname, tgtype FROM pg_trigger "
            "WHERE tgrelid = 'workflow_run_replays'::regclass "
            "AND NOT tgisinternal"
        )
    }
    assert triggers == {ROW_TRIGGER: 27, TRUNCATE_TRIGGER: 34}


async def insert_replay(
    connection: asyncpg.Connection[asyncpg.Record],
    run_id: UUID,
    source_id: UUID,
    mode: str,
    scope: str,
) -> None:
    await connection.execute(
        "INSERT INTO workflow_run_replays "
        "(workflow_run_id, source_workflow_run_id, mode, requested_scope) "
        "VALUES ($1, $2, $3, $4::jsonb)",
        run_id,
        source_id,
        mode,
        scope,
    )


def assert_immutable_error(error: asyncpg.PostgresError) -> None:
    assert error.sqlstate == IMMUTABILITY_SQLSTATE
    assert error.message == IMMUTABILITY_MESSAGE


async def assert_immutable_statement(
    connection: asyncpg.Connection[asyncpg.Record], statement: str, *args: object
) -> None:
    transaction = connection.transaction()
    await transaction.start()
    try:
        try:
            await connection.execute(statement, *args)
        except asyncpg.PostgresError as error:
            assert_immutable_error(error)
        else:
            pytest.fail("workflow replay lineage mutation was not rejected")
        with pytest.raises(asyncpg.InFailedSQLTransactionError):
            await connection.fetchval("SELECT 1")
    finally:
        await transaction.rollback()


async def assert_constraints(
    connection: asyncpg.Connection[asyncpg.Record], run_ids: list[UUID]
) -> None:
    source, full_empty, full_object, failed_empty, failed_object = run_ids[:5]
    await insert_replay(connection, full_empty, source, "full", "{}")
    await insert_replay(connection, full_object, source, "full", '{"step": "one"}')
    await insert_replay(connection, failed_empty, source, "failed_subgraph", "{}")
    await insert_replay(
        connection,
        failed_object,
        source,
        "failed_subgraph",
        '{"steps": ["one"]}',
    )
    assert await connection.fetchval("SELECT count(*) FROM workflow_run_replays") == 4

    for invalid_scope in ("[]", '"value"', "1", "true", "null"):
        with pytest.raises(asyncpg.CheckViolationError):
            await insert_replay(connection, run_ids[5], source, "full", invalid_scope)
    with pytest.raises(asyncpg.NotNullViolationError):
        await connection.execute(
            "INSERT INTO workflow_run_replays "
            "(workflow_run_id, source_workflow_run_id, mode, requested_scope) "
            "VALUES ($1, $2, 'full', NULL)",
            run_ids[5],
            source,
        )
    with pytest.raises(asyncpg.InvalidTextRepresentationError):
        await insert_replay(connection, run_ids[5], source, "partial", "{}")
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await insert_replay(connection, uuid4(), source, "full", "{}")
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await insert_replay(connection, run_ids[5], uuid4(), "full", "{}")
    with pytest.raises(asyncpg.CheckViolationError):
        await insert_replay(connection, run_ids[5], run_ids[5], "full", "{}")
    with pytest.raises(asyncpg.UniqueViolationError):
        await insert_replay(connection, full_empty, source, "full", "{}")

    await insert_replay(connection, run_ids[6], full_empty, "full", "{}")
    lineage = await connection.fetch(
        "SELECT workflow_run_id, source_workflow_run_id "
        "FROM workflow_run_replays WHERE workflow_run_id = ANY($1::uuid[]) "
        "ORDER BY workflow_run_id",
        [full_empty, run_ids[6]],
    )
    assert {
        row["workflow_run_id"]: row["source_workflow_run_id"] for row in lineage
    } == {full_empty: source, run_ids[6]: full_empty}

    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await connection.execute("DELETE FROM workflow_runs WHERE id = $1", source)
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await connection.execute(
            "DELETE FROM workflow_runs WHERE id = $1", failed_object
        )
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await connection.execute(
            "UPDATE workflow_runs SET id = $1 WHERE id = $2", uuid4(), source
        )

    await assert_immutable_statement(
        connection,
        "UPDATE workflow_run_replays SET requested_scope = '{}'::jsonb "
        "WHERE workflow_run_id = $1",
        full_object,
    )
    await assert_immutable_statement(
        connection,
        "DELETE FROM workflow_run_replays WHERE workflow_run_id = $1",
        full_object,
    )
    await assert_immutable_statement(connection, "TRUNCATE workflow_run_replays")


async def inspect_at_0023(database_url: URL, expected_run_ids: list[UUID]) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        assert await connection.fetchval(
            "SELECT to_regclass('public.workflow_run_replays') IS NULL"
        )
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT FROM pg_type WHERE typname = 'workflow_replay_mode')"
        )
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT FROM pg_proc WHERE proname = $1)",
            IMMUTABILITY_FUNCTION,
        )
        assert await connection.fetchval(
            "SELECT count(*) FROM workflow_runs WHERE id = ANY($1::uuid[])",
            expected_run_ids,
        ) == len(expected_run_ids)
    finally:
        await connection.close()


async def inspect_reupgrade(database_url: URL, expected_run_ids: list[UUID]) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        await assert_catalog(connection)
        assert await connection.fetchval(
            "SELECT count(*) FROM workflow_runs WHERE id = ANY($1::uuid[])",
            expected_run_ids,
        ) == len(expected_run_ids)
        assert (
            await connection.fetchval("SELECT count(*) FROM workflow_run_replays") == 0
        )
    finally:
        await connection.close()


async def create_preexisting_runs(database_url: URL) -> list[UUID]:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        _, _, run_ids = await insert_foundation(connection)
        return run_ids
    finally:
        await connection.close()


async def inspect_upgrade(database_url: URL, run_ids: list[UUID]) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        await assert_catalog(connection)
        assert await connection.fetchval(
            "SELECT count(*) FROM workflow_runs WHERE id = ANY($1::uuid[])",
            run_ids,
        ) == len(run_ids)
        assert (
            await connection.fetchval("SELECT count(*) FROM workflow_run_replays") == 0
        )
        await assert_constraints(connection, run_ids)
    finally:
        await connection.close()


def test_workflow_replay_upgrade_downgrade_reupgrade() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL",
        "taskforge_workflow_replay_mig",
    ) as database_url:
        configuration = Config("alembic.ini")
        rendered = database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        with migration_database_url(rendered):
            command.upgrade(configuration, "0023_execution_event_wakeups")
            run_ids = asyncio.run(create_preexisting_runs(database_url))

            command.upgrade(configuration, "0024_run_replay_lineage")
            asyncio.run(inspect_upgrade(database_url, run_ids))

            command.downgrade(configuration, "0023_execution_event_wakeups")
            asyncio.run(inspect_at_0023(database_url, run_ids))
            command.upgrade(configuration, "0024_run_replay_lineage")
            asyncio.run(inspect_reupgrade(database_url, run_ids))
