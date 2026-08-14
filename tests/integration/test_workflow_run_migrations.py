"""Opt-in workflow run migration verification against real PostgreSQL."""

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

RUN_TABLES = {
    "workflow_runs",
    "workflow_run_inputs",
    "task_runs",
    "workflow_run_idempotency",
}
IMMUTABILITY_FUNCTION = "reject_workflow_run_creation_snapshot_mutation"
IMMUTABILITY_SQLSTATE = "TF002"
IMMUTABILITY_MESSAGE = "workflow run creation snapshots are immutable"
ROW_TRIGGERS = {
    "workflow_run_inputs": "trg_workflow_run_inputs_reject_mutation",
    "workflow_run_idempotency": "trg_workflow_run_idempotency_reject_mutation",
}
TRUNCATE_TRIGGERS = {
    "workflow_run_inputs": "trg_workflow_run_inputs_reject_truncate",
    "workflow_run_idempotency": "trg_workflow_run_idempotency_reject_truncate",
}


async def insert_principal(connection: asyncpg.Connection[asyncpg.Record]) -> UUID:
    principal_id = uuid4()
    await connection.execute(
        "INSERT INTO api_principals (id, name) VALUES ($1, $2)",
        principal_id,
        f"run-owner-{uuid4().hex}",
    )
    return principal_id


async def insert_workflow_version(
    connection: asyncpg.Connection[asyncpg.Record],
    principal_id: UUID,
    *,
    step_identifier: str = "first",
) -> tuple[UUID, UUID]:
    workflow_id, version_id = uuid4(), uuid4()
    await connection.execute(
        "INSERT INTO workflow_definitions (id, owner_principal_id, name) "
        "VALUES ($1, $2, $3)",
        workflow_id,
        principal_id,
        f"run-workflow-{uuid4().hex}",
    )
    await connection.execute(
        "INSERT INTO workflow_versions "
        "(id, workflow_definition_id, version_number, name) "
        "VALUES ($1, $2, 1, $3)",
        version_id,
        workflow_id,
        "Run version",
    )
    await connection.execute(
        "INSERT INTO workflow_version_steps "
        "(workflow_version_id, step_identifier, task_type, parameters) "
        "VALUES ($1, $2, $3, $4::jsonb)",
        version_id,
        step_identifier,
        "test.task",
        "{}",
    )
    return workflow_id, version_id


async def insert_run(
    connection: asyncpg.Connection[asyncpg.Record],
    workflow_id: UUID,
    version_id: UUID,
    principal_id: UUID,
) -> UUID:
    run_id = uuid4()
    await connection.execute(
        "INSERT INTO workflow_runs "
        "(id, workflow_definition_id, workflow_version_id, "
        "requested_by_principal_id, status) VALUES ($1, $2, $3, $4, $5)",
        run_id,
        workflow_id,
        version_id,
        principal_id,
        "pending",
    )
    return run_id


def assert_immutable_error(error: asyncpg.PostgresError) -> None:
    assert error.sqlstate == IMMUTABILITY_SQLSTATE
    assert error.message == IMMUTABILITY_MESSAGE


async def assert_rejected_snapshot_statement(
    connection: asyncpg.Connection[asyncpg.Record],
    statement: str,
    *arguments: object,
    unchanged_query: str,
    unchanged_arguments: tuple[object, ...],
) -> None:
    before = await connection.fetchrow(unchanged_query, *unchanged_arguments)
    transaction = connection.transaction()
    await transaction.start()
    try:
        try:
            await connection.execute(statement, *arguments)
        except asyncpg.PostgresError as error:
            assert_immutable_error(error)
        else:
            pytest.fail("run creation snapshot mutation was not rejected")
        assert connection.is_in_transaction()
        with pytest.raises(asyncpg.InFailedSQLTransactionError):
            await connection.fetchval("SELECT 1")
    finally:
        await transaction.rollback()
    assert await connection.fetchrow(unchanged_query, *unchanged_arguments) == before


async def assert_run_schema_catalog(
    connection: asyncpg.Connection[asyncpg.Record],
) -> None:
    assert await connection.fetchval(
        "SELECT enum_range(NULL::workflow_run_status)::text[]"
    ) == ["pending", "running", "cancelling", "succeeded", "failed", "cancelled"]
    assert await connection.fetchval(
        "SELECT enum_range(NULL::task_run_status)::text[]"
    ) == [
        "blocked",
        "runnable",
        "dispatched",
        "claimed",
        "running",
        "retry_scheduled",
        "succeeded",
        "failed",
        "skipped",
        "cancelled",
        "retry_pending",
    ]
    assert await connection.fetchval(
        "SELECT EXISTS (SELECT FROM pg_proc p JOIN pg_namespace n "
        "ON n.oid = p.pronamespace WHERE n.nspname = 'public' "
        "AND p.proname = $1 AND p.pronargs = 0)",
        IMMUTABILITY_FUNCTION,
    )
    rows = await connection.fetch(
        "SELECT t.tgname, c.relname AS table_name, t.tgenabled::text AS enabled, "
        "t.tgtype FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'public' AND NOT t.tgisinternal "
        "AND t.tgname = ANY($1::text[]) ORDER BY t.tgname",
        [*ROW_TRIGGERS.values(), *TRUNCATE_TRIGGERS.values()],
    )
    assert len(rows) == 4
    for row in rows:
        table_name = row["table_name"]
        assert row["enabled"] == "O"
        if row["tgname"] == ROW_TRIGGERS[table_name]:
            assert row["tgtype"] == 27  # row-level BEFORE UPDATE OR DELETE
        else:
            assert row["tgname"] == TRUNCATE_TRIGGERS[table_name]
            assert row["tgtype"] == 34  # statement-level BEFORE TRUNCATE

    unnamed = await connection.fetchval(
        "SELECT count(*) FROM pg_constraint con JOIN pg_class c "
        "ON c.oid = con.conrelid JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'public' AND c.relname = ANY($1::text[]) "
        "AND (con.conname IS NULL OR btrim(con.conname) = '')",
        list(RUN_TABLES),
    )
    assert unnamed == 0
    assert (
        await connection.fetchval(
            "SELECT count(*) FROM pg_indexes WHERE schemaname = 'public' "
            "AND tablename = ANY($1::text[]) "
            "AND (indexname IS NULL OR btrim(indexname) = '')",
            list(RUN_TABLES),
        )
        == 0
    )
    assert await connection.fetchval(
        "SELECT EXISTS (SELECT FROM pg_indexes WHERE schemaname = 'public' "
        "AND indexname = 'ix_task_runs_workflow_version_id_step_identifier')"
    )
    assert not await connection.fetchval(
        "SELECT EXISTS (SELECT FROM pg_indexes WHERE schemaname = 'public' "
        "AND indexname = 'ix_task_runs_workflow_run_id_workflow_version_id')"
    )
    column = await connection.fetchrow(
        "SELECT data_type, is_nullable, column_default "
        "FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = 'task_runs' "
        "AND column_name = 'execution_timeout_seconds'"
    )
    assert dict(column) == {
        "data_type": "integer",
        "is_nullable": "YES",
        "column_default": None,
    }
    immutable_constraints = await connection.fetch(
        "SELECT c.relname AS table_name, con.convalidated "
        "FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid "
        "WHERE con.conname = ANY($1::text[]) ORDER BY c.relname",
        [
            "ck_workflow_versions_execution_timeout_seconds_valid",
            "ck_workflow_version_steps_execution_timeout_seconds_valid",
        ],
    )
    assert [
        (row["table_name"], row["convalidated"]) for row in immutable_constraints
    ] == [
        ("workflow_version_steps", False),
        ("workflow_versions", False),
    ]


async def inspect_upgraded_run_schema(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        await assert_run_schema_catalog(connection)
        principal_id = await insert_principal(connection)
        other_principal_id = await insert_principal(connection)
        workflow_id, version_id = await insert_workflow_version(
            connection, principal_id
        )
        other_workflow_id, other_version_id = await insert_workflow_version(
            connection, principal_id, step_identifier="other"
        )
        run_id = await insert_run(connection, workflow_id, version_id, principal_id)
        other_run_id = await insert_run(
            connection, other_workflow_id, other_version_id, principal_id
        )

        await connection.execute(
            "INSERT INTO workflow_run_inputs "
            "(workflow_run_id, payload, input_references) "
            "VALUES ($1, $2::jsonb, $3::jsonb)",
            run_id,
            '{"value": 1}',
            '{"artifact": {"kind": "object"}}',
        )
        await connection.execute(
            "INSERT INTO task_runs "
            "(id, workflow_run_id, workflow_version_id, step_identifier, status) "
            "VALUES ($1, $2, $3, $4, $5)",
            uuid4(),
            run_id,
            version_id,
            "first",
            "runnable",
        )
        await connection.execute(
            "INSERT INTO workflow_run_idempotency "
            "(principal_id, workflow_definition_id, idempotency_key_digest, "
            "request_fingerprint, workflow_run_id) VALUES ($1, $2, $3, $4, $5)",
            principal_id,
            workflow_id,
            "digest:v1:one",
            "fingerprint:v1:one",
            run_id,
        )
        assert await connection.fetchval(
            "SELECT created_at IS NOT NULL AND updated_at IS NOT NULL "
            "FROM workflow_runs WHERE id = $1",
            run_id,
        )

        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await insert_run(connection, workflow_id, other_version_id, principal_id)
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await insert_run(connection, workflow_id, version_id, uuid4())
        with pytest.raises(asyncpg.InvalidTextRepresentationError):
            await connection.execute(
                "INSERT INTO workflow_runs "
                "(id, workflow_definition_id, workflow_version_id, "
                "requested_by_principal_id, status) VALUES ($1, $2, $3, $4, $5)",
                uuid4(),
                workflow_id,
                version_id,
                principal_id,
                "unknown",
            )
        for column, value in (("payload", "[]"), ("input_references", "[]")):
            with pytest.raises(asyncpg.CheckViolationError):
                await connection.execute(
                    "INSERT INTO workflow_run_inputs "
                    "(workflow_run_id, payload, input_references) "
                    "VALUES ($1, $2::jsonb, $3::jsonb)",
                    other_run_id,
                    value if column == "payload" else "{}",
                    value if column == "input_references" else "{}",
                )
        with pytest.raises(asyncpg.UniqueViolationError):
            await connection.execute(
                "INSERT INTO task_runs "
                "(id, workflow_run_id, workflow_version_id, step_identifier, status) "
                "VALUES ($1, $2, $3, $4, $5)",
                uuid4(),
                run_id,
                version_id,
                "first",
                "blocked",
            )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await connection.execute(
                "INSERT INTO task_runs "
                "(id, workflow_run_id, workflow_version_id, step_identifier, status) "
                "VALUES ($1, $2, $3, $4, $5)",
                uuid4(),
                run_id,
                other_version_id,
                "other",
                "blocked",
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                "INSERT INTO task_runs "
                "(id, workflow_run_id, workflow_version_id, step_identifier, status) "
                "VALUES ($1, $2, $3, $4, $5)",
                uuid4(),
                run_id,
                version_id,
                "   ",
                "blocked",
            )

        mismatch_cases = (
            (
                other_principal_id,
                other_workflow_id,
                "principal-mismatch",
                other_run_id,
            ),
            (principal_id, workflow_id, "workflow-mismatch", other_run_id),
        )
        for scoped_principal, scoped_workflow, digest, target_run_id in mismatch_cases:
            with pytest.raises(asyncpg.ForeignKeyViolationError):
                await connection.execute(
                    "INSERT INTO workflow_run_idempotency "
                    "(principal_id, workflow_definition_id, idempotency_key_digest, "
                    "request_fingerprint, workflow_run_id) "
                    "VALUES ($1, $2, $3, $4, $5)",
                    scoped_principal,
                    scoped_workflow,
                    digest,
                    "fingerprint:v1:mismatch",
                    target_run_id,
                )
        for digest, fingerprint in (
            ("", "fingerprint:v1:valid"),
            ("   ", "fingerprint:v1:valid"),
            ("digest:v1:valid", ""),
            ("digest:v1:valid", "   "),
        ):
            with pytest.raises(asyncpg.CheckViolationError):
                await connection.execute(
                    "INSERT INTO workflow_run_idempotency "
                    "(principal_id, workflow_definition_id, idempotency_key_digest, "
                    "request_fingerprint, workflow_run_id) "
                    "VALUES ($1, $2, $3, $4, $5)",
                    principal_id,
                    other_workflow_id,
                    digest,
                    fingerprint,
                    other_run_id,
                )

        other_principal_run_id = await insert_run(
            connection,
            workflow_id,
            version_id,
            other_principal_id,
        )
        for scoped_principal, scoped_workflow, target_run_id in (
            (principal_id, other_workflow_id, other_run_id),
            (other_principal_id, workflow_id, other_principal_run_id),
        ):
            await connection.execute(
                "INSERT INTO workflow_run_idempotency "
                "(principal_id, workflow_definition_id, idempotency_key_digest, "
                "request_fingerprint, workflow_run_id) "
                "VALUES ($1, $2, $3, $4, $5)",
                scoped_principal,
                scoped_workflow,
                "digest:v1:one",
                "fingerprint:v1:one",
                target_run_id,
            )
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM workflow_run_idempotency "
                "WHERE idempotency_key_digest = $1",
                "digest:v1:one",
            )
            == 3
        )
        with pytest.raises(asyncpg.UniqueViolationError):
            await connection.execute(
                "INSERT INTO workflow_run_idempotency "
                "(principal_id, workflow_definition_id, idempotency_key_digest, "
                "request_fingerprint, workflow_run_id) "
                "VALUES ($1, $2, $3, $4, $5)",
                principal_id,
                workflow_id,
                "digest:v1:second-key",
                "fingerprint:v1:one",
                run_id,
            )

        input_query = "SELECT * FROM workflow_run_inputs WHERE workflow_run_id = $1"
        idempotency_query = (
            "SELECT * FROM workflow_run_idempotency WHERE workflow_run_id = $1"
        )
        await assert_rejected_snapshot_statement(
            connection,
            "UPDATE workflow_run_inputs SET payload = $1::jsonb "
            "WHERE workflow_run_id = $2",
            '{"changed": true}',
            run_id,
            unchanged_query=input_query,
            unchanged_arguments=(run_id,),
        )
        await assert_rejected_snapshot_statement(
            connection,
            "DELETE FROM workflow_run_inputs WHERE workflow_run_id = $1",
            run_id,
            unchanged_query=input_query,
            unchanged_arguments=(run_id,),
        )
        await assert_rejected_snapshot_statement(
            connection,
            "UPDATE workflow_run_idempotency SET request_fingerprint = $1 "
            "WHERE workflow_run_id = $2",
            "fingerprint:v1:changed",
            run_id,
            unchanged_query=idempotency_query,
            unchanged_arguments=(run_id,),
        )
        await assert_rejected_snapshot_statement(
            connection,
            "DELETE FROM workflow_run_idempotency WHERE workflow_run_id = $1",
            run_id,
            unchanged_query=idempotency_query,
            unchanged_arguments=(run_id,),
        )
        for table_name in ("workflow_run_inputs", "workflow_run_idempotency"):
            transaction = connection.transaction()
            await transaction.start()
            try:
                try:
                    await connection.execute(f"TRUNCATE TABLE {table_name}")
                except asyncpg.PostgresError as error:
                    assert_immutable_error(error)
                else:
                    pytest.fail("run creation snapshot truncate was not rejected")
                with pytest.raises(asyncpg.InFailedSQLTransactionError):
                    await connection.fetchval("SELECT 1")
            finally:
                await transaction.rollback()
    finally:
        await connection.close()


async def inspect_run_downgrade(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM pg_tables WHERE schemaname = 'public' "
                "AND tablename = ANY($1::text[])",
                list(RUN_TABLES),
            )
            == 0
        )
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT FROM pg_type WHERE typname = ANY($1::text[]))",
            ["workflow_run_status", "task_run_status"],
        )
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT FROM pg_proc WHERE proname = $1)",
            IMMUTABILITY_FUNCTION,
        )
        assert await connection.fetchval(
            "SELECT EXISTS (SELECT FROM pg_proc WHERE proname = "
            "'reject_workflow_version_snapshot_mutation')"
        )
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal "
                "AND tgname LIKE 'trg_workflow_version%_reject_mutation'"
            )
            == 3
        )
    finally:
        await connection.close()


def test_workflow_run_migration_constraints_and_reversible_boundary() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL",
        "taskforge_run_migration",
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        configuration = Config("alembic.ini")
        with migration_database_url(alembic_url):
            command.upgrade(configuration, "head")
            asyncio.run(inspect_upgraded_run_schema(database_url))
            command.downgrade(configuration, "0005_version_immutability")
            asyncio.run(inspect_run_downgrade(database_url))
