"""Real PostgreSQL validation for workflow-run cancellation persistence."""

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

REVISION = "0020_run_cancellation"
PREVIOUS_REVISION = "0019_dead_letter_redrive"
TABLE = "workflow_run_cancellation_requests"
IMMUTABILITY_FUNCTION = "reject_workflow_run_cancellation_request_mutation"
IMMUTABILITY_SQLSTATE = "TF007"


async def seed_run(connection: asyncpg.Connection) -> tuple[UUID, UUID, UUID]:
    principal_id, workflow_id, version_id, run_id = (
        uuid4(),
        uuid4(),
        uuid4(),
        uuid4(),
    )
    await connection.execute(
        "INSERT INTO api_principals (id, name) VALUES ($1, $2)",
        principal_id,
        f"cancellation-{principal_id.hex}",
    )
    await connection.execute(
        "INSERT INTO workflow_definitions (id, owner_principal_id, name) "
        "VALUES ($1, $2, 'cancellation')",
        workflow_id,
        principal_id,
    )
    await connection.execute(
        "INSERT INTO workflow_versions "
        "(id, workflow_definition_id, version_number, name) "
        "VALUES ($1, $2, 1, 'cancellation')",
        version_id,
        workflow_id,
    )
    await connection.execute(
        "INSERT INTO workflow_runs (id, workflow_definition_id, "
        "workflow_version_id, requested_by_principal_id, status) "
        "VALUES ($1, $2, $3, $4, 'running')",
        run_id,
        workflow_id,
        version_id,
        principal_id,
    )
    return principal_id, workflow_id, run_id


async def assert_schema(connection: asyncpg.Connection) -> None:
    columns = await connection.fetch(
        "SELECT column_name, data_type, is_nullable, column_default "
        "FROM information_schema.columns WHERE table_schema = 'public' "
        "AND table_name = $1 ORDER BY ordinal_position",
        TABLE,
    )
    assert [row["column_name"] for row in columns] == [
        "workflow_run_id",
        "requested_by_principal_id",
        "idempotency_key_digest",
        "request_fingerprint",
        "reason",
        "requested_at",
    ]
    assert columns[-1]["data_type"] == "timestamp with time zone"
    assert columns[-1]["is_nullable"] == "NO"
    assert columns[-1]["column_default"] == "statement_timestamp()"
    constraints = {
        row["conname"]
        for row in await connection.fetch(
            "SELECT conname FROM pg_constraint WHERE conrelid = $1::regclass",
            TABLE,
        )
    }
    assert constraints == {
        "pk_workflow_run_cancellation_requests",
        "fk_workflow_run_cancellation_requests_run",
        "fk_workflow_run_cancellation_requests_requester",
        "ck_workflow_run_cancellation_requests_key_digest_valid",
        "ck_workflow_run_cancellation_requests_fingerprint_valid",
        "ck_workflow_run_cancellation_requests_reason_valid",
    }
    foreign_keys = await connection.fetch(
        "SELECT confupdtype::text, confdeltype::text FROM pg_constraint "
        "WHERE conrelid = $1::regclass AND contype = 'f'",
        TABLE,
    )
    assert len(foreign_keys) == 2
    assert all(
        row["confupdtype"] == "r" and row["confdeltype"] == "r" for row in foreign_keys
    )
    assert (
        await connection.fetchval(
            "SELECT count(*) FROM pg_indexes WHERE schemaname = 'public' "
            "AND tablename = $1",
            TABLE,
        )
        == 1
    )  # The primary-key index only.


async def assert_invariants(connection: asyncpg.Connection) -> None:
    principal_id, _, run_id = await seed_run(connection)
    other_principal = uuid4()
    await connection.execute(
        "INSERT INTO api_principals (id, name) VALUES ($1, $2)",
        other_principal,
        f"cancellation-{other_principal.hex}",
    )
    await connection.execute(
        f"INSERT INTO {TABLE} (workflow_run_id, requested_by_principal_id, "
        "idempotency_key_digest, request_fingerprint, reason) "
        "VALUES ($1, $2, $3, $4, NULL)",
        run_id,
        other_principal,
        "a" * 64,
        "b" * 64,
    )
    original = await connection.fetchrow(f"SELECT * FROM {TABLE}")
    assert original is not None
    assert original["requested_at"].tzinfo is not None

    for requester, digest in (
        (other_principal, "a" * 64),
        (other_principal, "c" * 64),
        (principal_id, "d" * 64),
    ):
        with pytest.raises(asyncpg.UniqueViolationError):
            await connection.execute(
                f"INSERT INTO {TABLE} (workflow_run_id, "
                "requested_by_principal_id, idempotency_key_digest, "
                "request_fingerprint) VALUES ($1, $2, $3, $4)",
                run_id,
                requester,
                digest,
                "e" * 64,
            )
    assert await connection.fetchrow(f"SELECT * FROM {TABLE}") == original

    for digest, fingerprint in (("bad", "f" * 64), ("f" * 64, "bad")):
        _, _, another_run = await seed_run(connection)
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                f"INSERT INTO {TABLE} (workflow_run_id, "
                "requested_by_principal_id, idempotency_key_digest, "
                "request_fingerprint) VALUES ($1, $2, $3, $4)",
                another_run,
                principal_id,
                digest,
                fingerprint,
            )
    for reason in ("   ", "x" * 2001):
        _, _, another_run = await seed_run(connection)
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                f"INSERT INTO {TABLE} (workflow_run_id, "
                "requested_by_principal_id, idempotency_key_digest, "
                "request_fingerprint, reason) VALUES ($1, $2, $3, $4, $5)",
                another_run,
                principal_id,
                "1" * 64,
                "2" * 64,
                reason,
            )
    _, _, bounded_run = await seed_run(connection)
    await connection.execute(
        f"INSERT INTO {TABLE} (workflow_run_id, requested_by_principal_id, "
        "idempotency_key_digest, request_fingerprint, reason) "
        "VALUES ($1, $2, $3, $4, $5)",
        bounded_run,
        principal_id,
        "3" * 64,
        "4" * 64,
        "x" * 2000,
    )
    _, _, minimum_run = await seed_run(connection)
    await connection.execute(
        f"INSERT INTO {TABLE} (workflow_run_id, requested_by_principal_id, "
        "idempotency_key_digest, request_fingerprint, reason) "
        "VALUES ($1, $2, $3, $4, 'x')",
        minimum_run,
        principal_id,
        "7" * 64,
        "8" * 64,
    )

    _, _, unknown_requester_run = await seed_run(connection)
    for missing_run, missing_principal in (
        (uuid4(), principal_id),
        (unknown_requester_run, uuid4()),
    ):
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await connection.execute(
                f"INSERT INTO {TABLE} (workflow_run_id, "
                "requested_by_principal_id, idempotency_key_digest, "
                "request_fingerprint) VALUES ($1, $2, $3, $4)",
                missing_run,
                missing_principal,
                "5" * 64,
                "6" * 64,
            )

    for statement, arguments in (
        (
            f"UPDATE {TABLE} SET reason = 'changed' WHERE workflow_run_id = $1",
            (run_id,),
        ),
        (f"DELETE FROM {TABLE} WHERE workflow_run_id = $1", (run_id,)),
        (f"TRUNCATE {TABLE}", ()),
    ):
        transaction = connection.transaction()
        await transaction.start()
        try:
            with pytest.raises(asyncpg.PostgresError) as raised:
                await connection.execute(statement, *arguments)
            assert raised.value.sqlstate == IMMUTABILITY_SQLSTATE
        finally:
            await transaction.rollback()
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await connection.execute("DELETE FROM workflow_runs WHERE id = $1", run_id)
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await connection.execute(
            "UPDATE api_principals SET id = $1 WHERE id = $2",
            uuid4(),
            other_principal,
        )


async def assert_downgraded(connection: asyncpg.Connection) -> None:
    assert not await connection.fetchval(
        "SELECT to_regclass('public.workflow_run_cancellation_requests') IS NOT NULL"
    )
    assert not await connection.fetchval(
        "SELECT EXISTS (SELECT FROM pg_proc WHERE proname = $1)",
        IMMUTABILITY_FUNCTION,
    )
    assert await connection.fetchval("SELECT count(*) FROM workflow_runs") >= 1


async def seed_existing_run(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        await seed_run(connection)
    finally:
        await connection.close()


async def exercise_upgraded_schema(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        await assert_schema(connection)
        await assert_invariants(connection)
    finally:
        await connection.close()


async def inspect_downgraded_schema(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        await assert_downgraded(connection)
    finally:
        await connection.close()


async def inspect_reupgraded_schema(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        await assert_schema(connection)
    finally:
        await connection.close()


def test_cancellation_upgrade_downgrade_reupgrade() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_run_cancellation"
    ) as database_url:
        configuration = Config("alembic.ini")
        rendered = database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        with migration_database_url(rendered):
            command.upgrade(configuration, PREVIOUS_REVISION)
            asyncio.run(seed_existing_run(database_url))
            command.upgrade(configuration, REVISION)
            asyncio.run(exercise_upgraded_schema(database_url))
            command.downgrade(configuration, PREVIOUS_REVISION)
            asyncio.run(inspect_downgraded_schema(database_url))
            command.upgrade(configuration, REVISION)
            asyncio.run(inspect_reupgraded_schema(database_url))
