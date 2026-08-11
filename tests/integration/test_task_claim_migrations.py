"""Opt-in task-claim migration and concurrency verification."""

from __future__ import annotations

import asyncio
import os
from contextlib import suppress
from datetime import UTC, datetime, timedelta
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

CURRENT_CLAIM_INDEX = "uq_task_attempt_claims_current_task_attempt_id"
CURRENT_EXPIRY_INDEX = "ix_task_attempt_claims_current_lease_expires_at"
CURRENT_WORKER_INDEX = "ix_task_attempt_claims_current_worker_session_id"


async def assert_claim_table_absent(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' "
            "AND tablename = 'task_attempt_claims')"
        )
    finally:
        await connection.close()


async def assert_claim_catalog(
    connection: asyncpg.Connection[asyncpg.Record],
) -> None:
    columns = await connection.fetch(
        "SELECT column_name, data_type, is_nullable, column_default, is_identity "
        "FROM information_schema.columns WHERE table_schema = 'public' "
        "AND table_name = 'task_attempt_claims' ORDER BY ordinal_position"
    )
    assert [tuple(row.values()) for row in columns] == [
        ("task_attempt_id", "uuid", "NO", None, "NO"),
        ("generation", "bigint", "NO", None, "NO"),
        ("worker_session_id", "uuid", "NO", None, "NO"),
        (
            "acquired_at",
            "timestamp with time zone",
            "NO",
            "statement_timestamp()",
            "NO",
        ),
        ("lease_expires_at", "timestamp with time zone", "NO", None, "NO"),
        ("terminated_at", "timestamp with time zone", "YES", None, "NO"),
    ]
    constraints = await connection.fetch(
        "SELECT con.conname, con.contype::text, pg_get_constraintdef(con.oid) "
        "AS definition, con.confupdtype::text, "
        "con.confdeltype::text FROM pg_constraint con "
        "JOIN pg_class c ON c.oid = con.conrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'public' AND c.relname = 'task_attempt_claims'"
    )
    assert {row["conname"] for row in constraints} == {
        "ck_task_attempt_claims_generation_positive",
        "ck_task_attempt_claims_lease_expires_after_acquisition",
        "ck_task_attempt_claims_terminated_not_before_acquisition",
        "fk_task_attempt_claims_task_attempt_id_task_attempts",
        "fk_task_attempt_claims_worker_session_id_worker_sessions",
        "pk_task_attempt_claims",
    }
    foreign_keys = [row for row in constraints if row["contype"] == "f"]
    assert len(foreign_keys) == 2
    assert all(row["confupdtype"] == "r" for row in foreign_keys)
    assert all(row["confdeltype"] == "r" for row in foreign_keys)
    assert {(row["conname"], row["definition"]) for row in foreign_keys} == {
        (
            "fk_task_attempt_claims_task_attempt_id_task_attempts",
            "FOREIGN KEY (task_attempt_id) REFERENCES task_attempts(id) "
            "ON UPDATE RESTRICT ON DELETE RESTRICT",
        ),
        (
            "fk_task_attempt_claims_worker_session_id_worker_sessions",
            "FOREIGN KEY (worker_session_id) REFERENCES worker_sessions(id) "
            "ON UPDATE RESTRICT ON DELETE RESTRICT",
        ),
    }
    assert {
        (row["conname"], row["definition"])
        for row in constraints
        if row["contype"] in {"p", "c"}
    } == {
        (
            "pk_task_attempt_claims",
            "PRIMARY KEY (task_attempt_id, generation)",
        ),
        ("ck_task_attempt_claims_generation_positive", "CHECK ((generation > 0))"),
        (
            "ck_task_attempt_claims_lease_expires_after_acquisition",
            "CHECK ((lease_expires_at > acquired_at))",
        ),
        (
            "ck_task_attempt_claims_terminated_not_before_acquisition",
            "CHECK (((terminated_at IS NULL) OR (terminated_at >= acquired_at)))",
        ),
    }

    indexes = await connection.fetch(
        "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'public' "
        "AND indexname = ANY($1::text[])",
        [CURRENT_CLAIM_INDEX, CURRENT_EXPIRY_INDEX, CURRENT_WORKER_INDEX],
    )
    assert {row["indexname"] for row in indexes} == {
        CURRENT_CLAIM_INDEX,
        CURRENT_EXPIRY_INDEX,
        CURRENT_WORKER_INDEX,
    }
    assert all("WHERE (terminated_at IS NULL)" in row["indexdef"] for row in indexes)
    current_index = next(
        row["indexdef"] for row in indexes if row["indexname"] == CURRENT_CLAIM_INDEX
    )
    assert "UNIQUE INDEX" in current_index
    assert "(task_attempt_id) WHERE (terminated_at IS NULL)" in current_index
    expiry_index = next(
        row["indexdef"] for row in indexes if row["indexname"] == CURRENT_EXPIRY_INDEX
    )
    assert "(lease_expires_at) WHERE (terminated_at IS NULL)" in expiry_index
    worker_index = next(
        row["indexdef"] for row in indexes if row["indexname"] == CURRENT_WORKER_INDEX
    )
    assert "(worker_session_id) WHERE (terminated_at IS NULL)" in worker_index


async def insert_claim_dependencies(
    connection: asyncpg.Connection[asyncpg.Record],
) -> tuple[UUID, UUID, UUID]:
    principal_id, workflow_id, version_id, run_id = (
        uuid4(),
        uuid4(),
        uuid4(),
        uuid4(),
    )
    task_id, attempt_id = uuid4(), uuid4()
    identity_id, first_session_id, second_session_id = uuid4(), uuid4(), uuid4()
    await connection.execute(
        "INSERT INTO api_principals (id, name) VALUES ($1, $2)",
        principal_id,
        f"claim-owner-{uuid4().hex}",
    )
    await connection.execute(
        "INSERT INTO workflow_definitions (id, owner_principal_id, name) "
        "VALUES ($1, $2, $3)",
        workflow_id,
        principal_id,
        f"claim-workflow-{uuid4().hex}",
    )
    await connection.execute(
        "INSERT INTO workflow_versions "
        "(id, workflow_definition_id, version_number, name) VALUES ($1, $2, 1, $3)",
        version_id,
        workflow_id,
        "Claim version",
    )
    await connection.execute(
        "INSERT INTO workflow_version_steps "
        "(workflow_version_id, step_identifier, task_type, parameters) "
        "VALUES ($1, 'claim-step', 'test.task', '{}'::jsonb)",
        version_id,
    )
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
        "VALUES ($1, $2, $3, 'claim-step', 'dispatched')",
        task_id,
        run_id,
        version_id,
    )
    await connection.execute(
        "INSERT INTO task_attempts (id, task_run_id, attempt_number) "
        "VALUES ($1, $2, 1)",
        attempt_id,
        task_id,
    )
    await connection.execute(
        "INSERT INTO worker_identities (id, name) VALUES ($1, $2)",
        identity_id,
        f"claim-worker-{uuid4().hex}",
    )
    await connection.execute(
        "INSERT INTO worker_sessions (id, worker_identity_id) "
        "VALUES ($1, $3), ($2, $3)",
        first_session_id,
        second_session_id,
        identity_id,
    )
    return attempt_id, first_session_id, second_session_id


async def assert_two_connection_current_claim_race(
    database_url: URL,
    attempt_id: UUID,
    first_session_id: UUID,
    second_session_id: UUID,
) -> None:
    first = await asyncpg.connect(asyncpg_dsn(database_url))
    second = await asyncpg.connect(asyncpg_dsn(database_url))
    first_transaction = first.transaction()
    second_transaction = second.transaction()
    competing_insert: asyncio.Task[str] | None = None
    first_transaction_open = False
    second_transaction_open = False
    try:
        await first_transaction.start()
        first_transaction_open = True
        await second_transaction.start()
        second_transaction_open = True
        await first.execute(
            "INSERT INTO task_attempt_claims "
            "(task_attempt_id, worker_session_id, generation, lease_expires_at) "
            "VALUES ($1, $2, 1, statement_timestamp() + interval '1 minute')",
            attempt_id,
            first_session_id,
        )
        competing_insert = asyncio.create_task(
            second.execute(
                "INSERT INTO task_attempt_claims "
                "(task_attempt_id, worker_session_id, generation, "
                "lease_expires_at) VALUES "
                "($1, $2, 2, statement_timestamp() + interval '1 minute')",
                attempt_id,
                second_session_id,
            )
        )
        for _ in range(100):
            waiting = await first.fetchval(
                "SELECT wait_event_type = 'Lock' FROM pg_stat_activity WHERE pid = $1",
                second.get_server_pid(),
            )
            if waiting:
                break
            await asyncio.sleep(0.01)
        else:
            pytest.fail("competing claim did not wait on the uncommitted current claim")

        await first_transaction.commit()
        first_transaction_open = False
        with pytest.raises(asyncpg.UniqueViolationError):
            await competing_insert
        await second_transaction.rollback()
        second_transaction_open = False
    finally:
        if competing_insert is not None and not competing_insert.done():
            competing_insert.cancel()
            with suppress(asyncio.CancelledError):
                await competing_insert
        if second_transaction_open:
            with suppress(asyncpg.InterfaceError):
                await second_transaction.rollback()
        if first_transaction_open:
            with suppress(asyncpg.InterfaceError):
                await first_transaction.rollback()
        await first.close()
        await second.close()


async def inspect_claim_schema(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        await assert_claim_catalog(connection)
        (
            attempt_id,
            first_session_id,
            second_session_id,
        ) = await insert_claim_dependencies(connection)
        claimed_at = datetime.now(UTC)
        lease_expires_at = claimed_at + timedelta(minutes=1)
        await connection.execute(
            "INSERT INTO task_attempt_claims "
            "(task_attempt_id, worker_session_id, generation, acquired_at, "
            "lease_expires_at, terminated_at) VALUES ($1, $2, 1, $3, $4, $4)",
            attempt_id,
            first_session_id,
            claimed_at,
            lease_expires_at,
        )
        await connection.execute(
            "INSERT INTO task_attempt_claims "
            "(task_attempt_id, worker_session_id, generation, acquired_at, "
            "lease_expires_at) VALUES ($1, $2, 2, $3, $4)",
            attempt_id,
            second_session_id,
            claimed_at,
            lease_expires_at,
        )
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM task_attempt_claims WHERE task_attempt_id = $1",
                attempt_id,
            )
            == 2
        )

        invalid_values = [
            (0, claimed_at, lease_expires_at, None),
            (3, claimed_at, claimed_at, None),
            (3, claimed_at, lease_expires_at, claimed_at - timedelta(seconds=1)),
        ]
        for generation, claim_time, expiry, end_time in invalid_values:
            with pytest.raises(asyncpg.CheckViolationError):
                await connection.execute(
                    "INSERT INTO task_attempt_claims "
                    "(task_attempt_id, worker_session_id, generation, acquired_at, "
                    "lease_expires_at, terminated_at) "
                    "VALUES ($1, $2, $3, $4, $5, $6)",
                    attempt_id,
                    first_session_id,
                    generation,
                    claim_time,
                    expiry,
                    end_time,
                )
        with pytest.raises(asyncpg.UniqueViolationError):
            await connection.execute(
                "INSERT INTO task_attempt_claims "
                "(task_attempt_id, worker_session_id, generation, acquired_at, "
                "lease_expires_at, terminated_at) VALUES ($1, $2, 1, $3, $4, $4)",
                attempt_id,
                second_session_id,
                claimed_at,
                lease_expires_at,
            )
        for missing_attempt, missing_session in (
            (uuid4(), first_session_id),
            (attempt_id, uuid4()),
        ):
            with pytest.raises(asyncpg.ForeignKeyViolationError):
                await connection.execute(
                    "INSERT INTO task_attempt_claims "
                    "(task_attempt_id, worker_session_id, generation, "
                    "lease_expires_at, terminated_at) VALUES "
                    "($1, $2, 4, statement_timestamp() + interval '1 minute', "
                    "statement_timestamp())",
                    missing_attempt,
                    missing_session,
                )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await connection.execute(
                "DELETE FROM task_attempts WHERE id = $1", attempt_id
            )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await connection.execute(
                "DELETE FROM worker_sessions WHERE id = $1", first_session_id
            )
        await connection.execute(
            "DELETE FROM task_attempt_claims WHERE task_attempt_id = $1", attempt_id
        )
    finally:
        await connection.close()

    await assert_two_connection_current_claim_race(
        database_url, attempt_id, first_session_id, second_session_id
    )


async def assert_claim_schema_recreated(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        await assert_claim_catalog(connection)
    finally:
        await connection.close()


def test_task_claim_migration_constraints_and_reversible_boundary() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_claim_migration"
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        configuration = Config("alembic.ini")
        with migration_database_url(alembic_url):
            command.upgrade(configuration, "0008_worker_sessions_health")
            asyncio.run(assert_claim_table_absent(database_url))
            command.upgrade(configuration, "0009_task_claim_history")
            asyncio.run(inspect_claim_schema(database_url))
            command.downgrade(configuration, "0008_worker_sessions_health")
            asyncio.run(assert_claim_table_absent(database_url))
            command.upgrade(configuration, "head")
            asyncio.run(assert_claim_schema_recreated(database_url))
