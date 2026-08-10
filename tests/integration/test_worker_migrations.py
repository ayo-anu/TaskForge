"""Opt-in worker-session and heartbeat migration verification."""

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

WORKER_TABLES = {
    "worker_sessions",
    "worker_session_capabilities",
    "worker_session_health",
    "worker_heartbeats",
}
WORKER_INDEXES = {
    "ix_worker_sessions_worker_identity_id_registered_at_id",
    "ix_worker_sessions_open_registered_at_id",
    "ix_worker_session_capabilities_capability_worker_session_id",
    "ix_worker_session_health_last_seen_at_worker_session_id",
}


async def assert_worker_tables_absent(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM pg_tables WHERE schemaname = 'public' "
                "AND tablename = ANY($1::text[])",
                list(WORKER_TABLES),
            )
            == 0
        )
    finally:
        await connection.close()


async def assert_worker_catalog(connection: asyncpg.Connection[asyncpg.Record]) -> None:
    assert (
        await connection.fetchval(
            "SELECT count(*) FROM pg_tables WHERE schemaname = 'public' "
            "AND tablename = ANY($1::text[])",
            list(WORKER_TABLES),
        )
        == 4
    )
    constraints = await connection.fetch(
        "SELECT c.relname AS table_name, con.conname, con.contype::text, "
        "con.confupdtype::text, con.confdeltype::text "
        "FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'public' AND c.relname = ANY($1::text[])",
        list(WORKER_TABLES),
    )
    assert {(row["table_name"], row["conname"]) for row in constraints} == {
        ("worker_sessions", "pk_worker_sessions"),
        ("worker_sessions", "ck_worker_sessions_ended_not_before_registration"),
        ("worker_sessions", "fk_worker_sessions_worker_identity_id_worker_identities"),
        ("worker_session_capabilities", "pk_worker_session_capabilities"),
        (
            "worker_session_capabilities",
            "ck_worker_session_capabilities_capability_valid",
        ),
        (
            "worker_session_capabilities",
            "fk_worker_session_capabilities_worker_session",
        ),
        ("worker_session_health", "pk_worker_session_health"),
        ("worker_session_health", "ck_worker_session_health_last_sequence_nonnegative"),
        (
            "worker_session_health",
            "ck_worker_session_health_availability_not_after_last_seen",
        ),
        (
            "worker_session_health",
            "fk_worker_session_health_worker_session_id_worker_sessions",
        ),
        ("worker_heartbeats", "pk_worker_heartbeats"),
        ("worker_heartbeats", "ck_worker_heartbeats_sequence_positive"),
        ("worker_heartbeats", "fk_worker_heartbeats_worker_session_id_worker_sessions"),
    }
    foreign_keys = [row for row in constraints if row["contype"] == "f"]
    assert len(foreign_keys) == 4
    assert all(row["confupdtype"] == "r" for row in foreign_keys)
    assert all(row["confdeltype"] == "r" for row in foreign_keys)
    indexes = await connection.fetch(
        "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'public' "
        "AND indexname = ANY($1::text[])",
        list(WORKER_INDEXES),
    )
    assert {row["indexname"] for row in indexes} == WORKER_INDEXES
    open_index = next(
        row["indexdef"]
        for row in indexes
        if row["indexname"] == "ix_worker_sessions_open_registered_at_id"
    )
    assert "WHERE (ended_at IS NULL)" in open_index


async def insert_identity_and_sessions(
    connection: asyncpg.Connection[asyncpg.Record],
) -> tuple[UUID, UUID, UUID]:
    identity_id, first_session_id, second_session_id = uuid4(), uuid4(), uuid4()
    await connection.execute(
        "INSERT INTO worker_identities (id, name) VALUES ($1, $2)",
        identity_id,
        f"worker-{uuid4().hex}",
    )
    await connection.execute(
        "INSERT INTO worker_sessions (id, worker_identity_id) "
        "VALUES ($1, $3), ($2, $3)",
        first_session_id,
        second_session_id,
        identity_id,
    )
    return identity_id, first_session_id, second_session_id


async def assert_concurrent_uniqueness(
    database_url: URL, identity_id: UUID, session_id: UUID
) -> None:
    async def insert_distinct_session(session_to_insert: UUID) -> str:
        connection = await asyncpg.connect(asyncpg_dsn(database_url))
        try:
            result = await connection.execute(
                "INSERT INTO worker_sessions (id, worker_identity_id) VALUES ($1, $2)",
                session_to_insert,
                identity_id,
            )
            assert isinstance(result, str)
            return result
        finally:
            await connection.close()

    session_results = await asyncio.gather(
        insert_distinct_session(uuid4()), insert_distinct_session(uuid4())
    )
    assert len(session_results) == 2
    assert all(result == "INSERT 0 1" for result in session_results)

    async def insert_capability() -> str:
        connection = await asyncpg.connect(asyncpg_dsn(database_url))
        try:
            result = await connection.execute(
                "INSERT INTO worker_session_capabilities "
                "(worker_session_id, capability) VALUES ($1, 'document-processing')",
                session_id,
            )
            assert isinstance(result, str)
            return result
        finally:
            await connection.close()

    capability_results = await asyncio.gather(
        insert_capability(), insert_capability(), return_exceptions=True
    )
    assert sum(result == "INSERT 0 1" for result in capability_results) == 1
    assert (
        sum(
            isinstance(result, asyncpg.UniqueViolationError)
            for result in capability_results
        )
        == 1
    )

    async def insert_heartbeat() -> str:
        connection = await asyncpg.connect(asyncpg_dsn(database_url))
        try:
            result = await connection.execute(
                "INSERT INTO worker_heartbeats "
                "(worker_session_id, sequence, accepting_work) VALUES ($1, 1, true)",
                session_id,
            )
            assert isinstance(result, str)
            return result
        finally:
            await connection.close()

    heartbeat_results = await asyncio.gather(
        insert_heartbeat(), insert_heartbeat(), return_exceptions=True
    )
    assert sum(result == "INSERT 0 1" for result in heartbeat_results) == 1
    assert (
        sum(
            isinstance(result, asyncpg.UniqueViolationError)
            for result in heartbeat_results
        )
        == 1
    )


async def inspect_worker_schema(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        await assert_worker_catalog(connection)
        (
            identity_id,
            first_session_id,
            second_session_id,
        ) = await insert_identity_and_sessions(connection)
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM worker_sessions WHERE worker_identity_id = $1 "
                "AND ended_at IS NULL",
                identity_id,
            )
            == 2
        )
        registered_at = await connection.fetchval(
            "SELECT registered_at FROM worker_sessions WHERE id = $1",
            first_session_id,
        )
        assert registered_at is not None

        await connection.execute(
            "INSERT INTO worker_session_health "
            "(worker_session_id, last_seen_at, accepting_work, availability_changed_at) "
            "VALUES ($1, $2, true, $2)",
            first_session_id,
            registered_at,
        )
        assert (
            await connection.fetchval(
                "SELECT last_sequence FROM worker_session_health "
                "WHERE worker_session_id = $1",
                first_session_id,
            )
            == 0
        )

        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                "INSERT INTO worker_session_capabilities "
                "(worker_session_id, capability) VALUES ($1, 'Invalid Capability')",
                first_session_id,
            )
        for invalid_sequence in (0, -1):
            with pytest.raises(asyncpg.CheckViolationError):
                await connection.execute(
                    "INSERT INTO worker_heartbeats "
                    "(worker_session_id, sequence, accepting_work) "
                    "VALUES ($1, $2, true)",
                    second_session_id,
                    invalid_sequence,
                )
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                "UPDATE worker_session_health SET last_sequence = -1 "
                "WHERE worker_session_id = $1",
                first_session_id,
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                "UPDATE worker_sessions SET ended_at = $1 WHERE id = $2",
                datetime(2000, 1, 1, tzinfo=UTC),
                first_session_id,
            )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await connection.execute(
                "INSERT INTO worker_sessions (id, worker_identity_id) VALUES ($1, $2)",
                uuid4(),
                uuid4(),
            )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await connection.execute(
                "DELETE FROM worker_identities WHERE id = $1", identity_id
            )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await connection.execute(
                "DELETE FROM worker_sessions WHERE id = $1", first_session_id
            )
    finally:
        await connection.close()

    await assert_concurrent_uniqueness(database_url, identity_id, second_session_id)


async def assert_worker_schema_recreated(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        await assert_worker_catalog(connection)
    finally:
        await connection.close()


def test_worker_migration_constraints_and_reversible_boundary() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_worker_migration"
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        configuration = Config("alembic.ini")
        with migration_database_url(alembic_url):
            command.upgrade(configuration, "0007_attempt_dispatch_outbox")
            asyncio.run(assert_worker_tables_absent(database_url))
            command.upgrade(configuration, "0008_worker_sessions_health")
            asyncio.run(inspect_worker_schema(database_url))
            command.downgrade(configuration, "0007_attempt_dispatch_outbox")
            asyncio.run(assert_worker_tables_absent(database_url))
            command.upgrade(configuration, "head")
            asyncio.run(assert_worker_schema_recreated(database_url))
