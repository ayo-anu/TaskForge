"""Opt-in immutable task-claim event migration verification."""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
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
from tests.integration.test_task_claim_migrations import insert_claim_dependencies

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_MIGRATION_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_MIGRATION_INTEGRATION=1 explicitly",
    ),
]


async def assert_event_table_absent(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        assert not await connection.fetchval(
            "SELECT to_regclass('public.task_claim_events') IS NOT NULL"
        )
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT FROM pg_proc WHERE proname = "
            "'reject_task_claim_event_mutation')"
        )
    finally:
        await connection.close()


async def assert_event_catalog_and_immutability(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        columns = await connection.fetch(
            "SELECT column_name, data_type, is_nullable, column_default "
            "FROM information_schema.columns WHERE table_schema = 'public' "
            "AND table_name = 'task_claim_events' ORDER BY ordinal_position"
        )
        assert [tuple(row.values()) for row in columns] == [
            ("id", "uuid", "NO", None),
            ("task_attempt_id", "uuid", "NO", None),
            ("generation", "bigint", "NO", None),
            ("event_type", "character varying", "NO", None),
            ("occurred_at", "timestamp with time zone", "NO", None),
            ("previous_lease_expires_at", "timestamp with time zone", "YES", None),
            ("lease_expires_at", "timestamp with time zone", "NO", None),
        ]
        constraints = await connection.fetch(
            "SELECT conname, contype::text, pg_get_constraintdef(oid) AS definition, "
            "confupdtype::text, confdeltype::text FROM pg_constraint "
            "WHERE conrelid = 'task_claim_events'::regclass"
        )
        assert {row["conname"] for row in constraints} == {
            "pk_task_claim_events",
            "fk_task_claim_events_claim_generation",
            "ck_task_claim_events_event_type_valid",
            "ck_task_claim_events_event_shape_valid",
        }
        event_shape = next(
            row["definition"]
            for row in constraints
            if row["conname"] == "ck_task_claim_events_event_shape_valid"
        )
        assert "lease_expires_at > occurred_at" in event_shape
        assert "lease_expires_at > previous_lease_expires_at" in event_shape
        foreign_key = next(row for row in constraints if row["contype"] == "f")
        assert foreign_key["confupdtype"] == foreign_key["confdeltype"] == "r"
        assert "FOREIGN KEY (task_attempt_id, generation)" in foreign_key["definition"]
        indexes = await connection.fetch(
            "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'public' "
            "AND tablename = 'task_claim_events' ORDER BY indexname"
        )
        assert {row["indexname"] for row in indexes} == {
            "pk_task_claim_events",
            "uq_task_claim_events_acquired_generation",
            "uq_task_claim_events_renewal_transition",
        }
        assert not any("occurred_at" in row["indexdef"] for row in indexes)
        triggers = await connection.fetch(
            "SELECT tgname FROM pg_trigger WHERE tgrelid = "
            "'task_claim_events'::regclass AND NOT tgisinternal"
        )
        assert {row["tgname"] for row in triggers} == {
            "trg_task_claim_events_reject_mutation",
            "trg_task_claim_events_reject_truncate",
        }

        attempt_id, session_id, _ = await insert_claim_dependencies(connection)
        acquired_at = datetime.now(UTC)
        expiry = acquired_at + timedelta(minutes=1)
        await connection.execute(
            "INSERT INTO task_attempt_claims "
            "(task_attempt_id, generation, worker_session_id, acquired_at, "
            "lease_expires_at) VALUES ($1, 1, $2, $3, $4)",
            attempt_id,
            session_id,
            acquired_at,
            expiry,
        )
        await connection.execute(
            "INSERT INTO task_attempt_claims "
            "(task_attempt_id, generation, worker_session_id, acquired_at, "
            "lease_expires_at, terminated_at) VALUES "
            "($1, 2, $2, $3, $4, $4), ($1, 3, $2, $3, $4, $4)",
            attempt_id,
            session_id,
            acquired_at,
            expiry,
        )
        event_id = uuid4()
        await connection.execute(
            "INSERT INTO task_claim_events "
            "(id, task_attempt_id, generation, event_type, occurred_at, "
            "previous_lease_expires_at, lease_expires_at) "
            "VALUES ($1, $2, 1, 'claim_acquired', $3, NULL, $4)",
            event_id,
            attempt_id,
            acquired_at,
            expiry,
        )
        renewed_expiry = expiry + timedelta(minutes=1)
        await connection.execute(
            "INSERT INTO task_claim_events "
            "(id, task_attempt_id, generation, event_type, occurred_at, "
            "previous_lease_expires_at, lease_expires_at) "
            "VALUES ($1, $2, 1, 'lease_renewed', $3, $4, $5)",
            uuid4(),
            attempt_id,
            acquired_at + timedelta(seconds=1),
            expiry,
            renewed_expiry,
        )
        for generation, event_expiry in (
            (2, acquired_at),
            (3, acquired_at - timedelta(microseconds=1)),
        ):
            with pytest.raises(asyncpg.CheckViolationError):
                await connection.execute(
                    "INSERT INTO task_claim_events "
                    "(id, task_attempt_id, generation, event_type, occurred_at, "
                    "lease_expires_at) VALUES "
                    "($1, $2, $3, 'claim_acquired', $4, $5)",
                    uuid4(),
                    attempt_id,
                    generation,
                    acquired_at,
                    event_expiry,
                )
        invalid_renewals = (
            (2, None, renewed_expiry),
            (2, expiry, expiry),
            (3, expiry, expiry - timedelta(microseconds=1)),
        )
        for generation, previous_expiry, event_expiry in invalid_renewals:
            with pytest.raises(asyncpg.CheckViolationError):
                await connection.execute(
                    "INSERT INTO task_claim_events "
                    "(id, task_attempt_id, generation, event_type, occurred_at, "
                    "previous_lease_expires_at, lease_expires_at) VALUES "
                    "($1, $2, $3, 'lease_renewed', $4, $5, $6)",
                    uuid4(),
                    attempt_id,
                    generation,
                    acquired_at + timedelta(seconds=1),
                    previous_expiry,
                    event_expiry,
                )
        for statement in (
            "UPDATE task_claim_events SET lease_expires_at = lease_expires_at "
            "WHERE id = $1",
            "DELETE FROM task_claim_events WHERE id = $1",
        ):
            with pytest.raises(asyncpg.PostgresError) as raised:
                await connection.execute(statement, event_id)
            assert raised.value.sqlstate == "TF003"
        with pytest.raises(asyncpg.PostgresError) as raised:
            await connection.execute("TRUNCATE task_claim_events")
        assert raised.value.sqlstate == "TF003"

        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                "INSERT INTO task_claim_events "
                "(id, task_attempt_id, generation, event_type, occurred_at, "
                "lease_expires_at) VALUES ($1, $2, 1, 'lease_released', $3, $4)",
                uuid4(),
                attempt_id,
                acquired_at,
                expiry,
            )
        with pytest.raises(asyncpg.NotNullViolationError):
            await connection.execute(
                "INSERT INTO task_claim_events "
                "(id, task_attempt_id, generation, event_type, lease_expires_at) "
                "VALUES ($1, $2, 1, 'claim_acquired', $3)",
                uuid4(),
                attempt_id,
                expiry,
            )
    finally:
        await connection.close()


def test_task_claim_event_migration_is_exact_immutable_and_reversible() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL",
        "taskforge_claim_event_mig",
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        configuration = Config("alembic.ini")
        with migration_database_url(alembic_url):
            command.upgrade(configuration, "0009_task_claim_history")
            asyncio.run(assert_event_table_absent(database_url))
            command.upgrade(configuration, "0010_task_claim_events")
            asyncio.run(assert_event_catalog_and_immutability(database_url))
            command.downgrade(configuration, "0009_task_claim_history")
            asyncio.run(assert_event_table_absent(database_url))
            command.upgrade(configuration, "head")
            asyncio.run(assert_event_catalog_and_immutability(database_url))
