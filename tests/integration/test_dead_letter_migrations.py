"""Real PostgreSQL validation for dead-letter persistence invariants."""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from typing import cast
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

REVISION = "0018_dead_letter_persistence"
PREVIOUS_REVISION = "0017_recovery_result_events"
IMMUTABLE_TABLES = (
    "dead_letter_items",
    "dead_letter_operator_actions",
    "dead_letter_redrive_requests",
)


@dataclass(frozen=True)
class Facts:
    principal: UUID
    other_principal: UUID
    task_run: UUID
    other_task_run: UUID
    attempt: UUID
    other_attempt: UUID
    unlettered_attempt: UUID
    item: UUID
    other_item: UUID


async def seed_facts(connection: asyncpg.Connection) -> Facts:
    principal, other_principal = uuid4(), uuid4()
    workflow, version, run = uuid4(), uuid4(), uuid4()
    task_run, other_task_run = uuid4(), uuid4()
    attempt, other_attempt, unlettered_attempt = uuid4(), uuid4(), uuid4()
    worker_identity, worker_session = uuid4(), uuid4()
    dispatch, other_dispatch, unlettered_dispatch = uuid4(), uuid4(), uuid4()
    item, other_item = uuid4(), uuid4()

    await connection.executemany(
        "INSERT INTO api_principals (id, name) VALUES ($1, $2)",
        [
            (principal, f"dlq-{principal.hex}"),
            (other_principal, f"dlq-{other_principal.hex}"),
        ],
    )
    await connection.execute(
        "INSERT INTO worker_identities (id, name) VALUES ($1, $2)",
        worker_identity,
        f"dlq-worker-{worker_identity.hex}",
    )
    await connection.execute(
        "INSERT INTO worker_sessions (id, worker_identity_id) VALUES ($1, $2)",
        worker_session,
        worker_identity,
    )
    await connection.execute(
        "INSERT INTO workflow_definitions (id, owner_principal_id, name) "
        "VALUES ($1, $2, 'dead letters')",
        workflow,
        principal,
    )
    await connection.execute(
        "INSERT INTO workflow_versions "
        "(id, workflow_definition_id, version_number, name) "
        "VALUES ($1, $2, 1, 'dead letters')",
        version,
        workflow,
    )
    await connection.execute(
        "INSERT INTO workflow_version_steps "
        "(workflow_version_id, step_identifier, task_type, parameters) VALUES "
        "($1, 'one', 'test.task', '{}'::jsonb), "
        "($1, 'two', 'test.task', '{}'::jsonb)",
        version,
    )
    await connection.execute(
        "INSERT INTO workflow_runs (id, workflow_definition_id, "
        "workflow_version_id, requested_by_principal_id, status) "
        "VALUES ($1, $2, $3, $4, 'failed')",
        run,
        workflow,
        version,
        principal,
    )
    await connection.executemany(
        "INSERT INTO task_runs (id, workflow_run_id, workflow_version_id, "
        "step_identifier, status) VALUES ($1, $2, $3, $4, 'failed')",
        [
            (task_run, run, version, "one"),
            (other_task_run, run, version, "two"),
        ],
    )
    await connection.executemany(
        "INSERT INTO task_attempts (id, task_run_id, attempt_number) "
        "VALUES ($1, $2, $3)",
        [
            (attempt, task_run, 1),
            (other_attempt, other_task_run, 1),
            (unlettered_attempt, other_task_run, 2),
        ],
    )
    await connection.executemany(
        "INSERT INTO task_dispatch_outbox (id, task_attempt_id, route, payload) "
        "VALUES ($1, $2, 'test.task', '{}'::jsonb)",
        [
            (dispatch, attempt),
            (other_dispatch, other_attempt),
            (unlettered_dispatch, unlettered_attempt),
        ],
    )
    await connection.executemany(
        "INSERT INTO task_attempt_claims (task_attempt_id, generation, "
        "worker_session_id, lease_expires_at, terminated_at) "
        "VALUES ($1, 1, $2, statement_timestamp() + interval '1 minute', "
        "statement_timestamp())",
        [
            (attempt, worker_session),
            (other_attempt, worker_session),
            (unlettered_attempt, worker_session),
        ],
    )
    await connection.executemany(
        "INSERT INTO task_attempt_results (task_attempt_id, claim_generation, "
        "dispatch_id, result_kind, failure_kind, result_fingerprint) "
        "VALUES ($1, 1, $2, 'permanent_failure', 'handler_reported', $3)",
        [
            (attempt, dispatch, "a" * 64),
            (other_attempt, other_dispatch, "b" * 64),
            (unlettered_attempt, unlettered_dispatch, "c" * 64),
        ],
    )
    await connection.executemany(
        "INSERT INTO dead_letter_items "
        "(id, task_run_id, source_task_attempt_id, reason) "
        "VALUES ($1, $2, $3, 'permanent_failure')",
        [
            (item, task_run, attempt),
            (other_item, other_task_run, other_attempt),
        ],
    )
    await connection.executemany(
        "INSERT INTO dead_letter_status (dead_letter_item_id) VALUES ($1)",
        [(item,), (other_item,)],
    )
    return Facts(
        principal,
        other_principal,
        task_run,
        other_task_run,
        attempt,
        other_attempt,
        unlettered_attempt,
        item,
        other_item,
    )


async def assert_schema(connection: asyncpg.Connection) -> None:
    tables = {
        row["table_name"]
        for row in await connection.fetch(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name LIKE 'dead_letter_%'"
        )
    }
    assert tables == {
        "dead_letter_items",
        "dead_letter_status",
        "dead_letter_operator_actions",
        "dead_letter_redrive_requests",
    }
    status_columns = {
        row["column_name"]
        for row in await connection.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'dead_letter_status'"
        )
    }
    assert status_columns == {"dead_letter_item_id", "status", "updated_at"}
    assert not await connection.fetchval(
        "SELECT to_regclass('public.dead_letter_redrive_lineage') IS NOT NULL"
    )
    for table, timestamp in (
        ("dead_letter_items", "created_at"),
        ("dead_letter_status", "updated_at"),
        ("dead_letter_operator_actions", "occurred_at"),
        ("dead_letter_redrive_requests", "requested_at"),
    ):
        column = await connection.fetchrow(
            "SELECT data_type, column_default FROM information_schema.columns "
            "WHERE table_name = $1 AND column_name = $2",
            table,
            timestamp,
        )
        assert column["data_type"] == "timestamp with time zone"
        assert column["column_default"] == "statement_timestamp()"
    fk_actions = await connection.fetch(
        "SELECT confupdtype::text, confdeltype::text FROM pg_constraint "
        "WHERE contype = 'f' "
        "AND conrelid IN ('dead_letter_items'::regclass, "
        "'dead_letter_status'::regclass, "
        "'dead_letter_operator_actions'::regclass, "
        "'dead_letter_redrive_requests'::regclass)"
    )
    assert fk_actions
    assert all(
        row["confupdtype"] == "r" and row["confdeltype"] == "r" for row in fk_actions
    )
    assert await connection.fetchval(
        "SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE "
        "conname = 'uq_task_attempts_task_run_id_id')"
    )


async def assert_invariants(connection: asyncpg.Connection, facts: Facts) -> None:
    with pytest.raises(asyncpg.CheckViolationError):
        await connection.execute(
            "INSERT INTO dead_letter_items "
            "(id, task_run_id, source_task_attempt_id, reason) "
            "VALUES ($1, $2, $3, 'transient_failure')",
            uuid4(),
            facts.other_task_run,
            facts.unlettered_attempt,
        )
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await connection.execute(
            "INSERT INTO dead_letter_items "
            "(id, task_run_id, source_task_attempt_id, reason) "
            "VALUES ($1, $2, $3, 'permanent_failure')",
            uuid4(),
            facts.task_run,
            facts.unlettered_attempt,
        )
    with pytest.raises(asyncpg.UniqueViolationError):
        await connection.execute(
            "INSERT INTO dead_letter_items "
            "(id, task_run_id, source_task_attempt_id, reason) "
            "VALUES ($1, $2, $3, 'retry_exhausted')",
            uuid4(),
            facts.task_run,
            facts.attempt,
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await connection.execute(
            "UPDATE dead_letter_status SET status = 'redriving' "
            "WHERE dead_letter_item_id = $1",
            facts.item,
        )

    acknowledged = uuid4()
    await connection.execute(
        "INSERT INTO dead_letter_operator_actions "
        "(id, dead_letter_item_id, operator_principal_id, action_type, "
        "previous_status, new_status, reason) "
        "VALUES ($1, $2, $3, 'acknowledged', 'open', 'acknowledged', 'seen')",
        acknowledged,
        facts.item,
        facts.principal,
    )
    for action, previous, new, reason in (
        ("redrive_requested", "open", "acknowledged", None),
        ("resolved", "open", "resolved", None),
        ("acknowledged", "open", "acknowledged", "   "),
        ("acknowledged", "open", "acknowledged", "x" * 2001),
    ):
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                "INSERT INTO dead_letter_operator_actions "
                "(id, dead_letter_item_id, operator_principal_id, action_type, "
                "previous_status, new_status, reason) VALUES ($1, $2, $3, $4, "
                "$5, $6, $7)",
                uuid4(),
                facts.item,
                facts.principal,
                action,
                previous,
                new,
                reason,
            )

    request = uuid4()
    correlation = uuid4()
    await connection.execute(
        "INSERT INTO dead_letter_redrive_requests "
        "(id, dead_letter_item_id, requested_by_principal_id, "
        "idempotency_key_digest, request_fingerprint, correlation_id) "
        "VALUES ($1, $2, $3, $4, $5, $6)",
        request,
        facts.item,
        facts.principal,
        "c" * 64,
        "d" * 64,
        correlation,
    )
    with pytest.raises(asyncpg.UniqueViolationError):
        await connection.execute(
            "INSERT INTO dead_letter_redrive_requests "
            "(id, dead_letter_item_id, requested_by_principal_id, "
            "idempotency_key_digest, request_fingerprint) "
            "VALUES ($1, $2, $3, $4, $5)",
            uuid4(),
            facts.item,
            facts.principal,
            "c" * 64,
            "e" * 64,
        )
    await connection.execute(
        "INSERT INTO dead_letter_redrive_requests "
        "(id, dead_letter_item_id, requested_by_principal_id, "
        "idempotency_key_digest, request_fingerprint) "
        "VALUES ($1, $2, $3, $4, $5), ($6, $7, $8, $4, $5)",
        uuid4(),
        facts.item,
        facts.other_principal,
        "c" * 64,
        "d" * 64,
        uuid4(),
        facts.other_item,
        facts.principal,
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await connection.execute(
            "INSERT INTO dead_letter_redrive_requests "
            "(id, dead_letter_item_id, requested_by_principal_id, "
            "idempotency_key_digest, request_fingerprint) "
            "VALUES ($1, $2, $3, 'not-a-digest', $4)",
            uuid4(),
            facts.item,
            facts.principal,
            "f" * 64,
        )

    for table, row_id in (
        ("dead_letter_items", facts.item),
        ("dead_letter_operator_actions", acknowledged),
        ("dead_letter_redrive_requests", request),
    ):
        with pytest.raises(asyncpg.PostgresError) as raised:
            await connection.execute(
                f"UPDATE {table} SET id = id WHERE id = $1", row_id
            )
        assert raised.value.sqlstate == "TF006"
        with pytest.raises(asyncpg.PostgresError) as raised:
            await connection.execute(f"DELETE FROM {table} WHERE id = $1", row_id)
        assert raised.value.sqlstate == "TF006"
        cascade = " CASCADE" if table == "dead_letter_items" else ""
        with pytest.raises(asyncpg.PostgresError) as raised:
            await connection.execute(f"TRUNCATE {table}{cascade}")
        assert raised.value.sqlstate == "TF006"

    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await connection.execute(
            "DELETE FROM task_attempts WHERE id = $1",
            facts.attempt,
        )
    lineage = await connection.fetchrow(
        "SELECT request.id AS request_id, item.source_task_attempt_id, "
        "dispatch.id AS dispatch_id FROM dead_letter_redrive_requests request "
        "JOIN dead_letter_items item ON item.id = request.dead_letter_item_id "
        "JOIN task_dispatch_outbox dispatch "
        "ON dispatch.task_attempt_id = item.source_task_attempt_id "
        "WHERE request.id = $1",
        request,
    )
    assert lineage is not None
    assert lineage["request_id"] == request
    assert lineage["source_task_attempt_id"] == facts.attempt


async def assert_concurrent_idempotency(database_url: URL, facts: Facts) -> None:
    first = await asyncpg.connect(asyncpg_dsn(database_url))
    second = await asyncpg.connect(asyncpg_dsn(database_url))
    digest = "1" * 64

    async def insert(connection: asyncpg.Connection) -> UUID | None:
        return cast(
            UUID | None,
            await connection.fetchval(
                "INSERT INTO dead_letter_redrive_requests "
                "(id, dead_letter_item_id, requested_by_principal_id, "
                "idempotency_key_digest, request_fingerprint) "
                "VALUES ($1, $2, $3, $4, $5) "
                "ON CONFLICT DO NOTHING RETURNING id",
                uuid4(),
                facts.item,
                facts.principal,
                digest,
                "2" * 64,
            ),
        )

    try:
        results = await asyncio.gather(insert(first), insert(second))
        assert sum(result is not None for result in results) == 1
        assert (
            await first.fetchval(
                "SELECT count(*) FROM dead_letter_redrive_requests WHERE "
                "dead_letter_item_id = $1 AND requested_by_principal_id = $2 "
                "AND idempotency_key_digest = $3",
                facts.item,
                facts.principal,
                digest,
            )
            == 1
        )
    finally:
        await first.close()
        await second.close()


async def assert_concurrent_dead_letter_creation(
    database_url: URL, facts: Facts
) -> None:
    first = await asyncpg.connect(asyncpg_dsn(database_url))
    second = await asyncpg.connect(asyncpg_dsn(database_url))

    async def insert(connection: asyncpg.Connection) -> UUID | None:
        return cast(
            UUID | None,
            await connection.fetchval(
                "INSERT INTO dead_letter_items "
                "(id, task_run_id, source_task_attempt_id, reason) "
                "VALUES ($1, $2, $3, 'permanent_failure') "
                "ON CONFLICT DO NOTHING RETURNING id",
                uuid4(),
                facts.other_task_run,
                facts.unlettered_attempt,
            ),
        )

    try:
        results = await asyncio.gather(insert(first), insert(second))
        assert sum(result is not None for result in results) == 1
        assert (
            await first.fetchval(
                "SELECT count(*) FROM dead_letter_items "
                "WHERE source_task_attempt_id = $1",
                facts.unlettered_attempt,
            )
            == 1
        )
    finally:
        await first.close()
        await second.close()


async def exercise_schema(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        await assert_schema(connection)
        facts = await seed_facts(connection)
        await assert_invariants(connection, facts)
    finally:
        await connection.close()
    await assert_concurrent_dead_letter_creation(database_url, facts)
    await assert_concurrent_idempotency(database_url, facts)


async def assert_downgraded(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        for table in (
            "dead_letter_items",
            "dead_letter_status",
            "dead_letter_operator_actions",
            "dead_letter_redrive_requests",
        ):
            assert not await connection.fetchval(
                "SELECT to_regclass($1) IS NOT NULL", f"public.{table}"
            )
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE "
            "conname = 'uq_task_attempts_task_run_id_id')"
        )
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_proc WHERE "
            "proname = 'reject_dead_letter_history_mutation')"
        )
    finally:
        await connection.close()


async def assert_reupgraded(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        await assert_schema(connection)
    finally:
        await connection.close()


def test_dead_letter_upgrade_downgrade_reupgrade() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_dead_letter_mig"
    ) as database_url:
        configuration = Config("alembic.ini")
        rendered = database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        with migration_database_url(rendered):
            command.upgrade(configuration, PREVIOUS_REVISION)
            command.upgrade(configuration, REVISION)
            asyncio.run(exercise_schema(database_url))
            command.downgrade(configuration, PREVIOUS_REVISION)
            asyncio.run(assert_downgraded(database_url))
            command.upgrade(configuration, REVISION)
            asyncio.run(assert_reupgraded(database_url))
