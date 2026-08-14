"""Real PostgreSQL validation for immutable recovery-result events."""

from __future__ import annotations

import asyncio
import os
from uuid import UUID, uuid4

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL
from sqlalchemy.exc import DBAPIError

from taskforge.worker.results import (
    TaskExecutionFailureKind,
    TaskExecutionResultKind,
    task_result_fingerprint,
)
from tests.integration.postgresql import (
    asyncpg_dsn,
    migration_database_url,
    temporary_database,
)
from tests.integration.test_recovery_transition import recoverable_candidate

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_MIGRATION_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_MIGRATION_INTEGRATION=1 explicitly",
    ),
]

REVISION = "0017_recovery_result_events"
PREVIOUS_REVISION = "0016_claim_expired_result"


async def seed_pre_task_4_recovery(
    database_url: URL, *, terminate_claim: bool = True, add_ordinary_event: bool = False
) -> tuple[UUID, int, UUID | None]:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        candidate = await recoverable_candidate(connection)
        dispatch_id = await connection.fetchval(
            "SELECT id FROM task_dispatch_outbox WHERE task_attempt_id = $1",
            candidate.task_attempt_id,
        )
        fingerprint = task_result_fingerprint(
            result_kind=TaskExecutionResultKind.RETRYABLE_FAILURE,
            failure_kind=TaskExecutionFailureKind.CLAIM_EXPIRED,
            output=None,
        )
        await connection.execute(
            "INSERT INTO task_attempt_results (task_attempt_id, claim_generation, "
            "dispatch_id, result_kind, failure_kind, output, result_fingerprint) "
            "VALUES ($1, $2, $3, 'retryable_failure', 'claim_expired', NULL, $4)",
            candidate.task_attempt_id,
            candidate.generation,
            dispatch_id,
            fingerprint,
        )
        if terminate_claim:
            await connection.execute(
                "UPDATE task_attempt_claims SET terminated_at = statement_timestamp() "
                "WHERE task_attempt_id = $1 AND generation = $2",
                candidate.task_attempt_id,
                candidate.generation,
            )
        ordinary_event_id = uuid4() if add_ordinary_event else None
        if ordinary_event_id is not None:
            await connection.execute(
                "INSERT INTO task_result_events (id, task_attempt_id, "
                "claim_generation, worker_session_id, dispatch_id, event_type, "
                "result_kind, failure_kind, result_fingerprint) VALUES "
                "($1, $2, $3, $4, $5, 'result_stale_rejected', 'success', "
                "NULL, $6)",
                ordinary_event_id,
                candidate.task_attempt_id,
                candidate.generation,
                candidate.worker_session_id,
                dispatch_id,
                "e" * 64,
            )
        return candidate.task_attempt_id, candidate.generation, ordinary_event_id
    finally:
        await connection.close()


async def verify_upgrade(database_url: URL, attempt_id: UUID, generation: int) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        backfilled = await connection.fetchrow(
            "SELECT event_type, result_kind, failure_kind, result_fingerprint, "
            "occurred_at = (SELECT completed_at FROM task_attempt_results WHERE "
            "task_attempt_id = $1) AS same_time FROM task_result_events WHERE "
            "task_attempt_id = $1 AND claim_generation = $2 AND "
            "event_type = 'result_recovered'",
            attempt_id,
            generation,
        )
        assert backfilled is not None
        assert tuple(backfilled)[:3] == (
            "result_recovered",
            "retryable_failure",
            "claim_expired",
        )
        assert backfilled[4] is True

        base = await connection.fetchrow(
            "SELECT worker_session_id, dispatch_id FROM task_result_events WHERE "
            "task_attempt_id = $1 AND claim_generation = $2 AND "
            "event_type = 'result_recovered'",
            attempt_id,
            generation,
        )
        assert base is not None
        with pytest.raises(asyncpg.UniqueViolationError):
            await connection.execute(
                "INSERT INTO task_result_events (id, task_attempt_id, "
                "claim_generation, worker_session_id, dispatch_id, event_type, "
                "result_kind, failure_kind, result_fingerprint) VALUES "
                "($1, $2, $3, $4, $5, 'result_recovered', "
                "'retryable_failure', 'claim_expired', $6)",
                uuid4(),
                attempt_id,
                generation,
                base["worker_session_id"],
                base["dispatch_id"],
                backfilled["result_fingerprint"],
            )

        invalid = (
            ("result_recovered", "success", None),
            ("result_recovered", "retryable_failure", "handler_reported"),
            ("result_accepted", "retryable_failure", "claim_expired"),
            ("result_replayed", "retryable_failure", "claim_expired"),
            ("result_conflict_rejected", "retryable_failure", "claim_expired"),
            ("result_stale_rejected", "retryable_failure", "claim_expired"),
        )
        for index, (event_type, result_kind, failure_kind) in enumerate(invalid, 1):
            with pytest.raises(asyncpg.CheckViolationError):
                await connection.execute(
                    "INSERT INTO task_result_events (id, task_attempt_id, "
                    "claim_generation, worker_session_id, dispatch_id, event_type, "
                    "result_kind, failure_kind, result_fingerprint) VALUES "
                    "($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                    f"00000000-0000-0000-0000-{index:012d}",
                    attempt_id,
                    generation,
                    base["worker_session_id"],
                    base["dispatch_id"],
                    event_type,
                    result_kind,
                    failure_kind,
                    f"{index:064x}",
                )
    finally:
        await connection.close()


async def verify_downgrade(
    database_url: URL, attempt_id: UUID, ordinary_event_id: UUID
) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT FROM task_result_events WHERE task_attempt_id = $1 "
            "AND event_type = 'result_recovered')",
            attempt_id,
        )
        assert await connection.fetchval(
            "SELECT EXISTS (SELECT FROM task_result_events WHERE id = $1)",
            ordinary_event_id,
        )
        event_constraint = await connection.fetchval(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE "
            "conname = 'ck_task_result_events_event_type_valid'"
        )
        shape_constraint = await connection.fetchval(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE "
            "conname = 'ck_task_result_events_result_shape_valid'"
        )
        assert "result_recovered" not in event_constraint
        assert "claim_expired" not in shape_constraint
        with pytest.raises(asyncpg.PostgresError) as immutable_event:
            await connection.execute(
                "DELETE FROM task_result_events WHERE id = $1", ordinary_event_id
            )
        assert immutable_event.value.sqlstate == "TF004"
        with pytest.raises(asyncpg.PostgresError) as immutable:
            await connection.execute(
                "DELETE FROM task_attempt_results WHERE task_attempt_id = $1",
                attempt_id,
            )
        assert immutable.value.sqlstate == "TF004"
    finally:
        await connection.close()


def test_recovery_event_upgrade_backfill_constraints_and_downgrade() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_recovery_event_mig"
    ) as database_url:
        config = Config("alembic.ini")
        rendered = database_url.render_as_string(hide_password=False)
        with migration_database_url(rendered):
            command.upgrade(config, PREVIOUS_REVISION)
            attempt_id, generation, ordinary_event_id = asyncio.run(
                seed_pre_task_4_recovery(database_url, add_ordinary_event=True)
            )
            assert ordinary_event_id is not None
            command.upgrade(config, REVISION)
            asyncio.run(verify_upgrade(database_url, attempt_id, generation))
            command.downgrade(config, PREVIOUS_REVISION)
            asyncio.run(verify_downgrade(database_url, attempt_id, ordinary_event_id))


async def verify_failed_upgrade(database_url: URL, attempt_id: UUID) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        assert await connection.fetchval("SELECT version_num FROM alembic_version") == (
            PREVIOUS_REVISION
        )
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT FROM task_result_events WHERE "
            "task_attempt_id = $1 AND event_type = 'result_recovered')",
            attempt_id,
        )
        assert await connection.fetchval(
            "SELECT to_regclass('uq_task_result_events_recovered_generation') IS NULL"
        )
        event_constraint = await connection.fetchval(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE "
            "conname = 'ck_task_result_events_event_type_valid'"
        )
        shape_constraint = await connection.fetchval(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE "
            "conname = 'ck_task_result_events_result_shape_valid'"
        )
        assert "result_recovered" not in event_constraint
        assert "claim_expired" not in shape_constraint
    finally:
        await connection.close()


def test_recovery_event_upgrade_rejects_active_claim_atomically() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_recovery_event_mig"
    ) as database_url:
        config = Config("alembic.ini")
        rendered = database_url.render_as_string(hide_password=False)
        with migration_database_url(rendered):
            command.upgrade(config, PREVIOUS_REVISION)
            attempt_id, generation, _ = asyncio.run(
                seed_pre_task_4_recovery(database_url, terminate_claim=False)
            )
            with pytest.raises(
                DBAPIError, match="claim_expired result has an active claim"
            ):
                command.upgrade(config, REVISION)
            asyncio.run(verify_failed_upgrade(database_url, attempt_id))

            async def terminate_claim() -> None:
                connection = await asyncpg.connect(asyncpg_dsn(database_url))
                try:
                    await connection.execute(
                        "UPDATE task_attempt_claims SET terminated_at = "
                        "statement_timestamp() WHERE task_attempt_id = $1 AND "
                        "generation = $2",
                        attempt_id,
                        generation,
                    )
                finally:
                    await connection.close()

            asyncio.run(terminate_claim())
            command.upgrade(config, REVISION)
            command.downgrade(config, PREVIOUS_REVISION)
