"""PostgreSQL migration contract for standardized audit semantics."""

from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import asyncpg
import pytest
from alembic import command
from alembic.config import Config

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

_NEW_CONSTRAINTS = {
    "ck_audit_records_action_namespaced",
    "ck_workflow_run_execution_events_event_type_namespaced",
    "ck_task_claim_events_correlation_valid",
    "ck_task_result_events_correlation_valid",
    "ck_task_retry_events_correlation_valid",
    "ck_worker_heartbeats_correlation_valid",
}
_RESULT_CONSTRAINT = "ck_task_result_events_actor_component_valid"
_LEGACY_RESULT_CONSTRAINT = (
    "ck_task_result_events_ck_task_result_events_actor_compo_214b"
)


async def _insert_historical_invalid_heartbeat(database_url: object) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))  # type: ignore[arg-type]
    identity_id, session_id = uuid4(), uuid4()
    try:
        await connection.execute(
            "INSERT INTO worker_identities (id, name) VALUES ($1, $2)",
            identity_id,
            f"semantic-worker-{identity_id.hex}",
        )
        await connection.execute(
            "INSERT INTO worker_sessions (id, worker_identity_id) VALUES ($1, $2)",
            session_id,
            identity_id,
        )
        await connection.execute(
            "INSERT INTO worker_heartbeats "
            "(worker_session_id, sequence, accepting_work, worker_identity_id, "
            "correlation_id) VALUES ($1, 1, false, $2, E'invalid\\nlegacy')",
            session_id,
            identity_id,
        )
    finally:
        await connection.close()


async def _assert_upgrade_contract(database_url: object) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))  # type: ignore[arg-type]
    try:
        rows = await connection.fetch(
            "SELECT conname, pg_get_constraintdef(oid) AS definition, convalidated "
            "FROM pg_constraint WHERE conname = ANY($1::text[]) ORDER BY conname",
            sorted(_NEW_CONSTRAINTS),
        )
        assert {row["conname"] for row in rows} == _NEW_CONSTRAINTS
        assert all(row["convalidated"] is False for row in rows)
        definitions = {row["conname"]: row["definition"] for row in rows}
        assert "(action)::text ~" in definitions["ck_audit_records_action_namespaced"]
        assert (
            "(event_type)::text ~"
            in definitions["ck_workflow_run_execution_events_event_type_namespaced"]
        )
        for name in _NEW_CONSTRAINTS:
            if name.endswith("correlation_valid"):
                assert "length((correlation_id)::text) >= 1" in definitions[name]
                assert "length((correlation_id)::text) <= 128" in definitions[name]
                assert "(correlation_id)::text !~ '[^ -~]'" in definitions[name]
                assert "NOT VALID" in definitions[name]
        assert await connection.fetchval(
            "SELECT EXISTS (SELECT FROM pg_constraint WHERE conname=$1)",
            _RESULT_CONSTRAINT,
        )
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT FROM pg_constraint WHERE conname=$1)",
            _LEGACY_RESULT_CONSTRAINT,
        )
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM worker_heartbeats WHERE correlation_id = "
                "E'invalid\\nlegacy'"
            )
            == 1
        )

        principal_id = uuid4()
        await connection.execute(
            "INSERT INTO api_principals (id, name) VALUES ($1, $2)",
            principal_id,
            f"semantic-principal-{principal_id.hex}",
        )
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                "INSERT INTO audit_records "
                "(id, actor_kind, api_principal_id, action, outcome, resource_type) "
                "VALUES ($1, 'api_principal', $2, 'not_namespaced', 'accepted', "
                "'workflow')",
                uuid4(),
                principal_id,
            )
        await connection.execute("SET session_replication_role = replica")
        statements = (
            "INSERT INTO task_claim_events (id, task_attempt_id, generation, "
            "worker_identity_id, worker_session_id, correlation_id, event_type, "
            "occurred_at, lease_expires_at) VALUES "
            f"('{uuid4()}', '{uuid4()}', 1, '{uuid4()}', '{uuid4()}', E'bad\\n', "
            "'claim_acquired', statement_timestamp(), statement_timestamp() + interval '1 minute')",
            "INSERT INTO task_result_events (id, task_attempt_id, claim_generation, "
            "worker_session_id, worker_identity_id, correlation_id, dispatch_id, "
            "event_type, result_kind, result_fingerprint) VALUES "
            f"('{uuid4()}', '{uuid4()}', 1, '{uuid4()}', '{uuid4()}', E'bad\\n', "
            f"'{uuid4()}', 'result_accepted', 'success', '{'0' * 64}')",
            "INSERT INTO task_retry_events (id, task_run_id, event_type, "
            "actor_component, correlation_id, retry_attempt_number) VALUES "
            f"('{uuid4()}', '{uuid4()}', 'retry_dispatched', 'retry_dispatch', "
            "E'bad\\n', 2)",
            "INSERT INTO worker_heartbeats (worker_session_id, sequence, "
            "accepting_work, worker_identity_id, correlation_id) VALUES "
            f"('{uuid4()}', 2, false, '{uuid4()}', E'bad\\n')",
        )
        for statement in statements:
            with pytest.raises(asyncpg.CheckViolationError):
                await connection.execute(statement)
    finally:
        await connection.close()


async def _assert_downgrade_contract(database_url: object) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))  # type: ignore[arg-type]
    try:
        names = {
            row["conname"]
            for row in await connection.fetch(
                "SELECT conname FROM pg_constraint WHERE conname = ANY($1::text[])",
                sorted(
                    _NEW_CONSTRAINTS | {_RESULT_CONSTRAINT, _LEGACY_RESULT_CONSTRAINT}
                ),
            )
        }
        assert names == {_LEGACY_RESULT_CONSTRAINT}
    finally:
        await connection.close()


def test_audit_semantics_upgrade_downgrade_reupgrade() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_audit_semantics"
    ) as database_url:
        configuration = Config("alembic.ini")
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        with migration_database_url(alembic_url):
            command.upgrade(configuration, "0025_complete_audit_history")
            asyncio.run(_insert_historical_invalid_heartbeat(database_url))
            command.upgrade(configuration, "head")
            asyncio.run(_assert_upgrade_contract(database_url))
            command.downgrade(configuration, "0025_complete_audit_history")
            asyncio.run(_assert_downgrade_contract(database_url))
            command.upgrade(configuration, "head")
            asyncio.run(_assert_upgrade_contract(database_url))
