"""Task-4 PostgreSQL migration, ACL, index recovery, and plan contracts."""

from __future__ import annotations

import asyncio
import importlib
import os
from pathlib import Path

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

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TASK4_INDEXES = {
    "ix_audit_records_occurred_at_id",
    "ix_audit_records_resource_occurred_at_id",
    "ix_audit_records_action_occurred_at_id",
    "ix_audit_records_correlation_occurred_at_id",
    "ix_audit_records_actor_occurred_at_id",
    "ix_audit_records_rejected_reason_occurred_at_id",
    "ix_workflow_run_execution_events_run_occurred_at_id",
    "ix_task_claim_events_attempt_occurred_at_id",
    "ix_worker_heartbeats_identity_received_session_sequence",
    "ix_workflow_run_replays_source_created_at_run",
}
MIGRATION = importlib.import_module(
    "migrations.versions.0028_add_authorized_history_queries"
)


def test_0027_0028_downgrade_reupgrade_and_exact_acls() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_history_queries"
    ) as database_url:
        config = Config(str(PROJECT_ROOT / "alembic.ini"))
        url = database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        with migration_database_url(url):
            command.upgrade(config, "0027_enforce_history_privileges")
            asyncio.run(_assert_state(database_url, upgraded=False))
            command.upgrade(config, "0028_authorized_history_queries")
            asyncio.run(_assert_state(database_url, upgraded=True))
            command.downgrade(config, "0027_enforce_history_privileges")
            asyncio.run(_assert_state(database_url, upgraded=False))
            command.upgrade(config, "0028_authorized_history_queries")
            asyncio.run(_assert_state(database_url, upgraded=True))


def test_invalid_owned_index_is_recovered_without_touching_unrelated_index() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_history_queries"
    ) as database_url:
        config = Config(str(PROJECT_ROOT / "alembic.ini"))
        url = database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        with migration_database_url(url):
            command.upgrade(config, "0027_enforce_history_privileges")
            asyncio.run(_make_invalid_and_unrelated_indexes(database_url))
            command.upgrade(config, "0028_authorized_history_queries")
            asyncio.run(_assert_recovery(database_url))


def test_valid_same_name_different_definition_fails_without_dropping() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_history_queries"
    ) as database_url:
        config = Config(str(PROJECT_ROOT / "alembic.ini"))
        url = database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        with migration_database_url(url):
            command.upgrade(config, "0027_enforce_history_privileges")
            asyncio.run(_make_valid_collision(database_url))
            with pytest.raises(RuntimeError, match="unexpected definition"):
                command.upgrade(config, "0028_authorized_history_queries")
            asyncio.run(_assert_collision_preserved(database_url))


def test_valid_exact_owned_index_is_preserved() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_history_queries"
    ) as database_url:
        config = Config(str(PROJECT_ROOT / "alembic.ini"))
        url = database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        with migration_database_url(url):
            command.upgrade(config, "0027_enforce_history_privileges")
            original_oid = asyncio.run(_make_exact_index(database_url))
            command.upgrade(config, "0028_authorized_history_queries")
            asyncio.run(_assert_index_oid(database_url, original_oid))


async def _assert_state(database_url: object, *, upgraded: bool) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))  # type: ignore[arg-type]
    try:
        assert (
            bool(
                await connection.fetchval(
                    "SELECT has_table_privilege('taskforge_runtime','audit_records','SELECT')"
                )
            )
            is upgraded
        )
        rows = await connection.fetch(
            "SELECT indexrelid::regclass::text name FROM pg_index "
            "WHERE indexrelid::regclass::text LIKE 'ix_%'"
        )
        rendered = {row["name"] for row in rows}
        assert TASK4_INDEXES.issubset(rendered) is upgraded
        legacy = "ix_workflow_run_replays_source_workflow_run_id"
        assert (legacy in rendered) is (not upgraded)
        if upgraded:
            invalid = await connection.fetchval(
                "SELECT count(*) FROM pg_index WHERE NOT indisvalid AND "
                "indexrelid::regclass::text=ANY($1::text[])",
                sorted(TASK4_INDEXES),
            )
            assert invalid == 0
            definitions = await connection.fetch(
                "SELECT indexrelid::regclass::text name,pg_get_indexdef(indexrelid) definition "
                "FROM pg_index WHERE indexrelid::regclass::text=ANY($1::text[])",
                sorted(TASK4_INDEXES),
            )
            assert {
                row["name"]: MIGRATION._normalized(row["definition"])
                for row in definitions
            } == {
                name: MIGRATION._normalized(f"CREATE INDEX {name} {definition}")
                for name, definition in MIGRATION._INDEXES.items()
            }
    finally:
        await connection.close()


async def _make_invalid_and_unrelated_indexes(database_url: object) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))  # type: ignore[arg-type]
    try:
        await connection.execute(
            "CREATE INDEX unrelated_task4_probe ON workflow_definitions (name)"
        )
        await connection.execute(
            "CREATE INDEX ix_audit_records_occurred_at_id ON audit_records (occurred_at DESC, id DESC)"
        )
        await connection.execute("SET allow_system_table_mods=on")
        await connection.execute(
            "UPDATE pg_index SET indisvalid=false WHERE "
            "indexrelid='ix_audit_records_occurred_at_id'::regclass"
        )
    finally:
        await connection.close()


async def _assert_recovery(database_url: object) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))  # type: ignore[arg-type]
    try:
        assert await connection.fetchval(
            "SELECT indisvalid FROM pg_index WHERE "
            "indexrelid='ix_audit_records_occurred_at_id'::regclass"
        )
        assert await connection.fetchval(
            "SELECT to_regclass('unrelated_task4_probe') IS NOT NULL"
        )
    finally:
        await connection.close()


async def _make_valid_collision(database_url: object) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))  # type: ignore[arg-type]
    try:
        await connection.execute(
            "CREATE INDEX ix_audit_records_occurred_at_id ON audit_records (id)"
        )
    finally:
        await connection.close()


async def _assert_collision_preserved(database_url: object) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))  # type: ignore[arg-type]
    try:
        definition = await connection.fetchval(
            "SELECT pg_get_indexdef('ix_audit_records_occurred_at_id'::regclass)"
        )
        assert definition.endswith("USING btree (id)")
        assert await connection.fetchval(
            "SELECT to_regclass('ix_workflow_run_replays_source_workflow_run_id') "
            "IS NOT NULL"
        )
        assert not await connection.fetchval(
            "SELECT has_table_privilege('taskforge_runtime','audit_records','SELECT')"
        )
    finally:
        await connection.close()


async def _make_exact_index(database_url: object) -> int:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))  # type: ignore[arg-type]
    try:
        await connection.execute(
            "CREATE INDEX ix_audit_records_occurred_at_id "
            "ON audit_records (occurred_at DESC,id DESC)"
        )
        return await connection.fetchval(
            "SELECT 'ix_audit_records_occurred_at_id'::regclass::oid"
        )
    finally:
        await connection.close()


async def _assert_index_oid(database_url: object, expected: int) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))  # type: ignore[arg-type]
    try:
        assert (
            await connection.fetchval(
                "SELECT 'ix_audit_records_occurred_at_id'::regclass::oid"
            )
            == expected
        )
    finally:
        await connection.close()
