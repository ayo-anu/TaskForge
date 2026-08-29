"""Failure-safe temporary PostgreSQL databases for opt-in integration tests."""

from __future__ import annotations

import asyncio
import json
import os
import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from uuid import UUID, uuid4

import asyncpg
import pytest
from sqlalchemy.engine import URL, make_url

SAFE_DATABASE_NAME = re.compile(
    r"\A(?:taskforge_migration_test|taskforge_auth_test|"
    r"taskforge_authorization_test|taskforge_protected_route_test|"
    r"taskforge_credential_bootstrap|taskforge_workflow_persistence|"
    r"taskforge_workflow_route|taskforge_run_migration|"
    r"taskforge_version_resolution|taskforge_run_creation|"
    r"taskforge_run_idempotency|taskforge_workflow_run_route|"
    r"taskforge_task_dispatch|taskforge_broker_dispatch|"
    r"taskforge_worker_migration|taskforge_worker_registration|"
    r"taskforge_claim_migration|taskforge_claim_event_mig|"
    r"taskforge_claim_acquisition|taskforge_claim_renewal|"
    r"taskforge_claim_events|taskforge_task_results|taskforge_result_migration|"
    r"taskforge_retry_migration|taskforge_retry_event_mig|"
    r"taskforge_retry_transition|"
    r"taskforge_retry_scanner|taskforge_retry_inspection|"
    r"taskforge_recovery_scanner|taskforge_recovery_transition|"
    r"taskforge_m13_crash|"
    r"taskforge_stale_recovery|"
    r"taskforge_recovery_migration|taskforge_recovery_event_mig|"
    r"taskforge_dead_letter_mig|taskforge_dead_letter_ops|"
    r"taskforge_dead_letter_redrive|taskforge_run_cancellation|"
    r"taskforge_execution_event_mig|taskforge_execution_events|"
    r"taskforge_workflow_replay_mig|taskforge_audit_mig|"
    r"taskforge_audit_semantics|taskforge_history_queries|"
    r"taskforge_history_export)_[0-9a-f]{32}\Z"
    r"|taskforge_history_privileges_[0-9a-f]{32}\Z"
    r"|taskforge_rate_limit_[0-9a-f]{32}\Z"
    r"|taskforge_cred_lifecycle_[0-9a-f]{32}\Z"
)


@dataclass(frozen=True)
class ExpectedStatusExecutionEvent:
    task_run_id: UUID | None
    previous_status: str
    status: str


async def assert_status_execution_events(
    connection: asyncpg.Connection[asyncpg.Record],
    workflow_run_id: UUID,
    expected: tuple[ExpectedStatusExecutionEvent, ...],
) -> None:
    """Assert the complete ordered status-event stream for one workflow run."""
    rows = await connection.fetch(
        "SELECT cursor, task_run_id, event_type, payload FROM "
        "workflow_run_execution_events WHERE workflow_run_id = $1 ORDER BY cursor",
        workflow_run_id,
    )
    assert [row["cursor"] for row in rows] == list(range(1, len(expected) + 1))
    assert len(rows) == len(expected)
    for row, item in zip(rows, expected, strict=True):
        assert row["task_run_id"] == item.task_run_id
        assert row["event_type"] == (
            "workflow_run.status_changed"
            if item.task_run_id is None
            else "task_run.status_changed"
        )
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        assert payload == {
            "previous_status": item.previous_status,
            "status": item.status,
        }


def required_administrative_url(environment_variable: str) -> URL:
    raw_url = os.getenv(environment_variable)
    if not raw_url:
        pytest.fail(f"{environment_variable} is required")
    return make_url(raw_url)


def asyncpg_dsn(url: URL) -> str:
    return url.set(drivername="postgresql").render_as_string(hide_password=False)


def assert_safe_database_name(database_name: str) -> None:
    if SAFE_DATABASE_NAME.fullmatch(database_name) is None:
        raise RuntimeError("refusing operation for an unsafe temporary database name")


async def create_database(administrative_url: URL, database_name: str) -> None:
    assert_safe_database_name(database_name)
    connection = await asyncpg.connect(asyncpg_dsn(administrative_url))
    try:
        await connection.execute(
            "DO $block$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE "
            "rolname='taskforge_runtime') THEN CREATE ROLE taskforge_runtime "
            "LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION "
            "NOBYPASSRLS; END IF; END $block$"
        )
        await connection.execute(f'CREATE DATABASE "{database_name}"')
    finally:
        await connection.close()


async def drop_database(administrative_url: URL, database_name: str) -> None:
    assert_safe_database_name(database_name)
    connection = await asyncpg.connect(asyncpg_dsn(administrative_url))
    try:
        await connection.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = $1 AND pid <> pg_backend_pid()",
            database_name,
        )
        await connection.execute(f'DROP DATABASE IF EXISTS "{database_name}"')
    finally:
        await connection.close()


@contextmanager
def temporary_database(
    environment_variable: str,
    name_prefix: str,
) -> Iterator[URL]:
    administrative_url = required_administrative_url(environment_variable)
    database_name = f"{name_prefix}_{uuid4().hex}"
    assert_safe_database_name(database_name)
    database_url = administrative_url.set(database=database_name)

    asyncio.run(create_database(administrative_url, database_name))
    try:
        yield database_url
    finally:
        asyncio.run(drop_database(administrative_url, database_name))


@contextmanager
def migration_database_url(database_url: str) -> Iterator[None]:
    original = os.environ.get("TASKFORGE_DATABASE_URL")
    os.environ["TASKFORGE_DATABASE_URL"] = database_url
    try:
        yield
    finally:
        if original is None:
            os.environ.pop("TASKFORGE_DATABASE_URL", None)
        else:
            os.environ["TASKFORGE_DATABASE_URL"] = original
