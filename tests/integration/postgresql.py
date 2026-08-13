"""Failure-safe temporary PostgreSQL databases for opt-in integration tests."""

from __future__ import annotations

import asyncio
import os
import re
from collections.abc import Iterator
from contextlib import contextmanager
from uuid import uuid4

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
    r"taskforge_claim_events|taskforge_task_results|taskforge_result_migration)_[0-9a-f]{32}\Z"
)


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
