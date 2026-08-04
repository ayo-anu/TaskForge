"""Opt-in identity migration verification against an isolated PostgreSQL database."""

from __future__ import annotations

import asyncio
import os
import re
import secrets
from collections.abc import Iterator
from contextlib import contextmanager
from uuid import uuid4

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL, make_url

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_MIGRATION_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_MIGRATION_INTEGRATION=1 explicitly",
    ),
]

SAFE_DATABASE_NAME = re.compile(r"\Ataskforge_migration_test_[0-9a-f]{32}\Z")
IDENTITY_TABLES = {
    "api_credentials",
    "api_principal_roles",
    "api_principals",
    "worker_credentials",
    "worker_identities",
}


def required_administrative_url() -> URL:
    """Load an explicit administrative URL without a repository fallback."""
    raw_url = os.getenv("TASKFORGE_MIGRATION_TEST_DATABASE_URL")
    if not raw_url:
        pytest.fail("TASKFORGE_MIGRATION_TEST_DATABASE_URL is required")
    return make_url(raw_url)


def asyncpg_dsn(url: URL) -> str:
    """Render an asyncpg-compatible URL only at the database boundary."""
    return url.set(drivername="postgresql").render_as_string(hide_password=False)


def assert_safe_database_name(database_name: str) -> None:
    """Refuse destructive SQL unless the generated name is exact and bounded."""
    if SAFE_DATABASE_NAME.fullmatch(database_name) is None:
        raise RuntimeError("refusing cleanup for an unsafe temporary database name")


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
def migration_database_url(database_url: str) -> Iterator[None]:
    """Temporarily supply Alembic's URL and always restore process state."""
    original = os.environ.get("TASKFORGE_DATABASE_URL")
    os.environ["TASKFORGE_DATABASE_URL"] = database_url
    try:
        yield
    finally:
        if original is None:
            os.environ.pop("TASKFORGE_DATABASE_URL", None)
        else:
            os.environ["TASKFORGE_DATABASE_URL"] = original


async def inspect_upgraded_schema(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        tables = set(
            await connection.fetchval(
                "SELECT array_agg(tablename ORDER BY tablename) "
                "FROM pg_tables WHERE schemaname = 'public' "
                "AND tablename <> 'alembic_version'"
            )
            or []
        )
        assert tables == IDENTITY_TABLES

        verifier_columns = await connection.fetch(
            "SELECT table_name, data_type, is_nullable "
            "FROM information_schema.columns "
            "WHERE table_schema = 'public' "
            "AND column_name = 'credential_verifier' "
            "ORDER BY table_name"
        )
        assert [tuple(row.values()) for row in verifier_columns] == [
            ("api_credentials", "text", "NO"),
            ("worker_credentials", "text", "NO"),
        ]

        verifier_indexes = await connection.fetchval(
            "SELECT count(*) FROM pg_indexes WHERE schemaname = 'public' "
            "AND indexdef ILIKE '%credential_verifier%'"
        )
        assert verifier_indexes == 0

        principal_id = uuid4()
        worker_id = uuid4()
        await connection.execute(
            "INSERT INTO api_principals (id, name) VALUES ($1, $2)",
            principal_id,
            f"principal-{uuid4().hex}",
        )
        await connection.execute(
            "INSERT INTO worker_identities (id, name) VALUES ($1, $2)",
            worker_id,
            f"worker-{uuid4().hex}",
        )
        await connection.execute(
            "INSERT INTO api_principal_roles (principal_id, role) VALUES ($1, $2)",
            principal_id,
            "viewer",
        )
        await connection.execute(
            "INSERT INTO api_credentials "
            "(id, principal_id, credential_verifier) VALUES ($1, $2, $3)",
            uuid4(),
            principal_id,
            secrets.token_hex(32),
        )
        await connection.execute(
            "INSERT INTO worker_credentials "
            "(id, worker_identity_id, credential_verifier) VALUES ($1, $2, $3)",
            uuid4(),
            worker_id,
            secrets.token_hex(32),
        )

        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                "INSERT INTO api_principal_roles (principal_id, role) VALUES ($1, $2)",
                principal_id,
                "unapproved_role",
            )
        with pytest.raises(asyncpg.UniqueViolationError):
            await connection.execute(
                "INSERT INTO api_principal_roles (principal_id, role) VALUES ($1, $2)",
                principal_id,
                "viewer",
            )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await connection.execute(
                "INSERT INTO api_credentials "
                "(id, principal_id, credential_verifier) VALUES ($1, $2, $3)",
                uuid4(),
                uuid4(),
                secrets.token_hex(32),
            )
        with pytest.raises(asyncpg.NotNullViolationError):
            await connection.execute(
                "INSERT INTO worker_credentials "
                "(id, worker_identity_id, credential_verifier) VALUES ($1, $2, NULL)",
                uuid4(),
                worker_id,
            )
    finally:
        await connection.close()


async def inspect_downgraded_schema(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        remaining = await connection.fetchval(
            "SELECT count(*) FROM pg_tables WHERE schemaname = 'public' "
            "AND tablename = ANY($1::text[])",
            list(IDENTITY_TABLES),
        )
        assert remaining == 0
    finally:
        await connection.close()


def test_identity_migration_upgrades_and_downgrades_cleanly() -> None:
    administrative_url = required_administrative_url()
    database_name = f"taskforge_migration_test_{uuid4().hex}"
    assert_safe_database_name(database_name)
    database_url = administrative_url.set(database=database_name)
    alembic_url = database_url.set(drivername="postgresql+asyncpg").render_as_string(
        hide_password=False
    )
    configuration = Config("alembic.ini")

    asyncio.run(create_database(administrative_url, database_name))
    try:
        with migration_database_url(alembic_url):
            command.upgrade(configuration, "head")
            asyncio.run(inspect_upgraded_schema(database_url))
            command.downgrade(configuration, "base")
            asyncio.run(inspect_downgraded_schema(database_url))
    finally:
        asyncio.run(drop_database(administrative_url, database_name))
