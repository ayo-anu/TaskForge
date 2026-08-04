"""Opt-in identity migration verification against an isolated PostgreSQL database."""

from __future__ import annotations

import asyncio
import os
import secrets
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

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_MIGRATION_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_MIGRATION_INTEGRATION=1 explicitly",
    ),
]

IDENTITY_TABLES = {
    "api_credentials",
    "api_principal_roles",
    "api_principals",
    "worker_credentials",
    "worker_identities",
}


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
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL",
        "taskforge_migration_test",
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        configuration = Config("alembic.ini")
        with migration_database_url(alembic_url):
            command.upgrade(configuration, "0001_identity")
            asyncio.run(inspect_upgraded_schema(database_url))
            command.downgrade(configuration, "base")
            asyncio.run(inspect_downgraded_schema(database_url))
