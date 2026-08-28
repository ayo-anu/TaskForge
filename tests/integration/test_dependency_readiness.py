"""Opt-in readiness verification against authoritative PostgreSQL."""

from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import pytest
from sqlalchemy import make_url, text

from taskforge.api.dependencies import build_readiness_coordinator
from taskforge.persistence.database import build_async_engine
from taskforge.settings import Settings

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_READINESS_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_READINESS_INTEGRATION=1 explicitly",
    ),
]


def _settings(
    database_url: str, *, user: str | None = None, password: str = ""
) -> Settings:
    url = make_url(database_url)
    return Settings(
        postgres_host=url.host or "127.0.0.1",
        postgres_port=url.port or 5432,
        postgres_database=url.database or "postgres",
        postgres_user=user or url.username or "postgres",
        postgres_password=password or url.password or "",
        rabbitmq_password="unused-rabbitmq-secret",
        database_pool_size=2,
    )


def test_real_postgresql_outage_recovery_and_transaction_isolation() -> None:
    database_url = os.environ["TASKFORGE_READINESS_TEST_DATABASE_URL"]
    role = f"taskforge_readiness_{uuid4().hex}"
    role_password = "readiness-test-password"
    admin_engine = build_async_engine(_settings(database_url))
    application_engine = build_async_engine(
        _settings(database_url, user=role, password=role_password)
    )
    readiness = build_readiness_coordinator(
        _settings(database_url, user=role, password=role_password), application_engine
    )

    async def exercise() -> None:
        async with admin_engine.begin() as connection:
            await connection.execute(
                text(f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{role_password}'")
            )
        try:
            readiness.observe_execution_stream(True)
            assert (await readiness.start()).status == "ready"

            async with application_engine.connect() as authoritative_connection:
                transaction = await authoritative_connection.begin()
                before = await authoritative_connection.scalar(
                    text("SELECT txid_current()")
                )
                assert (await readiness.snapshot()).status == "ready"
                after = await authoritative_connection.scalar(
                    text("SELECT txid_current()")
                )
                assert before == after
                assert transaction.is_active
                await transaction.rollback()

            async with admin_engine.begin() as connection:
                await connection.execute(text(f'ALTER ROLE "{role}" NOLOGIN'))
            await application_engine.dispose()
            assert (await readiness.snapshot()).status == "not_ready"

            async with admin_engine.begin() as connection:
                await connection.execute(text(f'ALTER ROLE "{role}" LOGIN'))
            await application_engine.dispose()
            assert (await readiness.snapshot()).status == "ready"
        finally:
            await application_engine.dispose()
            async with admin_engine.begin() as connection:
                await connection.execute(text(f'DROP ROLE IF EXISTS "{role}"'))
            await admin_engine.dispose()

    asyncio.run(exercise())
