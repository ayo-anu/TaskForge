"""Real PostgreSQL fixed-window migration and concurrency guarantees."""

from __future__ import annotations

import asyncio
import os

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from taskforge.persistence.database import build_session_factory
from taskforge.persistence.rate_limits import SQLAlchemyRateLimitRepository
from taskforge.rate_limits import RateLimit, RateLimitPolicy, rate_limit_key
from tests.integration.postgresql import (
    asyncpg_dsn,
    migration_database_url,
    temporary_database,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_RATE_LIMIT_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_RATE_LIMIT_INTEGRATION=1 explicitly",
    ),
]


async def verify(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        columns = await connection.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='rate_limit_counters' ORDER BY ordinal_position"
        )
        assert [row["column_name"] for row in columns] == [
            "policy",
            "key_digest",
            "window_started_at",
            "count",
            "updated_at",
        ]
    finally:
        await connection.close()
    engines = [create_async_engine(database_url, pool_size=10) for _ in range(3)]
    repositories = [
        SQLAlchemyRateLimitRepository(
            build_session_factory(engine),
            timeout_seconds=5,
            cleanup_retention_seconds=600,
        )
        for engine in engines
    ]
    policy = RateLimitPolicy.RUN_CREATE
    try:
        fresh_key = rate_limit_key(policy, "api_principal", "fresh")
        decisions = await asyncio.gather(
            *(
                repositories[index % len(repositories)].consume(
                    policy, fresh_key, RateLimit(7, 60)
                )
                for index in range(40)
            )
        )
        assert sum(item.allowed for item in decisions) == 7

        existing_key = rate_limit_key(policy, "api_principal", "existing")
        for _ in range(5):
            assert (
                await repositories[0].consume(policy, existing_key, RateLimit(7, 60))
            ).allowed
        contention = await asyncio.gather(
            *(
                repositories[index % len(repositories)].consume(
                    policy, existing_key, RateLimit(7, 60)
                )
                for index in range(30)
            )
        )
        assert sum(item.allowed for item in contention) == 2

        reset_key = rate_limit_key(policy, "api_principal", "reset")
        assert (
            await repositories[0].consume(policy, reset_key, RateLimit(1, 1))
        ).allowed
        assert not (
            await repositories[1].consume(policy, reset_key, RateLimit(1, 1))
        ).allowed
        await asyncio.sleep(1.05)
        assert (
            await repositories[2].consume(policy, reset_key, RateLimit(1, 1))
        ).allowed
    finally:
        await asyncio.gather(*(engine.dispose() for engine in engines))


def test_rate_limit_migration_and_replica_wide_atomicity() -> None:
    with temporary_database(
        "TASKFORGE_RATE_LIMIT_TEST_DATABASE_URL", "taskforge_rate_limit"
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        with migration_database_url(alembic_url):
            command.upgrade(Config("alembic.ini"), "head")
        asyncio.run(verify(database_url.set(drivername="postgresql+asyncpg")))
