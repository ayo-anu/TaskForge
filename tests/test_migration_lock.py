"""Bounded database-local session migration-lock tests."""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import AsyncConnection

from taskforge.persistence.migration_lock import (
    MIGRATION_LOCK_NAMESPACE,
    MigrationLockStateError,
    MigrationLockTimeout,
    acquire_migration_lock,
    acquire_migration_lock_sync,
    release_migration_lock,
    release_migration_lock_sync,
    validate_migration_lock_timeout,
)


class AsyncFakeConnection:
    def __init__(self, results: list[object]) -> None:
        self.results = results
        self.calls: list[tuple[str, dict[str, int]]] = []

    async def scalar(self, statement: object, parameters: dict[str, int]) -> object:
        self.calls.append((str(statement), parameters))
        return self.results.pop(0)


class SyncFakeConnection:
    def __init__(self, results: list[object]) -> None:
        self.results = results
        self.calls: list[tuple[str, dict[str, int]]] = []

    def scalar(self, statement: object, parameters: dict[str, int]) -> object:
        self.calls.append((str(statement), parameters))
        return self.results.pop(0)


def test_async_lock_waits_then_releases_the_same_database_scoped_key() -> None:
    connection = AsyncFakeConnection([False, True, True])
    times = iter((0.0, 0.1, 0.2, 0.3))
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    async def exercise() -> None:
        await acquire_migration_lock(
            cast(AsyncConnection, cast(Any, connection)),
            1,
            monotonic=lambda: next(times),
            sleep=sleep,
        )
        await release_migration_lock(cast(AsyncConnection, cast(Any, connection)))

    asyncio.run(exercise())

    assert sleeps == [0.1]
    assert len(connection.calls) == 3
    assert all(
        call[1] == {"namespace": MIGRATION_LOCK_NAMESPACE} for call in connection.calls
    )
    assert "current_database" in connection.calls[0][0]
    assert "pg_try_advisory_lock" in connection.calls[0][0]
    assert "pg_advisory_unlock" in connection.calls[-1][0]


def test_async_lock_timeout_never_reports_acquisition() -> None:
    connection = AsyncFakeConnection([False, False])
    times = iter((0.0, 0.5, 1.0))

    async def exercise() -> None:
        with pytest.raises(MigrationLockTimeout):
            await acquire_migration_lock(
                cast(AsyncConnection, cast(Any, connection)),
                1,
                monotonic=lambda: next(times),
                sleep=lambda delay: asyncio.sleep(0),
            )

    asyncio.run(exercise())
    assert len(connection.calls) == 2


def test_sync_direct_alembic_path_acquires_and_releases_once() -> None:
    connection = SyncFakeConnection([True, True])

    acquire_migration_lock_sync(
        cast(Connection, cast(Any, connection)), 1, monotonic=lambda: 0.0
    )
    release_migration_lock_sync(cast(Connection, cast(Any, connection)))

    assert len(connection.calls) == 2
    assert "pg_try_advisory_lock" in connection.calls[0][0]
    assert "pg_advisory_unlock" in connection.calls[1][0]


def test_false_unlock_is_a_lock_ownership_error() -> None:
    connection = SyncFakeConnection([False])

    with pytest.raises(MigrationLockStateError):
        release_migration_lock_sync(cast(Connection, cast(Any, connection)))


@pytest.mark.parametrize("value", (0, 3601))
def test_lock_timeout_range_is_fail_closed(value: int) -> None:
    with pytest.raises(ValueError):
        validate_migration_lock_timeout(value)
