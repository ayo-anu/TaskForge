"""Database-local session advisory locking for schema administration."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable

from sqlalchemy import Connection, text
from sqlalchemy.ext.asyncio import AsyncConnection

MIGRATION_LOCK_NAMESPACE = 1_413_893_447
DEFAULT_MIGRATION_LOCK_TIMEOUT_SECONDS = 300
MINIMUM_MIGRATION_LOCK_TIMEOUT_SECONDS = 1
MAXIMUM_MIGRATION_LOCK_TIMEOUT_SECONDS = 3_600
LOCK_POLL_INTERVAL_SECONDS = 0.1

_LOCK_KEY_SQL = """
(
    CAST(:namespace AS bigint) << 32
) | (
    SELECT oid::bigint
    FROM pg_catalog.pg_database
    WHERE datname = pg_catalog.current_database()
)
"""
_TRY_LOCK_SQL = text(f"SELECT pg_catalog.pg_try_advisory_lock({_LOCK_KEY_SQL})")
_UNLOCK_SQL = text(f"SELECT pg_catalog.pg_advisory_unlock({_LOCK_KEY_SQL})")


class MigrationLockTimeout(RuntimeError):
    """Raised before schema work when another administrator holds the lock."""


class MigrationLockStateError(RuntimeError):
    """Raised when an explicitly held lock cannot be released exactly once."""


def validate_migration_lock_timeout(value: int) -> int:
    """Return a bounded lock timeout or reject it before database work."""
    if (
        not MINIMUM_MIGRATION_LOCK_TIMEOUT_SECONDS
        <= value
        <= (MAXIMUM_MIGRATION_LOCK_TIMEOUT_SECONDS)
    ):
        raise ValueError("migration lock timeout must be between 1 and 3600 seconds")
    return value


def _parameters() -> dict[str, int]:
    return {"namespace": MIGRATION_LOCK_NAMESPACE}


async def acquire_migration_lock(
    connection: AsyncConnection,
    timeout_seconds: int,
    *,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    """Acquire the database-local session lock within one total deadline."""
    timeout = validate_migration_lock_timeout(timeout_seconds)
    deadline = monotonic() + timeout
    while True:
        acquired = await connection.scalar(_TRY_LOCK_SQL, _parameters())
        if acquired is True:
            return
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise MigrationLockTimeout(
                f"migration lock was not acquired within {timeout} seconds"
            )
        await sleep(min(LOCK_POLL_INTERVAL_SECONDS, remaining))


async def release_migration_lock(connection: AsyncConnection) -> None:
    """Release one acquisition owned by this database session."""
    released = await connection.scalar(_UNLOCK_SQL, _parameters())
    if released is not True:
        raise MigrationLockStateError("migration lock was not held by this session")


def acquire_migration_lock_sync(
    connection: Connection,
    timeout_seconds: int,
    *,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Acquire the same lock for direct synchronous Alembic execution."""
    timeout = validate_migration_lock_timeout(timeout_seconds)
    deadline = monotonic() + timeout
    while True:
        acquired = connection.scalar(_TRY_LOCK_SQL, _parameters())
        if acquired is True:
            return
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise MigrationLockTimeout(
                f"migration lock was not acquired within {timeout} seconds"
            )
        sleep(min(LOCK_POLL_INTERVAL_SECONDS, remaining))


def release_migration_lock_sync(connection: Connection) -> None:
    """Release one direct-Alembic acquisition on the same session."""
    released = connection.scalar(_UNLOCK_SQL, _parameters())
    if released is not True:
        raise MigrationLockStateError("migration lock was not held by this session")
