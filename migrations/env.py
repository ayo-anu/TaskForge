"""Alembic migration environment configured only from process environment."""

from __future__ import annotations

import asyncio
import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import Connection, pool
from sqlalchemy.ext.asyncio import async_engine_from_config

from taskforge.persistence.migration_lock import (
    DEFAULT_MIGRATION_LOCK_TIMEOUT_SECONDS,
    acquire_migration_lock_sync,
    release_migration_lock_sync,
    validate_migration_lock_timeout,
)
from taskforge.persistence.schema import metadata

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = metadata


def database_url() -> str:
    """Return the explicitly supplied migration URL without a fallback secret."""
    try:
        return os.environ["TASKFORGE_DATABASE_URL"]
    except KeyError as error:
        raise RuntimeError(
            "TASKFORGE_DATABASE_URL is required for database migration commands"
        ) from error


def migration_lock_timeout_seconds() -> int:
    """Load the bounded direct-Alembic lock timeout."""
    raw_value = os.getenv(
        "TASKFORGE_MIGRATION_LOCK_TIMEOUT_SECONDS",
        str(DEFAULT_MIGRATION_LOCK_TIMEOUT_SECONDS),
    )
    try:
        value = int(raw_value)
    except ValueError as error:
        raise RuntimeError("migration lock timeout must be a whole number") from error
    return validate_migration_lock_timeout(value)


def run_migrations_offline() -> None:
    """Run migrations without creating a database connection."""
    context.configure(
        url=database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations(connection: Connection) -> None:
    """Run migrations on a synchronous connection supplied by SQLAlchemy."""
    lock_held = config.attributes.get("taskforge_migration_lock_held", False)
    if lock_held and config.attributes.get("connection") is None:
        raise RuntimeError("migration lock marker requires an injected connection")
    owns_lock = not lock_held
    if owns_lock:
        acquire_migration_lock_sync(connection, migration_lock_timeout_seconds())
        connection.commit()
    try:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )

        with context.begin_transaction():
            context.run_migrations()
    finally:
        if owns_lock:
            if connection.in_transaction():
                connection.rollback()
            release_migration_lock_sync(connection)
            connection.commit()


async def run_migrations_online() -> None:
    """Create a short-lived async engine and run online migrations."""
    if config.attributes.get("taskforge_migration_lock_held", False):
        raise RuntimeError("migration lock marker requires an injected connection")
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = database_url()
    connectable = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(run_migrations)

    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
elif config.attributes.get("connection") is not None:
    run_migrations(config.attributes["connection"])
else:
    asyncio.run(run_migrations_online())
