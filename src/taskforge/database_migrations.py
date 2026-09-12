"""Locked, fail-closed owner migration entry point."""

from __future__ import annotations

import asyncio
import sys
import time
from enum import StrEnum
from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.engine import URL, Connection
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from taskforge.persistence.migration_lock import (
    acquire_migration_lock,
    release_migration_lock,
)
from taskforge.persistence.schema_compatibility import EXPECTED_SCHEMA_REVISION
from taskforge.settings import MigrationSettings

ALEMBIC_CONFIGURATION = Path.cwd() / "alembic.ini"

_VERSION_TABLE = text(
    "SELECT pg_catalog.to_regclass('public.alembic_version') IS NOT NULL"
)
_CURRENT_REVISIONS = text(
    "SELECT version_num FROM public.alembic_version ORDER BY version_num"
)
_PUBLIC_SCHEMA_HAS_OBJECTS = text(
    """
    SELECT
        EXISTS (
            SELECT 1
            FROM pg_catalog.pg_class AS object
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = object.relnamespace
            WHERE namespace.nspname = 'public'
        )
        OR EXISTS (
            SELECT 1
            FROM pg_catalog.pg_proc AS object
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = object.pronamespace
            WHERE namespace.nspname = 'public'
        )
        OR EXISTS (
            SELECT 1
            FROM pg_catalog.pg_type AS object
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = object.typnamespace
            WHERE namespace.nspname = 'public'
        )
    """
)
_SCHEMA_FUNCTION_CONTRACT = text(
    """
    SELECT
        pg_catalog.pg_get_userbyid(procedure.proowner) AS function_owner,
        pg_catalog.pg_get_userbyid(database.datdba) AS database_owner,
        CURRENT_USER AS migration_owner,
        procedure.prosecdef AS security_definer,
        procedure.provolatile AS volatility,
        procedure.proparallel AS parallel_safety,
        procedure.proconfig AS configuration,
        pg_catalog.pg_get_function_result(procedure.oid) AS result_type,
        pg_catalog.has_function_privilege(0, procedure.oid, 'EXECUTE')
            AS public_execute,
        pg_catalog.has_function_privilege(
            'taskforge_runtime', procedure.oid, 'EXECUTE'
        ) AS runtime_execute,
        pg_catalog.has_function_privilege(
            'taskforge_runtime', procedure.oid, 'EXECUTE WITH GRANT OPTION'
        ) AS runtime_grant_option
    FROM pg_catalog.pg_proc AS procedure
    JOIN pg_catalog.pg_namespace AS namespace
      ON namespace.oid = procedure.pronamespace
    JOIN pg_catalog.pg_database AS database
      ON database.datname = pg_catalog.current_database()
    WHERE namespace.nspname = 'public'
      AND procedure.proname = 'taskforge_schema_revisions'
      AND pg_catalog.pg_get_function_identity_arguments(procedure.oid) = ''
    """
)
_RUNTIME_VERSION_TABLE_PRIVILEGES = text(
    """
    SELECT
        pg_catalog.has_table_privilege(
            'taskforge_runtime', 'public.alembic_version', 'SELECT'
        ) AS can_select,
        pg_catalog.has_table_privilege(
            'taskforge_runtime', 'public.alembic_version', 'INSERT'
        ) AS can_insert,
        pg_catalog.has_table_privilege(
            'taskforge_runtime', 'public.alembic_version', 'UPDATE'
        ) AS can_update,
        pg_catalog.has_table_privilege(
            'taskforge_runtime', 'public.alembic_version', 'DELETE'
        ) AS can_delete
    """
)
_REPORTED_SCHEMA_REVISIONS = text("SELECT public.taskforge_schema_revisions()")


class MigrationAction(StrEnum):
    VERIFY = "verify"
    UPGRADE = "upgrade"


class UnsafeDatabaseState(RuntimeError):
    """Raised before DDL for a state the runner must not infer around."""


def _database_url(settings: MigrationSettings) -> URL:
    return URL.create(
        drivername="postgresql+asyncpg",
        username=settings.postgres_owner_user,
        password=settings.postgres_owner_password.get_secret_value(),
        host=settings.postgres_host,
        port=settings.postgres_port,
        database=settings.postgres_database,
    )


def _configuration() -> Config:
    return Config(ALEMBIC_CONFIGURATION)


def _known_ancestors(script: ScriptDirectory, head: str) -> set[str]:
    revisions: set[str] = set()
    current = script.get_revision(head)
    while current is not None:
        revisions.add(current.revision)
        down_revision = current.down_revision
        if down_revision is None:
            break
        if not isinstance(down_revision, str):
            raise UnsafeDatabaseState("the TaskForge migration graph is not linear")
        current = script.get_revision(down_revision)
    return revisions


async def classify_database_state(
    connection: AsyncConnection, script: ScriptDirectory
) -> MigrationAction:
    """Classify only states that can be migrated without guessing or stamping."""
    head = script.get_current_head()
    if head != EXPECTED_SCHEMA_REVISION:
        raise UnsafeDatabaseState(
            "the packaged migration head does not match the application contract"
        )
    has_version_table = await connection.scalar(_VERSION_TABLE)
    if has_version_table is not True:
        has_objects = await connection.scalar(_PUBLIC_SCHEMA_HAS_OBJECTS)
        if has_objects is True:
            raise UnsafeDatabaseState(
                "database has an unversioned nonempty public schema"
            )
        return MigrationAction.UPGRADE

    result = await connection.execute(_CURRENT_REVISIONS)
    revisions = tuple(result.scalars())
    if revisions == (head,):
        return MigrationAction.VERIFY
    if len(revisions) != 1:
        raise UnsafeDatabaseState(
            "database has an empty, divergent, or multiple revision state"
        )
    if revisions[0] not in _known_ancestors(script, head):
        raise UnsafeDatabaseState("database revision is not an ancestor of this build")
    return MigrationAction.UPGRADE


async def verify_schema_contract(connection: AsyncConnection) -> None:
    """Verify the complete runtime schema contract without repairing drift."""
    result = await connection.execute(_SCHEMA_FUNCTION_CONTRACT)
    functions = result.mappings().all()
    if len(functions) != 1:
        raise UnsafeDatabaseState(
            "post-migration schema revision function signature is invalid"
        )
    function = functions[0]
    if not (
        function["function_owner"]
        == function["database_owner"]
        == function["migration_owner"]
        and function["security_definer"] is True
        and function["volatility"] in ("s", b"s")
        and function["parallel_safety"] in ("s", b"s")
        and list(function["configuration"] or ()) == ["search_path=pg_catalog"]
        and function["result_type"] == "text[]"
        and function["public_execute"] is False
        and function["runtime_execute"] is True
        and function["runtime_grant_option"] is False
    ):
        raise UnsafeDatabaseState(
            "post-migration schema revision function contract has drifted"
        )

    privileges = (
        (await connection.execute(_RUNTIME_VERSION_TABLE_PRIVILEGES)).mappings().one()
    )
    if any(privileges.values()):
        raise UnsafeDatabaseState(
            "runtime role has direct Alembic revision table privileges"
        )

    revisions = await connection.scalar(_REPORTED_SCHEMA_REVISIONS)
    if revisions != [EXPECTED_SCHEMA_REVISION]:
        raise UnsafeDatabaseState(
            "post-migration schema contract did not report the expected head"
        )


def _upgrade_on_connection(connection: Connection, configuration: Config) -> int:
    configuration.attributes["connection"] = connection
    configuration.attributes["taskforge_migration_lock_held"] = True
    command.upgrade(configuration, "head")
    backend_pid = connection.scalar(text("SELECT pg_catalog.pg_backend_pid()"))
    if not isinstance(backend_pid, int):
        raise UnsafeDatabaseState("database did not report a migration backend PID")
    return backend_pid


async def _run() -> None:
    settings = MigrationSettings()
    configuration = _configuration()
    script = ScriptDirectory.from_config(configuration)
    engine = create_async_engine(
        _database_url(settings), pool_pre_ping=True, hide_parameters=True
    )
    started = time.monotonic()
    action: MigrationAction | None = None
    try:
        async with engine.connect() as connection:
            locked = False
            try:
                await acquire_migration_lock(
                    connection, settings.migration_lock_timeout_seconds
                )
                locked = True
                preflight_backend_pid = await connection.scalar(
                    text("SELECT pg_catalog.pg_backend_pid()")
                )
                action = await classify_database_state(connection, script)
                await connection.commit()
                if action is MigrationAction.UPGRADE:
                    alembic_backend_pid = await connection.run_sync(
                        lambda sync_connection: _upgrade_on_connection(
                            sync_connection, configuration
                        )
                    )
                    if alembic_backend_pid != preflight_backend_pid:
                        raise UnsafeDatabaseState(
                            "Alembic did not reuse the locked database session"
                        )
                await verify_schema_contract(connection)
                postflight_backend_pid = await connection.scalar(
                    text("SELECT pg_catalog.pg_backend_pid()")
                )
                if postflight_backend_pid != preflight_backend_pid:
                    raise UnsafeDatabaseState(
                        "postflight did not reuse the locked database session"
                    )
                await connection.commit()
            finally:
                if connection.in_transaction():
                    await connection.rollback()
                if locked:
                    await release_migration_lock(connection)
                    await connection.commit()
    finally:
        await engine.dispose()
    elapsed = time.monotonic() - started
    print(
        "TaskForge migration completed "
        f"action={action.value if action is not None else 'unknown'} "
        f"revision={EXPECTED_SCHEMA_REVISION} duration_seconds={elapsed:.3f}"
    )


def main() -> int:
    """Run migrations with safe diagnostics and a process-level exit contract."""
    try:
        asyncio.run(_run())
    except ValidationError:
        print("TaskForge migration configuration is invalid", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("TaskForge migration interrupted", file=sys.stderr)
        return 1
    except Exception as error:
        print(
            f"TaskForge migration failed error_type={type(error).__name__}",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
