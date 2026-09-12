"""Real PostgreSQL migration ownership, state, privilege, and locking contracts."""

from __future__ import annotations

import asyncio
import os
import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from taskforge.database_migrations import UnsafeDatabaseState, _run
from taskforge.persistence import migration_lock
from taskforge.persistence.migration_lock import (
    MigrationLockTimeout,
    acquire_migration_lock,
    release_migration_lock,
)
from taskforge.persistence.schema_compatibility import EXPECTED_SCHEMA_REVISION
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

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BOOTSTRAP_SCRIPT = PROJECT_ROOT / "docker/postgres/init-taskforge-roles.sh"
RUNTIME_ROLE = "taskforge_runtime"
RUNTIME_PASSWORD = "taskforge-safe-migration-runtime"
FUNCTION_SIGNATURE = "public.taskforge_schema_revisions()"
MIGRATION_ENVIRONMENT_NAMES = {
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DB",
    "POSTGRES_OWNER_USER",
    "POSTGRES_OWNER_PASSWORD",
    "TASKFORGE_MIGRATION_LOCK_TIMEOUT_SECONDS",
}


@contextmanager
def _runner_environment(database_url: URL, *, timeout: int = 5) -> Iterator[None]:
    previous = {name: os.environ.get(name) for name in MIGRATION_ENVIRONMENT_NAMES}
    values = {
        "POSTGRES_HOST": database_url.host or "",
        "POSTGRES_PORT": str(database_url.port or 5432),
        "POSTGRES_DB": database_url.database or "",
        "POSTGRES_OWNER_USER": database_url.username or "",
        "POSTGRES_OWNER_PASSWORD": database_url.password or "",
        "TASKFORGE_MIGRATION_LOCK_TIMEOUT_SECONDS": str(timeout),
    }
    os.environ.update(values)
    try:
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def _bootstrap(
    database_url: URL, *, timeout: int = 5, check: bool = True
) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment.update(
        {
            "PGHOST": database_url.host or "",
            "PGPORT": str(database_url.port or 5432),
            "PGPASSWORD": database_url.password or "",
            "POSTGRES_DB": database_url.database or "",
            "POSTGRES_USER": database_url.username or "",
            "TASKFORGE_RUNTIME_USER": RUNTIME_ROLE,
            "TASKFORGE_RUNTIME_PASSWORD": RUNTIME_PASSWORD,
            "TASKFORGE_MIGRATION_LOCK_TIMEOUT_SECONDS": str(timeout),
        }
    )
    return subprocess.run(
        ["sh", str(BOOTSTRAP_SCRIPT)],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        check=check,
        text=True,
        timeout=timeout + 10,
    )


def _runtime_url(owner_url: URL) -> URL:
    return owner_url.set(username=RUNTIME_ROLE, password=RUNTIME_PASSWORD)


async def _revision_contract(owner_url: URL) -> None:
    owner = await asyncpg.connect(asyncpg_dsn(owner_url))
    await owner.execute(f"ALTER ROLE {RUNTIME_ROLE} PASSWORD '{RUNTIME_PASSWORD}'")
    runtime = await asyncpg.connect(asyncpg_dsn(_runtime_url(owner_url)))
    try:
        assert (
            await owner.fetchval("SELECT version_num FROM public.alembic_version")
            == EXPECTED_SCHEMA_REVISION
        )
        function = await owner.fetchrow(
            "SELECT procedure.prosecdef, procedure.provolatile, "
            "procedure.proparallel, procedure.proconfig, "
            "pg_catalog.pg_get_userbyid(procedure.proowner) AS owner "
            "FROM pg_catalog.pg_proc AS procedure "
            "JOIN pg_catalog.pg_namespace AS namespace "
            "ON namespace.oid = procedure.pronamespace "
            "WHERE namespace.nspname = 'public' "
            "AND procedure.proname = 'taskforge_schema_revisions' "
            "AND pg_catalog.pg_get_function_identity_arguments(procedure.oid) = ''"
        )
        assert function is not None
        assert function["prosecdef"] is True
        assert function["provolatile"] == b"s"
        assert function["proparallel"] == b"s"
        assert function["proconfig"] == ["search_path=pg_catalog"]
        assert function["owner"] == await owner.fetchval("SELECT current_user")
        assert function["owner"] != RUNTIME_ROLE
        assert await owner.fetchval(
            "SELECT pg_catalog.has_function_privilege($1, $2, 'EXECUTE')",
            RUNTIME_ROLE,
            FUNCTION_SIGNATURE,
        )
        assert not await owner.fetchval(
            "SELECT pg_catalog.has_function_privilege(0, $1, 'EXECUTE')",
            FUNCTION_SIGNATURE,
        )
        assert not await owner.fetchval(
            "SELECT pg_catalog.has_function_privilege("
            "$1, $2, 'EXECUTE WITH GRANT OPTION')",
            RUNTIME_ROLE,
            FUNCTION_SIGNATURE,
        )
        assert await runtime.fetchval("SELECT public.taskforge_schema_revisions()") == [
            EXPECTED_SCHEMA_REVISION
        ]
        for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE"):
            assert not await owner.fetchval(
                "SELECT pg_catalog.has_table_privilege($1, $2, $3)",
                RUNTIME_ROLE,
                "public.alembic_version",
                privilege,
            )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await runtime.fetch("SELECT version_num FROM public.alembic_version")
    finally:
        await runtime.close()
        await owner.close()


def test_clean_upgrade_noop_known_ancestor_and_runtime_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_safe_migration"
    ) as database_url:
        _bootstrap(database_url)
        with monkeypatch.context() as injected:
            injected.setattr(
                migration_lock,
                "acquire_migration_lock_sync",
                lambda *args, **kwargs: pytest.fail(
                    "injected runner connection reacquired migration lock"
                ),
            )
            with _runner_environment(database_url):
                asyncio.run(_run())
        with _runner_environment(database_url):
            asyncio.run(_revision_contract(database_url))
            asyncio.run(_run())

        rendered = database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        acquisitions = 0
        releases = 0
        original_acquire = migration_lock.acquire_migration_lock_sync
        original_release = migration_lock.release_migration_lock_sync

        def count_acquire(*args: object, **kwargs: object) -> None:
            nonlocal acquisitions
            acquisitions += 1
            original_acquire(*args, **kwargs)  # type: ignore[arg-type]

        def count_release(*args: object, **kwargs: object) -> None:
            nonlocal releases
            releases += 1
            original_release(*args, **kwargs)  # type: ignore[arg-type]

        with monkeypatch.context() as direct:
            direct.setattr(migration_lock, "acquire_migration_lock_sync", count_acquire)
            direct.setattr(migration_lock, "release_migration_lock_sync", count_release)
            with migration_database_url(rendered):
                command.downgrade(Config("alembic.ini"), "0031_lock_worker_authority")
        assert acquisitions == releases == 1
        with _runner_environment(database_url):
            asyncio.run(_run())
        asyncio.run(_revision_contract(database_url))


async def _prepare_dirty_schema(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        await connection.execute("CREATE TABLE unversioned_operator_table(id integer)")
    finally:
        await connection.close()


async def _prepare_dirty_type(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        await connection.execute(
            "CREATE TYPE public.unversioned_operator_state "
            "AS ENUM ('created_without_alembic')"
        )
    finally:
        await connection.close()


async def _dirty_type_was_preserved(database_url: URL) -> bool:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        return bool(
            await connection.fetchval(
                "SELECT pg_catalog.to_regtype("
                "'public.unversioned_operator_state') IS NOT NULL "
                "AND pg_catalog.to_regclass('public.alembic_version') IS NULL"
            )
        )
    finally:
        await connection.close()


async def _prepare_unknown_revision(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        await connection.execute(
            "UPDATE public.alembic_version SET version_num = 'future_unknown_head'"
        )
        await connection.execute(
            "CREATE TABLE migration_preservation_marker(id integer PRIMARY KEY)"
        )
    finally:
        await connection.close()


async def _marker_exists(database_url: URL) -> bool:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        return bool(
            await connection.fetchval(
                "SELECT pg_catalog.to_regclass("
                "'public.migration_preservation_marker') IS NOT NULL"
            )
        )
    finally:
        await connection.close()


def test_dirty_unversioned_and_unknown_revision_are_refused_without_repair() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_safe_migration"
    ) as dirty_url:
        _bootstrap(dirty_url)
        asyncio.run(_prepare_dirty_schema(dirty_url))
        with _runner_environment(dirty_url), pytest.raises(UnsafeDatabaseState):
            asyncio.run(_run())

    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_safe_migration"
    ) as dirty_type_url:
        _bootstrap(dirty_type_url)
        asyncio.run(_prepare_dirty_type(dirty_type_url))
        with _runner_environment(dirty_type_url), pytest.raises(UnsafeDatabaseState):
            asyncio.run(_run())
        assert asyncio.run(_dirty_type_was_preserved(dirty_type_url))

    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_safe_migration"
    ) as unknown_url:
        _bootstrap(unknown_url)
        with _runner_environment(unknown_url):
            asyncio.run(_run())
        asyncio.run(_prepare_unknown_revision(unknown_url))
        with _runner_environment(unknown_url), pytest.raises(UnsafeDatabaseState):
            asyncio.run(_run())
        assert asyncio.run(_marker_exists(unknown_url))


async def _drift_schema_contract(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        await connection.execute(
            "GRANT EXECUTE ON FUNCTION public.taskforge_schema_revisions() TO PUBLIC"
        )
        await connection.execute(
            "ALTER FUNCTION public.taskforge_schema_revisions() "
            "SET search_path = public"
        )
    finally:
        await connection.close()


async def _assert_schema_contract_remains_drifted(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        assert (
            await connection.fetchval("SELECT version_num FROM public.alembic_version")
            == EXPECTED_SCHEMA_REVISION
        )
        assert await connection.fetchval(
            "SELECT pg_catalog.has_function_privilege(0, $1, 'EXECUTE')",
            FUNCTION_SIGNATURE,
        )
        assert await connection.fetchval(
            "SELECT procedure.proconfig = ARRAY['search_path=public']::text[] "
            "FROM pg_catalog.pg_proc AS procedure "
            "JOIN pg_catalog.pg_namespace AS namespace "
            "ON namespace.oid = procedure.pronamespace "
            "WHERE namespace.nspname = 'public' "
            "AND procedure.proname = 'taskforge_schema_revisions' "
            "AND pg_catalog.pg_get_function_identity_arguments(procedure.oid) = ''"
        )
    finally:
        await connection.close()


def test_at_head_contract_drift_is_refused_without_automatic_repair() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_safe_migration"
    ) as database_url:
        _bootstrap(database_url)
        with _runner_environment(database_url):
            asyncio.run(_run())
        asyncio.run(_drift_schema_contract(database_url))

        with _runner_environment(database_url), pytest.raises(UnsafeDatabaseState):
            asyncio.run(_run())

        asyncio.run(_assert_schema_contract_remains_drifted(database_url))


async def _migration_runner_serialization(database_url: URL) -> None:
    engine = create_async_engine(database_url.set(drivername="postgresql+asyncpg"))
    try:
        async with engine.connect() as holder:
            await acquire_migration_lock(holder, 2)
            with _runner_environment(database_url):
                first = asyncio.create_task(_run())
                second = asyncio.create_task(_run())
                await asyncio.sleep(0.2)
                assert not first.done()
                assert not second.done()
                await release_migration_lock(holder)
                await holder.commit()
                await asyncio.gather(first, second)
    finally:
        await engine.dispose()


async def _hold_lock_while_runner_times_out(database_url: URL) -> None:
    engine = create_async_engine(database_url.set(drivername="postgresql+asyncpg"))
    try:
        async with engine.connect() as holder:
            await acquire_migration_lock(holder, 2)
            with _runner_environment(database_url, timeout=1):
                with pytest.raises(MigrationLockTimeout):
                    await _run()
            await release_migration_lock(holder)
            await holder.commit()
    finally:
        await engine.dispose()


async def _bootstrap_and_migration_serialization(database_url: URL) -> None:
    engine = create_async_engine(database_url.set(drivername="postgresql+asyncpg"))
    try:
        async with engine.connect() as holder:
            await acquire_migration_lock(holder, 2)
            with _runner_environment(database_url):
                first_bootstrap = asyncio.create_task(
                    asyncio.to_thread(_bootstrap, database_url)
                )
                second_bootstrap = asyncio.create_task(
                    asyncio.to_thread(_bootstrap, database_url)
                )
                migration = asyncio.create_task(_run())
                await asyncio.sleep(0.2)
                assert not first_bootstrap.done()
                assert not second_bootstrap.done()
                assert not migration.done()
                await release_migration_lock(holder)
                await holder.commit()
                first_result, second_result, _ = await asyncio.gather(
                    first_bootstrap, second_bootstrap, migration
                )
                assert first_result.returncode == 0
                assert second_result.returncode == 0
    finally:
        await engine.dispose()


def test_migrators_serialize_and_timeout_without_losing_persistent_state() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_safe_migration"
    ) as database_url:
        _bootstrap(database_url)
        with _runner_environment(database_url):
            asyncio.run(_run())
        asyncio.run(_migration_runner_serialization(database_url))
        asyncio.run(_bootstrap_and_migration_serialization(database_url))
        asyncio.run(_hold_lock_while_runner_times_out(database_url))
        asyncio.run(_revision_contract(database_url))


async def _drop_runtime_and_acquire_lock(
    database_url: URL,
) -> asyncpg.Connection[asyncpg.Record]:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    await connection.execute(f'DROP ROLE IF EXISTS "{RUNTIME_ROLE}"')
    key = await connection.fetchval(
        "SELECT (CAST(1413893447 AS bigint) << 32) | "
        "(SELECT oid::bigint FROM pg_catalog.pg_database "
        "WHERE datname = pg_catalog.current_database())"
    )
    await connection.fetchval("SELECT pg_catalog.pg_advisory_lock($1)", key)
    return connection


async def _bootstrap_timeout_scenario(database_url: URL) -> None:
    holder = await _drop_runtime_and_acquire_lock(database_url)
    try:
        result = await asyncio.to_thread(
            _bootstrap, database_url, timeout=1, check=False
        )
        assert result.returncode != 0
        assert "timed out" in result.stderr
    finally:
        await holder.close()


def test_bootstrap_waits_before_reconciliation_and_timeout_releases_session() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_safe_migration"
    ) as database_url:
        asyncio.run(_bootstrap_timeout_scenario(database_url))

        _bootstrap(database_url)
        with _runner_environment(database_url):
            asyncio.run(_run())
        asyncio.run(_revision_contract(database_url))


async def _prepare_bootstrap_precondition_failure(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        await connection.execute(f'DROP ROLE IF EXISTS "{RUNTIME_ROLE}"')
        await connection.execute(
            "CREATE ROLE incompatible_taskforge_object_owner NOLOGIN"
        )
        await connection.execute("CREATE TABLE api_credentials(id integer)")
        await connection.execute(
            "ALTER TABLE api_credentials OWNER TO incompatible_taskforge_object_owner"
        )
    finally:
        await connection.close()


async def _repair_bootstrap_precondition(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        await connection.execute("DROP TABLE api_credentials")
        await connection.execute("DROP ROLE incompatible_taskforge_object_owner")
    finally:
        await connection.close()


async def _runtime_exists_and_lock_is_available(database_url: URL) -> tuple[bool, bool]:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        runtime_exists = bool(
            await connection.fetchval(
                "SELECT EXISTS(SELECT FROM pg_catalog.pg_roles WHERE rolname = $1)",
                RUNTIME_ROLE,
            )
        )
        key = await connection.fetchval(
            "SELECT (CAST(1413893447 AS bigint) << 32) | "
            "(SELECT oid::bigint FROM pg_catalog.pg_database "
            "WHERE datname = pg_catalog.current_database())"
        )
        available = bool(
            await connection.fetchval("SELECT pg_catalog.pg_try_advisory_lock($1)", key)
        )
        if available:
            await connection.fetchval("SELECT pg_catalog.pg_advisory_unlock($1)", key)
        return runtime_exists, available
    finally:
        await connection.close()


def test_bootstrap_sql_failure_stops_later_work_and_releases_session_lock() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_safe_migration"
    ) as database_url:
        asyncio.run(_prepare_bootstrap_precondition_failure(database_url))
        result = _bootstrap(database_url, check=False)

        assert result.returncode != 0
        runtime_exists, lock_available = asyncio.run(
            _runtime_exists_and_lock_is_available(database_url)
        )
        assert runtime_exists is False
        assert lock_available is True

        asyncio.run(_repair_bootstrap_precondition(database_url))
        _bootstrap(database_url)
        assert asyncio.run(_runtime_exists_and_lock_is_available(database_url)) == (
            True,
            True,
        )


def test_bootstrap_is_one_persistent_fail_fast_psql_session() -> None:
    script = BOOTSTRAP_SCRIPT.read_text(encoding="utf-8")

    assert script.count("psql -v ON_ERROR_STOP=1") == 1
    assert script.count("<<'SQL'") == 1
    assert script.count("taskforge.bootstrap_backend_pid") >= 3
    assert "clock_timestamp()" in script
    assert "pg_try_advisory_lock" in script
    assert "pg_advisory_unlock" in script
