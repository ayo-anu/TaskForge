"""Runtime worker-authority function, privilege, and lock-time contracts."""

from __future__ import annotations

import asyncio
import os
import subprocess
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.persistence.workers import SQLAlchemyWorkerRegistrationRepository
from taskforge.worker.domain import RegisteredWorkerSession, WorkerRegistration
from taskforge.worker.persistence_ports import WorkerRegistrationAuthorityRejected
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
RUNTIME_PASSWORD = "taskforge-worker-authority-runtime"
FUNCTION_SIGNATURE = "public.lock_valid_worker_authority(uuid, uuid)"


def _run_bootstrap(database_url: URL) -> None:
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
        }
    )
    subprocess.run(
        ["sh", str(BOOTSTRAP_SCRIPT)],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        check=True,
        text=True,
    )


async def _drop_unconfigured_runtime_role(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        await connection.execute(f'DROP ROLE "{RUNTIME_ROLE}"')
    finally:
        await connection.close()


def _runtime_url(database_url: URL) -> URL:
    return database_url.set(username=RUNTIME_ROLE, password=RUNTIME_PASSWORD)


async def _seed_authority(
    connection: asyncpg.Connection[asyncpg.Record],
    *,
    expires_at: datetime | None = None,
) -> AuthenticatedWorker:
    identity_id = uuid4()
    credential_id = uuid4()
    created_at = datetime.now(UTC) - timedelta(hours=1)
    await connection.execute(
        "INSERT INTO worker_identities (id, name, created_at) VALUES ($1, $2, $3)",
        identity_id,
        f"authority-function-worker-{uuid4().hex}",
        created_at,
    )
    await connection.execute(
        "INSERT INTO worker_credentials "
        "(id, worker_identity_id, credential_verifier, created_at, expires_at) "
        "VALUES ($1, $2, 'unused-authority-test-verifier', $3, $4)",
        credential_id,
        identity_id,
        created_at,
        expires_at or datetime.now(UTC) + timedelta(hours=1),
    )
    return AuthenticatedWorker(identity_id, credential_id)


async def _call_authority_function(
    connection: asyncpg.Connection[asyncpg.Record],
    authority: AuthenticatedWorker,
) -> bool:
    return bool(
        await connection.fetchval(
            "SELECT public.lock_valid_worker_authority("
            "$1::pg_catalog.uuid, $2::pg_catalog.uuid)",
            authority.worker_identity_id,
            authority.credential_id,
        )
    )


async def _assert_function_and_runtime_boundary(
    owner_url: URL,
    runtime_url: URL,
) -> None:
    owner = await asyncpg.connect(asyncpg_dsn(owner_url))
    runtime = await asyncpg.connect(asyncpg_dsn(runtime_url))
    try:
        function = await owner.fetchrow(
            "SELECT procedure.prosecdef, procedure.provolatile, "
            "procedure.proparallel, procedure.proconfig, "
            "pg_catalog.pg_get_userbyid(procedure.proowner) AS owner "
            "FROM pg_catalog.pg_proc AS procedure "
            "JOIN pg_catalog.pg_namespace AS namespace "
            "ON namespace.oid = procedure.pronamespace "
            "WHERE namespace.nspname = 'public' "
            "AND procedure.proname = 'lock_valid_worker_authority' "
            "AND pg_catalog.pg_get_function_identity_arguments(procedure.oid) "
            "= 'p_worker_identity_id uuid, p_credential_id uuid'"
        )
        assert function is not None
        assert function["prosecdef"] is True
        assert function["provolatile"] == b"v"
        assert function["proparallel"] == b"u"
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
        assert not await owner.fetchval(
            "SELECT pg_catalog.has_schema_privilege($1, 'public', 'CREATE')",
            RUNTIME_ROLE,
        )
        assert await owner.fetchval(
            "SELECT NOT EXISTS ("
            "SELECT FROM pg_catalog.pg_class "
            "WHERE relowner = (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = $1)"
            ") AND NOT EXISTS ("
            "SELECT FROM pg_catalog.pg_namespace "
            "WHERE nspowner = (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = $1)"
            ") AND NOT EXISTS ("
            "SELECT FROM pg_catalog.pg_proc "
            "WHERE proowner = (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = $1)"
            ")",
            RUNTIME_ROLE,
        )
        for table in ("worker_identities", "worker_credentials"):
            assert not await owner.fetchval(
                "SELECT pg_catalog.has_table_privilege($1, $2, 'UPDATE')",
                RUNTIME_ROLE,
                table,
            )
            columns = await owner.fetch(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = $1",
                table,
            )
            assert columns
            for column in columns:
                assert not await owner.fetchval(
                    "SELECT pg_catalog.has_column_privilege($1, $2, $3, 'UPDATE')",
                    RUNTIME_ROLE,
                    table,
                    column["column_name"],
                )

        authority = await _seed_authority(owner)
        assert await runtime.fetchval("SELECT current_user") == RUNTIME_ROLE
        assert await _call_authority_function(runtime, authority)
        assert not await _call_authority_function(
            runtime, AuthenticatedWorker(uuid4(), uuid4())
        )
        assert not await _call_authority_function(
            runtime, AuthenticatedWorker(authority.worker_identity_id, uuid4())
        )

        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await runtime.execute(
                "SELECT id FROM worker_identities WHERE id = $1 FOR SHARE",
                authority.worker_identity_id,
            )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await runtime.execute(
                "SELECT id FROM worker_credentials WHERE id = $1 FOR SHARE",
                authority.credential_id,
            )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await runtime.execute(
                "UPDATE worker_identities SET disabled_at = clock_timestamp() "
                "WHERE id = $1",
                authority.worker_identity_id,
            )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await runtime.execute(
                "UPDATE worker_credentials SET revoked_at = clock_timestamp() "
                "WHERE id = $1",
                authority.credential_id,
            )

        wrong_credential = await _seed_authority(owner)
        assert not await _call_authority_function(
            runtime,
            AuthenticatedWorker(
                authority.worker_identity_id, wrong_credential.credential_id
            ),
        )
        await owner.execute(
            "UPDATE worker_identities SET disabled_at = clock_timestamp() WHERE id = $1",
            authority.worker_identity_id,
        )
        assert not await _call_authority_function(runtime, authority)

        revoked = await _seed_authority(owner)
        await owner.execute(
            "UPDATE worker_credentials SET revoked_at = clock_timestamp() WHERE id = $1",
            revoked.credential_id,
        )
        assert not await _call_authority_function(runtime, revoked)

        expired = await _seed_authority(
            owner, expires_at=datetime.now(UTC) - timedelta(seconds=1)
        )
        assert not await _call_authority_function(runtime, expired)
    finally:
        await runtime.close()
        await owner.close()


async def _wait_for_lock_wait(
    observer: asyncpg.Connection[asyncpg.Record],
    application_name: str,
) -> None:
    for _ in range(200):
        if await observer.fetchval(
            "SELECT EXISTS (SELECT FROM pg_catalog.pg_stat_activity "
            "WHERE application_name = $1 AND wait_event_type = 'Lock')",
            application_name,
        ):
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"{application_name} did not enter a database lock wait")


async def _assert_expiry_is_checked_after_identity_lock(
    owner_url: URL,
    runtime_url: URL,
) -> None:
    owner = await asyncpg.connect(asyncpg_dsn(owner_url))
    runtime = await asyncpg.connect(
        asyncpg_dsn(runtime_url),
        server_settings={"application_name": "authority-expiry-waiter"},
    )
    observer = await asyncpg.connect(asyncpg_dsn(owner_url))
    transaction = owner.transaction()
    pending: asyncio.Task[bool] | None = None
    try:
        expires_at = await owner.fetchval(
            "SELECT clock_timestamp() + interval '3 seconds'"
        )
        authority = await _seed_authority(owner, expires_at=expires_at)
        await transaction.start()
        await owner.execute(
            "SELECT id FROM worker_identities WHERE id = $1 FOR UPDATE",
            authority.worker_identity_id,
        )
        pending = asyncio.create_task(_call_authority_function(runtime, authority))
        await _wait_for_lock_wait(observer, "authority-expiry-waiter")

        while await observer.fetchval(
            "SELECT pg_catalog.clock_timestamp() <= $1", expires_at
        ):
            await asyncio.sleep(0.02)

        await transaction.commit()
        assert not await pending
    finally:
        if pending is not None and not pending.done():
            pending.cancel()
            with suppress(asyncio.CancelledError):
                await pending
        if owner.is_in_transaction():
            await transaction.rollback()
        await observer.close()
        await runtime.close()
        await owner.close()


def _registration_repository(
    runtime_url: URL,
    application_name: str,
) -> tuple[SQLAlchemyWorkerRegistrationRepository, AsyncEngine]:
    engine = create_async_engine(
        runtime_url.set(drivername="postgresql+asyncpg"),
        connect_args={"server_settings": {"application_name": application_name}},
    )
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    return SQLAlchemyWorkerRegistrationRepository(sessions), engine


def _authority_mutation(
    authority: AuthenticatedWorker,
    authority_table: str,
) -> tuple[str, UUID]:
    if authority_table == "worker_identities":
        return (
            "UPDATE worker_identities SET disabled_at = clock_timestamp() "
            "WHERE id = $1",
            authority.worker_identity_id,
        )
    if authority_table == "worker_credentials":
        return (
            "UPDATE worker_credentials SET revoked_at = clock_timestamp() "
            "WHERE id = $1",
            authority.credential_id,
        )
    raise AssertionError(f"unexpected authority table {authority_table}")


async def _assert_no_registration_rows(
    connection: asyncpg.Connection[asyncpg.Record],
    session_id: UUID,
) -> None:
    row = await connection.fetchrow(
        "SELECT "
        "(SELECT count(*) FROM worker_sessions WHERE id = $1) AS sessions, "
        "(SELECT count(*) FROM worker_session_capabilities "
        " WHERE worker_session_id = $1) AS capabilities, "
        "(SELECT count(*) FROM worker_session_health "
        " WHERE worker_session_id = $1) AS health",
        session_id,
    )
    assert row is not None
    assert tuple(row) == (0, 0, 0)


async def _assert_administrator_first_registration(
    owner_url: URL,
    runtime_url: URL,
    authority_table: str,
    *,
    commit_mutation: bool,
) -> None:
    seed_connection = await asyncpg.connect(asyncpg_dsn(owner_url))
    try:
        authority = await _seed_authority(seed_connection)
    finally:
        await seed_connection.close()
    application_name = (
        f"registration-behind-{authority_table}-"
        f"{'commit' if commit_mutation else 'rollback'}"
    )
    repository, engine = _registration_repository(runtime_url, application_name)
    mutator = await asyncpg.connect(asyncpg_dsn(owner_url))
    observer = await asyncpg.connect(asyncpg_dsn(owner_url))
    transaction = mutator.transaction()
    registration: asyncio.Task[RegisteredWorkerSession] | None = None
    session_id = uuid4()
    await transaction.start()
    try:
        statement, target_id = _authority_mutation(authority, authority_table)
        await mutator.execute(statement, target_id)
        registration = asyncio.create_task(
            repository.register_session(
                authority,
                session_id,
                WorkerRegistration(("pipeline.processing",)),
            )
        )
        await _wait_for_lock_wait(observer, application_name)

        if commit_mutation:
            await transaction.commit()
            with pytest.raises(WorkerRegistrationAuthorityRejected):
                await registration
            await _assert_no_registration_rows(observer, session_id)
        else:
            await transaction.rollback()
            registered = await registration
            assert registered.id == session_id
    finally:
        if registration is not None and not registration.done():
            registration.cancel()
            with suppress(asyncio.CancelledError):
                await registration
        if mutator.is_in_transaction():
            await transaction.rollback()
        await observer.close()
        await mutator.close()
        await engine.dispose()


async def _assert_registration_first(
    owner_url: URL,
    runtime_url: URL,
    authority_table: str,
) -> None:
    seed_connection = await asyncpg.connect(asyncpg_dsn(owner_url))
    try:
        authority = await _seed_authority(seed_connection)
    finally:
        await seed_connection.close()
    registration_name = f"registration-first-{authority_table}"
    mutation_name = f"mutation-behind-registration-{authority_table}"
    repository, engine = _registration_repository(runtime_url, registration_name)
    table_blocker = await asyncpg.connect(asyncpg_dsn(owner_url))
    mutator = await asyncpg.connect(
        asyncpg_dsn(owner_url),
        server_settings={"application_name": mutation_name},
    )
    observer = await asyncpg.connect(asyncpg_dsn(owner_url))
    table_transaction = table_blocker.transaction()
    mutation_transaction = mutator.transaction()
    registration: asyncio.Task[RegisteredWorkerSession] | None = None
    mutation: asyncio.Task[str] | None = None
    session_id = uuid4()
    await table_transaction.start()
    await mutation_transaction.start()
    try:
        await table_blocker.execute(
            "LOCK TABLE worker_sessions IN ACCESS EXCLUSIVE MODE"
        )
        registration = asyncio.create_task(
            repository.register_session(
                authority,
                session_id,
                WorkerRegistration(("pipeline.processing",)),
            )
        )
        await _wait_for_lock_wait(observer, registration_name)

        statement, target_id = _authority_mutation(authority, authority_table)
        mutation = asyncio.create_task(mutator.execute(statement, target_id))
        await _wait_for_lock_wait(observer, mutation_name)

        await table_transaction.commit()
        registered = await registration
        assert registered.id == session_id
        await mutation
        await mutation_transaction.commit()
        assert not await _call_authority_function(observer, authority)
    finally:
        for pending in (registration, mutation):
            if pending is not None and not pending.done():
                pending.cancel()
                with suppress(asyncio.CancelledError):
                    await pending
        if table_blocker.is_in_transaction():
            await table_transaction.rollback()
        if mutator.is_in_transaction():
            await mutation_transaction.rollback()
        await observer.close()
        await mutator.close()
        await table_blocker.close()
        await engine.dispose()


async def _assert_registration_concurrency_matrix(
    owner_url: URL,
    runtime_url: URL,
) -> None:
    for authority_table in ("worker_identities", "worker_credentials"):
        await _assert_administrator_first_registration(
            owner_url, runtime_url, authority_table, commit_mutation=True
        )
        await _assert_administrator_first_registration(
            owner_url, runtime_url, authority_table, commit_mutation=False
        )
        await _assert_registration_first(owner_url, runtime_url, authority_table)


async def _assert_function_absent(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        assert not await connection.fetchval(
            "SELECT pg_catalog.to_regprocedure($1) IS NOT NULL",
            FUNCTION_SIGNATURE,
        )
    finally:
        await connection.close()


async def _assert_function_execute_acl(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        assert await connection.fetchval(
            "SELECT pg_catalog.has_function_privilege($1, $2, 'EXECUTE')",
            RUNTIME_ROLE,
            FUNCTION_SIGNATURE,
        )
        assert not await connection.fetchval(
            "SELECT pg_catalog.has_function_privilege(0, $1, 'EXECUTE')",
            FUNCTION_SIGNATURE,
        )
    finally:
        await connection.close()


def test_worker_authority_upgrade_downgrade_reupgrade() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL",
        "taskforge_worker_authority",
    ) as database_url:
        asyncio.run(_drop_unconfigured_runtime_role(database_url))
        _run_bootstrap(database_url)
        rendered = database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        configuration = Config("alembic.ini")
        with migration_database_url(rendered):
            command.upgrade(configuration, "0030_credential_lifecycle")
            asyncio.run(_assert_function_absent(database_url))
            command.upgrade(configuration, "head")
            asyncio.run(
                _assert_function_and_runtime_boundary(
                    database_url, _runtime_url(database_url)
                )
            )
            asyncio.run(
                _assert_expiry_is_checked_after_identity_lock(
                    database_url, _runtime_url(database_url)
                )
            )
            asyncio.run(
                _assert_registration_concurrency_matrix(
                    database_url, _runtime_url(database_url)
                )
            )
            command.downgrade(configuration, "0030_credential_lifecycle")
            asyncio.run(_assert_function_absent(database_url))
            command.upgrade(configuration, "head")
            asyncio.run(_assert_function_execute_acl(database_url))
