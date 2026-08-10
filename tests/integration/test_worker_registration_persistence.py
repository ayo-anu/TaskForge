"""Opt-in PostgreSQL verification for atomic worker registration."""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.persistence.workers import SQLAlchemyWorkerRegistrationRepository
from taskforge.worker.domain import WorkerRegistration
from taskforge.worker.persistence_ports import (
    WorkerRegistrationAuthorityRejected,
    WorkerRegistrationRecordConflict,
)
from tests.integration.postgresql import (
    asyncpg_dsn,
    migration_database_url,
    temporary_database,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_WORKER_REGISTRATION_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_WORKER_REGISTRATION_INTEGRATION=1 explicitly",
    ),
]


async def seed_authority(
    database_url: URL,
    *,
    disabled: bool = False,
    revoked: bool = False,
    expired: bool = False,
) -> AuthenticatedWorker:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        identity_id, credential_id = uuid4(), uuid4()
        now = datetime.now(UTC)
        created_at = now - timedelta(hours=1)
        await connection.execute(
            "INSERT INTO worker_identities (id, name, created_at, disabled_at) "
            "VALUES ($1, $2, $3, $4)",
            identity_id,
            f"registration-worker-{uuid4().hex}",
            created_at,
            now if disabled else None,
        )
        await connection.execute(
            "INSERT INTO worker_credentials "
            "(id, worker_identity_id, credential_verifier, created_at, expires_at, "
            "revoked_at) VALUES ($1, $2, 'unused-test-verifier', $3, $4, $5)",
            credential_id,
            identity_id,
            created_at,
            now - timedelta(seconds=1) if expired else now + timedelta(hours=1),
            now if revoked else None,
        )
        return AuthenticatedWorker(identity_id, credential_id)
    finally:
        await connection.close()


def repository_for(
    database_url: URL,
) -> tuple[SQLAlchemyWorkerRegistrationRepository, object]:
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg"),
        connect_args={"server_settings": {"application_name": "worker-registration"}},
    )
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    return SQLAlchemyWorkerRegistrationRepository(sessions), engine


async def assert_complete_registration(database_url: URL) -> None:
    authority = await seed_authority(database_url)
    repository, engine = repository_for(database_url)
    session_id = uuid4()
    try:
        registered = await repository.register_session(
            authority,
            session_id,
            WorkerRegistration(("documents", "notifications.email")),
        )
        assert registered.id == session_id
        assert registered.capabilities == ("documents", "notifications.email")

        connection = await asyncpg.connect(asyncpg_dsn(database_url))
        try:
            aggregate = await connection.fetchrow(
                "SELECT s.registered_at, h.last_sequence, h.last_seen_at, "
                "h.accepting_work, h.availability_changed_at, "
                "(SELECT count(*) FROM worker_session_capabilities c "
                " WHERE c.worker_session_id = s.id) AS capability_count, "
                "(SELECT count(*) FROM worker_heartbeats b "
                " WHERE b.worker_session_id = s.id) AS heartbeat_count "
                "FROM worker_sessions s JOIN worker_session_health h "
                "ON h.worker_session_id = s.id WHERE s.id = $1",
                session_id,
            )
            assert aggregate is not None
            assert aggregate["registered_at"] == registered.registered_at
            assert aggregate["last_sequence"] == 0
            assert aggregate["last_seen_at"] == aggregate["registered_at"]
            assert aggregate["availability_changed_at"] == aggregate["registered_at"]
            assert aggregate["accepting_work"] is False
            assert aggregate["capability_count"] == 2
            assert aggregate["heartbeat_count"] == 0
        finally:
            await connection.close()
    finally:
        await engine.dispose()  # type: ignore[attr-defined]


async def assert_empty_and_concurrent_sessions(database_url: URL) -> None:
    authority = await seed_authority(database_url)
    repository, engine = repository_for(database_url)
    session_ids = (uuid4(), uuid4(), uuid4())
    try:
        empty = await repository.register_session(
            authority, session_ids[0], WorkerRegistration(())
        )
        assert empty.capabilities == ()
        first, second = await asyncio.gather(
            repository.register_session(
                authority, session_ids[1], WorkerRegistration(("documents",))
            ),
            repository.register_session(
                authority, session_ids[2], WorkerRegistration(("documents",))
            ),
        )
        assert {first.id, second.id} == {session_ids[1], session_ids[2]}

        connection = await asyncpg.connect(asyncpg_dsn(database_url))
        try:
            assert await connection.fetchval(
                "SELECT count(*) FROM worker_sessions WHERE worker_identity_id = $1",
                authority.worker_identity_id,
            ) == len(session_ids)
        finally:
            await connection.close()
    finally:
        await engine.dispose()  # type: ignore[attr-defined]


async def assert_atomic_rollback(database_url: URL) -> None:
    authority = await seed_authority(database_url)
    repository, engine = repository_for(database_url)
    failed_session_id = uuid4()
    try:
        with pytest.raises(WorkerRegistrationRecordConflict):
            await repository.register_session(
                authority,
                failed_session_id,
                WorkerRegistration(("documents", "documents")),
            )
        connection = await asyncpg.connect(asyncpg_dsn(database_url))
        try:
            counts = await connection.fetchrow(
                "SELECT "
                "(SELECT count(*) FROM worker_sessions WHERE id = $1) sessions, "
                "(SELECT count(*) FROM worker_session_capabilities "
                " WHERE worker_session_id = $1) capabilities, "
                "(SELECT count(*) FROM worker_session_health "
                " WHERE worker_session_id = $1) health",
                failed_session_id,
            )
            assert counts is not None
            assert tuple(counts) == (0, 0, 0)
        finally:
            await connection.close()
    finally:
        await engine.dispose()  # type: ignore[attr-defined]


async def assert_invalid_authority_rejected(database_url: URL) -> None:
    repository, engine = repository_for(database_url)
    try:
        for options in (
            {"disabled": True},
            {"revoked": True},
            {"expired": True},
        ):
            authority = await seed_authority(database_url, **options)
            with pytest.raises(WorkerRegistrationAuthorityRejected):
                await repository.register_session(
                    authority, uuid4(), WorkerRegistration(())
                )
    finally:
        await engine.dispose()  # type: ignore[attr-defined]


async def wait_for_lock_wait(
    observer: asyncpg.Connection[asyncpg.Record],
    application_name: str,
) -> None:
    for _ in range(100):
        waiting = await observer.fetchval(
            "SELECT EXISTS (SELECT FROM pg_stat_activity "
            "WHERE application_name = $1 AND wait_event_type = 'Lock')",
            application_name,
        )
        if waiting:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"{application_name} did not enter a database lock wait")


async def assert_identity_then_credential_lock_order(database_url: URL) -> None:
    authority = await seed_authority(database_url)
    repository, engine = repository_for(database_url)
    credential_blocker = await asyncpg.connect(asyncpg_dsn(database_url))
    disabler = await asyncpg.connect(
        asyncpg_dsn(database_url), server_settings={"application_name": "disabler"}
    )
    observer = await asyncpg.connect(asyncpg_dsn(database_url))
    blocker_transaction = credential_blocker.transaction()
    disabler_transaction = disabler.transaction()
    await blocker_transaction.start()
    await disabler_transaction.start()
    try:
        await credential_blocker.execute(
            "SELECT id FROM worker_credentials WHERE id = $1 FOR UPDATE",
            authority.credential_id,
        )
        registration_task = asyncio.create_task(
            repository.register_session(authority, uuid4(), WorkerRegistration(()))
        )
        await wait_for_lock_wait(observer, "worker-registration")

        disable_task = asyncio.create_task(
            disabler.execute(
                "UPDATE worker_identities SET disabled_at = statement_timestamp() "
                "WHERE id = $1",
                authority.worker_identity_id,
            )
        )
        await wait_for_lock_wait(observer, "disabler")

        await blocker_transaction.commit()
        registered = await registration_task
        assert registered.id is not None
        await disable_task
        await disabler_transaction.commit()
    finally:
        if not registration_task.done():
            registration_task.cancel()
        if not disable_task.done():
            disable_task.cancel()
        if credential_blocker.is_in_transaction():
            await blocker_transaction.rollback()
        if disabler.is_in_transaction():
            await disabler_transaction.rollback()
        await credential_blocker.close()
        await disabler.close()
        await observer.close()
        await engine.dispose()  # type: ignore[attr-defined]


async def assert_disablement_first_rejects_registration(database_url: URL) -> None:
    authority = await seed_authority(database_url)
    repository, engine = repository_for(database_url)
    disabler = await asyncpg.connect(asyncpg_dsn(database_url))
    observer = await asyncpg.connect(asyncpg_dsn(database_url))
    disable_transaction = disabler.transaction()
    attempted_session_id = uuid4()
    await disable_transaction.start()
    try:
        await disabler.execute(
            "UPDATE worker_identities SET disabled_at = statement_timestamp() "
            "WHERE id = $1",
            authority.worker_identity_id,
        )
        registration_task = asyncio.create_task(
            repository.register_session(
                authority,
                attempted_session_id,
                WorkerRegistration(("documents",)),
            )
        )
        await wait_for_lock_wait(observer, "worker-registration")

        await disable_transaction.commit()
        with pytest.raises(WorkerRegistrationAuthorityRejected):
            await registration_task

        counts = await observer.fetchrow(
            "SELECT "
            "(SELECT count(*) FROM worker_sessions WHERE id = $1) sessions, "
            "(SELECT count(*) FROM worker_session_capabilities "
            " WHERE worker_session_id = $1) capabilities, "
            "(SELECT count(*) FROM worker_session_health "
            " WHERE worker_session_id = $1) health",
            attempted_session_id,
        )
        assert counts is not None
        assert tuple(counts) == (0, 0, 0)
    finally:
        if not registration_task.done():
            registration_task.cancel()
        if disabler.is_in_transaction():
            await disable_transaction.rollback()
        await disabler.close()
        await observer.close()
        await engine.dispose()  # type: ignore[attr-defined]


def test_worker_registration_postgresql_invariants() -> None:
    with temporary_database(
        "TASKFORGE_WORKER_REGISTRATION_TEST_DATABASE_URL",
        "taskforge_worker_registration",
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        configuration = Config("alembic.ini")
        with migration_database_url(alembic_url):
            command.upgrade(configuration, "head")
        asyncio.run(assert_complete_registration(database_url))
        asyncio.run(assert_empty_and_concurrent_sessions(database_url))
        asyncio.run(assert_atomic_rollback(database_url))
        asyncio.run(assert_invalid_authority_rejected(database_url))
        asyncio.run(assert_identity_then_credential_lock_order(database_url))
        asyncio.run(assert_disablement_first_rejects_registration(database_url))
