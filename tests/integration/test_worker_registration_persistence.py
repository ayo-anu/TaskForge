"""Opt-in PostgreSQL verification for atomic worker registration."""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from typing import Protocol
from uuid import UUID, uuid4

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.persistence.workers import (
    SQLAlchemyWorkerHeartbeatRepository,
    SQLAlchemyWorkerInspectionRepository,
    SQLAlchemyWorkerRegistrationRepository,
    _session_inspection_statement,
)
from taskforge.worker.domain import (
    WorkerHealthThresholds,
    WorkerHeartbeat,
    WorkerRegistration,
    WorkerSessionHealthStatus,
    WorkerSessionPageCursor,
)
from taskforge.worker.persistence_ports import (
    WorkerHeartbeatAuthorityRejected,
    WorkerHeartbeatInvariantViolation,
    WorkerHeartbeatPersistenceUnavailable,
    WorkerHeartbeatReplayConflict,
    WorkerHeartbeatSequenceGap,
    WorkerHeartbeatSessionInactive,
    WorkerHeartbeatSessionUnavailable,
    WorkerHeartbeatStale,
    WorkerInspectionInvariantViolation,
    WorkerRegistrationAuthorityRejected,
    WorkerRegistrationRecordConflict,
)
from taskforge.worker.schema import worker_sessions
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


class TransactionProtocol(Protocol):
    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...


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


def heartbeat_repository_for(
    database_url: URL,
    application_name: str,
) -> tuple[SQLAlchemyWorkerHeartbeatRepository, object]:
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg"),
        connect_args={"server_settings": {"application_name": application_name}},
    )
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    return SQLAlchemyWorkerHeartbeatRepository(sessions), engine


async def seed_registered_session(
    database_url: URL,
) -> tuple[AuthenticatedWorker, UUID]:
    authority = await seed_authority(database_url)
    repository, engine = repository_for(database_url)
    session_id = uuid4()
    try:
        await repository.register_session(authority, session_id, WorkerRegistration(()))
    finally:
        await engine.dispose()  # type: ignore[attr-defined]
    return authority, session_id


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


async def health_snapshot(
    connection: asyncpg.Connection[asyncpg.Record], session_id: UUID
) -> tuple[object, ...]:
    row = await connection.fetchrow(
        "SELECT last_sequence, last_seen_at, accepting_work, "
        "availability_changed_at FROM worker_session_health "
        "WHERE worker_session_id = $1",
        session_id,
    )
    assert row is not None
    return tuple(row)


async def assert_heartbeat_sequence_and_replay_semantics(database_url: URL) -> None:
    authority, session_id = await seed_registered_session(database_url)
    repository, engine = heartbeat_repository_for(database_url, "heartbeat-semantics")
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        first = await repository.apply_heartbeat(
            authority, session_id, WorkerHeartbeat(1, True)
        )
        first_history = await connection.fetchrow(
            "SELECT received_at, accepting_work FROM worker_heartbeats "
            "WHERE worker_session_id = $1 AND sequence = 1",
            session_id,
        )
        assert first_history is not None
        assert first.last_sequence == 1
        assert first.accepting_work is True
        assert first.last_seen_at == first_history["received_at"]
        assert first.availability_changed_at == first_history["received_at"]

        replayed = await repository.apply_heartbeat(
            authority, session_id, WorkerHeartbeat(1, True)
        )
        assert replayed == first
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM worker_heartbeats WHERE worker_session_id = $1",
                session_id,
            )
            == 1
        )

        with pytest.raises(WorkerHeartbeatReplayConflict):
            await repository.apply_heartbeat(
                authority, session_id, WorkerHeartbeat(1, False)
            )
        assert await health_snapshot(connection, session_id) == (
            first.last_sequence,
            first.last_seen_at,
            first.accepting_work,
            first.availability_changed_at,
        )

        second = await repository.apply_heartbeat(
            authority, session_id, WorkerHeartbeat(2, True)
        )
        second_history_at = await connection.fetchval(
            "SELECT received_at FROM worker_heartbeats "
            "WHERE worker_session_id = $1 AND sequence = 2",
            session_id,
        )
        assert second.last_sequence == 2
        assert second.last_seen_at == second_history_at
        assert second.accepting_work is True
        assert second.availability_changed_at == first.availability_changed_at

        before_bounded_replay = await health_snapshot(connection, session_id)
        before_history_count = await connection.fetchval(
            "SELECT count(*) FROM worker_heartbeats WHERE worker_session_id = $1",
            session_id,
        )
        with pytest.raises(WorkerHeartbeatStale):
            await repository.apply_heartbeat(
                authority, session_id, WorkerHeartbeat(1, True)
            )
        assert await health_snapshot(connection, session_id) == before_bounded_replay
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM worker_heartbeats WHERE worker_session_id = $1",
                session_id,
            )
            == before_history_count
        )

        with pytest.raises(WorkerHeartbeatSequenceGap):
            await repository.apply_heartbeat(
                authority, session_id, WorkerHeartbeat(4, False)
            )
        assert await health_snapshot(connection, session_id) == before_bounded_replay
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM worker_heartbeats WHERE worker_session_id = $1",
                session_id,
            )
            == before_history_count
        )

        await connection.execute(
            "DELETE FROM worker_heartbeats "
            "WHERE worker_session_id = $1 AND sequence = 2",
            session_id,
        )
        with pytest.raises(WorkerHeartbeatInvariantViolation):
            await repository.apply_heartbeat(
                authority, session_id, WorkerHeartbeat(2, True)
            )
        assert await health_snapshot(connection, session_id) == before_bounded_replay
    finally:
        await connection.close()
        await engine.dispose()  # type: ignore[attr-defined]


async def wait_for_lock_wait_count(
    observer: asyncpg.Connection[asyncpg.Record],
    application_name: str,
    expected: int,
) -> None:
    for _ in range(100):
        waiting = await observer.fetchval(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE application_name = $1 AND wait_event_type = 'Lock'",
            application_name,
        )
        if waiting >= expected:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(
        f"{application_name} did not reach {expected} database lock waits"
    )


async def lock_health(
    database_url: URL, session_id: UUID
) -> tuple[asyncpg.Connection[asyncpg.Record], TransactionProtocol]:
    blocker = await asyncpg.connect(asyncpg_dsn(database_url))
    transaction = blocker.transaction()
    await transaction.start()
    await blocker.execute(
        "SELECT worker_session_id FROM worker_session_health "
        "WHERE worker_session_id = $1 FOR UPDATE",
        session_id,
    )
    return blocker, transaction


async def assert_concurrent_same_sequence(database_url: URL) -> None:
    for conflicting in (False, True):
        authority, session_id = await seed_registered_session(database_url)
        application_name = f"heartbeat-same-{int(conflicting)}"
        repository, engine = heartbeat_repository_for(database_url, application_name)
        observer = await asyncpg.connect(asyncpg_dsn(database_url))
        blocker, blocker_transaction = await lock_health(database_url, session_id)
        try:
            first_task = asyncio.create_task(
                repository.apply_heartbeat(
                    authority, session_id, WorkerHeartbeat(1, True)
                )
            )
            second_task = asyncio.create_task(
                repository.apply_heartbeat(
                    authority,
                    session_id,
                    WorkerHeartbeat(1, False if conflicting else True),
                )
            )
            await wait_for_lock_wait_count(observer, application_name, 2)
            await blocker_transaction.commit()
            results = await asyncio.gather(
                first_task, second_task, return_exceptions=True
            )
            if conflicting:
                assert (
                    sum(
                        isinstance(item, WorkerHeartbeatReplayConflict)
                        for item in results
                    )
                    == 1
                )
                assert sum(not isinstance(item, BaseException) for item in results) == 1
            else:
                assert all(not isinstance(item, BaseException) for item in results)
                assert results[0] == results[1]
            assert (
                await observer.fetchval(
                    "SELECT count(*) FROM worker_heartbeats "
                    "WHERE worker_session_id = $1",
                    session_id,
                )
                == 1
            )
        finally:
            if blocker.is_in_transaction():
                await blocker_transaction.rollback()
            await blocker.close()
            await observer.close()
            await engine.dispose()  # type: ignore[attr-defined]


async def assert_consecutive_ordering(database_url: URL) -> None:
    for next_first in (False, True):
        authority, session_id = await seed_registered_session(database_url)
        application_name = f"heartbeat-consecutive-{int(next_first)}"
        repository, engine = heartbeat_repository_for(database_url, application_name)
        observer = await asyncpg.connect(asyncpg_dsn(database_url))
        blocker, blocker_transaction = await lock_health(database_url, session_id)
        try:
            first_sequence = 2 if next_first else 1
            queued_first = asyncio.create_task(
                repository.apply_heartbeat(
                    authority,
                    session_id,
                    WorkerHeartbeat(first_sequence, True),
                )
            )
            await wait_for_lock_wait_count(observer, application_name, 1)
            queued_second = asyncio.create_task(
                repository.apply_heartbeat(
                    authority,
                    session_id,
                    WorkerHeartbeat(1 if next_first else 2, True),
                )
            )
            await wait_for_lock_wait_count(observer, application_name, 2)
            await blocker_transaction.commit()
            first_result, second_result = await asyncio.gather(
                queued_first, queued_second, return_exceptions=True
            )
            if next_first:
                assert isinstance(first_result, WorkerHeartbeatSequenceGap)
                assert not isinstance(second_result, BaseException)
                assert second_result.last_sequence == 1
            else:
                assert not isinstance(first_result, BaseException)
                assert not isinstance(second_result, BaseException)
                assert first_result.last_sequence == 1
                assert second_result.last_sequence == 2
        finally:
            if blocker.is_in_transaction():
                await blocker_transaction.rollback()
            await blocker.close()
            await observer.close()
            await engine.dispose()  # type: ignore[attr-defined]


async def assert_session_scope_and_lifecycle(database_url: URL) -> None:
    owner, session_id = await seed_registered_session(database_url)
    other = await seed_authority(database_url)
    repository, engine = heartbeat_repository_for(database_url, "heartbeat-scope")
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        for authority, target in ((other, session_id), (owner, uuid4())):
            with pytest.raises(WorkerHeartbeatSessionUnavailable):
                await repository.apply_heartbeat(
                    authority, target, WorkerHeartbeat(1, False)
                )
        await connection.execute(
            "UPDATE worker_sessions SET ended_at = statement_timestamp() WHERE id = $1",
            session_id,
        )
        with pytest.raises(WorkerHeartbeatSessionInactive):
            await repository.apply_heartbeat(
                owner, session_id, WorkerHeartbeat(1, False)
            )
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM worker_heartbeats WHERE worker_session_id = $1",
                session_id,
            )
            == 0
        )
    finally:
        await connection.close()
        await engine.dispose()  # type: ignore[attr-defined]


async def assert_heartbeat_authority_races(database_url: URL) -> None:
    for authority_table, key_column, authority_error in (
        ("worker_identities", "id", "disabled_at"),
        ("worker_credentials", "id", "revoked_at"),
    ):
        authority, session_id = await seed_registered_session(database_url)
        repository, engine = heartbeat_repository_for(
            database_url, f"heartbeat-authority-{authority_table}"
        )
        mutator = await asyncpg.connect(asyncpg_dsn(database_url))
        observer = await asyncpg.connect(asyncpg_dsn(database_url))
        transaction = mutator.transaction()
        await transaction.start()
        try:
            target_id = (
                authority.worker_identity_id
                if authority_table == "worker_identities"
                else authority.credential_id
            )
            await mutator.execute(
                f"UPDATE {authority_table} SET {authority_error} = "
                f"statement_timestamp() WHERE {key_column} = $1",
                target_id,
            )
            heartbeat_task = asyncio.create_task(
                repository.apply_heartbeat(
                    authority, session_id, WorkerHeartbeat(1, False)
                )
            )
            await wait_for_lock_wait(observer, f"heartbeat-authority-{authority_table}")
            await transaction.commit()
            with pytest.raises(WorkerHeartbeatAuthorityRejected):
                await heartbeat_task
            assert (
                await observer.fetchval(
                    "SELECT count(*) FROM worker_heartbeats "
                    "WHERE worker_session_id = $1",
                    session_id,
                )
                == 0
            )
        finally:
            if mutator.is_in_transaction():
                await transaction.rollback()
            await mutator.close()
            await observer.close()
            await engine.dispose()  # type: ignore[attr-defined]

    for authority_table, key_column, authority_field in (
        ("worker_identities", "id", "disabled_at"),
        ("worker_credentials", "id", "revoked_at"),
    ):
        authority, session_id = await seed_registered_session(database_url)
        heartbeat_name = f"heartbeat-first-{authority_table}"
        mutator_name = f"heartbeat-mutator-{authority_table}"
        repository, engine = heartbeat_repository_for(database_url, heartbeat_name)
        blocker, blocker_transaction = await lock_health(database_url, session_id)
        mutator = await asyncpg.connect(
            asyncpg_dsn(database_url),
            server_settings={"application_name": mutator_name},
        )
        observer = await asyncpg.connect(asyncpg_dsn(database_url))
        mutation_transaction = mutator.transaction()
        await mutation_transaction.start()
        try:
            heartbeat_task = asyncio.create_task(
                repository.apply_heartbeat(
                    authority, session_id, WorkerHeartbeat(1, False)
                )
            )
            await wait_for_lock_wait(observer, heartbeat_name)
            target_id = (
                authority.worker_identity_id
                if authority_table == "worker_identities"
                else authority.credential_id
            )
            mutation_task = asyncio.create_task(
                mutator.execute(
                    f"UPDATE {authority_table} SET {authority_field} = "
                    f"statement_timestamp() WHERE {key_column} = $1",
                    target_id,
                )
            )
            await wait_for_lock_wait(observer, mutator_name)
            await blocker_transaction.commit()
            heartbeat = await heartbeat_task
            assert heartbeat.last_sequence == 1
            await mutation_task
            await mutation_transaction.commit()
        finally:
            if blocker.is_in_transaction():
                await blocker_transaction.rollback()
            if mutator.is_in_transaction():
                await mutation_transaction.rollback()
            await blocker.close()
            await mutator.close()
            await observer.close()
            await engine.dispose()  # type: ignore[attr-defined]


async def assert_heartbeat_rollback_after_history(database_url: URL) -> None:
    authority, session_id = await seed_registered_session(database_url)
    repository, engine = heartbeat_repository_for(database_url, "heartbeat-rollback")
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        before = await health_snapshot(connection, session_id)
        await connection.execute(
            "CREATE FUNCTION reject_test_health_update() RETURNS trigger "
            "LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test health failure'; END $$"
        )
        await connection.execute(
            "CREATE TRIGGER reject_test_health_update BEFORE UPDATE "
            "ON worker_session_health FOR EACH ROW "
            "EXECUTE FUNCTION reject_test_health_update()"
        )
        with pytest.raises(WorkerHeartbeatPersistenceUnavailable):
            await repository.apply_heartbeat(
                authority, session_id, WorkerHeartbeat(1, True)
            )
        assert await health_snapshot(connection, session_id) == before
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM worker_heartbeats WHERE worker_session_id = $1",
                session_id,
            )
            == 0
        )
    finally:
        await connection.execute(
            "DROP TRIGGER IF EXISTS reject_test_health_update ON worker_session_health"
        )
        await connection.execute("DROP FUNCTION IF EXISTS reject_test_health_update()")
        await connection.close()
        await engine.dispose()  # type: ignore[attr-defined]


async def assert_worker_inspection(database_url: URL) -> None:
    thresholds = WorkerHealthThresholds(30, 120)
    authority = await seed_authority(database_url)
    registration, registration_engine = repository_for(database_url)
    engine = create_async_engine(database_url.set(drivername="postgresql+asyncpg"))
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    inspection = SQLAlchemyWorkerInspectionRepository(sessions)
    heartbeat = SQLAlchemyWorkerHeartbeatRepository(sessions)
    session_ids = [uuid4() for _ in range(8)]
    try:
        for index, session_id in enumerate(session_ids):
            capabilities = ("notifications.email", "documents") if index == 0 else ()
            await registration.register_session(
                authority,
                session_id,
                WorkerRegistration(capabilities),
            )

        first = await inspection.get_session(session_ids[0], thresholds)
        assert first.session.health.status is WorkerSessionHealthStatus.HEALTHY
        assert first.session.health.last_sequence == 0
        assert first.session.health.accepting_work is False
        assert first.session.capabilities == ("documents", "notifications.email")
        assert first.session.identity.enabled is True

        reference_connection = await asyncpg.connect(asyncpg_dsn(database_url))
        try:
            reference = await reference_connection.fetchval(
                "SELECT statement_timestamp() + interval '1 hour'"
            )
        finally:
            await reference_connection.close()
        boundary_times = (
            reference - timedelta(seconds=30) + timedelta(microseconds=1),
            reference - timedelta(seconds=30),
            reference - timedelta(seconds=30) - timedelta(microseconds=1),
            reference - timedelta(seconds=120) + timedelta(microseconds=1),
            reference - timedelta(seconds=120),
            reference - timedelta(seconds=120) - timedelta(microseconds=1),
        )
        connection = await asyncpg.connect(asyncpg_dsn(database_url))
        try:
            for session_id, last_seen in zip(
                session_ids[1:7], boundary_times, strict=True
            ):
                await connection.execute(
                    "UPDATE worker_session_health SET last_seen_at = $2, "
                    "availability_changed_at = LEAST(availability_changed_at, $2) "
                    "WHERE worker_session_id = $1",
                    session_id,
                    last_seen,
                )
            await connection.execute(
                "UPDATE worker_sessions SET ended_at = $2 WHERE id = $1",
                session_ids[7],
                reference,
            )
        finally:
            await connection.close()

        async with sessions() as session:
            statuses: dict[UUID, WorkerSessionHealthStatus] = {}
            for session_id in session_ids[1:]:
                row = (
                    await session.execute(
                        _session_inspection_statement(
                            thresholds, reference_time=reference
                        ).where(worker_sessions.c.id == session_id)
                    )
                ).one()
                statuses[session_id] = WorkerSessionHealthStatus(row.health_status)
        assert statuses == {
            session_ids[1]: WorkerSessionHealthStatus.HEALTHY,
            session_ids[2]: WorkerSessionHealthStatus.STALE,
            session_ids[3]: WorkerSessionHealthStatus.STALE,
            session_ids[4]: WorkerSessionHealthStatus.STALE,
            session_ids[5]: WorkerSessionHealthStatus.OFFLINE,
            session_ids[6]: WorkerSessionHealthStatus.OFFLINE,
            session_ids[7]: WorkerSessionHealthStatus.ENDED,
        }

        connection = await asyncpg.connect(asyncpg_dsn(database_url))
        try:
            await connection.execute(
                "UPDATE worker_identities SET disabled_at = $2 WHERE id = $1",
                authority.worker_identity_id,
                reference,
            )
        finally:
            await connection.close()
        disabled = await inspection.get_session(session_ids[1], thresholds)
        assert disabled.session.identity.enabled is False
        assert disabled.session.health.status in {
            WorkerSessionHealthStatus.HEALTHY,
            WorkerSessionHealthStatus.OFFLINE,
        }
        connection = await asyncpg.connect(asyncpg_dsn(database_url))
        try:
            await connection.execute(
                "UPDATE worker_identities SET disabled_at = NULL WHERE id = $1",
                authority.worker_identity_id,
            )
        finally:
            await connection.close()

        all_page = await inspection.list_sessions(
            worker_identity_id=authority.worker_identity_id,
            health_status=None,
            thresholds=thresholds,
            limit=100,
            cursor=None,
        )
        assert len(all_page.items) == 8
        assert [item.health.last_seen_at for item in all_page.items] == sorted(
            item.health.last_seen_at for item in all_page.items
        )
        assert {item.identity.id for item in all_page.items} == {
            authority.worker_identity_id
        }
        page_one = await inspection.list_sessions(
            worker_identity_id=None,
            health_status=None,
            thresholds=thresholds,
            limit=2,
            cursor=None,
        )
        assert page_one.next_cursor is not None
        page_two = await inspection.list_sessions(
            worker_identity_id=None,
            health_status=None,
            thresholds=thresholds,
            limit=2,
            cursor=page_one.next_cursor,
        )
        assert page_two.observation.reference_time == (
            page_one.observation.reference_time
        )
        assert {item.id for item in page_one.items}.isdisjoint(
            item.id for item in page_two.items
        )

        fixed_cursor = WorkerSessionPageCursor(
            reference,
            datetime.min.replace(tzinfo=UTC),
            UUID(int=0),
            authority.worker_identity_id,
            WorkerSessionHealthStatus.HEALTHY,
            thresholds,
        )
        stable_page = await inspection.list_sessions(
            worker_identity_id=authority.worker_identity_id,
            health_status=WorkerSessionHealthStatus.HEALTHY,
            thresholds=thresholds,
            limit=100,
            cursor=fixed_cursor,
        )
        assert stable_page.observation.reference_time == reference
        assert {item.id for item in stable_page.items} == {session_ids[1]}
        later_reference = reference + timedelta(microseconds=2)
        later_cursor = WorkerSessionPageCursor(
            later_reference,
            datetime.min.replace(tzinfo=UTC),
            UUID(int=0),
            authority.worker_identity_id,
            WorkerSessionHealthStatus.STALE,
            thresholds,
        )
        later_page = await inspection.list_sessions(
            worker_identity_id=authority.worker_identity_id,
            health_status=WorkerSessionHealthStatus.STALE,
            thresholds=thresholds,
            limit=100,
            cursor=later_cursor,
        )
        assert session_ids[1] in {item.id for item in later_page.items}

        await heartbeat.apply_heartbeat(
            authority, session_ids[0], WorkerHeartbeat(1, True)
        )
        await heartbeat.apply_heartbeat(
            authority, session_ids[0], WorkerHeartbeat(2, False)
        )
        history_one = await inspection.list_heartbeats(
            session_ids[0], before_sequence=None, limit=1
        )
        assert [item.sequence for item in history_one.items] == [2]
        assert history_one.next_before_sequence == 2
        history_two = await inspection.list_heartbeats(
            session_ids[0], before_sequence=2, limit=1
        )
        assert [item.sequence for item in history_two.items] == [1]
        assert history_two.next_before_sequence is None

        visibility = await asyncpg.connect(asyncpg_dsn(database_url))
        transaction = visibility.transaction()
        await transaction.start()
        try:
            received_at = await visibility.fetchval("SELECT statement_timestamp()")
            await visibility.execute(
                "INSERT INTO worker_heartbeats "
                "(worker_session_id, sequence, received_at, accepting_work) "
                "VALUES ($1, 3, $2, true)",
                session_ids[0],
                received_at,
            )
            await visibility.execute(
                "UPDATE worker_session_health SET last_sequence = 3, "
                "last_seen_at = $2, accepting_work = true, "
                "availability_changed_at = $2 WHERE worker_session_id = $1",
                session_ids[0],
                received_at,
            )
            before_commit = await inspection.get_session(session_ids[0], thresholds)
            assert before_commit.session.health.last_sequence == 2
            await transaction.commit()
            after_commit = await inspection.get_session(session_ids[0], thresholds)
            assert after_commit.session.health.last_sequence == 3
            assert after_commit.session.health.accepting_work is True
        finally:
            if not visibility.is_closed():
                await visibility.close()

        before = await _inspection_counts(database_url)
        await inspection.get_session(session_ids[0], thresholds)
        await inspection.list_sessions(
            worker_identity_id=None,
            health_status=None,
            thresholds=thresholds,
            limit=2,
            cursor=None,
        )
        await inspection.list_heartbeats(session_ids[0], before_sequence=None, limit=2)
        assert await _inspection_counts(database_url) == before

        connection = await asyncpg.connect(asyncpg_dsn(database_url))
        try:
            await connection.execute(
                "DELETE FROM worker_session_health WHERE worker_session_id = $1",
                session_ids[7],
            )
        finally:
            await connection.close()
        with pytest.raises(WorkerInspectionInvariantViolation):
            await inspection.get_session(session_ids[7], thresholds)
    finally:
        await registration_engine.dispose()  # type: ignore[attr-defined]
        await engine.dispose()


async def _inspection_counts(database_url: URL) -> tuple[int, int, int]:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        return (
            await connection.fetchval("SELECT count(*) FROM worker_sessions"),
            await connection.fetchval("SELECT count(*) FROM worker_session_health"),
            await connection.fetchval("SELECT count(*) FROM worker_heartbeats"),
        )
    finally:
        await connection.close()


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
        asyncio.run(assert_heartbeat_sequence_and_replay_semantics(database_url))
        asyncio.run(assert_concurrent_same_sequence(database_url))
        asyncio.run(assert_consecutive_ordering(database_url))
        asyncio.run(assert_session_scope_and_lifecycle(database_url))
        asyncio.run(assert_heartbeat_authority_races(database_url))
        asyncio.run(assert_heartbeat_rollback_after_history(database_url))
        asyncio.run(assert_worker_inspection(database_url))
