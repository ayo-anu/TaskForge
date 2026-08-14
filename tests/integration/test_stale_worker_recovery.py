"""Real PostgreSQL stale-worker-session recovery and heartbeat races."""

from __future__ import annotations

import asyncio
import os
from datetime import datetime
from uuid import UUID, uuid4

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from taskforge.persistence.database import build_session_factory
from taskforge.persistence.recovery import (
    SQLAlchemyRecoveryCandidateRepository,
    SQLAlchemyStaleWorkerSessionRecoveryRepository,
)
from taskforge.persistence.workers import SQLAlchemyWorkerHeartbeatRepository
from taskforge.recovery.domain import StaleWorkerSessionCandidate
from taskforge.recovery.scanner import RecoveryCandidateScanner
from taskforge.recovery.service import (
    StaleWorkerSessionRecoveryOutcome,
    StaleWorkerSessionRecoveryService,
    StaleWorkerSessionRecoveryServiceUnavailable,
)
from taskforge.worker.service import (
    WorkerHeartbeatService,
    WorkerSessionInactive,
)
from tests.integration.postgresql import (
    asyncpg_dsn,
    migration_database_url,
    temporary_database,
)
from tests.integration.test_recovery_transition import wait_until_lock_blocked
from tests.integration.test_task_claim_acquisition import WorkerFacts, add_worker

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_RECOVERY_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_RECOVERY_INTEGRATION=1 explicitly",
    ),
]


async def stale_candidate(
    connection: asyncpg.Connection[asyncpg.Record],
) -> tuple[StaleWorkerSessionCandidate, WorkerFacts]:
    worker = await add_worker(connection)
    row = await connection.fetchrow(
        "UPDATE worker_session_health SET last_sequence = 1, "
        "last_seen_at = statement_timestamp() - interval '30 seconds', "
        "accepting_work = true, availability_changed_at = "
        "statement_timestamp() - interval '30 seconds' "
        "WHERE worker_session_id = $1 RETURNING last_sequence, last_seen_at, "
        "accepting_work, statement_timestamp() AS observed_at",
        worker.session_id,
    )
    assert row is not None
    await connection.execute(
        "INSERT INTO worker_heartbeats "
        "(worker_session_id, sequence, received_at, accepting_work) "
        "VALUES ($1, 1, $2, true)",
        worker.session_id,
        row["last_seen_at"],
    )
    return (
        StaleWorkerSessionCandidate(
            worker.session_id,
            worker.authenticated.worker_identity_id,
            row["last_sequence"],
            row["last_seen_at"],
            row["accepting_work"],
            row["observed_at"],
        ),
        worker,
    )


async def session_facts(
    connection: asyncpg.Connection[asyncpg.Record], session_id: UUID
) -> tuple[datetime | None, int, datetime, bool, datetime, int]:
    row = await connection.fetchrow(
        "SELECT s.ended_at, h.last_sequence, h.last_seen_at, h.accepting_work, "
        "h.availability_changed_at, (SELECT count(*) FROM worker_heartbeats b "
        "WHERE b.worker_session_id = s.id) AS heartbeat_count "
        "FROM worker_sessions s JOIN worker_session_health h ON "
        "h.worker_session_id = s.id WHERE s.id = $1",
        session_id,
    )
    assert row is not None
    return tuple(row)


async def boundary_candidate(
    connection: asyncpg.Connection[asyncpg.Record],
    *,
    reference_time: datetime,
    offset_microseconds: int,
) -> StaleWorkerSessionCandidate:
    candidate, worker = await stale_candidate(connection)
    last_seen_at = await connection.fetchval(
        "SELECT $1::timestamptz - interval '30 seconds' "
        "+ make_interval(secs => $2::double precision / 1000000)",
        reference_time,
        offset_microseconds,
    )
    assert isinstance(last_seen_at, datetime)
    await connection.execute(
        "UPDATE worker_sessions SET registered_at = $2::timestamptz "
        "- interval '1 minute' "
        "WHERE id = $1",
        worker.session_id,
        reference_time,
    )
    await connection.execute(
        "UPDATE worker_session_health SET last_seen_at = $2, "
        "availability_changed_at = $2 WHERE worker_session_id = $1",
        worker.session_id,
        last_seen_at,
    )
    await connection.execute(
        "UPDATE worker_heartbeats SET received_at = $2 WHERE "
        "worker_session_id = $1 AND sequence = 1",
        worker.session_id,
        last_seen_at,
    )
    return StaleWorkerSessionCandidate(
        candidate.worker_session_id,
        candidate.worker_identity_id,
        candidate.last_sequence,
        last_seen_at,
        candidate.accepting_work,
        reference_time,
    )


async def exercise_stale_recovery(database_url: URL) -> None:
    monitor = await asyncpg.connect(asyncpg_dsn(database_url))
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg"), pool_size=6
    )
    sessions = build_session_factory(engine)
    service = StaleWorkerSessionRecoveryService(
        SQLAlchemyStaleWorkerSessionRecoveryRepository(sessions)
    )
    heartbeat = WorkerHeartbeatService(SQLAlchemyWorkerHeartbeatRepository(sessions))
    try:
        boundary, _ = await stale_candidate(monitor)
        before = await session_facts(monitor, boundary.worker_session_id)
        ended = await service.end_stale_session(boundary, stale_after_seconds=30)
        assert ended.outcome is StaleWorkerSessionRecoveryOutcome.SESSION_ENDED
        after = await session_facts(monitor, boundary.worker_session_id)
        assert after[0] == ended.ended_at
        assert after[1:] == before[1:]
        assert (
            await service.end_stale_session(boundary, stale_after_seconds=30)
        ).outcome is StaleWorkerSessionRecoveryOutcome.SESSION_ALREADY_ENDED

        heartbeat_first, heartbeat_worker = await stale_candidate(monitor)
        worker = heartbeat_worker
        await heartbeat.heartbeat(
            worker.authenticated,
            worker.session_id,
            sequence=2,
            accepting_work=False,
        )
        refreshed = await service.end_stale_session(
            heartbeat_first, stale_after_seconds=30
        )
        assert (
            refreshed.outcome is StaleWorkerSessionRecoveryOutcome.CANDIDATE_REFRESHED
        )
        assert (
            await monitor.fetchval(
                "SELECT ended_at FROM worker_sessions WHERE id = $1",
                worker.session_id,
            )
            is None
        )

        concurrent, _ = await stale_candidate(monitor)
        concurrent_results = await asyncio.gather(
            service.end_stale_session(concurrent, stale_after_seconds=30),
            service.end_stale_session(concurrent, stale_after_seconds=30),
        )
        assert {item.outcome for item in concurrent_results} == {
            StaleWorkerSessionRecoveryOutcome.SESSION_ENDED,
            StaleWorkerSessionRecoveryOutcome.SESSION_ALREADY_ENDED,
        }

        original, original_worker = await stale_candidate(monitor)
        new_session_id = uuid4()
        now = await monitor.fetchval("SELECT statement_timestamp()")
        assert isinstance(now, datetime)
        await monitor.execute(
            "INSERT INTO worker_sessions (id, worker_identity_id) VALUES ($1, $2)",
            new_session_id,
            original_worker.authenticated.worker_identity_id,
        )
        await monitor.execute(
            "INSERT INTO worker_session_health (worker_session_id, last_sequence, "
            "last_seen_at, accepting_work, availability_changed_at) "
            "VALUES ($1, 0, $2, false, $2)",
            new_session_id,
            now,
        )
        await service.end_stale_session(original, stale_after_seconds=30)
        assert (
            await monitor.fetchval(
                "SELECT ended_at FROM worker_sessions WHERE id = $1", new_session_id
            )
            is None
        )

        lock_winner, lock_worker = await stale_candidate(monitor)
        blocker = await asyncpg.connect(asyncpg_dsn(database_url))
        transition_engine = create_async_engine(
            database_url.set(drivername="postgresql+asyncpg"),
            connect_args={"server_settings": {"application_name": "stale-winner"}},
        )
        heartbeat_engine = create_async_engine(
            database_url.set(drivername="postgresql+asyncpg"),
            connect_args={"server_settings": {"application_name": "late-heartbeat"}},
        )
        transition_service = StaleWorkerSessionRecoveryService(
            SQLAlchemyStaleWorkerSessionRecoveryRepository(
                build_session_factory(transition_engine)
            )
        )
        late_heartbeat = WorkerHeartbeatService(
            SQLAlchemyWorkerHeartbeatRepository(build_session_factory(heartbeat_engine))
        )
        await monitor.execute(
            "CREATE FUNCTION pause_stale_session_end() RETURNS trigger LANGUAGE "
            "plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(13003); "
            "RETURN NEW; END $$"
        )
        await monitor.execute(
            "CREATE TRIGGER trg_pause_stale_session_end BEFORE UPDATE OF ended_at "
            "ON worker_sessions FOR EACH ROW EXECUTE FUNCTION "
            "pause_stale_session_end()"
        )
        await blocker.execute("SELECT pg_advisory_lock(13003)")
        transition = asyncio.create_task(
            transition_service.end_stale_session(lock_winner, stale_after_seconds=30)
        )
        await wait_until_lock_blocked(monitor, "stale-winner")
        submitted_heartbeat = asyncio.create_task(
            late_heartbeat.heartbeat(
                lock_worker.authenticated,
                lock_worker.session_id,
                sequence=2,
                accepting_work=True,
            )
        )
        await wait_until_lock_blocked(monitor, "late-heartbeat")
        assert not transition.done() and not submitted_heartbeat.done()
        await blocker.execute("SELECT pg_advisory_unlock(13003)")
        assert (
            await transition
        ).outcome is StaleWorkerSessionRecoveryOutcome.SESSION_ENDED
        with pytest.raises(WorkerSessionInactive):
            await submitted_heartbeat
        assert (await session_facts(monitor, lock_winner.worker_session_id))[1:] == (
            1,
            lock_winner.last_seen_at,
            True,
            lock_winner.last_seen_at,
            1,
        )
        await monitor.execute(
            "DROP TRIGGER trg_pause_stale_session_end ON worker_sessions"
        )
        await monitor.execute("DROP FUNCTION pause_stale_session_end()")
        await blocker.close()
        await transition_engine.dispose()
        await heartbeat_engine.dispose()

        rollback, _ = await stale_candidate(monitor)
        await monitor.execute(
            "CREATE FUNCTION reject_stale_session_end() RETURNS trigger LANGUAGE "
            "plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected failure'; END $$"
        )
        await monitor.execute(
            "CREATE TRIGGER trg_reject_stale_session_end BEFORE UPDATE OF ended_at "
            "ON worker_sessions FOR EACH ROW EXECUTE FUNCTION "
            "reject_stale_session_end()"
        )
        with pytest.raises(StaleWorkerSessionRecoveryServiceUnavailable):
            await service.end_stale_session(rollback, stale_after_seconds=30)
        assert (
            await monitor.fetchval(
                "SELECT ended_at FROM worker_sessions WHERE id = $1",
                rollback.worker_session_id,
            )
            is None
        )
        await monitor.execute(
            "DROP TRIGGER trg_reject_stale_session_end ON worker_sessions"
        )
        await monitor.execute("DROP FUNCTION reject_stale_session_end()")

        reference_time = await monitor.fetchval("SELECT statement_timestamp()")
        assert isinstance(reference_time, datetime)
        await monitor.execute("CREATE SCHEMA recovery_test_clock")
        await monitor.execute(
            "CREATE TABLE recovery_test_clock.reference_time "
            "(value timestamptz NOT NULL)"
        )
        await monitor.execute(
            "INSERT INTO recovery_test_clock.reference_time (value) VALUES ($1)",
            reference_time,
        )
        await monitor.execute(
            "CREATE FUNCTION recovery_test_clock.statement_timestamp() "
            "RETURNS timestamptz LANGUAGE sql STABLE AS $$ "
            "SELECT value FROM recovery_test_clock.reference_time $$"
        )
        exactly_stale = await boundary_candidate(
            monitor, reference_time=reference_time, offset_microseconds=0
        )
        minimally_newer = await boundary_candidate(
            monitor, reference_time=reference_time, offset_microseconds=1
        )
        clearly_older = await boundary_candidate(
            monitor, reference_time=reference_time, offset_microseconds=-1_000_000
        )
        clock_engine = create_async_engine(
            database_url.set(drivername="postgresql+asyncpg"),
            connect_args={
                "server_settings": {
                    "search_path": "recovery_test_clock,public,pg_catalog"
                }
            },
        )
        clock_sessions = build_session_factory(clock_engine)
        clock_scanner = RecoveryCandidateScanner(
            SQLAlchemyRecoveryCandidateRepository(clock_sessions),
            worker_stale_after_seconds=30,
        )
        clock_transition = StaleWorkerSessionRecoveryService(
            SQLAlchemyStaleWorkerSessionRecoveryRepository(clock_sessions)
        )
        boundary_page = await clock_scanner.scan_stale_worker_sessions(limit=100)
        assert boundary_page.observed_at == reference_time
        boundary_ids = {item.worker_session_id for item in boundary_page.items}
        assert exactly_stale.worker_session_id in boundary_ids
        assert minimally_newer.worker_session_id not in boundary_ids
        assert clearly_older.worker_session_id in boundary_ids
        assert (
            await clock_transition.end_stale_session(
                exactly_stale, stale_after_seconds=30
            )
        ).outcome is StaleWorkerSessionRecoveryOutcome.SESSION_ENDED
        assert (
            await clock_transition.end_stale_session(
                minimally_newer, stale_after_seconds=30
            )
        ).outcome is StaleWorkerSessionRecoveryOutcome.CANDIDATE_REFRESHED
        assert (
            await clock_transition.end_stale_session(
                clearly_older, stale_after_seconds=30
            )
        ).outcome is StaleWorkerSessionRecoveryOutcome.SESSION_ENDED
        await clock_engine.dispose()
        await monitor.execute("DROP SCHEMA recovery_test_clock CASCADE")
    finally:
        await engine.dispose()
        await monitor.close()


def test_real_postgresql_stale_worker_session_recovery() -> None:
    with temporary_database(
        "TASKFORGE_RECOVERY_TEST_DATABASE_URL", "taskforge_stale_recovery"
    ) as database_url:
        config = Config("alembic.ini")
        with migration_database_url(database_url.render_as_string(hide_password=False)):
            command.upgrade(config, "head")
        asyncio.run(exercise_stale_recovery(database_url))
