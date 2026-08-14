"""Real PostgreSQL verification for bounded advisory recovery scans."""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID, uuid4

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from taskforge.persistence.database import build_session_factory
from taskforge.persistence.recovery import SQLAlchemyRecoveryCandidateRepository
from taskforge.recovery.domain import (
    ExpiredClaimScanCursor,
    StaleWorkerSessionScanCursor,
)
from taskforge.recovery.scanner import RecoveryCandidateScanner
from tests.integration.postgresql import (
    asyncpg_dsn,
    migration_database_url,
    temporary_database,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_RECOVERY_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_RECOVERY_INTEGRATION=1 explicitly",
    ),
]


@dataclass(frozen=True)
class ClaimFacts:
    attempt_id: UUID
    task_run_id: UUID
    session_id: UUID
    lease_expires_at: datetime


@dataclass(frozen=True)
class SessionFacts:
    identity_id: UUID
    session_id: UUID
    last_seen_at: datetime


async def add_session(
    connection: asyncpg.Connection[asyncpg.Record],
    *,
    last_seen_at: datetime,
    ended: bool = False,
    accepting_work: bool = True,
    sequence: int = 1,
) -> SessionFacts:
    identity_id, session_id = uuid4(), uuid4()
    await connection.execute(
        "INSERT INTO worker_identities (id, name) VALUES ($1, $2)",
        identity_id,
        f"recovery-worker-{uuid4().hex}",
    )
    await connection.execute(
        "INSERT INTO worker_sessions "
        "(id, worker_identity_id, registered_at, ended_at) "
        "VALUES ($1, $2, $3, $4)",
        session_id,
        identity_id,
        last_seen_at,
        last_seen_at if ended else None,
    )
    await connection.execute(
        "INSERT INTO worker_session_health "
        "(worker_session_id, last_sequence, last_seen_at, accepting_work, "
        "availability_changed_at) VALUES ($1, $2, $3, $4, $3)",
        session_id,
        sequence,
        last_seen_at,
        accepting_work,
    )
    return SessionFacts(identity_id, session_id, last_seen_at)


async def add_claim(
    connection: asyncpg.Connection[asyncpg.Record],
    session_id: UUID,
    *,
    lease_expires_at: datetime,
    attempt_id: UUID | None = None,
    task_status: str = "claimed",
    run_status: str = "running",
    terminated: bool = False,
    newer_attempt: bool = False,
    workflow_policy: str | None = None,
) -> ClaimFacts:
    principal_id, workflow_id, version_id, workflow_run_id = (
        uuid4(),
        uuid4(),
        uuid4(),
        uuid4(),
    )
    task_run_id = uuid4()
    attempt_id = uuid4() if attempt_id is None else attempt_id
    step = f"step-{uuid4().hex}"
    await connection.execute(
        "INSERT INTO api_principals (id, name) VALUES ($1, $2)",
        principal_id,
        f"recovery-owner-{uuid4().hex}",
    )
    await connection.execute(
        "INSERT INTO workflow_definitions (id, owner_principal_id, name) "
        "VALUES ($1, $2, $3)",
        workflow_id,
        principal_id,
        f"recovery-workflow-{uuid4().hex}",
    )
    await connection.execute(
        "INSERT INTO workflow_versions "
        "(id, workflow_definition_id, version_number, name, execution_policy) "
        "VALUES ($1, $2, 1, 'recovery-v1', $3::jsonb)",
        version_id,
        workflow_id,
        workflow_policy,
    )
    await connection.execute(
        "INSERT INTO workflow_version_steps "
        "(workflow_version_id, step_identifier, task_type, parameters) "
        "VALUES ($1, $2, 'test.task', '{}'::jsonb)",
        version_id,
        step,
    )
    await connection.execute(
        "INSERT INTO workflow_runs "
        "(id, workflow_definition_id, workflow_version_id, "
        "requested_by_principal_id, status) VALUES ($1, $2, $3, $4, $5)",
        workflow_run_id,
        workflow_id,
        version_id,
        principal_id,
        run_status,
    )
    await connection.execute(
        "INSERT INTO task_runs "
        "(id, workflow_run_id, workflow_version_id, step_identifier, status) "
        "VALUES ($1, $2, $3, $4, $5)",
        task_run_id,
        workflow_run_id,
        version_id,
        step,
        task_status,
    )
    await connection.execute(
        "INSERT INTO task_attempts (id, task_run_id, attempt_number) "
        "VALUES ($1, $2, 1)",
        attempt_id,
        task_run_id,
    )
    await connection.execute(
        "INSERT INTO task_attempt_claims "
        "(task_attempt_id, generation, worker_session_id, acquired_at, "
        "lease_expires_at, terminated_at) VALUES "
        "($1, 1, $2, $3::timestamptz - interval '1 minute', $3, $4)",
        attempt_id,
        session_id,
        lease_expires_at,
        lease_expires_at if terminated else None,
    )
    if newer_attempt:
        await connection.execute(
            "INSERT INTO task_attempts (id, task_run_id, attempt_number) "
            "VALUES ($1, $2, 2)",
            uuid4(),
            task_run_id,
        )
    return ClaimFacts(attempt_id, task_run_id, session_id, lease_expires_at)


async def exercise_recovery_scans(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    engine = create_async_engine(database_url.set(drivername="postgresql+asyncpg"))
    repository = SQLAlchemyRecoveryCandidateRepository(build_session_factory(engine))
    scanner = RecoveryCandidateScanner(repository, worker_stale_after_seconds=30)
    try:
        now = await connection.fetchval("SELECT statement_timestamp()")
        assert isinstance(now, datetime)
        worker = await add_session(connection, last_seen_at=now - timedelta(minutes=10))
        expiry = now - timedelta(minutes=5)
        tied = [
            await add_claim(connection, worker.session_id, lease_expires_at=expiry)
            for _ in range(3)
        ]
        running = await add_claim(
            connection,
            worker.session_id,
            lease_expires_at=now - timedelta(minutes=4),
            task_status="running",
        )
        future = await add_claim(
            connection,
            worker.session_id,
            lease_expires_at=now + timedelta(hours=1),
        )
        terminated = await add_claim(
            connection,
            worker.session_id,
            lease_expires_at=expiry,
            terminated=True,
        )
        terminal = await add_claim(
            connection,
            worker.session_id,
            lease_expires_at=expiry,
            task_status="succeeded",
        )
        superseded = await add_claim(
            connection,
            worker.session_id,
            lease_expires_at=expiry,
            newer_attempt=True,
        )

        first = await scanner.scan_expired_claims(limit=2)
        assert len(first.items) <= 2
        assert first.next_cursor is not None
        claim_pages = [first]
        claim_cursor: ExpiredClaimScanCursor | None = first.next_cursor
        while claim_cursor is not None:
            page = await scanner.scan_expired_claims(limit=2, cursor=claim_cursor)
            claim_pages.append(page)
            claim_cursor = page.next_cursor
        assert all(len(page.items) <= 2 for page in claim_pages)
        assert all(page.observed_at == first.observed_at for page in claim_pages)
        discovered = {
            item.task_attempt_id for page in claim_pages for item in page.items
        }
        assert discovered == {fact.attempt_id for fact in tied} | {running.attempt_id}
        assert (
            not {
                future.attempt_id,
                terminated.attempt_id,
                terminal.attempt_id,
                superseded.attempt_id,
            }
            & discovered
        )

        empty = claim_pages[-1]
        assert empty.items == ()
        assert empty.observed_at == first.observed_at

        boundary_time = await connection.fetchval("SELECT statement_timestamp()")
        assert isinstance(boundary_time, datetime)
        ineligible_terminal = await add_claim(
            connection,
            worker.session_id,
            lease_expires_at=boundary_time,
            attempt_id=UUID("90000000-0000-0000-0000-000000000001"),
            task_status="succeeded",
        )
        ineligible_superseded = await add_claim(
            connection,
            worker.session_id,
            lease_expires_at=boundary_time,
            attempt_id=UUID("90000000-0000-0000-0000-000000000002"),
            newer_attempt=True,
        )
        boundary_eligible = await add_claim(
            connection,
            worker.session_id,
            lease_expires_at=boundary_time,
            attempt_id=UUID("90000000-0000-0000-0000-000000000003"),
        )
        before_boundary = ExpiredClaimScanCursor(
            boundary_time,
            boundary_time - timedelta(microseconds=1),
            UUID(int=(1 << 128) - 1),
            1,
        )

        ineligible_window = await scanner.scan_expired_claims(
            limit=2, cursor=before_boundary
        )

        assert ineligible_window.items == ()
        assert ineligible_window.next_cursor is not None
        assert ineligible_window.next_cursor.task_attempt_id == (
            ineligible_superseded.attempt_id
        )
        assert ineligible_terminal.attempt_id < ineligible_superseded.attempt_id
        continuation = await scanner.scan_expired_claims(
            limit=2, cursor=ineligible_window.next_cursor
        )
        assert tuple(item.task_attempt_id for item in continuation.items) == (
            boundary_eligible.attempt_id,
        )
        assert continuation.observed_at == boundary_time
        assert continuation.observed_at == ineligible_window.observed_at
        assert continuation.items[0].lease_expires_at == continuation.observed_at
        assert continuation.next_cursor is None

        stale_time = now - timedelta(minutes=2)
        tied_stale = [
            await add_session(connection, last_seen_at=stale_time) for _ in range(3)
        ]
        no_heartbeat = await add_session(
            connection,
            last_seen_at=now - timedelta(minutes=3),
            accepting_work=False,
            sequence=0,
        )
        fresh = await add_session(connection, last_seen_at=now - timedelta(seconds=5))
        closed = await add_session(
            connection, last_seen_at=now - timedelta(minutes=4), ended=True
        )

        stale_first = await scanner.scan_stale_worker_sessions(limit=2)
        assert len(stale_first.items) == 2
        assert stale_first.next_cursor is not None
        stale_second = await scanner.scan_stale_worker_sessions(
            limit=2, cursor=stale_first.next_cursor
        )
        assert stale_second.next_cursor is not None
        stale_third = await scanner.scan_stale_worker_sessions(
            limit=2, cursor=stale_second.next_cursor
        )
        stale_ids = {
            item.worker_session_id
            for page in (stale_first, stale_second, stale_third)
            for item in page.items
        }
        expected_stale = {worker.session_id, no_heartbeat.session_id} | {
            item.session_id for item in tied_stale
        }
        assert stale_ids == expected_stale
        assert fresh.session_id not in stale_ids
        assert closed.session_id not in stale_ids

        boundary_time = await connection.fetchval("SELECT statement_timestamp()")
        assert isinstance(boundary_time, datetime)
        boundary_session = await add_session(
            connection, last_seen_at=boundary_time - timedelta(seconds=30)
        )
        boundary_cursor = StaleWorkerSessionScanCursor(
            boundary_time,
            boundary_time - timedelta(days=1),
            UUID(int=(1 << 128) - 1),
            30,
        )
        boundary_page = await scanner.scan_stale_worker_sessions(
            limit=100, cursor=boundary_cursor
        )
        assert boundary_session.session_id in {
            item.worker_session_id for item in boundary_page.items
        }

        observed_stale = next(
            item
            for page in (stale_first, stale_second, stale_third)
            for item in page.items
            if item.worker_session_id == no_heartbeat.session_id
        )
        await connection.execute(
            "UPDATE worker_session_health SET last_sequence = 1, "
            "last_seen_at = statement_timestamp(), availability_changed_at = "
            "statement_timestamp() WHERE worker_session_id = $1",
            no_heartbeat.session_id,
        )
        refreshed = await scanner.scan_stale_worker_sessions(limit=100)
        assert no_heartbeat.session_id not in {
            item.worker_session_id for item in refreshed.items
        }
        durable_health = await connection.fetchrow(
            "SELECT last_sequence, last_seen_at FROM worker_session_health "
            "WHERE worker_session_id = $1",
            no_heartbeat.session_id,
        )
        assert durable_health is not None
        assert (
            durable_health["last_sequence"],
            durable_health["last_seen_at"],
        ) != (observed_stale.last_sequence, observed_stale.last_seen_at)

        observed_claim = next(item for page in claim_pages for item in page.items)
        await connection.execute(
            "UPDATE task_attempt_claims SET lease_expires_at = "
            "statement_timestamp() + interval '5 minutes' "
            "WHERE task_attempt_id = $1 AND generation = $2",
            observed_claim.task_attempt_id,
            observed_claim.generation,
        )
        refreshed_claims = await scanner.scan_expired_claims(limit=100)
        assert observed_claim.task_attempt_id not in {
            item.task_attempt_id for item in refreshed_claims.items
        }

        concurrent = await asyncio.gather(
            scanner.scan_expired_claims(limit=100),
            scanner.scan_expired_claims(limit=100),
        )
        assert (
            concurrent[0].items[0].task_attempt_id
            == concurrent[1].items[0].task_attempt_id
        )
    finally:
        await engine.dispose()
        await connection.close()


def test_real_postgresql_recovery_scanner() -> None:
    with temporary_database(
        "TASKFORGE_RECOVERY_TEST_DATABASE_URL", "taskforge_recovery_scanner"
    ) as database_url:
        config = Config("alembic.ini")
        with migration_database_url(database_url.render_as_string(hide_password=False)):
            command.upgrade(config, "head")
        asyncio.run(exercise_recovery_scans(database_url))
