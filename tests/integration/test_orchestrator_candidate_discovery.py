"""Real PostgreSQL evidence for bounded orchestrator candidate discovery."""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL

from taskforge.orchestrator.domain import DiscoveryCursor, DiscoveryHighWater
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.orchestrator import SQLAlchemyOrchestratorCandidateRepository
from tests.integration.postgresql import (
    asyncpg_dsn,
    migration_database_url,
    temporary_database,
)
from tests.integration.test_authentication_persistence import settings_for

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_ORCHESTRATOR_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_ORCHESTRATOR_INTEGRATION=1 explicitly",
    ),
]


async def seed_catalog(
    connection: asyncpg.Connection[asyncpg.Record],
) -> tuple[UUID, UUID, UUID]:
    principal_id, workflow_id, version_id = uuid4(), uuid4(), uuid4()
    await connection.execute(
        "INSERT INTO api_principals (id, name) VALUES ($1, $2)",
        principal_id,
        f"orchestrator-owner-{uuid4().hex}",
    )
    await connection.execute(
        "INSERT INTO workflow_definitions (id, owner_principal_id, name) "
        "VALUES ($1, $2, $3)",
        workflow_id,
        principal_id,
        f"orchestrator-workflow-{uuid4().hex}",
    )
    await connection.execute(
        "INSERT INTO workflow_versions "
        "(id, workflow_definition_id, version_number, name) "
        "VALUES ($1, $2, 1, 'orchestrator-v1')",
        version_id,
        workflow_id,
    )
    await connection.execute(
        "INSERT INTO workflow_version_steps "
        "(workflow_version_id, step_identifier, task_type, parameters) "
        "VALUES ($1, 'step', 'test.task', '{}'::jsonb)",
        version_id,
    )
    return principal_id, workflow_id, version_id


async def add_run(
    connection: asyncpg.Connection[asyncpg.Record],
    catalog: tuple[UUID, UUID, UUID],
    *,
    run_id: UUID,
    status: str,
    created_at: datetime,
) -> None:
    principal_id, workflow_id, version_id = catalog
    await connection.execute(
        "INSERT INTO workflow_runs "
        "(id, workflow_definition_id, workflow_version_id, "
        "requested_by_principal_id, status, created_at, updated_at) "
        "VALUES ($1, $2, $3, $4, $5, $6, $6)",
        run_id,
        workflow_id,
        version_id,
        principal_id,
        status,
        created_at,
    )


async def add_task(
    connection: asyncpg.Connection[asyncpg.Record],
    catalog: tuple[UUID, UUID, UUID],
    *,
    run_id: UUID,
    task_id: UUID,
    status: str,
    created_at: datetime,
) -> None:
    await add_run(
        connection,
        catalog,
        run_id=run_id,
        status="running",
        created_at=created_at,
    )
    await connection.execute(
        "INSERT INTO task_runs "
        "(id, workflow_run_id, workflow_version_id, step_identifier, status, "
        "created_at, updated_at) VALUES ($1, $2, $3, 'step', $4, $5, $5)",
        task_id,
        run_id,
        catalog[2],
        status,
        created_at,
    )


async def collect_active(
    repository: SQLAlchemyOrchestratorCandidateRepository,
    high: DiscoveryHighWater,
    *,
    limit: int,
    cursor: DiscoveryCursor | None = None,
) -> tuple[list[UUID], DiscoveryCursor | None]:
    found: list[UUID] = []
    while True:
        page = await repository.list_active_workflow_runs(
            high_water=high, cursor=cursor, limit=limit
        )
        assert len(page.items) <= limit
        found.extend(item.workflow_run_id for item in page.items)
        cursor = page.next_cursor
        if cursor is None:
            return found, None


async def collect_runnable(
    repository: SQLAlchemyOrchestratorCandidateRepository,
    high: DiscoveryHighWater,
    *,
    limit: int,
    cursor: DiscoveryCursor | None = None,
) -> list[UUID]:
    found: list[UUID] = []
    while True:
        page = await repository.list_runnable_tasks(
            high_water=high, cursor=cursor, limit=limit
        )
        assert len(page.items) <= limit
        found.extend(item.task_run_id for item in page.items)
        cursor = page.next_cursor
        if cursor is None:
            return found


async def collect_retry_pending(
    repository: SQLAlchemyOrchestratorCandidateRepository,
    high: DiscoveryHighWater,
    *,
    limit: int,
    cursor: DiscoveryCursor | None = None,
) -> list[UUID]:
    found: list[UUID] = []
    while True:
        page = await repository.list_retry_pending_tasks(
            high_water=high, cursor=cursor, limit=limit
        )
        assert len(page.items) <= limit
        found.extend(item.task_run_id for item in page.items)
        cursor = page.next_cursor
        if cursor is None:
            return found


async def verify_candidate_scans(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    repository = SQLAlchemyOrchestratorCandidateRepository(
        build_session_factory(engine)
    )
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        catalog = await seed_catalog(connection)
        tied_at = datetime(2026, 1, 1, tzinfo=UTC)
        active_ids = tuple(UUID(int=value) for value in (11, 12, 13))
        for run_id, status in zip(
            active_ids, ("pending", "running", "cancelling"), strict=True
        ):
            await add_run(
                connection,
                catalog,
                run_id=run_id,
                status=status,
                created_at=tied_at,
            )
        terminal_id = UUID(int=14)
        await add_run(
            connection,
            catalog,
            run_id=terminal_id,
            status="succeeded",
            created_at=tied_at,
        )

        active_high = await repository.capture_active_run_high_water()
        assert active_high is not None
        active, _ = await collect_active(repository, active_high, limit=2)
        assert active == list(active_ids)
        assert len(active) == len(set(active))
        assert terminal_id not in active

        # Restart means discarding process-local state; still-eligible rows return.
        restarted, _ = await collect_active(repository, active_high, limit=2)
        assert restarted == active

        runnable_ids = tuple(UUID(int=value) for value in (21, 22, 23))
        retry_ids = tuple(UUID(int=value) for value in (31, 32, 33))
        for task_id in runnable_ids:
            await add_task(
                connection,
                catalog,
                run_id=uuid4(),
                task_id=task_id,
                status="runnable",
                created_at=tied_at + timedelta(seconds=1),
            )
        for task_id in retry_ids:
            await add_task(
                connection,
                catalog,
                run_id=uuid4(),
                task_id=task_id,
                status="retry_pending",
                created_at=tied_at + timedelta(seconds=2),
            )
        await add_task(
            connection,
            catalog,
            run_id=uuid4(),
            task_id=UUID(int=39),
            status="blocked",
            created_at=tied_at + timedelta(seconds=2),
        )

        runnable_high = await repository.capture_runnable_task_high_water()
        retry_high = await repository.capture_retry_pending_high_water()
        assert runnable_high is not None and retry_high is not None
        assert await collect_runnable(repository, runnable_high, limit=2) == list(
            runnable_ids
        )
        assert await collect_retry_pending(repository, retry_high, limit=2) == list(
            retry_ids
        )

        # A row after each fixed tuple is excluded now and included next sweep.
        late_active, late_runnable, late_retry = uuid4(), uuid4(), uuid4()
        late_at = tied_at + timedelta(days=1)
        await add_run(
            connection,
            catalog,
            run_id=late_active,
            status="running",
            created_at=late_at,
        )
        await add_task(
            connection,
            catalog,
            run_id=uuid4(),
            task_id=late_runnable,
            status="runnable",
            created_at=late_at,
        )
        await add_task(
            connection,
            catalog,
            run_id=uuid4(),
            task_id=late_retry,
            status="retry_pending",
            created_at=late_at,
        )
        assert (
            late_active
            not in (await collect_active(repository, active_high, limit=2))[0]
        )
        assert late_runnable not in await collect_runnable(
            repository, runnable_high, limit=2
        )
        assert late_retry not in await collect_retry_pending(
            repository, retry_high, limit=2
        )
        next_active_high = await repository.capture_active_run_high_water()
        next_runnable_high = await repository.capture_runnable_task_high_water()
        next_retry_high = await repository.capture_retry_pending_high_water()
        assert next_active_high and next_runnable_high and next_retry_high
        assert (
            late_active
            in (await collect_active(repository, next_active_high, limit=2))[0]
        )
        assert late_runnable in await collect_runnable(
            repository, next_runnable_high, limit=2
        )
        assert late_retry in await collect_retry_pending(
            repository, next_retry_high, limit=2
        )

        # Behind-cursor status entry is deferred; ahead-of-cursor entry is visible.
        behind_id, first_id, ahead_id = UUID(int=40), UUID(int=41), UUID(int=42)
        transition_at = tied_at + timedelta(days=2)
        for task_id, status in (
            (behind_id, "blocked"),
            (first_id, "runnable"),
            (ahead_id, "blocked"),
        ):
            await add_task(
                connection,
                catalog,
                run_id=uuid4(),
                task_id=task_id,
                status=status,
                created_at=transition_at,
            )
        transition_high = await repository.capture_runnable_task_high_water()
        assert transition_high is not None
        first_page = await repository.list_runnable_tasks(
            high_water=transition_high, cursor=None, limit=1
        )
        assert first_page.next_cursor is not None
        # Earlier existing rows precede this group, so advance until this sentinel.
        transition_cursor = first_page.next_cursor
        while first_page.items[-1].task_run_id != first_id:
            first_page = await repository.list_runnable_tasks(
                high_water=transition_high, cursor=transition_cursor, limit=1
            )
            assert first_page.next_cursor is not None
            transition_cursor = first_page.next_cursor
        await connection.execute(
            "UPDATE task_runs SET status='runnable' WHERE id=ANY($1::uuid[])",
            [behind_id, ahead_id],
        )
        remaining = await collect_runnable(
            repository, transition_high, limit=1, cursor=transition_cursor
        )
        assert behind_id not in remaining
        assert ahead_id in remaining
        fresh_high = await repository.capture_runnable_task_high_water()
        assert fresh_high is not None
        assert behind_id in await collect_runnable(repository, fresh_high, limit=100)

        # A transaction with an older server transaction timestamp may commit
        # after the cursor passed it; a fresh sweep must rediscover the row.
        late_connection = await asyncpg.connect(asyncpg_dsn(database_url))
        late_tx = late_connection.transaction()
        await late_tx.start()
        transaction_time = await late_connection.fetchval("SELECT current_timestamp")
        assert isinstance(transaction_time, datetime)
        marker_a, marker_b, late_transaction_id = (
            UUID(int=1001),
            UUID(int=1002),
            UUID(int=1000),
        )
        await add_run(
            connection,
            catalog,
            run_id=marker_a,
            status="running",
            created_at=transaction_time + timedelta(seconds=1),
        )
        await add_run(
            connection,
            catalog,
            run_id=marker_b,
            status="running",
            created_at=transaction_time + timedelta(seconds=2),
        )
        late_high = await repository.capture_active_run_high_water()
        assert late_high is not None
        late_page = await repository.list_active_workflow_runs(
            high_water=late_high, cursor=None, limit=1
        )
        late_cursor = late_page.next_cursor
        assert late_cursor is not None
        while late_page.items[-1].workflow_run_id != marker_a:
            late_page = await repository.list_active_workflow_runs(
                high_water=late_high, cursor=late_cursor, limit=1
            )
            late_cursor = late_page.next_cursor
            assert late_cursor is not None
        await late_connection.execute(
            "INSERT INTO workflow_runs "
            "(id, workflow_definition_id, workflow_version_id, "
            "requested_by_principal_id, status) VALUES ($1, $2, $3, $4, 'running')",
            late_transaction_id,
            catalog[1],
            catalog[2],
            catalog[0],
        )
        await late_tx.commit()
        await late_connection.close()
        current_tail, _ = await collect_active(
            repository, late_high, limit=1, cursor=late_cursor
        )
        assert late_transaction_id not in current_tail
        post_commit_high = await repository.capture_active_run_high_water()
        assert post_commit_high is not None
        assert (
            late_transaction_id
            in (await collect_active(repository, post_commit_high, limit=100))[0]
        )

        # Normal continuous inserts beyond H cannot extend this already-open sweep.
        finite_high = await repository.capture_active_run_high_water()
        assert finite_high is not None
        producer_started = asyncio.Event()
        producer_done = asyncio.Event()
        producer_ids: list[UUID] = []

        async def producer() -> None:
            producer_connection = await asyncpg.connect(asyncpg_dsn(database_url))
            try:
                producer_started.set()
                for offset in range(10):
                    producer_id = uuid4()
                    producer_ids.append(producer_id)
                    await add_run(
                        producer_connection,
                        catalog,
                        run_id=producer_id,
                        status="running",
                        created_at=(
                            finite_high.created_at + timedelta(seconds=offset + 1)
                        ),
                    )
                    await asyncio.sleep(0)
            finally:
                producer_done.set()
                await producer_connection.close()

        producer_task = asyncio.create_task(producer())
        await producer_started.wait()
        finite, _ = await collect_active(repository, finite_high, limit=1)
        await producer_done.wait()
        await producer_task
        assert len(finite) == len(set(finite))
        assert not set(finite).intersection(producer_ids)
        after_producer = await repository.capture_active_run_high_water()
        assert after_producer is not None
        next_items = (await collect_active(repository, after_producer, limit=100))[0]
        assert set(producer_ids).issubset(next_items)

        # Record query-plan evidence without changing schema.
        plans = await connection.fetchval(
            "SELECT count(*) FROM pg_indexes WHERE tablename IN "
            "('workflow_runs', 'task_runs')"
        )
        assert isinstance(plans, int)
        statements = {
            "active_runs": (
                "SELECT id FROM workflow_runs WHERE status IN "
                "('pending','running','cancelling') "
                "ORDER BY created_at,id LIMIT 100"
            ),
            "runnable_tasks": (
                "SELECT t.id FROM task_runs t JOIN workflow_runs r "
                "ON r.id=t.workflow_run_id "
                "AND r.workflow_version_id=t.workflow_version_id "
                "WHERE t.status='runnable' "
                "AND r.status IN ('pending','running') "
                "ORDER BY t.created_at,t.id LIMIT 100"
            ),
            "retry_pending_tasks": (
                "SELECT t.id FROM task_runs t JOIN workflow_runs r "
                "ON r.id=t.workflow_run_id "
                "AND r.workflow_version_id=t.workflow_version_id "
                "WHERE t.status='retry_pending' "
                "AND r.status IN ('pending','running') "
                "ORDER BY t.created_at,t.id LIMIT 100"
            ),
        }
        for name, statement in statements.items():
            plan = await connection.fetch("EXPLAIN (ANALYZE, BUFFERS) " + statement)
            assert plan and any("Limit" in row[0] for row in plan)
            print(f"EXPLAIN {name}:\n" + "\n".join(row[0] for row in plan))
    finally:
        await engine.dispose()
        await connection.close()


def test_real_postgresql_orchestrator_candidate_discovery() -> None:
    with temporary_database(
        "TASKFORGE_ORCHESTRATOR_TEST_DATABASE_URL", "taskforge_m21_workload"
    ) as database_url:
        config = Config("alembic.ini")
        with migration_database_url(database_url.render_as_string(hide_password=False)):
            command.upgrade(config, "head")
        asyncio.run(verify_candidate_scans(database_url))
