"""Opt-in PostgreSQL verification for Task 3 cancellation propagation."""

from __future__ import annotations

import asyncio
import os
from contextlib import suppress
from uuid import UUID

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import select, text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.claims.authority import TaskClaimResultAuthorityIssuer
from taskforge.claims.domain import TaskClaimOutcome, TaskClaimRejected
from taskforge.claims.service import TaskClaimService
from taskforge.identity.authorization import OwnerFilter
from taskforge.persistence.claims import SQLAlchemyTaskClaimRepository
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.runs import SQLAlchemyWorkflowRunRepository
from taskforge.runs.domain import (
    LatestWorkflowVersion,
    WorkflowRunCancellationOutcome,
    WorkflowRunStatus,
    create_workflow_run_input,
)
from taskforge.runs.schema import (
    workflow_run_cancellation_requests,
    workflow_runs,
)
from taskforge.runs.service import WorkflowRunService, WorkflowRunServiceUnavailable
from tests.integration.postgresql import (
    asyncpg_dsn,
    migration_database_url,
    temporary_database,
)
from tests.integration.test_authentication_persistence import settings_for
from tests.integration.test_task_claim_acquisition import (
    add_dispatched_task,
    add_worker,
    wait_for_lock_waiter,
)
from tests.integration.test_workflow_run_dependency_failure_propagation import (
    seed_failure_graph,
    set_statuses,
    status_map,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION=1 explicitly",
    ),
]


async def _stored_run_and_request_count(
    sessions: async_sessionmaker[AsyncSession], run_id: UUID
) -> tuple[str, int]:
    async with sessions() as session:
        status = await session.scalar(
            select(workflow_runs.c.status).where(workflow_runs.c.id == run_id)
        )
        count = await session.scalar(
            select(text("count(*)"))
            .select_from(workflow_run_cancellation_requests)
            .where(workflow_run_cancellation_requests.c.workflow_run_id == run_id)
        )
    assert status is not None and count is not None
    return str(status), int(count)


async def _verify(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    repository = SQLAlchemyWorkflowRunRepository(sessions)
    service = WorkflowRunService(repository)
    try:
        owner_id, workflow_id, _ = await seed_failure_graph(sessions)
        created = await service.create_run(
            workflow_id,
            owner_principal_id=owner_id,
            requested_by_principal_id=owner_id,
            selection=LatestWorkflowVersion(),
            input_snapshot=create_workflow_run_input({}, {}),
        )
        await set_statuses(
            sessions,
            created.id,
            a="blocked",
            b="runnable",
            c="retry_pending",
            independent="retry_scheduled",
            join="dispatched",
            left="claimed",
            right="running",
        )

        accepted = await service.cancel_run(
            created.id,
            OwnerFilter.only(owner_id),
            requested_by_principal_id=owner_id,
            idempotency_key="propagation-accepted-key",
            reason="operator request",
        )
        assert accepted.outcome is WorkflowRunCancellationOutcome.NEWLY_ACCEPTED
        assert await status_map(sessions, created.id) == {
            "a": "cancelled",
            "b": "cancelled",
            "c": "cancelled",
            "independent": "cancelled",
            "join": "cancelled",
            "left": "claimed",
            "right": "running",
        }

        # Only an exact retry may heal missed pre-dispatch suppression.
        await set_statuses(sessions, created.id, a="runnable", b="blocked")
        unrelated = await service.cancel_run(
            created.id,
            OwnerFilter.only(owner_id),
            requested_by_principal_id=owner_id,
            idempotency_key="propagation-unrelated-key",
            reason="operator request",
        )
        assert unrelated.outcome is WorkflowRunCancellationOutcome.ALREADY_CANCELLING
        states = await status_map(sessions, created.id)
        assert states["a"] == "runnable" and states["b"] == "blocked"

        exact = await service.cancel_run(
            created.id,
            OwnerFilter.only(owner_id),
            requested_by_principal_id=owner_id,
            idempotency_key="propagation-accepted-key",
            reason="operator request",
        )
        assert exact.outcome is WorkflowRunCancellationOutcome.EXACT_RETRY
        states = await status_map(sessions, created.id)
        assert states["a"] == "cancelled" and states["b"] == "cancelled"

        # The internal operation is repeatable and cancellation-first reconciliation
        # never promotes dependency work while the workflow remains cancelling.
        await set_statuses(sessions, created.id, c="retry_scheduled")
        first = await service.suppress_unstarted_tasks(created.id)
        second = await service.suppress_unstarted_tasks(created.id)
        assert first.cancelled_count == 1 and second.cancelled_count == 0
        await set_statuses(sessions, created.id, independent="blocked")
        reconciled = await service.reconcile_workflow_run(created.id)
        assert reconciled.final_status is WorkflowRunStatus.CANCELLING
        assert reconciled.cancelled_transition_count == 1
        assert (await status_map(sessions, created.id))["independent"] == "cancelled"

        # A task-update failure rolls back request insertion and the run transition.
        rollback = await service.create_run(
            workflow_id,
            owner_principal_id=owner_id,
            requested_by_principal_id=owner_id,
            selection=LatestWorkflowVersion(),
            input_snapshot=create_workflow_run_input({}, {}),
        )
        async with sessions.begin() as session:
            await session.execute(
                text(
                    """
                    CREATE FUNCTION reject_task3_suppression() RETURNS trigger
                    LANGUAGE plpgsql AS $$ BEGIN
                        RAISE EXCEPTION 'injected Task 3 suppression failure';
                    END $$
                    """
                )
            )
            await session.execute(
                text(
                    """
                    CREATE TRIGGER reject_task3_suppression_trigger
                    BEFORE UPDATE ON task_runs FOR EACH ROW
                    WHEN (NEW.workflow_run_id = '"""
                    + str(rollback.id)
                    + """'::uuid)
                    EXECUTE FUNCTION reject_task3_suppression()
                    """
                )
            )
        with pytest.raises(WorkflowRunServiceUnavailable):
            await service.cancel_run(
                rollback.id,
                OwnerFilter.only(owner_id),
                requested_by_principal_id=owner_id,
                idempotency_key="propagation-rollback-key",
                reason=None,
            )
        assert await _stored_run_and_request_count(sessions, rollback.id) == (
            "pending",
            0,
        )
    finally:
        await engine.dispose()


async def _verify_dispatched_claim_races(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    run_service = WorkflowRunService(SQLAlchemyWorkflowRunRepository(sessions))
    claim_service = TaskClaimService(
        SQLAlchemyTaskClaimRepository(sessions, worker_stale_after_seconds=30),
        TaskClaimResultAuthorityIssuer(b"task4-claim-race-secret-value-32"),
        lease_seconds=60,
    )
    setup = await asyncpg.connect(asyncpg_dsn(database_url))
    blocker = await asyncpg.connect(asyncpg_dsn(database_url))
    observer = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        # Settlement commits first: the later claim sees cancelled durable state.
        first_worker = await add_worker(setup)
        first_dispatch = await add_dispatched_task(setup)
        first_owner = await setup.fetchval(
            "SELECT w.owner_principal_id FROM workflow_runs wr "
            "JOIN workflow_definitions w "
            "ON w.id = wr.workflow_definition_id WHERE wr.id = $1",
            first_dispatch.workflow_run_id,
        )
        accepted = await run_service.cancel_run(
            first_dispatch.workflow_run_id,
            OwnerFilter.only(first_owner),
            requested_by_principal_id=first_owner,
            idempotency_key="settlement-first-race-key",
            reason=None,
        )
        assert accepted.outcome is WorkflowRunCancellationOutcome.NEWLY_ACCEPTED
        with pytest.raises(TaskClaimRejected):
            await claim_service.claim_task(
                first_worker.authenticated, first_worker.session_id, first_dispatch
            )
        assert (
            await setup.fetchval(
                "SELECT status::text FROM task_runs WHERE id = $1",
                first_dispatch.task_run_id,
            )
            == "cancelled"
        )

        # Claim queues first on the common workflow lock. Once released it claims;
        # cancellation queues behind it and preserves the now-claimed task.
        second_worker = await add_worker(setup)
        second_dispatch = await add_dispatched_task(setup)
        second_owner = await setup.fetchval(
            "SELECT w.owner_principal_id FROM workflow_runs wr "
            "JOIN workflow_definitions w "
            "ON w.id = wr.workflow_definition_id WHERE wr.id = $1",
            second_dispatch.workflow_run_id,
        )
        transaction = blocker.transaction()
        await transaction.start()
        claim: asyncio.Task[object] | None = None
        cancellation: asyncio.Task[object] | None = None
        try:
            await blocker.execute(
                "SELECT id FROM workflow_runs WHERE id = $1 FOR UPDATE",
                second_dispatch.workflow_run_id,
            )
            claim = asyncio.create_task(
                claim_service.claim_task(
                    second_worker.authenticated,
                    second_worker.session_id,
                    second_dispatch,
                )
            )
            await wait_for_lock_waiter(observer)
            cancellation = asyncio.create_task(
                run_service.cancel_run(
                    second_dispatch.workflow_run_id,
                    OwnerFilter.only(second_owner),
                    requested_by_principal_id=second_owner,
                    idempotency_key="claim-first-race-key",
                    reason=None,
                )
            )
            await wait_for_lock_waiter(observer, minimum=2)
            await transaction.commit()
            claimed, cancelled = await asyncio.wait_for(
                asyncio.gather(claim, cancellation), timeout=5
            )
            assert claimed.outcome is TaskClaimOutcome.ACQUIRED_ACTIVE
            assert cancelled.outcome is WorkflowRunCancellationOutcome.NEWLY_ACCEPTED
            assert (
                await setup.fetchval(
                    "SELECT status::text FROM task_runs WHERE id = $1",
                    second_dispatch.task_run_id,
                )
                == "claimed"
            )
        finally:
            for pending in (claim, cancellation):
                if pending is not None and not pending.done():
                    pending.cancel()
                    with suppress(asyncio.CancelledError):
                        await pending
            if blocker.is_in_transaction():
                await transaction.rollback()
    finally:
        await setup.close()
        await blocker.close()
        await observer.close()
        await engine.dispose()


def test_cancellation_propagation_is_atomic_and_preserves_in_flight_work() -> None:
    with temporary_database(
        "TASKFORGE_WORKFLOW_PERSISTENCE_TEST_DATABASE_URL",
        "taskforge_run_cancellation",
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        configuration = Config("alembic.ini")
        with migration_database_url(alembic_url):
            command.upgrade(configuration, "head")
        asyncio.run(_verify(database_url))
        asyncio.run(_verify_dispatched_claim_races(database_url))
