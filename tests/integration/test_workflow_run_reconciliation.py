"""Opt-in bounded workflow reconciliation verification against PostgreSQL."""

from __future__ import annotations

import asyncio
import os
from uuid import UUID

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.engine import URL

from taskforge.identity.authorization import OwnerFilter
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.runs import SQLAlchemyWorkflowRunRepository
from taskforge.runs.domain import (
    LatestWorkflowVersion,
    TaskRunStatus,
    WorkflowRunStatus,
    create_workflow_run_input,
)
from taskforge.runs.service import WorkflowRunService, WorkflowRunServiceUnavailable
from tests.integration.postgresql import migration_database_url, temporary_database
from tests.integration.test_authentication_persistence import settings_for
from tests.integration.test_workflow_run_dependency_failure_propagation import (
    seed_failure_graph,
    set_statuses,
    status_map,
)
from tests.integration.test_workflow_run_state_evaluation import (
    run_projection,
    set_all_tasks,
    set_run_status,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION=1 explicitly",
    ),
]


async def verify_reconciliation(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    service = WorkflowRunService(SQLAlchemyWorkflowRunRepository(sessions))
    try:
        owner_id, workflow_id, _ = await seed_failure_graph(sessions)
        created = await service.create_run(
            workflow_id,
            owner_filter=OwnerFilter.only(owner_id),
            requested_by_principal_id=owner_id,
            selection=LatestWorkflowVersion(),
            input_snapshot=create_workflow_run_input({}, {}),
        )

        initial = await service.reconcile_workflow_run(created.id)
        assert initial.found and initial.quiescent
        assert initial.iterations == 2
        assert initial.final_status is WorkflowRunStatus.RUNNING
        assert initial.workflow_transition_count == 1

        # Late pending success requires two Task 3 transitions but stops as soon
        # as terminal state is observed, without a confirmation cycle.
        await set_run_status(sessions, created.id, WorkflowRunStatus.PENDING)
        await set_all_tasks(sessions, created.id, TaskRunStatus.SUCCEEDED)
        succeeded = await service.reconcile_workflow_run(created.id)
        assert succeeded.iterations == 2
        assert succeeded.workflow_transition_count == 2
        assert succeeded.final_status is WorkflowRunStatus.SUCCEEDED
        assert succeeded.quiescent and not succeeded.bound_reached

        terminal_projection = await run_projection(sessions, created.id)
        terminal_no_op = await service.reconcile_workflow_run(created.id)
        assert terminal_no_op.iterations == 1
        assert terminal_no_op.workflow_transition_count == 0
        assert await run_projection(sessions, created.id) == terminal_projection

        # Task 2 settles the complete failed graph before Task 3 derives failure.
        await set_run_status(sessions, created.id, WorkflowRunStatus.PENDING)
        await set_all_tasks(sessions, created.id, TaskRunStatus.BLOCKED)
        await set_statuses(sessions, created.id, a="failed", independent="succeeded")
        failed = await service.reconcile_workflow_run(created.id)
        assert failed.iterations == 2
        assert failed.skipped_transition_count == 5
        assert failed.workflow_transition_count == 2
        assert failed.final_status is WorkflowRunStatus.FAILED
        assert set((await status_map(sessions, created.id)).values()) == {
            "failed",
            "skipped",
            "succeeded",
        }

        # A cancellation-owned terminal task graph finalizes before returning.
        await set_run_status(sessions, created.id, WorkflowRunStatus.PENDING)
        await service.cancel_run(
            created.id,
            OwnerFilter.only(owner_id),
            requested_by_principal_id=owner_id,
            idempotency_key="reconciliation-cancel-key",
            reason="test cancellation",
        )
        cancelling = await service.reconcile_workflow_run(created.id)
        assert cancelling.iterations == 1
        assert cancelling.final_status is WorkflowRunStatus.CANCELLED
        assert cancelling.workflow_transition_count == 1
        assert cancelling.quiescent

        # A failure after Task 1 committed leaves durable progress. Retrying after
        # the injected Task 3 failure continues from that state.
        await set_run_status(sessions, created.id, WorkflowRunStatus.PENDING)
        await set_all_tasks(sessions, created.id, TaskRunStatus.BLOCKED)
        async with sessions.begin() as session:
            await session.execute(
                text(
                    """
                    CREATE FUNCTION reject_reconcile_run_update() RETURNS trigger
                    LANGUAGE plpgsql AS $$ BEGIN
                        RAISE EXCEPTION 'injected reconciliation failure';
                    END $$
                    """
                )
            )
            await session.execute(
                text(
                    """
                    CREATE TRIGGER reject_reconcile_run_update_trigger
                    BEFORE UPDATE ON workflow_runs FOR EACH ROW
                    EXECUTE FUNCTION reject_reconcile_run_update()
                    """
                )
            )
        with pytest.raises(WorkflowRunServiceUnavailable):
            await service.reconcile_workflow_run(created.id)
        states_after_failure = await status_map(sessions, created.id)
        assert states_after_failure["a"] == "runnable"
        assert states_after_failure["independent"] == "runnable"
        assert (await run_projection(sessions, created.id))[0] == "pending"
        async with sessions.begin() as session:
            await session.execute(
                text(
                    "DROP TRIGGER reject_reconcile_run_update_trigger ON workflow_runs"
                )
            )
            await session.execute(text("DROP FUNCTION reject_reconcile_run_update()"))
        recovered = await service.reconcile_workflow_run(created.id)
        assert recovered.final_status is WorkflowRunStatus.RUNNING
        assert recovered.quiescent

        # Concurrent reconcilers may split call-local credit but converge on one
        # valid durable lifecycle.
        await set_run_status(sessions, created.id, WorkflowRunStatus.PENDING)
        await set_all_tasks(sessions, created.id, TaskRunStatus.SUCCEEDED)
        concurrent = await asyncio.gather(
            service.reconcile_workflow_run(created.id),
            service.reconcile_workflow_run(created.id),
        )
        assert (await run_projection(sessions, created.id))[0] == "succeeded"
        assert all(result.quiescent for result in concurrent)
        assert sum(result.workflow_transition_count for result in concurrent) == 2

        missing = await service.reconcile_workflow_run(UUID(int=0))
        assert not missing.found
        assert not missing.quiescent
        assert not missing.bound_reached
    finally:
        await engine.dispose()


def test_workflow_run_reconciliation_is_bounded_restartable_and_concurrent() -> None:
    with temporary_database(
        "TASKFORGE_WORKFLOW_PERSISTENCE_TEST_DATABASE_URL",
        "taskforge_run_creation",
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        configuration = Config("alembic.ini")
        with migration_database_url(alembic_url):
            command.upgrade(configuration, "head")
        asyncio.run(verify_reconciliation(database_url))
