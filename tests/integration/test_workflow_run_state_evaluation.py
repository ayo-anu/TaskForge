"""Opt-in workflow-run state evaluation verification against PostgreSQL."""

from __future__ import annotations

import asyncio
import os
from uuid import UUID

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import select, text, update
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.runs import SQLAlchemyWorkflowRunRepository
from taskforge.runs.domain import (
    LatestWorkflowVersion,
    TaskRunStatus,
    WorkflowRunEvaluationResult,
    WorkflowRunStatus,
    create_workflow_run_input,
)
from taskforge.runs.schema import task_runs, workflow_runs
from taskforge.runs.service import WorkflowRunService, WorkflowRunServiceUnavailable
from tests.integration.postgresql import migration_database_url, temporary_database
from tests.integration.test_authentication_persistence import settings_for
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


async def set_run_status(
    sessions: async_sessionmaker[AsyncSession],
    run_id: UUID,
    status: WorkflowRunStatus,
) -> None:
    async with sessions.begin() as session:
        await session.execute(
            update(workflow_runs)
            .where(workflow_runs.c.id == run_id)
            .values(status=status.value)
        )


async def run_projection(
    sessions: async_sessionmaker[AsyncSession], run_id: UUID
) -> tuple[str, object]:
    async with sessions() as session:
        row = (
            await session.execute(
                select(workflow_runs.c.status, workflow_runs.c.updated_at).where(
                    workflow_runs.c.id == run_id
                )
            )
        ).one()
    return row.status, row.updated_at


async def set_all_tasks(
    sessions: async_sessionmaker[AsyncSession], run_id: UUID, status: TaskRunStatus
) -> None:
    async with sessions.begin() as session:
        await session.execute(
            update(task_runs)
            .where(task_runs.c.workflow_run_id == run_id)
            .values(status=status.value)
        )


async def verify_workflow_state_evaluation(database_url: URL) -> None:
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

        # Initial runnable roots prove execution progress, but only one transition
        # is permitted in this invocation.
        initial = await service.evaluate_workflow_run_state(created.id)
        assert initial == WorkflowRunEvaluationResult(
            created.id,
            True,
            WorkflowRunStatus.PENDING,
            WorkflowRunStatus.RUNNING,
        )

        before_running_no_op = await run_projection(sessions, created.id)
        running_no_op = await service.evaluate_workflow_run_state(created.id)
        assert not running_no_op.transitioned
        assert await run_projection(sessions, created.id) == before_running_no_op

        # Late all-success evaluation preserves pending -> running -> succeeded.
        await set_run_status(sessions, created.id, WorkflowRunStatus.PENDING)
        await set_all_tasks(sessions, created.id, TaskRunStatus.SUCCEEDED)
        first_success = await service.evaluate_workflow_run_state(created.id)
        second_success = await service.evaluate_workflow_run_state(created.id)
        assert (
            first_success.previous_status,
            first_success.resulting_status,
        ) == (WorkflowRunStatus.PENDING, WorkflowRunStatus.RUNNING)
        assert (
            second_success.previous_status,
            second_success.resulting_status,
        ) == (WorkflowRunStatus.RUNNING, WorkflowRunStatus.SUCCEEDED)

        # A skipped task prevents success.
        await set_run_status(sessions, created.id, WorkflowRunStatus.RUNNING)
        await set_all_tasks(sessions, created.id, TaskRunStatus.SUCCEEDED)
        await set_statuses(sessions, created.id, c="skipped")
        skipped_no_success = await service.evaluate_workflow_run_state(created.id)
        assert not skipped_no_success.transitioned

        # Failure requires a failed task and complete succeeded/failed/skipped settlement.
        await set_run_status(sessions, created.id, WorkflowRunStatus.RUNNING)
        await set_all_tasks(sessions, created.id, TaskRunStatus.SKIPPED)
        await set_statuses(sessions, created.id, a="failed", independent="succeeded")
        failure = await service.evaluate_workflow_run_state(created.id)
        assert (
            failure.previous_status,
            failure.resulting_status,
        ) == (WorkflowRunStatus.RUNNING, WorkflowRunStatus.FAILED)

        for active_status in (
            TaskRunStatus.BLOCKED,
            TaskRunStatus.RUNNABLE,
            TaskRunStatus.DISPATCHED,
            TaskRunStatus.CLAIMED,
            TaskRunStatus.RUNNING,
            TaskRunStatus.RETRY_SCHEDULED,
            TaskRunStatus.CANCELLED,
        ):
            await set_run_status(sessions, created.id, WorkflowRunStatus.RUNNING)
            await set_all_tasks(sessions, created.id, TaskRunStatus.SKIPPED)
            await set_statuses(
                sessions, created.id, a="failed", independent=active_status.value
            )
            result = await service.evaluate_workflow_run_state(created.id)
            assert not result.transitioned

        # Pending failure evidence still performs only pending -> running.
        await set_run_status(sessions, created.id, WorkflowRunStatus.PENDING)
        await set_all_tasks(sessions, created.id, TaskRunStatus.SKIPPED)
        await set_statuses(sessions, created.id, a="failed")
        pending_failure = await service.evaluate_workflow_run_state(created.id)
        assert (
            pending_failure.previous_status,
            pending_failure.resulting_status,
        ) == (WorkflowRunStatus.PENDING, WorkflowRunStatus.RUNNING)

        # Skipped/cancelled alone are not pending execution evidence.
        for inactive_status in (TaskRunStatus.SKIPPED, TaskRunStatus.CANCELLED):
            await set_run_status(sessions, created.id, WorkflowRunStatus.PENDING)
            await set_all_tasks(sessions, created.id, inactive_status)
            result = await service.evaluate_workflow_run_state(created.id)
            assert not result.transitioned

        # Cancelling and terminal workflow states are immutable no-ops.
        for run_status in (
            WorkflowRunStatus.CANCELLING,
            WorkflowRunStatus.SUCCEEDED,
            WorkflowRunStatus.FAILED,
            WorkflowRunStatus.CANCELLED,
        ):
            await set_run_status(sessions, created.id, run_status)
            await set_all_tasks(sessions, created.id, TaskRunStatus.SUCCEEDED)
            before = await run_projection(sessions, created.id)
            result = await service.evaluate_workflow_run_state(created.id)
            assert result.found and not result.transitioned
            assert result.previous_status is result.resulting_status is run_status
            assert await run_projection(sessions, created.id) == before

        missing_id = UUID(int=0)
        assert await service.evaluate_workflow_run_state(missing_id) == (
            WorkflowRunEvaluationResult(missing_id, False, None, None)
        )

        # Concurrent late evaluation may perform two serialized calls, but each
        # call reports exactly one lifecycle edge and never pending -> terminal.
        await set_run_status(sessions, created.id, WorkflowRunStatus.PENDING)
        await set_all_tasks(sessions, created.id, TaskRunStatus.SUCCEEDED)
        concurrent = await asyncio.gather(
            service.evaluate_workflow_run_state(created.id),
            service.evaluate_workflow_run_state(created.id),
        )
        observed_edges = {
            (result.previous_status, result.resulting_status) for result in concurrent
        }
        assert observed_edges == {
            (WorkflowRunStatus.PENDING, WorkflowRunStatus.RUNNING),
            (WorkflowRunStatus.RUNNING, WorkflowRunStatus.SUCCEEDED),
        }

        # The run row is the shared progression lock and is acquired before task
        # evaluation. A same-run evaluator waits; unrelated task rows are not locked.
        await set_run_status(sessions, created.id, WorkflowRunStatus.PENDING)
        await set_all_tasks(sessions, created.id, TaskRunStatus.RUNNABLE)
        holder = sessions()
        await holder.begin()
        await holder.execute(
            select(workflow_runs.c.id)
            .where(workflow_runs.c.id == created.id)
            .with_for_update()
        )
        waiting = asyncio.create_task(service.evaluate_workflow_run_state(created.id))
        await asyncio.sleep(0.05)
        assert not waiting.done()
        await holder.commit()
        await holder.close()
        assert (await waiting).resulting_status is WorkflowRunStatus.RUNNING

        # A database rejection after the lock rolls back the status update.
        await set_run_status(sessions, created.id, WorkflowRunStatus.PENDING)
        async with sessions.begin() as session:
            await session.execute(
                text(
                    """
                    CREATE FUNCTION reject_test_run_update() RETURNS trigger
                    LANGUAGE plpgsql AS $$ BEGIN
                        RAISE EXCEPTION 'injected workflow update failure';
                    END $$
                    """
                )
            )
            await session.execute(
                text(
                    """
                    CREATE TRIGGER reject_test_run_update_trigger
                    BEFORE UPDATE ON workflow_runs FOR EACH ROW
                    EXECUTE FUNCTION reject_test_run_update()
                    """
                )
            )
        with pytest.raises(WorkflowRunServiceUnavailable):
            await service.evaluate_workflow_run_state(created.id)
        assert (await run_projection(sessions, created.id))[0] == "pending"
        async with sessions.begin() as session:
            await session.execute(
                text("DROP TRIGGER reject_test_run_update_trigger ON workflow_runs")
            )
            await session.execute(text("DROP FUNCTION reject_test_run_update()"))

        # Task 1 and Task 2 share the same lock and compose without invalid edges.
        await set_run_status(sessions, created.id, WorkflowRunStatus.PENDING)
        await set_all_tasks(sessions, created.id, TaskRunStatus.BLOCKED)
        task_one_race = await asyncio.gather(
            service.transition_runnable_tasks(created.id),
            service.evaluate_workflow_run_state(created.id),
        )
        assert task_one_race[1].resulting_status in (
            WorkflowRunStatus.PENDING,
            WorkflowRunStatus.RUNNING,
        )
        assert (await status_map(sessions, created.id))["a"] == "runnable"

        await set_run_status(sessions, created.id, WorkflowRunStatus.PENDING)
        await set_all_tasks(sessions, created.id, TaskRunStatus.BLOCKED)
        await set_statuses(sessions, created.id, a="failed", independent="succeeded")
        await asyncio.gather(
            service.propagate_dependency_failures(created.id),
            service.evaluate_workflow_run_state(created.id),
        )
        assert (await run_projection(sessions, created.id))[0] == "running"
        settled = await service.evaluate_workflow_run_state(created.id)
        assert settled.resulting_status is WorkflowRunStatus.FAILED
    finally:
        await engine.dispose()


def test_workflow_run_state_evaluation_is_guarded_and_serialized() -> None:
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
        asyncio.run(verify_workflow_state_evaluation(database_url))
