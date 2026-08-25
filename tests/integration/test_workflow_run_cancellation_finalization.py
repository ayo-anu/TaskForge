"""Opt-in PostgreSQL verification for workflow cancellation finalization."""

from __future__ import annotations

import asyncio
import os
from uuid import UUID

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import select, update
from sqlalchemy.engine import URL

from taskforge.identity.authorization import OwnerFilter
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.runs import SQLAlchemyWorkflowRunRepository
from taskforge.runs.domain import (
    CancellationFinalizationOutcome,
    LatestWorkflowVersion,
    TaskRunStatus,
    WorkflowRunStatus,
    create_workflow_run_input,
)
from taskforge.runs.schema import (
    task_runs,
    workflow_runs,
)
from taskforge.runs.service import (
    WorkflowRunCancellationInvariantError,
    WorkflowRunService,
)
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


async def _create_cancelling_run(
    service: WorkflowRunService,
    workflow_id: UUID,
    owner_id: UUID,
) -> UUID:
    created = await service.create_run(
        workflow_id,
        owner_principal_id=owner_id,
        requested_by_principal_id=owner_id,
        selection=LatestWorkflowVersion(),
        input_snapshot=create_workflow_run_input({}, {}),
    )
    await service.cancel_run(
        created.id,
        OwnerFilter.only(owner_id),
        requested_by_principal_id=owner_id,
        idempotency_key=f"finalize-{created.id}",
        reason="operator requested",
    )
    return created.id


async def _verify(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    service = WorkflowRunService(SQLAlchemyWorkflowRunRepository(sessions))
    try:
        owner_id, workflow_id, _ = await seed_failure_graph(sessions)

        # Every terminal mixture is eligible; earlier authoritative outcomes survive.
        for mixture in (
            {"a": "succeeded"},
            {"a": "failed", "b": "succeeded"},
            {"a": "succeeded", "b": "failed", "c": "skipped"},
        ):
            run_id = await _create_cancelling_run(service, workflow_id, owner_id)
            await set_statuses(sessions, run_id, **mixture)
            before = await status_map(sessions, run_id)
            result = await service.finalize_workflow_run_cancellation(run_id)
            assert result.outcome is CancellationFinalizationOutcome.FINALIZED
            assert result.resulting_status is WorkflowRunStatus.CANCELLED
            assert await status_map(sessions, run_id) == before
            replay = await service.finalize_workflow_run_cancellation(run_id)
            assert replay.outcome is CancellationFinalizationOutcome.ALREADY_CANCELLED

        # Every nonterminal state prevents finalization.
        for status in (
            TaskRunStatus.BLOCKED,
            TaskRunStatus.RUNNABLE,
            TaskRunStatus.DISPATCHED,
            TaskRunStatus.CLAIMED,
            TaskRunStatus.RUNNING,
            TaskRunStatus.RETRY_PENDING,
            TaskRunStatus.RETRY_SCHEDULED,
        ):
            run_id = await _create_cancelling_run(service, workflow_id, owner_id)
            async with sessions.begin() as session:
                task_id = await session.scalar(
                    select(task_runs.c.id)
                    .where(task_runs.c.workflow_run_id == run_id)
                    .limit(1)
                )
                assert task_id is not None
                await session.execute(
                    update(task_runs)
                    .where(task_runs.c.id == task_id)
                    .values(status=status.value)
                )
            waiting = await service.finalize_workflow_run_cancellation(run_id)
            assert (
                waiting.outcome
                is CancellationFinalizationOutcome.AWAITING_TASK_SETTLEMENT
            )
            assert waiting.resulting_status is WorkflowRunStatus.CANCELLING

        # Concurrent finalizers serialize on the run and produce one transition.
        run_id = await _create_cancelling_run(service, workflow_id, owner_id)
        concurrent = await asyncio.gather(
            service.finalize_workflow_run_cancellation(run_id),
            service.finalize_workflow_run_cancellation(run_id),
        )
        assert {result.outcome for result in concurrent} == {
            CancellationFinalizationOutcome.FINALIZED,
            CancellationFinalizationOutcome.ALREADY_CANCELLED,
        }

        # Cancellation states without their canonical intent fail closed.
        corrupt = await service.create_run(
            workflow_id,
            owner_principal_id=owner_id,
            requested_by_principal_id=owner_id,
            selection=LatestWorkflowVersion(),
            input_snapshot=create_workflow_run_input({}, {}),
        )
        async with sessions.begin() as session:
            await session.execute(
                update(workflow_runs)
                .where(workflow_runs.c.id == corrupt.id)
                .values(status=WorkflowRunStatus.CANCELLING.value)
            )
        with pytest.raises(WorkflowRunCancellationInvariantError):
            await service.finalize_workflow_run_cancellation(corrupt.id)

        # Inspection exposes only canonical metadata, recovery count, and caveats.
        inspected_id = await _create_cancelling_run(service, workflow_id, owner_id)
        inspected = await service.get_run(inspected_id, owner_principal_id=owner_id)
        assert inspected.cancellation is not None
        assert inspected.cancellation.requested_by_principal_id == owner_id
        assert inspected.cancellation.reason == "operator requested"
        assert inspected.cancellation.recovered_cancellation_count == 0
        assert len(inspected.cancellation.caveats) == 3

        # A legitimate cancellation-owned run cannot be normally re-derived.
        evaluated = await service.evaluate_workflow_run_state(inspected_id)
        assert evaluated.previous_status is WorkflowRunStatus.CANCELLING
        assert evaluated.resulting_status is WorkflowRunStatus.CANCELLING
        async with sessions() as session:
            stored = await session.scalar(
                select(workflow_runs.c.status).where(workflow_runs.c.id == inspected_id)
            )
        assert stored == WorkflowRunStatus.CANCELLING.value
    finally:
        await engine.dispose()


def test_workflow_cancellation_finalization_invariants_and_concurrency() -> None:
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
        asyncio.run(_verify(database_url))
