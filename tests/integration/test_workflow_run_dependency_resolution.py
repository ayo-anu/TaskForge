"""Opt-in runnable-transition verification against PostgreSQL."""

from __future__ import annotations

import asyncio
import os
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import insert, select, update
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.runs import SQLAlchemyWorkflowRunRepository
from taskforge.runs.domain import (
    ExplicitWorkflowVersion,
    LatestWorkflowVersion,
    TaskRunStatus,
    WorkflowRunStatus,
    create_workflow_run_input,
)
from taskforge.runs.schema import task_runs, workflow_runs
from taskforge.runs.service import WorkflowRunService
from taskforge.workflows.schema import (
    workflow_definitions,
    workflow_version_steps,
    workflow_versions,
)
from tests.integration.postgresql import migration_database_url, temporary_database
from tests.integration.test_authentication_persistence import settings_for
from tests.integration.test_workflow_run_creation import seed_workflow

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION=1 explicitly",
    ),
]


async def task_status(
    sessions: async_sessionmaker[AsyncSession], run_id: UUID, step: str
) -> str:
    async with sessions() as session:
        value = await session.scalar(
            select(task_runs.c.status).where(
                task_runs.c.workflow_run_id == run_id,
                task_runs.c.step_identifier == step,
            )
        )
    assert isinstance(value, str)
    return value


async def verify_dependency_resolution(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    repository = SQLAlchemyWorkflowRunRepository(sessions)
    service = WorkflowRunService(repository)
    try:
        owner_id, _, workflow_id, _, version_two_id = await seed_workflow(sessions)
        single = await service.create_run(
            workflow_id,
            owner_principal_id=owner_id,
            requested_by_principal_id=owner_id,
            selection=ExplicitWorkflowVersion(1),
            input_snapshot=create_workflow_run_input({}, {}),
        )

        # The repository is the authoritative dependency-state matrix: only a
        # succeeded predecessor can promote the dependent leaf.
        for predecessor_status in TaskRunStatus:
            async with sessions.begin() as session:
                await session.execute(
                    update(task_runs)
                    .where(
                        task_runs.c.workflow_run_id == single.id,
                        task_runs.c.step_identifier == "root",
                    )
                    .values(status=predecessor_status.value)
                )
                await session.execute(
                    update(task_runs)
                    .where(
                        task_runs.c.workflow_run_id == single.id,
                        task_runs.c.step_identifier == "leaf",
                    )
                    .values(status=TaskRunStatus.BLOCKED.value)
                )
            await service.transition_runnable_tasks(single.id)
            expected = (
                TaskRunStatus.RUNNABLE.value
                if predecessor_status is TaskRunStatus.SUCCEEDED
                else TaskRunStatus.BLOCKED.value
            )
            assert await task_status(sessions, single.id, "leaf") == expected

        join_run = await service.create_run(
            workflow_id,
            owner_principal_id=owner_id,
            requested_by_principal_id=owner_id,
            selection=LatestWorkflowVersion(),
            input_snapshot=create_workflow_run_input({}, {}),
        )

        async def set_join(left: str, right: str, candidate: str = "blocked") -> None:
            async with sessions.begin() as session:
                for step, status in (
                    ("left", left),
                    ("right", right),
                    ("join", candidate),
                ):
                    await session.execute(
                        update(task_runs)
                        .where(
                            task_runs.c.workflow_run_id == join_run.id,
                            task_runs.c.step_identifier == step,
                        )
                        .values(status=status)
                    )

        for left, right, expected in (
            ("running", "runnable", "blocked"),
            ("succeeded", "running", "blocked"),
            ("succeeded", "failed", "blocked"),
            ("succeeded", "skipped", "blocked"),
            ("succeeded", "cancelled", "blocked"),
            ("succeeded", "succeeded", "runnable"),
        ):
            await set_join(left, right)
            await service.transition_runnable_tasks(join_run.id)
            assert await task_status(sessions, join_run.id, "join") == expected

        # An active blocked root is recoverable from the empty predecessor set.
        await set_join("blocked", "runnable", "blocked")
        root_result = await service.transition_runnable_tasks(join_run.id)
        assert "left" in root_result.transitioned_step_identifiers
        assert await task_status(sessions, join_run.id, "join") == "blocked"

        # Every candidate state other than blocked is immutable for this operation.
        for candidate_status in TaskRunStatus:
            if candidate_status is TaskRunStatus.BLOCKED:
                continue
            await set_join("succeeded", "succeeded", candidate_status.value)
            result = await service.transition_runnable_tasks(join_run.id)
            assert result.transitioned_count == 0
            assert (
                await task_status(sessions, join_run.id, "join")
                == candidate_status.value
            )

        # Cancelling and terminal runs suppress otherwise eligible transitions.
        for run_status in (
            WorkflowRunStatus.CANCELLING,
            WorkflowRunStatus.SUCCEEDED,
            WorkflowRunStatus.FAILED,
            WorkflowRunStatus.CANCELLED,
        ):
            await set_join("succeeded", "succeeded")
            async with sessions.begin() as session:
                await session.execute(
                    update(workflow_runs)
                    .where(workflow_runs.c.id == join_run.id)
                    .values(status=run_status.value)
                )
            result = await service.transition_runnable_tasks(join_run.id)
            assert result.transitioned_count == 0
            assert await task_status(sessions, join_run.id, "join") == "blocked"

        # Mutable definition state and a newer version cannot alter this run's graph.
        newer_version_id = uuid4()
        async with sessions.begin() as session:
            await session.execute(
                update(workflow_runs)
                .where(workflow_runs.c.id == join_run.id)
                .values(status=WorkflowRunStatus.RUNNING.value)
            )
            await session.execute(
                update(workflow_definitions)
                .where(workflow_definitions.c.id == workflow_id)
                .values(status="disabled")
            )
            await session.execute(
                insert(workflow_versions).values(
                    id=newer_version_id,
                    workflow_definition_id=workflow_id,
                    version_number=3,
                    name="structurally different",
                )
            )
            await session.execute(
                insert(workflow_version_steps).values(
                    workflow_version_id=newer_version_id,
                    step_identifier="unrelated",
                    task_type="test.task",
                    parameters={},
                )
            )
        await set_join("succeeded", "succeeded")

        first, second = await asyncio.gather(
            service.transition_runnable_tasks(join_run.id),
            service.transition_runnable_tasks(join_run.id),
        )
        assert first.transitioned_count + second.transitioned_count == 1
        assert first.workflow_run_id == second.workflow_run_id == join_run.id
        assert await task_status(sessions, join_run.id, "join") == "runnable"

        no_op = await service.transition_runnable_tasks(join_run.id)
        assert no_op.transitioned_count == 0
        assert no_op.transitioned_task_ids == ()
        assert no_op.transitioned_step_identifiers == ()
        assert join_run.workflow_version_id == version_two_id
    finally:
        await engine.dispose()


def test_dependency_resolution_is_guarded_idempotent_and_immutable() -> None:
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
        asyncio.run(verify_dependency_resolution(database_url))
