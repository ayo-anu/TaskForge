"""Opt-in dependency-failure propagation verification against PostgreSQL."""

from __future__ import annotations

import asyncio
import os
from datetime import datetime
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import insert, select, update
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.identity.schema import api_principals
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.runs import SQLAlchemyWorkflowRunRepository
from taskforge.runs.domain import (
    LatestWorkflowVersion,
    TaskRunStatus,
    WorkflowRunStatus,
    create_workflow_run_input,
)
from taskforge.runs.schema import task_runs, workflow_runs
from taskforge.runs.service import WorkflowRunService
from taskforge.workflows.schema import (
    workflow_definitions,
    workflow_version_dependencies,
    workflow_version_steps,
    workflow_versions,
)
from tests.integration.postgresql import migration_database_url, temporary_database
from tests.integration.test_authentication_persistence import settings_for

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION=1 explicitly",
    ),
]


async def seed_failure_graph(
    sessions: async_sessionmaker[AsyncSession],
) -> tuple[UUID, UUID, UUID]:
    owner_id, workflow_id, version_id = uuid4(), uuid4(), uuid4()
    steps = ("a", "b", "c", "independent", "join", "left", "right")
    edges = (
        ("a", "b"),
        ("b", "c"),
        ("a", "left"),
        ("a", "right"),
        ("left", "join"),
        ("right", "join"),
    )
    async with sessions.begin() as session:
        await session.execute(
            insert(api_principals).values(
                id=owner_id, name=f"failure-owner-{uuid4().hex}"
            )
        )
        await session.execute(
            insert(workflow_definitions).values(
                id=workflow_id,
                owner_principal_id=owner_id,
                name="failure graph",
                status="enabled",
            )
        )
        await session.execute(
            insert(workflow_versions).values(
                id=version_id,
                workflow_definition_id=workflow_id,
                version_number=1,
                name="failure graph v1",
            )
        )
        await session.execute(
            insert(workflow_version_steps),
            [
                {
                    "workflow_version_id": version_id,
                    "step_identifier": step,
                    "task_type": "test.task",
                    "parameters": {},
                }
                for step in steps
            ],
        )
        await session.execute(
            insert(workflow_version_dependencies),
            [
                {
                    "workflow_version_id": version_id,
                    "predecessor_step_identifier": predecessor,
                    "successor_step_identifier": successor,
                }
                for predecessor, successor in edges
            ],
        )
    return owner_id, workflow_id, version_id


async def set_statuses(
    sessions: async_sessionmaker[AsyncSession],
    run_id: UUID,
    **statuses: str,
) -> None:
    async with sessions.begin() as session:
        for step, status in statuses.items():
            await session.execute(
                update(task_runs)
                .where(
                    task_runs.c.workflow_run_id == run_id,
                    task_runs.c.step_identifier == step,
                )
                .values(status=status)
            )


async def status_map(
    sessions: async_sessionmaker[AsyncSession], run_id: UUID
) -> dict[str, str]:
    async with sessions() as session:
        rows = (
            await session.execute(
                select(task_runs.c.step_identifier, task_runs.c.status).where(
                    task_runs.c.workflow_run_id == run_id
                )
            )
        ).all()
    return {row.step_identifier: row.status for row in rows}


async def task_updated_at(
    sessions: async_sessionmaker[AsyncSession], run_id: UUID, step: str
) -> datetime:
    async with sessions() as session:
        value = await session.scalar(
            select(task_runs.c.updated_at).where(
                task_runs.c.workflow_run_id == run_id,
                task_runs.c.step_identifier == step,
            )
        )
    assert isinstance(value, datetime)
    return value


async def verify_failure_propagation(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    service = WorkflowRunService(SQLAlchemyWorkflowRunRepository(sessions))
    try:
        owner_id, workflow_id, version_id = await seed_failure_graph(sessions)
        created = await service.create_run(
            workflow_id,
            owner_principal_id=owner_id,
            requested_by_principal_id=owner_id,
            selection=LatestWorkflowVersion(),
            input_snapshot=create_workflow_run_input({}, {}),
        )

        # Only failed and skipped predecessors seed propagation.
        for predecessor_status in TaskRunStatus:
            await set_statuses(
                sessions,
                created.id,
                a=predecessor_status.value,
                b="blocked",
                c="blocked",
                left="blocked",
                right="blocked",
                join="blocked",
            )
            await service.propagate_dependency_failures(created.id)
            states = await status_map(sessions, created.id)
            expected = (
                "skipped"
                if predecessor_status in (TaskRunStatus.FAILED, TaskRunStatus.SKIPPED)
                else "blocked"
            )
            assert states["b"] == expected

        # Propagation crosses an already skipped intermediate from a prior call.
        await set_statuses(
            sessions,
            created.id,
            a="failed",
            b="skipped",
            c="blocked",
            left="succeeded",
            right="succeeded",
            join="runnable",
        )
        skipped_intermediate_timestamp = await task_updated_at(
            sessions, created.id, "b"
        )
        across_prior_skip = await service.propagate_dependency_failures(created.id)
        assert "c" in across_prior_skip.skipped_step_identifiers
        assert "b" not in across_prior_skip.skipped_step_identifiers
        assert await task_updated_at(sessions, created.id, "b") == (
            skipped_intermediate_timestamp
        )
        assert (await status_map(sessions, created.id))["c"] == "skipped"

        # Progressed intermediates are a conservative traversal boundary.
        for intermediate in (
            TaskRunStatus.RUNNABLE,
            TaskRunStatus.DISPATCHED,
            TaskRunStatus.CLAIMED,
            TaskRunStatus.RUNNING,
            TaskRunStatus.RETRY_PENDING,
            TaskRunStatus.RETRY_SCHEDULED,
            TaskRunStatus.SUCCEEDED,
            TaskRunStatus.CANCELLED,
        ):
            await set_statuses(
                sessions, created.id, a="failed", b=intermediate.value, c="blocked"
            )
            await service.propagate_dependency_failures(created.id)
            assert (await status_map(sessions, created.id))["c"] == "blocked"

        for intermediate in (TaskRunStatus.FAILED, TaskRunStatus.SKIPPED):
            await set_statuses(
                sessions, created.id, a="succeeded", b=intermediate.value, c="blocked"
            )
            await service.propagate_dependency_failures(created.id)
            assert (await status_map(sessions, created.id))["c"] == "skipped"

        # Dependencies are conjunctive: any failed required predecessor skips a join.
        for left, right, expected in (
            ("succeeded", "succeeded", "blocked"),
            ("failed", "succeeded", "skipped"),
            ("skipped", "running", "skipped"),
            ("running", "succeeded", "blocked"),
        ):
            await set_statuses(
                sessions,
                created.id,
                a="succeeded",
                left=left,
                right=right,
                join="blocked",
            )
            await service.propagate_dependency_failures(created.id)
            assert (await status_map(sessions, created.id))["join"] == expected

        # Cancelling and terminal runs suppress dependency-failure propagation.
        for run_status in (
            WorkflowRunStatus.CANCELLING,
            WorkflowRunStatus.SUCCEEDED,
            WorkflowRunStatus.FAILED,
            WorkflowRunStatus.CANCELLED,
        ):
            await set_statuses(sessions, created.id, a="failed", b="blocked")
            async with sessions.begin() as session:
                await session.execute(
                    update(workflow_runs)
                    .where(workflow_runs.c.id == created.id)
                    .values(status=run_status.value)
                )
            result = await service.propagate_dependency_failures(created.id)
            assert result.skipped_count == 0
            assert (await status_map(sessions, created.id))["b"] == "blocked"

        # A later version and mutable definition state cannot affect this run.
        newer_version_id = uuid4()
        async with sessions.begin() as session:
            await session.execute(
                update(workflow_runs)
                .where(workflow_runs.c.id == created.id)
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
                    version_number=2,
                    name="unrelated v2",
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

        await set_statuses(
            sessions,
            created.id,
            a="failed",
            b="blocked",
            c="blocked",
            left="blocked",
            right="blocked",
            join="blocked",
        )
        first, second = await asyncio.gather(
            service.propagate_dependency_failures(created.id),
            service.propagate_dependency_failures(created.id),
        )
        assert first.skipped_count + second.skipped_count == 5
        assert set(
            first.skipped_step_identifiers + second.skipped_step_identifiers
        ) == {
            "b",
            "c",
            "join",
            "left",
            "right",
        }
        assert created.workflow_version_id == version_id

        before_no_op = await task_updated_at(sessions, created.id, "b")
        no_op = await service.propagate_dependency_failures(created.id)
        assert no_op.skipped_count == 0
        assert await task_updated_at(sessions, created.id, "b") == before_no_op
        assert (await status_map(sessions, created.id))["independent"] == "runnable"
    finally:
        await engine.dispose()


def test_dependency_failure_propagation_is_transitive_guarded_and_idempotent() -> None:
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
        asyncio.run(verify_failure_propagation(database_url))
