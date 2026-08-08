"""Opt-in atomic workflow run creation verification against PostgreSQL."""

from __future__ import annotations

import asyncio
import os
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import func, insert, select, text, update
from sqlalchemy.engine import URL
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.identity.schema import api_principals
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.runs import SQLAlchemyWorkflowRunRepository
from taskforge.runs.domain import (
    ExplicitWorkflowVersion,
    LatestWorkflowVersion,
    NewTaskRun,
    NewWorkflowRun,
    TaskRunStatus,
    WorkflowRunStatus,
    WorkflowRunTargetUnavailable,
    create_workflow_run_input,
    materialize_initial_tasks,
)
from taskforge.runs.schema import (
    task_runs,
    workflow_run_idempotency,
    workflow_run_inputs,
    workflow_runs,
)
from taskforge.runs.service import (
    WorkflowRunPersistenceConflict,
    WorkflowRunService,
    WorkflowRunTargetNotFound,
)
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


async def seed_workflow(
    sessions: async_sessionmaker[AsyncSession],
) -> tuple[UUID, UUID, UUID, UUID, UUID]:
    owner_id, other_owner_id = uuid4(), uuid4()
    workflow_id = uuid4()
    version_one_id, version_two_id = uuid4(), uuid4()
    async with sessions.begin() as session:
        await session.execute(
            insert(api_principals),
            [
                {"id": owner_id, "name": f"owner-{uuid4().hex}"},
                {"id": other_owner_id, "name": f"owner-{uuid4().hex}"},
            ],
        )
        await session.execute(
            insert(workflow_definitions).values(
                id=workflow_id,
                owner_principal_id=owner_id,
                name="creation workflow",
                status="enabled",
            )
        )
        await session.execute(
            insert(workflow_versions),
            [
                {
                    "id": version_one_id,
                    "workflow_definition_id": workflow_id,
                    "version_number": 1,
                    "name": "one",
                },
                {
                    "id": version_two_id,
                    "workflow_definition_id": workflow_id,
                    "version_number": 2,
                    "name": "two",
                },
            ],
        )
        await session.execute(
            insert(workflow_version_steps),
            [
                {
                    "workflow_version_id": version_one_id,
                    "step_identifier": identifier,
                    "task_type": "test.task",
                    "parameters": {},
                }
                for identifier in ("root", "leaf")
            ]
            + [
                {
                    "workflow_version_id": version_two_id,
                    "step_identifier": identifier,
                    "task_type": "test.task",
                    "parameters": {},
                }
                for identifier in ("left", "right", "join")
            ],
        )
        await session.execute(
            insert(workflow_version_dependencies),
            [
                {
                    "workflow_version_id": version_one_id,
                    "predecessor_step_identifier": "root",
                    "successor_step_identifier": "leaf",
                },
                {
                    "workflow_version_id": version_two_id,
                    "predecessor_step_identifier": "left",
                    "successor_step_identifier": "join",
                },
                {
                    "workflow_version_id": version_two_id,
                    "predecessor_step_identifier": "right",
                    "successor_step_identifier": "join",
                },
            ],
        )
    return owner_id, other_owner_id, workflow_id, version_one_id, version_two_id


async def verify_creation(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    repository = SQLAlchemyWorkflowRunRepository(sessions)
    service = WorkflowRunService(repository)
    try:
        (
            owner_id,
            other_owner_id,
            workflow_id,
            version_one_id,
            version_two_id,
        ) = await seed_workflow(sessions)
        accepted = create_workflow_run_input(
            {"value": 1}, {"artifact": {"kind": "object"}}
        )
        explicit = await service.create_run(
            workflow_id,
            owner_principal_id=owner_id,
            requested_by_principal_id=owner_id,
            selection=ExplicitWorkflowVersion(1),
            input_snapshot=accepted,
        )
        latest = await service.create_run(
            workflow_id,
            owner_principal_id=owner_id,
            requested_by_principal_id=owner_id,
            selection=LatestWorkflowVersion(),
            input_snapshot=create_workflow_run_input({}, {}),
        )
        assert explicit.workflow_version_id == version_one_id
        assert latest.workflow_version_id == version_two_id
        assert explicit.status is latest.status is WorkflowRunStatus.PENDING
        assert explicit.created_at.tzinfo is not None

        async with sessions() as session:
            explicit_input = (
                await session.execute(
                    select(workflow_run_inputs).where(
                        workflow_run_inputs.c.workflow_run_id == explicit.id
                    )
                )
            ).one()
            assert explicit_input.payload == {"value": 1}
            assert explicit_input.input_references == {"artifact": {"kind": "object"}}
            rows = (
                await session.execute(
                    select(task_runs.c.step_identifier, task_runs.c.status)
                    .where(task_runs.c.workflow_run_id == latest.id)
                    .order_by(task_runs.c.step_identifier)
                )
            ).all()
            assert [(row.step_identifier, row.status) for row in rows] == [
                ("join", TaskRunStatus.BLOCKED.value),
                ("left", TaskRunStatus.RUNNABLE.value),
                ("right", TaskRunStatus.RUNNABLE.value),
            ]

        with pytest.raises(WorkflowRunTargetNotFound):
            await service.create_run(
                workflow_id,
                owner_principal_id=other_owner_id,
                requested_by_principal_id=other_owner_id,
                selection=LatestWorkflowVersion(),
                input_snapshot=create_workflow_run_input({}, {}),
            )

        before = await count_creation_rows(sessions)
        with pytest.raises(WorkflowRunPersistenceConflict):
            await service.create_run(
                workflow_id,
                owner_principal_id=owner_id,
                requested_by_principal_id=uuid4(),
                selection=LatestWorkflowVersion(),
                input_snapshot=create_workflow_run_input({}, {}),
            )
        assert await count_creation_rows(sessions) == before

        visibility_run = NewWorkflowRun(uuid4(), owner_id)
        creation = repository.creation_transaction()
        async with creation:
            prepared = await creation.prepare_creation_target(
                workflow_id, owner_id, ExplicitWorkflowVersion(1)
            )
            assert prepared is not None and prepared.snapshot is not None
            initial = materialize_initial_tasks(prepared.snapshot)
            tasks = tuple(
                NewTaskRun(uuid4(), task.step_identifier, task.status)
                for task in initial
            )
            await creation.insert_complete_run(
                prepared,
                visibility_run,
                create_workflow_run_input({}, {}),
                tasks,
            )
            async with sessions() as observer:
                assert (
                    await observer.scalar(
                        select(func.count())
                        .select_from(workflow_runs)
                        .where(workflow_runs.c.id == visibility_run.id)
                    )
                    == 0
                )
            await creation.commit()
        async with sessions() as observer:
            assert (
                await observer.scalar(
                    select(func.count())
                    .select_from(task_runs)
                    .where(task_runs.c.workflow_run_id == visibility_run.id)
                )
                == 2
            )

        transaction = repository.creation_transaction()
        async with transaction:
            prepared = await transaction.prepare_creation_target(
                workflow_id, owner_id, LatestWorkflowVersion()
            )
            assert prepared is not None
            async with sessions.begin() as contender:
                await contender.execute(text("SET LOCAL lock_timeout = '100ms'"))
                with pytest.raises(DBAPIError):
                    await contender.execute(
                        update(workflow_definitions)
                        .where(workflow_definitions.c.id == workflow_id)
                        .values(status="disabled")
                    )

        async with sessions.begin() as session:
            await session.execute(
                update(workflow_definitions)
                .where(workflow_definitions.c.id == workflow_id)
                .values(status="disabled")
            )
        with pytest.raises(WorkflowRunTargetUnavailable):
            await service.create_run(
                workflow_id,
                owner_principal_id=owner_id,
                requested_by_principal_id=owner_id,
                selection=LatestWorkflowVersion(),
                input_snapshot=create_workflow_run_input({}, {}),
            )

        async with sessions() as session:
            assert (
                await session.scalar(
                    select(func.count()).select_from(workflow_run_idempotency)
                )
                == 0
            )
    finally:
        await engine.dispose()


async def count_creation_rows(
    sessions: async_sessionmaker[AsyncSession],
) -> tuple[int, int, int]:
    async with sessions() as session:
        counts = []
        for table in (workflow_runs, workflow_run_inputs, task_runs):
            value = await session.scalar(select(func.count()).select_from(table))
            counts.append(int(value or 0))
    return counts[0], counts[1], counts[2]


def test_workflow_run_creation_is_atomic_locked_and_complete() -> None:
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
        asyncio.run(verify_creation(database_url))
