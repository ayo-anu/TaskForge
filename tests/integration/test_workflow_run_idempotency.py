"""Opt-in workflow run idempotency verification against PostgreSQL."""

from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import func, insert, select, update
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.identity.authorization import OwnerFilter
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.runs import SQLAlchemyWorkflowRunRepository
from taskforge.runs.domain import (
    LatestWorkflowVersion,
    WorkflowRunIdempotencyConflict,
    WorkflowRunTargetUnavailable,
    create_workflow_run_idempotency,
    create_workflow_run_input,
)
from taskforge.runs.schema import (
    task_runs,
    workflow_run_idempotency,
    workflow_run_inputs,
    workflow_runs,
)
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

KEY = "abcdefghijklmnop"


async def creation_counts(
    sessions: async_sessionmaker[AsyncSession],
) -> tuple[int, int, int, int]:
    async with sessions() as session:
        values = []
        for table in (
            workflow_runs,
            workflow_run_inputs,
            task_runs,
            workflow_run_idempotency,
        ):
            count = await session.scalar(select(func.count()).select_from(table))
            values.append(int(count or 0))
    return values[0], values[1], values[2], values[3]


async def verify_idempotency(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    service = WorkflowRunService(SQLAlchemyWorkflowRunRepository(sessions))
    try:
        owner_id, _, workflow_id, _, version_two_id = await seed_workflow(sessions)
        accepted = create_workflow_run_input(
            {"value": 1}, {"artifact": {"kind": "object"}}
        )
        first = await service.create_idempotent_run(
            workflow_id,
            owner_filter=OwnerFilter.only(owner_id),
            requested_by_principal_id=owner_id,
            selection=LatestWorkflowVersion(),
            input_snapshot=accepted,
            idempotency_key=KEY,
        )
        replay = await service.create_idempotent_run(
            workflow_id,
            owner_filter=OwnerFilter.only(owner_id),
            requested_by_principal_id=owner_id,
            selection=LatestWorkflowVersion(),
            input_snapshot=accepted,
            idempotency_key=KEY,
        )
        assert replay == first
        assert first.workflow_version_id == version_two_id
        assert await creation_counts(sessions) == (1, 1, 3, 1)

        with pytest.raises(WorkflowRunIdempotencyConflict):
            await service.create_idempotent_run(
                workflow_id,
                owner_filter=OwnerFilter.only(owner_id),
                requested_by_principal_id=owner_id,
                selection=LatestWorkflowVersion(),
                input_snapshot=create_workflow_run_input({"value": 2}, {}),
                idempotency_key=KEY,
            )
        assert await creation_counts(sessions) == (1, 1, 3, 1)

        async with sessions.begin() as session:
            version_three_id = uuid4()
            await session.execute(
                insert(workflow_versions).values(
                    id=version_three_id,
                    workflow_definition_id=workflow_id,
                    version_number=3,
                    name="three",
                )
            )
            await session.execute(
                insert(workflow_version_steps).values(
                    workflow_version_id=version_three_id,
                    step_identifier="new-root",
                    task_type="test.task",
                    parameters={},
                )
            )
        after_publication = await service.create_idempotent_run(
            workflow_id,
            owner_filter=OwnerFilter.only(owner_id),
            requested_by_principal_id=owner_id,
            selection=LatestWorkflowVersion(),
            input_snapshot=accepted,
            idempotency_key=KEY,
        )
        assert after_publication.id == first.id
        assert after_publication.workflow_version_id == version_two_id

        async with sessions.begin() as session:
            await session.execute(
                update(workflow_definitions)
                .where(workflow_definitions.c.id == workflow_id)
                .values(status="disabled")
            )
        after_disable = await service.create_idempotent_run(
            workflow_id,
            owner_filter=OwnerFilter.only(owner_id),
            requested_by_principal_id=owner_id,
            selection=LatestWorkflowVersion(),
            input_snapshot=accepted,
            idempotency_key=KEY,
        )
        assert after_disable.id == first.id
        with pytest.raises(WorkflowRunTargetUnavailable):
            await service.create_idempotent_run(
                workflow_id,
                owner_filter=OwnerFilter.only(owner_id),
                requested_by_principal_id=owner_id,
                selection=LatestWorkflowVersion(),
                input_snapshot=accepted,
                idempotency_key="different-key-123",
            )

        async with sessions.begin() as session:
            await session.execute(
                update(workflow_definitions)
                .where(workflow_definitions.c.id == workflow_id)
                .values(status="enabled")
            )
        identical = await asyncio.gather(
            *(
                service.create_idempotent_run(
                    workflow_id,
                    owner_filter=OwnerFilter.only(owner_id),
                    requested_by_principal_id=owner_id,
                    selection=LatestWorkflowVersion(),
                    input_snapshot=create_workflow_run_input({}, {}),
                    idempotency_key="concurrent-key-1",
                )
                for _ in range(2)
            )
        )
        assert identical[0].id == identical[1].id
        assert await creation_counts(sessions) == (2, 2, 4, 2)

        outcomes = await asyncio.gather(
            service.create_idempotent_run(
                workflow_id,
                owner_filter=OwnerFilter.only(owner_id),
                requested_by_principal_id=owner_id,
                selection=LatestWorkflowVersion(),
                input_snapshot=create_workflow_run_input({"side": "a"}, {}),
                idempotency_key="concurrent-key-2",
            ),
            service.create_idempotent_run(
                workflow_id,
                owner_filter=OwnerFilter.only(owner_id),
                requested_by_principal_id=owner_id,
                selection=LatestWorkflowVersion(),
                input_snapshot=create_workflow_run_input({"side": "b"}, {}),
                idempotency_key="concurrent-key-2",
            ),
            return_exceptions=True,
        )
        assert (
            sum(isinstance(value, WorkflowRunIdempotencyConflict) for value in outcomes)
            == 1
        )
        assert await creation_counts(sessions) == (3, 3, 5, 3)

        expected = create_workflow_run_idempotency(
            KEY,
            workflow_definition_id=workflow_id,
            requested_by_principal_id=owner_id,
            selection=LatestWorkflowVersion(),
            input_snapshot=accepted,
        )
        async with sessions() as session:
            stored = (
                await session.execute(
                    select(workflow_run_idempotency).where(
                        workflow_run_idempotency.c.workflow_run_id == first.id
                    )
                )
            ).one()
            assert stored.idempotency_key_digest == expected.key_digest
            assert stored.request_fingerprint == expected.request_fingerprint
            assert KEY not in stored.idempotency_key_digest
    finally:
        await engine.dispose()


def test_workflow_run_idempotency_is_atomic_scoped_and_concurrent() -> None:
    with temporary_database(
        "TASKFORGE_WORKFLOW_PERSISTENCE_TEST_DATABASE_URL",
        "taskforge_run_idempotency",
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        configuration = Config("alembic.ini")
        with migration_database_url(alembic_url):
            command.upgrade(configuration, "head")
        asyncio.run(verify_idempotency(database_url))
