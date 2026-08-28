"""Opt-in workflow version resolution verification against PostgreSQL."""

from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import func, insert, select, update
from sqlalchemy.engine import URL

from taskforge.identity.authorization import OwnerFilter
from taskforge.identity.schema import api_principals
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.runs import SQLAlchemyWorkflowRunRepository
from taskforge.runs.domain import ExplicitWorkflowVersion, LatestWorkflowVersion
from taskforge.runs.schema import (
    task_runs,
    workflow_run_idempotency,
    workflow_run_inputs,
    workflow_runs,
)
from taskforge.runs.service import (
    WorkflowRunService,
    WorkflowRunTargetNotFound,
    WorkflowVersionUnavailable,
)
from taskforge.workflows.schema import workflow_definitions, workflow_versions
from tests.integration.postgresql import migration_database_url, temporary_database
from tests.integration.test_authentication_persistence import settings_for

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION=1 explicitly",
    ),
]


async def verify_resolution(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    service = WorkflowRunService(SQLAlchemyWorkflowRunRepository(sessions))
    owner_id, other_owner_id = uuid4(), uuid4()
    workflow_id, other_workflow_id = uuid4(), uuid4()
    version_one_id, version_two_id, other_version_id = uuid4(), uuid4(), uuid4()
    try:
        async with sessions.begin() as session:
            await session.execute(
                insert(api_principals),
                [
                    {"id": owner_id, "name": f"owner-{uuid4().hex}"},
                    {"id": other_owner_id, "name": f"owner-{uuid4().hex}"},
                ],
            )
            await session.execute(
                insert(workflow_definitions),
                [
                    {
                        "id": workflow_id,
                        "owner_principal_id": owner_id,
                        "name": "resolved",
                        "status": "enabled",
                    },
                    {
                        "id": other_workflow_id,
                        "owner_principal_id": other_owner_id,
                        "name": "other",
                        "status": "enabled",
                    },
                ],
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
                    {
                        "id": other_version_id,
                        "workflow_definition_id": other_workflow_id,
                        "version_number": 3,
                        "name": "other",
                    },
                ],
            )

        explicit = await service.resolve_version(
            workflow_id,
            owner_filter=OwnerFilter.only(owner_id),
            selection=ExplicitWorkflowVersion(1),
        )
        latest = await service.resolve_version(
            workflow_id,
            owner_filter=OwnerFilter.only(owner_id),
            selection=LatestWorkflowVersion(),
        )
        assert explicit.workflow_version_id == version_one_id
        assert latest.workflow_version_id == version_two_id
        assert latest.version_number == 2

        with pytest.raises(WorkflowRunTargetNotFound):
            await service.resolve_version(
                workflow_id,
                owner_filter=OwnerFilter.only(other_owner_id),
                selection=LatestWorkflowVersion(),
            )
        with pytest.raises(WorkflowVersionUnavailable):
            await service.resolve_version(
                workflow_id,
                owner_filter=OwnerFilter.only(owner_id),
                selection=ExplicitWorkflowVersion(3),
            )

        async with sessions.begin() as session:
            await session.execute(
                update(workflow_definitions)
                .where(workflow_definitions.c.id == workflow_id)
                .values(status="disabled")
            )
        from taskforge.runs.domain import WorkflowRunTargetUnavailable

        with pytest.raises(WorkflowRunTargetUnavailable):
            await service.resolve_version(
                workflow_id,
                owner_filter=OwnerFilter.only(owner_id),
                selection=LatestWorkflowVersion(),
            )

        async with sessions() as session:
            for table in (
                workflow_runs,
                workflow_run_inputs,
                task_runs,
                workflow_run_idempotency,
            ):
                count = await session.scalar(select(func.count()).select_from(table))
                assert count == 0
    finally:
        await engine.dispose()


def test_workflow_version_resolution_is_owner_scoped_and_read_only() -> None:
    with temporary_database(
        "TASKFORGE_WORKFLOW_PERSISTENCE_TEST_DATABASE_URL",
        "taskforge_version_resolution",
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        configuration = Config("alembic.ini")
        with migration_database_url(alembic_url):
            command.upgrade(configuration, "head")
        asyncio.run(verify_resolution(database_url))
