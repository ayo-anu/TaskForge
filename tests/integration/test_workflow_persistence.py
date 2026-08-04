"""Opt-in workflow transaction verification against isolated PostgreSQL."""

from __future__ import annotations

import asyncio
import os
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import func, insert, select, update
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.identity.schema import api_principals
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.workflows import SQLAlchemyWorkflowRepository
from taskforge.workflows.domain import (
    DraftDependency,
    DraftWorkflowStep,
    WorkflowDefinitionStatus,
    WorkflowDraft,
)
from taskforge.workflows.schema import (
    workflow_definitions,
    workflow_draft_dependencies,
    workflow_draft_steps,
)
from taskforge.workflows.service import (
    WorkflowNotFound,
    WorkflowOwnerDisabled,
    WorkflowOwnerNotFound,
    WorkflowPersistenceConflict,
    WorkflowService,
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


def workflow(owner_id: UUID, *, workflow_id: UUID | None = None) -> WorkflowDraft:
    first = DraftWorkflowStep(uuid4(), "first", "test.task", {"value": 1})
    second = DraftWorkflowStep(
        uuid4(), "second", "test.task", {"nested": [True, None, "value"]}
    )
    return WorkflowDraft(
        id=workflow_id or uuid4(),
        owner_principal_id=owner_id,
        name="Persistent workflow",
        description="A transactional draft",
        status=WorkflowDefinitionStatus.DRAFT,
        steps=(first, second),
        dependencies=(DraftDependency(uuid4(), "first", "second"),),
    )


async def count_workflow_rows(
    sessions: async_sessionmaker[AsyncSession], workflow_id: UUID
) -> tuple[int, ...]:
    counts: list[int] = []
    async with sessions() as session:
        for table in (
            workflow_definitions,
            workflow_draft_steps,
            workflow_draft_dependencies,
        ):
            identifier = (
                table.c.workflow_definition_id
                if "workflow_definition_id" in table.c
                else table.c.id
            )
            count = await session.scalar(
                select(func.count()).select_from(table).where(identifier == workflow_id)
            )
            counts.append(int(count or 0))
    return tuple(counts)


async def verify_workflow_persistence(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    repository = SQLAlchemyWorkflowRepository(sessions)
    service = WorkflowService(repository)
    owner_id, other_owner_id, disabled_owner_id = uuid4(), uuid4(), uuid4()
    try:
        async with sessions.begin() as session:
            await session.execute(
                insert(api_principals),
                [
                    {
                        "id": owner_id,
                        "name": f"owner-{uuid4().hex}",
                        "disabled_at": None,
                    },
                    {
                        "id": other_owner_id,
                        "name": f"owner-{uuid4().hex}",
                        "disabled_at": None,
                    },
                    {
                        "id": disabled_owner_id,
                        "name": f"owner-{uuid4().hex}",
                        "disabled_at": None,
                    },
                ],
            )
            await session.execute(
                update(api_principals)
                .where(api_principals.c.id == disabled_owner_id)
                .values(disabled_at=func.current_timestamp())
            )

        created_input = workflow(owner_id)
        created = await service.create(created_input)
        found = await service.get(
            created_input.id,
            owner_principal_id=owner_id,
        )
        assert found == created
        assert found.draft.steps[0].identifier == "first"
        assert found.draft.steps[0].parameters == {"value": 1}
        assert found.draft.dependencies[0].predecessor_identifier == "first"
        assert found.created_at.tzinfo is not None
        assert found.updated_at.tzinfo is not None

        with pytest.raises(WorkflowNotFound):
            await service.get(
                created_input.id,
                owner_principal_id=other_owner_id,
            )
        owner_list = await service.list(owner_principal_id=owner_id, limit=10_000)
        other_list = await service.list(
            owner_principal_id=other_owner_id,
            limit=10_000,
        )
        assert [summary.id for summary in owner_list.items] == [created_input.id]
        assert other_list.items == ()

        invalid = workflow(owner_id)
        invalid = WorkflowDraft(
            id=invalid.id,
            owner_principal_id=invalid.owner_principal_id,
            name=invalid.name,
            description=invalid.description,
            status=invalid.status,
            steps=invalid.steps,
            dependencies=(DraftDependency(uuid4(), "first", "first"),),
        )
        with pytest.raises(WorkflowPersistenceConflict):
            await service.create(invalid)
        assert await count_workflow_rows(sessions, invalid.id) == (0, 0, 0)

        with pytest.raises(WorkflowOwnerNotFound):
            await service.create(workflow(uuid4()))
        with pytest.raises(WorkflowOwnerDisabled):
            await service.create(workflow(disabled_owner_id))

        concurrent_input = workflow(owner_id)
        outcomes = await asyncio.gather(
            service.create(concurrent_input),
            service.create(concurrent_input),
            return_exceptions=True,
        )
        assert sum(not isinstance(outcome, BaseException) for outcome in outcomes) == 1
        assert (
            sum(
                isinstance(outcome, WorkflowPersistenceConflict) for outcome in outcomes
            )
            == 1
        )
        assert await count_workflow_rows(sessions, concurrent_input.id) == (1, 2, 1)

        same_name = workflow(owner_id)
        await service.create(same_name)
        assert (
            len((await service.list(owner_principal_id=owner_id, limit=10_000)).items)
            == 3
        )

        first_page = await service.list(owner_principal_id=owner_id, limit=2)
        assert len(first_page.items) == 2
        assert first_page.next_cursor is not None
        initial_ids = {
            created_input.id,
            concurrent_input.id,
            same_name.id,
        }
        inserted_between_pages = workflow(owner_id)
        await service.create(inserted_between_pages)
        second_page = await service.list(
            owner_principal_id=owner_id,
            limit=2,
            cursor=first_page.next_cursor,
        )
        traversed_ids = {
            *(item.id for item in first_page.items),
            *(item.id for item in second_page.items),
        }
        assert traversed_ids == initial_ids
        assert inserted_between_pages.id not in traversed_ids
        assert second_page.next_cursor is None

    finally:
        await engine.dispose()


def test_real_workflow_create_read_list_rollback_and_concurrency() -> None:
    with temporary_database(
        "TASKFORGE_WORKFLOW_PERSISTENCE_TEST_DATABASE_URL",
        "taskforge_workflow_persistence",
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        with migration_database_url(alembic_url):
            command.upgrade(Config("alembic.ini"), "head")
        asyncio.run(verify_workflow_persistence(database_url))
