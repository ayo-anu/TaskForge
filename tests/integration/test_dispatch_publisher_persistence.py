"""Opt-in dispatch publisher persistence verification against PostgreSQL."""

from __future__ import annotations

import asyncio
import os

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import delete, select, update
from sqlalchemy.engine import URL

from taskforge.dispatch.publisher_ports import (
    DispatchPublicationInvariantConflict,
    PublicationAcknowledgement,
)
from taskforge.dispatch.service import TaskDispatchService
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.dispatch import (
    SQLAlchemyDispatchOutboxRepository,
    SQLAlchemyTaskDispatchRepository,
)
from taskforge.runs.schema import task_dispatch_outbox
from taskforge.workflows.task_types import TaskTypeDefinition, TaskTypeRegistry
from tests.integration.postgresql import migration_database_url, temporary_database
from tests.integration.test_authentication_persistence import settings_for
from tests.integration.test_task_dispatch_creation import (
    AcceptParameters,
    seed_runnable_task,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION=1 explicitly",
    ),
]


async def verify_publisher_persistence(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    registry = TaskTypeRegistry(
        (
            TaskTypeDefinition(
                "document.extract", "document-workers", AcceptParameters()
            ),
        )
    )
    dispatch_service = TaskDispatchService(
        SQLAlchemyTaskDispatchRepository(sessions), registry
    )
    repository = SQLAlchemyDispatchOutboxRepository(sessions)
    try:
        for _ in range(4):
            workflow_run_id, task_run_id, _ = await seed_runnable_task(sessions)
            await dispatch_service.dispatch_task(workflow_run_id, task_run_id)

        first_page = await repository.list_unpublished_page(after=None, limit=2)
        second_page = await repository.list_unpublished_page(
            after=first_page[-1].cursor, limit=2
        )
        ordered = (*first_page, *second_page)
        assert len(ordered) == 4
        assert [item.cursor for item in ordered] == sorted(
            (item.cursor for item in ordered),
            key=lambda cursor: (cursor.created_at, cursor.dispatch_id),
        )
        capped = await repository.observe_unpublished_backlog(limit=2)
        assert capped.pending == 2
        assert capped.saturated is True
        assert capped.oldest_created_at == ordered[0].created_at
        assert capped.observed_at.tzinfo is not None

        concurrent = await asyncio.gather(
            repository.record_accepted_publication(ordered[0]),
            repository.record_accepted_publication(ordered[0]),
        )
        assert set(concurrent) == {
            PublicationAcknowledgement.RECORDED,
            PublicationAcknowledgement.ALREADY_RECORDED,
        }
        async with sessions() as session:
            published_at = await session.scalar(
                select(task_dispatch_outbox.c.published_at).where(
                    task_dispatch_outbox.c.id == ordered[0].dispatch_id
                )
            )
        assert published_at is not None and published_at.tzinfo is not None

        exact = await repository.observe_unpublished_backlog(limit=10)
        assert exact.pending == 3
        assert exact.saturated is False
        assert exact.oldest_created_at == ordered[1].created_at

        restarted = SQLAlchemyDispatchOutboxRepository(sessions)
        remaining = await restarted.list_unpublished_page(after=None, limit=10)
        assert [item.dispatch_id for item in remaining] == [
            item.dispatch_id for item in ordered[1:]
        ]

        stale = remaining[0]
        async with sessions.begin() as session:
            await session.execute(
                update(task_dispatch_outbox)
                .where(task_dispatch_outbox.c.id == stale.dispatch_id)
                .values(route="capability.changed")
            )
        with pytest.raises(DispatchPublicationInvariantConflict):
            await repository.record_accepted_publication(stale)

        missing = remaining[1]
        async with sessions.begin() as session:
            await session.execute(
                delete(task_dispatch_outbox).where(
                    task_dispatch_outbox.c.id == missing.dispatch_id
                )
            )
        with pytest.raises(DispatchPublicationInvariantConflict):
            await repository.record_accepted_publication(missing)
    finally:
        await engine.dispose()


def test_dispatch_publisher_persistence_is_restartable_and_exact() -> None:
    with temporary_database(
        "TASKFORGE_WORKFLOW_PERSISTENCE_TEST_DATABASE_URL",
        "taskforge_task_dispatch",
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        configuration = Config("alembic.ini")
        with migration_database_url(alembic_url):
            command.upgrade(configuration, "head")
        asyncio.run(verify_publisher_persistence(database_url))
