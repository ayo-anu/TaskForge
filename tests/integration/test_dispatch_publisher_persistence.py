"""Opt-in dispatch publisher persistence verification against PostgreSQL."""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import delete, func, select, update
from sqlalchemy.engine import URL

from taskforge.claims.authority import TaskClaimResultAuthorityIssuer
from taskforge.claims.domain import (
    TaskClaimOutcome,
    TaskClaimRejected,
    TaskClaimRejectionReason,
)
from taskforge.claims.service import TaskClaimService
from taskforge.dispatch.envelope import deserialize_dispatch_envelope
from taskforge.dispatch.publisher import TaskDispatchPublisher
from taskforge.dispatch.publisher_ports import (
    BrokerDispatchPublication,
    DispatchPublicationInvariantConflict,
    PublicationAcknowledgement,
)
from taskforge.dispatch.service import TaskDispatchService
from taskforge.orchestrator.workloads import OutboxPublicationWorkload
from taskforge.persistence.claims import SQLAlchemyTaskClaimRepository
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.dispatch import (
    SQLAlchemyDispatchOutboxRepository,
    SQLAlchemyTaskDispatchRepository,
)
from taskforge.runs.schema import (
    task_attempt_claims,
    task_attempt_results,
    task_attempts,
    task_claim_events,
    task_dispatch_outbox,
    task_runs,
)
from taskforge.workflows.task_types import TaskTypeDefinition, TaskTypeRegistry
from tests.integration.postgresql import (
    asyncpg_dsn,
    migration_database_url,
    temporary_database,
)
from tests.integration.test_authentication_persistence import settings_for
from tests.integration.test_task_claim_acquisition import add_worker
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


@dataclass
class RecordingBroker:
    publications: list[BrokerDispatchPublication] = field(default_factory=list)

    async def publish(self, publication: BrokerDispatchPublication) -> None:
        self.publications.append(publication)


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

        startup_high_water = await repository.capture_startup_replay_high_water()
        assert startup_high_water is not None
        assert startup_high_water.cursor == ordered[0].cursor
        assert startup_high_water.captured_at.tzinfo is not None
        assert (
            await repository.record_accepted_publication(ordered[1])
            is PublicationAcknowledgement.RECORDED
        )
        startup_page = await repository.list_startup_replay_page(
            high_water=startup_high_water,
            after=None,
            limit=10,
        )
        assert [item.dispatch_id for item in startup_page.records] == [
            ordered[0].dispatch_id
        ]
        assert startup_page.next_cursor is None

        exact = await repository.observe_unpublished_backlog(limit=10)
        assert exact.pending == 2
        assert exact.saturated is False
        assert exact.oldest_created_at == ordered[2].created_at

        restarted = SQLAlchemyDispatchOutboxRepository(sessions)
        remaining = await restarted.list_unpublished_page(after=None, limit=10)
        assert [item.dispatch_id for item in remaining] == [
            item.dispatch_id for item in ordered[2:]
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


async def verify_multiple_startup_replayers(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    registry = TaskTypeRegistry(
        (
            TaskTypeDefinition(
                "document.extract", "document-workers", AcceptParameters()
            ),
        )
    )
    repository = SQLAlchemyDispatchOutboxRepository(sessions)
    dispatch_service = TaskDispatchService(
        SQLAlchemyTaskDispatchRepository(sessions), registry
    )
    try:
        workflow_run_id, task_run_id, _ = await seed_runnable_task(sessions)
        await dispatch_service.dispatch_task(workflow_run_id, task_run_id)
        records = await repository.list_unpublished_page(after=None, limit=10)
        assert len(records) == 1
        stored = records[0]
        assert (
            await repository.record_accepted_publication(stored)
            is PublicationAcknowledgement.RECORDED
        )
        async with sessions() as session:
            published_at = await session.scalar(
                select(task_dispatch_outbox.c.published_at).where(
                    task_dispatch_outbox.c.id == stored.dispatch_id
                )
            )
            baseline_attempts = await session.scalar(
                select(func.count(task_attempts.c.id)).where(
                    task_attempts.c.task_run_id == task_run_id
                )
            )
            baseline_outbox = await session.scalar(
                select(func.count(task_dispatch_outbox.c.id)).where(
                    task_dispatch_outbox.c.task_attempt_id == stored.task_attempt_id
                )
            )
        assert published_at is not None
        assert baseline_attempts == baseline_outbox == 1

        broker_a, broker_b = RecordingBroker(), RecordingBroker()
        workload_a = OutboxPublicationWorkload(
            TaskDispatchPublisher(repository, broker_a), batch_size=10
        )
        workload_b = OutboxPublicationWorkload(
            TaskDispatchPublisher(repository, broker_b), batch_size=10
        )
        first_a, first_b = await asyncio.gather(
            workload_a.run_once(), workload_b.run_once()
        )
        assert first_a.candidates == first_a.transitions == 1
        assert first_b.candidates == first_b.transitions == 1
        second_a, second_b = await workload_a.run_once(), await workload_b.run_once()
        assert second_a.candidates == second_a.transitions == 0
        assert second_b.candidates == second_b.transitions == 0
        assert [item.dispatch_id for item in broker_a.publications] == [
            stored.dispatch_id
        ]
        assert [item.dispatch_id for item in broker_b.publications] == [
            stored.dispatch_id
        ]
        assert broker_a.publications[0] == broker_b.publications[0]

        async with sessions() as session:
            assert (
                await session.scalar(
                    select(task_dispatch_outbox.c.published_at).where(
                        task_dispatch_outbox.c.id == stored.dispatch_id
                    )
                )
                == published_at
            )
            assert (
                await session.scalar(
                    select(func.count(task_attempts.c.id)).where(
                        task_attempts.c.task_run_id == task_run_id
                    )
                )
                == baseline_attempts
            )
            assert (
                await session.scalar(
                    select(func.count(task_dispatch_outbox.c.id)).where(
                        task_dispatch_outbox.c.task_attempt_id == stored.task_attempt_id
                    )
                )
                == baseline_outbox
            )
            assert (
                await session.scalar(
                    select(func.count(task_attempt_claims.c.task_attempt_id)).where(
                        task_attempt_claims.c.task_attempt_id == stored.task_attempt_id
                    )
                )
                == 0
            )
            assert (
                await session.scalar(
                    select(func.count(task_attempt_results.c.task_attempt_id)).where(
                        task_attempt_results.c.task_attempt_id == stored.task_attempt_id
                    )
                )
                == 0
            )
            assert (
                await session.scalar(
                    select(task_runs.c.status).where(task_runs.c.id == task_run_id)
                )
                == "dispatched"
            )

        # Duplicate transport deliveries still pass through the ordinary
        # claim/idempotency boundary: one authority is issued, its replay is
        # stable, and a different worker cannot acquire competing authority.
        connection = await asyncpg.connect(asyncpg_dsn(database_url))
        try:
            worker_a = await add_worker(connection, capability="document-workers")
            worker_b = await add_worker(connection, capability="document-workers")
        finally:
            await connection.close()
        claims = TaskClaimService(
            SQLAlchemyTaskClaimRepository(sessions, worker_stale_after_seconds=30),
            TaskClaimResultAuthorityIssuer(b"startup-replay-claim-authority-secret"),
            lease_seconds=30,
        )
        dispatch = deserialize_dispatch_envelope(broker_a.publications[0].body)
        acquired = await claims.claim_task(
            worker_a.authenticated, worker_a.session_id, dispatch
        )
        replayed = await claims.claim_task(
            worker_a.authenticated, worker_a.session_id, dispatch
        )
        assert acquired.outcome is TaskClaimOutcome.ACQUIRED_ACTIVE
        assert replayed.outcome is TaskClaimOutcome.REPLAYED_ACTIVE
        assert replayed.claim.generation == acquired.claim.generation
        with pytest.raises(TaskClaimRejected) as rejected:
            await claims.claim_task(
                worker_b.authenticated, worker_b.session_id, dispatch
            )
        assert rejected.value.reason is TaskClaimRejectionReason.ALREADY_AUTHORITATIVE
        async with sessions() as session:
            assert (
                await session.scalar(
                    select(func.count(task_attempt_claims.c.task_attempt_id)).where(
                        task_attempt_claims.c.task_attempt_id == stored.task_attempt_id,
                        task_attempt_claims.c.terminated_at.is_(None),
                    )
                )
                == 1
            )
            assert (
                await session.scalar(
                    select(func.count(task_claim_events.c.id)).where(
                        task_claim_events.c.task_attempt_id == stored.task_attempt_id,
                        task_claim_events.c.event_type == "claim_acquired",
                    )
                )
                == 1
            )
    finally:
        await engine.dispose()


def test_multiple_startup_replayers_preserve_durable_dispatch_identity() -> None:
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
        asyncio.run(verify_multiple_startup_replayers(database_url))
