"""Opt-in real RabbitMQ and PostgreSQL dispatch publication verification."""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from uuid import UUID, uuid4

import aio_pika
import pytest
from aio_pika.abc import AbstractIncomingMessage, AbstractQueue
from alembic import command
from alembic.config import Config
from sqlalchemy import select
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.broker.rabbitmq import RabbitMQDispatchPublisher
from taskforge.dispatch.publisher import TaskDispatchPublisher
from taskforge.dispatch.publisher_ports import (
    BrokerDispatchPublication,
    BrokerUnavailable,
    DispatchAcknowledgementPersistenceFailure,
    DispatchBrokerPublisher,
    DispatchOutboxRepository,
    PublicationAcknowledgement,
    StoredDispatch,
    UnpublishedDispatchCursor,
)
from taskforge.dispatch.service import DispatchedTask, TaskDispatchService
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
        os.getenv("TASKFORGE_RUN_BROKER_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_BROKER_INTEGRATION=1 explicitly",
    ),
]


@dataclass
class FailOnceAcknowledgementRepository:
    delegate: DispatchOutboxRepository
    failed: bool = False

    async def list_unpublished_page(
        self, *, after: UnpublishedDispatchCursor | None, limit: int
    ) -> tuple[StoredDispatch, ...]:
        return await self.delegate.list_unpublished_page(after=after, limit=limit)

    async def record_accepted_publication(
        self, expected: StoredDispatch
    ) -> PublicationAcknowledgement:
        if not self.failed:
            self.failed = True
            raise DispatchAcknowledgementPersistenceFailure
        return await self.delegate.record_accepted_publication(expected)


class TwoPublisherBarrier:
    def __init__(self, delegate: DispatchBrokerPublisher) -> None:
        self._delegate = delegate
        self._arrived = 0
        self._ready = asyncio.Event()

    async def publish(self, publication: BrokerDispatchPublication) -> None:
        self._arrived += 1
        if self._arrived == 2:
            self._ready.set()
        await asyncio.wait_for(self._ready.wait(), timeout=2)
        await self._delegate.publish(publication)


async def create_dispatch(
    service: TaskDispatchService,
    sessions: async_sessionmaker[AsyncSession],
) -> DispatchedTask:
    workflow_run_id, task_run_id, _ = await seed_runnable_task(sessions)
    return await service.dispatch_task(workflow_run_id, task_run_id)


async def get_message(queue: AbstractQueue) -> AbstractIncomingMessage:
    message = await queue.get(timeout=3, fail=True)
    assert message is not None
    await message.ack()
    return message


async def assert_unpublished(
    sessions: async_sessionmaker[AsyncSession], dispatch_id: UUID
) -> None:
    async with sessions() as session:
        published_at = await session.scalar(
            select(task_dispatch_outbox.c.published_at).where(
                task_dispatch_outbox.c.id == dispatch_id
            )
        )
    assert published_at is None


async def verify_broker_publication(database_url: URL, amqp_url: str) -> None:
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
    outbox = SQLAlchemyDispatchOutboxRepository(sessions)
    topology_connection = await aio_pika.connect_robust(amqp_url)
    publisher_connection = await aio_pika.connect(amqp_url)
    exchange_name = f"taskforge.dispatch.test.{uuid4().hex}"
    try:
        topology_channel = await topology_connection.channel()
        exchange = await topology_channel.declare_exchange(
            exchange_name, aio_pika.ExchangeType.DIRECT, auto_delete=True
        )
        queue = await topology_channel.declare_queue(exclusive=True, auto_delete=True)
        await queue.bind(exchange, routing_key="capability.document-workers")

        publisher_channel = await publisher_connection.channel(
            publisher_confirms=True, on_return_raises=True
        )
        publisher_exchange = await publisher_channel.get_exchange(exchange_name)
        rabbit = RabbitMQDispatchPublisher(publisher_exchange, timeout_seconds=2)

        first = await create_dispatch(dispatch_service, sessions)
        result = await TaskDispatchPublisher(outbox, rabbit).reconcile_unpublished(
            page_size=10, pass_limit=10
        )
        message = await get_message(queue)
        assert result.acknowledged == 1
        assert message.message_id == str(first.dispatch_id)
        assert str(first.dispatch_id).encode() in message.body

        crash_window = await create_dispatch(dispatch_service, sessions)
        failing = FailOnceAcknowledgementRepository(outbox)
        with pytest.raises(DispatchAcknowledgementPersistenceFailure):
            await TaskDispatchPublisher(failing, rabbit).reconcile_unpublished(
                page_size=10, pass_limit=10
            )
        first_copy = await get_message(queue)
        assert first_copy.message_id == str(crash_window.dispatch_id)
        await assert_unpublished(sessions, crash_window.dispatch_id)

        restarted = TaskDispatchPublisher(
            SQLAlchemyDispatchOutboxRepository(sessions), rabbit
        )
        recovered = await restarted.reconcile_unpublished(page_size=10, pass_limit=10)
        second_copy = await get_message(queue)
        assert recovered.acknowledged == 1
        assert second_copy.message_id == first_copy.message_id

        concurrent = await create_dispatch(dispatch_service, sessions)
        barrier = TwoPublisherBarrier(rabbit)
        outcomes = await asyncio.gather(
            TaskDispatchPublisher(outbox, barrier).reconcile_unpublished(
                page_size=10, pass_limit=10
            ),
            TaskDispatchPublisher(outbox, barrier).reconcile_unpublished(
                page_size=10, pass_limit=10
            ),
        )
        duplicate_messages = [await get_message(queue), await get_message(queue)]
        assert {message.message_id for message in duplicate_messages} == {
            str(concurrent.dispatch_id)
        }
        assert sum(outcome.acknowledged for outcome in outcomes) == 1
        assert sum(outcome.already_acknowledged for outcome in outcomes) == 1

        unavailable = await create_dispatch(dispatch_service, sessions)
        await publisher_connection.close()
        with pytest.raises(BrokerUnavailable):
            await TaskDispatchPublisher(outbox, rabbit).reconcile_unpublished(
                page_size=10, pass_limit=10
            )
        await assert_unpublished(sessions, unavailable.dispatch_id)

        recovery_connection = await aio_pika.connect(amqp_url)
        try:
            recovery_channel = await recovery_connection.channel(
                publisher_confirms=True, on_return_raises=True
            )
            recovery_exchange = await recovery_channel.get_exchange(exchange_name)
            recovery_broker = RabbitMQDispatchPublisher(
                recovery_exchange, timeout_seconds=2
            )
            await TaskDispatchPublisher(outbox, recovery_broker).reconcile_unpublished(
                page_size=10, pass_limit=10
            )
            recovered_message = await get_message(queue)
            assert recovered_message.message_id == str(unavailable.dispatch_id)
        finally:
            await recovery_connection.close()
    finally:
        if not publisher_connection.is_closed:
            await publisher_connection.close()
        await topology_connection.close()
        await engine.dispose()


def test_real_broker_publication_restart_and_duplicates() -> None:
    amqp_url = os.getenv("TASKFORGE_BROKER_TEST_AMQP_URL")
    if not amqp_url:
        pytest.fail("TASKFORGE_BROKER_TEST_AMQP_URL is required")
    with temporary_database(
        "TASKFORGE_BROKER_TEST_DATABASE_URL",
        "taskforge_broker_dispatch",
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        configuration = Config("alembic.ini")
        with migration_database_url(alembic_url):
            command.upgrade(configuration, "head")
        asyncio.run(verify_broker_publication(database_url, amqp_url))
