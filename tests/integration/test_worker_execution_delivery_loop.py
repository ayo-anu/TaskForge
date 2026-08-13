"""Real PostgreSQL/RabbitMQ worker result-to-ack boundary tests."""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

import aio_pika
import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from taskforge.broker.consumer import (
    RabbitMQDispatchConsumer,
    RabbitMQDispatchDelivery,
)
from taskforge.claims.authority import TaskClaimResultAuthorityIssuer
from taskforge.claims.service import TaskClaimService
from taskforge.dispatch.envelope import (
    DispatchEnvelope,
    serialize_dispatch_envelope,
)
from taskforge.persistence.claims import SQLAlchemyTaskClaimRepository
from taskforge.persistence.database import build_session_factory
from taskforge.persistence.task_results import SQLAlchemyTaskResultRepository
from taskforge.persistence.task_start import SQLAlchemyTaskStartRepository
from taskforge.worker.consumer_ports import (
    BrokerConsumerUnavailable,
    BrokerDispatchDelivery,
    DispatchDeliveryControl,
)
from taskforge.worker.execution import WorkerExecutionConsumer
from taskforge.worker.handlers import (
    TaskContext,
    TaskHandlerDefinition,
    TaskHandlerRegistry,
)
from taskforge.worker.result_submission import TaskResultSubmissionService
from taskforge.worker.start import TaskStartService
from taskforge.workflows.task_types import (
    JSONMapping,
    TaskTypeDefinition,
    TaskTypeRegistry,
    WorkflowValidationIssue,
)
from tests.integration.postgresql import (
    asyncpg_dsn,
    migration_database_url,
    temporary_database,
)
from tests.integration.test_task_claim_acquisition import (
    WorkerFacts,
    add_dispatched_task,
    add_worker,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_CLAIM_INTEGRATION") != "1"
        or os.getenv("TASKFORGE_RUN_BROKER_INTEGRATION") != "1",
        reason="enable both PostgreSQL claim and RabbitMQ integration explicitly",
    ),
]

_AUTHORITY_SECRET = b"task5-delivery-loop-authority-secret"


@dataclass(frozen=True)
class AcceptParameters:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        del parameters
        return ()


class CommitObservingControl:
    def __init__(
        self,
        delegate: DispatchDeliveryControl,
        database_url: URL,
        task_attempt_id: UUID,
        *,
        fail_ack: bool = False,
    ) -> None:
        self._delegate = delegate
        self._database_url = database_url
        self._task_attempt_id = task_attempt_id
        self._fail_ack = fail_ack
        self.ack_attempted = asyncio.Event()
        self.ack_count = 0

    @property
    def delivery(self) -> BrokerDispatchDelivery:
        return self._delegate.delivery

    async def acknowledge(self) -> None:
        connection = await asyncpg.connect(asyncpg_dsn(self._database_url))
        try:
            state = await connection.fetchrow(
                "SELECT tr.status::text, tac.terminated_at IS NOT NULL, "
                "EXISTS (SELECT FROM task_attempt_results tar WHERE "
                "tar.task_attempt_id = ta.id) "
                "FROM task_runs tr JOIN task_attempts ta ON ta.task_run_id = tr.id "
                "JOIN task_attempt_claims tac ON tac.task_attempt_id = ta.id "
                "WHERE ta.id = $1",
                self._task_attempt_id,
            )
            assert tuple(state) == ("succeeded", True, True)
        finally:
            await connection.close()
        self.ack_count += 1
        self.ack_attempted.set()
        if self._fail_ack:
            raise BrokerConsumerUnavailable
        await self._delegate.acknowledge()

    async def reject(self, *, requeue: bool) -> None:
        await self._delegate.reject(requeue=requeue)


def registry(handler: Any) -> TaskHandlerRegistry:
    task_types = TaskTypeRegistry(
        (TaskTypeDefinition("test.task", "test-capability", AcceptParameters()),)
    )
    return TaskHandlerRegistry(
        (TaskHandlerDefinition("test.task", "test-capability", handler),), task_types
    )


def execution_consumer(
    sessions: Any,
    worker: WorkerFacts,
    handler: Any,
) -> WorkerExecutionConsumer:
    issuer = TaskClaimResultAuthorityIssuer(_AUTHORITY_SECRET)
    return WorkerExecutionConsumer(
        TaskClaimService(
            SQLAlchemyTaskClaimRepository(sessions, worker_stale_after_seconds=30),
            issuer,
            lease_seconds=60,
        ),
        TaskStartService(SQLAlchemyTaskStartRepository(sessions)),
        TaskResultSubmissionService(SQLAlchemyTaskResultRepository(sessions), issuer),
        registry(handler),
        worker.authenticated,
        worker.session_id,
    )


async def publish(exchange: Any, dispatch: DispatchEnvelope) -> bytes:
    body = serialize_dispatch_envelope(dispatch)
    await exchange.publish(
        aio_pika.Message(
            body,
            content_type="application/json",
            content_encoding="utf-8",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=str(dispatch.dispatch_id),
        ),
        routing_key=dispatch.route,
    )
    return body


async def exercise(database_url: URL, amqp_url: str) -> None:
    setup = await asyncpg.connect(asyncpg_dsn(database_url))
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
    )
    sessions = build_session_factory(engine)
    connection = await aio_pika.connect(amqp_url)
    suffix = uuid4().hex
    exchange_name = f"taskforge.worker.task5.exchange.{suffix}"
    queue_name = f"taskforge.worker.task5.queue.{suffix}"
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)
    exchange = await channel.declare_exchange(
        exchange_name, aio_pika.ExchangeType.DIRECT, durable=True, auto_delete=False
    )
    queue = await channel.declare_queue(queue_name, durable=True, auto_delete=False)
    await queue.bind(exchange, routing_key="capability.test-capability")
    invocations = 0

    async def handler(context: TaskContext) -> object:
        nonlocal invocations
        del context
        invocations += 1
        return {"ok": True}

    worker = await add_worker(setup)
    coordinator = execution_consumer(sessions, worker, handler)
    try:
        successful = await add_dispatched_task(setup)
        await publish(exchange, successful)
        success_complete = asyncio.Event()
        success_error: list[BaseException] = []
        observed_success: list[CommitObservingControl] = []

        async def consume_success(control: DispatchDeliveryControl) -> None:
            observed = CommitObservingControl(
                control, database_url, successful.task_attempt_id
            )
            observed_success.append(observed)
            try:
                await coordinator.consume(observed)
            except BaseException as error:
                success_error.append(error)
            finally:
                success_complete.set()

        broker_consumer = RabbitMQDispatchConsumer(queue)
        success_tag = await broker_consumer.consume(consume_success)
        await asyncio.wait_for(success_complete.wait(), timeout=5)
        assert success_error == []
        assert observed_success[0].ack_count == 1
        await broker_consumer.cancel(success_tag)
        assert await queue.get(fail=False, timeout=0.5) is None

        crash = await add_dispatched_task(setup)
        await publish(exchange, crash)
        commit_before_ack = asyncio.Event()
        crash_error: list[BaseException] = []

        async def consume_without_ack(control: DispatchDeliveryControl) -> None:
            observed = CommitObservingControl(
                control, database_url, crash.task_attempt_id, fail_ack=True
            )
            try:
                await coordinator.consume(observed)
            except BaseException as error:
                crash_error.append(error)
            finally:
                commit_before_ack.set()

        crash_tag = await broker_consumer.consume(consume_without_ack)
        await asyncio.wait_for(commit_before_ack.wait(), timeout=5)
        assert len(crash_error) == 1
        assert isinstance(crash_error[0], BrokerConsumerUnavailable)
        await channel.close()

        redelivery_channel = await connection.channel()
        redelivery_queue = await redelivery_channel.get_queue(queue_name)
        redelivered = await redelivery_queue.get(timeout=5)
        assert redelivered is not None
        assert redelivered.redelivered
        await coordinator.consume(RabbitMQDispatchDelivery(redelivered))
        assert invocations == 2
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM task_attempt_results WHERE task_attempt_id = $1",
                crash.task_attempt_id,
            )
            == 1
        )
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM task_result_events WHERE task_attempt_id = $1",
                crash.task_attempt_id,
            )
            == 1
        )
        assert await redelivery_queue.get(fail=False, timeout=0.5) is None
        del crash_tag
    finally:
        if not connection.is_closed:
            cleanup = await connection.channel()
            await cleanup.queue_delete(queue_name, if_unused=False, if_empty=False)
            await cleanup.exchange_delete(exchange_name)
            await connection.close()
        await setup.close()
        await engine.dispose()


def test_real_result_commit_ack_and_crash_window_redelivery() -> None:
    amqp_url = os.getenv("TASKFORGE_BROKER_TEST_AMQP_URL")
    if not amqp_url:
        pytest.fail("TASKFORGE_BROKER_TEST_AMQP_URL is required")
    with temporary_database(
        "TASKFORGE_CLAIM_TEST_DATABASE_URL", "taskforge_task5_delivery_loop"
    ) as database_url:
        configuration = Config("alembic.ini")
        with migration_database_url(database_url.render_as_string(hide_password=False)):
            command.upgrade(configuration, "head")
        asyncio.run(exercise(database_url, amqp_url))
