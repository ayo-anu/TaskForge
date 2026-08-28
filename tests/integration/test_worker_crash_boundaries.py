"""Abrupt worker-process crash tests across durable execution boundaries."""

from __future__ import annotations

import asyncio
import multiprocessing
import os
from dataclasses import dataclass
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

import aio_pika
import asyncpg
import pytest
from aio_pika.abc import AbstractExchange
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import create_async_engine

import taskforge.claims.authority as claim_authority
from taskforge.broker.consumer import RabbitMQDispatchConsumer
from taskforge.broker.rabbitmq import RabbitMQDispatchPublisher
from taskforge.claims.domain import IssuedTaskClaim
from taskforge.claims.service import TaskClaimService
from taskforge.dispatch.envelope import (
    DispatchEnvelope,
    deserialize_dispatch_envelope,
)
from taskforge.dispatch.publisher import TaskDispatchPublisher
from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.persistence.claims import SQLAlchemyTaskClaimRepository
from taskforge.persistence.database import build_session_factory
from taskforge.persistence.dispatch import SQLAlchemyDispatchOutboxRepository
from taskforge.persistence.recovery import (
    SQLAlchemyExpiredClaimRecoveryRepository,
    SQLAlchemyRecoveryCandidateRepository,
)
from taskforge.persistence.retries import SQLAlchemyRetryTransitionRepository
from taskforge.persistence.task_results import SQLAlchemyTaskResultRepository
from taskforge.persistence.task_start import SQLAlchemyTaskStartRepository
from taskforge.rate_limits import AllowAllRateLimiter
from taskforge.recovery.scanner import RecoveryCandidateScanner
from taskforge.recovery.service import (
    ExpiredClaimRecoveryOutcome,
    ExpiredClaimRecoveryService,
)
from taskforge.retries.scanner import DueRetryScanner
from taskforge.worker.consumer_ports import (
    BrokerDispatchDelivery,
    DispatchDeliveryControl,
)
from taskforge.worker.execution import WorkerExecutionConsumer
from taskforge.worker.handlers import (
    TaskContext,
    TaskHandlerDefinition,
    TaskHandlerRegistry,
)
from taskforge.worker.result_submission import (
    TaskResultSubmissionReceipt,
    TaskResultSubmissionRequest,
    TaskResultSubmissionService,
)
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
        or os.getenv("TASKFORGE_RUN_RECOVERY_INTEGRATION") != "1"
        or os.getenv("TASKFORGE_RUN_BROKER_INTEGRATION") != "1",
        reason="enable claim, recovery, and RabbitMQ integration explicitly",
    ),
]

_AUTHORITY_SECRET = b"m13-task5-crash-boundary-authority"
_CRASH_EXIT_CODE = 86
_LEASE_SECONDS = 3


class CrashPoint(StrEnum):
    BEFORE_CLAIM = "before_claim"
    AFTER_CLAIM = "after_claim"
    AFTER_SIDE_EFFECT = "after_side_effect"
    AFTER_RESULT_COMMIT = "after_result_commit"


@dataclass(frozen=True)
class AcceptParameters:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        del parameters
        return ()


@dataclass(frozen=True)
class ChildConfiguration:
    database_url: str
    amqp_url: str
    queue_name: str
    authenticated_worker: AuthenticatedWorker
    worker_session_id: UUID
    crash_point: CrashPoint


class CrashBeforeClaim:
    def __init__(self, delegate: TaskClaimService) -> None:
        self._delegate = delegate

    async def claim_task(self, *args: Any) -> IssuedTaskClaim:
        del args
        os._exit(_CRASH_EXIT_CODE)


class CrashAfterClaim:
    def __init__(self, delegate: TaskClaimService) -> None:
        self._delegate = delegate

    async def claim_task(self, *args: Any) -> IssuedTaskClaim:
        await self._delegate.claim_task(*args)
        os._exit(_CRASH_EXIT_CODE)


class CrashAfterResultCommit:
    def __init__(self, delegate: TaskResultSubmissionService) -> None:
        self._delegate = delegate

    async def submit_result(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        request: TaskResultSubmissionRequest,
    ) -> TaskResultSubmissionReceipt:
        receipt = await self._delegate.submit_result(
            authenticated_worker, worker_session_id, request
        )
        del receipt
        os._exit(_CRASH_EXIT_CODE)


class ObservingControl:
    def __init__(self, delegate: DispatchDeliveryControl) -> None:
        self._delegate = delegate
        self.actions: list[str] = []
        self.attempt_number = deserialize_dispatch_envelope(
            delegate.delivery.body
        ).attempt_number

    @property
    def delivery(self) -> BrokerDispatchDelivery:
        return self._delegate.delivery

    async def acknowledge(self) -> None:
        self.actions.append("ack")
        await self._delegate.acknowledge()

    async def reject(self, *, requeue: bool) -> None:
        self.actions.append(f"reject:{requeue}")
        await self._delegate.reject(requeue=requeue)


def task_types() -> TaskTypeRegistry:
    return TaskTypeRegistry(
        (TaskTypeDefinition("test.task", "test-capability", AcceptParameters()),)
    )


def handlers(handler: Any) -> TaskHandlerRegistry:
    return TaskHandlerRegistry(
        (TaskHandlerDefinition("test.task", "test-capability", handler),),
        task_types(),
    )


def execution_consumer(
    sessions: Any,
    worker: WorkerFacts,
    handler: Any,
    *,
    crash_point: CrashPoint | None = None,
) -> WorkerExecutionConsumer:
    issuer = claim_authority.TaskClaimResultAuthorityIssuer(_AUTHORITY_SECRET)
    claim: Any = TaskClaimService(
        SQLAlchemyTaskClaimRepository(sessions, worker_stale_after_seconds=30),
        issuer,
        lease_seconds=_LEASE_SECONDS,
    )
    if crash_point is CrashPoint.BEFORE_CLAIM:
        claim = CrashBeforeClaim(claim)
    elif crash_point is CrashPoint.AFTER_CLAIM:
        claim = CrashAfterClaim(claim)
    result: Any = TaskResultSubmissionService(
        SQLAlchemyTaskResultRepository(sessions),
        issuer,
        rate_limiter=AllowAllRateLimiter(),
    )
    if crash_point is CrashPoint.AFTER_RESULT_COMMIT:
        result = CrashAfterResultCommit(result)
    return WorkerExecutionConsumer(
        claim,
        TaskStartService(SQLAlchemyTaskStartRepository(sessions)),
        result,
        handlers(handler),
        worker.authenticated,
        worker.session_id,
    )


async def record_effect(database_url: str, context: TaskContext) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(make_url(database_url)))
    try:
        async with connection.transaction():
            await connection.execute(
                "INSERT INTO crash_test_handler_invocations "
                "(task_run_id, task_attempt_id) VALUES ($1, $2)",
                context.task_run_id,
                context.task_attempt_id,
            )
            await connection.execute(
                "INSERT INTO crash_test_effects (task_run_id) VALUES ($1) "
                "ON CONFLICT (task_run_id) DO NOTHING",
                context.task_run_id,
            )
    finally:
        await connection.close()


async def record_invocation(database_url: str, context: TaskContext) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(make_url(database_url)))
    try:
        await connection.execute(
            "INSERT INTO crash_test_handler_invocations "
            "(task_run_id, task_attempt_id) VALUES ($1, $2)",
            context.task_run_id,
            context.task_attempt_id,
        )
    finally:
        await connection.close()


async def child_main(configuration: ChildConfiguration) -> None:
    database_url = make_url(configuration.database_url)
    engine = create_async_engine(database_url.set(drivername="postgresql+asyncpg"))
    sessions = build_session_factory(engine)
    connection = await aio_pika.connect(configuration.amqp_url)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)
    queue = await channel.get_queue(configuration.queue_name)

    async def handler(context: TaskContext) -> object:
        if configuration.crash_point is CrashPoint.AFTER_SIDE_EFFECT:
            await record_effect(configuration.database_url, context)
            os._exit(_CRASH_EXIT_CODE)
        await record_invocation(configuration.database_url, context)
        return {"ok": True}

    worker = WorkerFacts(
        configuration.authenticated_worker, configuration.worker_session_id
    )
    consumer = execution_consumer(
        sessions, worker, handler, crash_point=configuration.crash_point
    )
    await RabbitMQDispatchConsumer(queue).consume(consumer.consume)
    await asyncio.Future()


def crash_worker_entry(configuration: ChildConfiguration) -> None:
    asyncio.run(child_main(configuration))


async def publish_unpublished(sessions: Any, exchange: AbstractExchange) -> None:
    result = await TaskDispatchPublisher(
        SQLAlchemyDispatchOutboxRepository(sessions),
        RabbitMQDispatchPublisher(exchange, timeout_seconds=3),
    ).reconcile_unpublished(page_size=10, pass_limit=10)
    assert result.durable_invalid == 0


async def wait_for_process(process: Any) -> None:
    await asyncio.wait_for(asyncio.to_thread(process.join), timeout=10)
    assert process.exitcode == _CRASH_EXIT_CODE


async def wait_for_expiry(
    connection: asyncpg.Connection[asyncpg.Record], task_attempt_id: UUID
) -> None:
    for _ in range(200):
        expired = await connection.fetchval(
            "SELECT lease_expires_at <= statement_timestamp() "
            "FROM task_attempt_claims WHERE task_attempt_id = $1 "
            "AND terminated_at IS NULL",
            task_attempt_id,
        )
        if expired:
            return
        await asyncio.sleep(0.05)
    pytest.fail("claim did not expire before the test deadline")


async def recover_and_dispatch(
    sessions: Any,
    connection: asyncpg.Connection[asyncpg.Record],
    exchange: AbstractExchange,
    attempt_id: UUID,
) -> UUID:
    scanner = RecoveryCandidateScanner(
        SQLAlchemyRecoveryCandidateRepository(sessions),
        worker_stale_after_seconds=30,
    )
    page = await scanner.scan_expired_claims(limit=10)
    candidate = next(item for item in page.items if item.task_attempt_id == attempt_id)
    recovered = await ExpiredClaimRecoveryService(
        SQLAlchemyExpiredClaimRecoveryRepository(sessions)
    ).recover_expired_claim(candidate)
    assert recovered.outcome is ExpiredClaimRecoveryOutcome.RETRY_SCHEDULED
    due = await DueRetryScanner(
        SQLAlchemyRetryTransitionRepository(sessions), task_types()
    ).scan_due_retries(batch_size=10)
    assert due.dispatched == 1
    await publish_unpublished(sessions, exchange)
    replacement_id = await connection.fetchval(
        "SELECT id FROM task_attempts WHERE task_run_id = $1 AND attempt_number = 2",
        candidate.task_run_id,
    )
    assert isinstance(replacement_id, UUID)
    return replacement_id


async def consume_until(
    queue: Any,
    consumer: WorkerExecutionConsumer,
    *,
    expected_deliveries: int,
) -> list[ObservingControl]:
    observed: list[ObservingControl] = []
    completed = asyncio.Event()

    async def observe(control: DispatchDeliveryControl) -> None:
        wrapped = ObservingControl(control)
        observed.append(wrapped)
        await consumer.consume(wrapped)
        if len(observed) >= expected_deliveries:
            completed.set()

    broker = RabbitMQDispatchConsumer(queue)
    tag = await broker.consume(observe)
    try:
        await asyncio.wait_for(completed.wait(), timeout=10)
    finally:
        await broker.cancel(tag)
    return observed


async def prepare_task(
    connection: asyncpg.Connection[asyncpg.Record], *, retry: bool
) -> DispatchEnvelope:
    policy = None
    if retry:
        policy = (
            '{"retry_policy":{"maximum_attempts":2,'
            '"initial_delay_seconds":0,"multiplier":2,'
            '"maximum_delay_seconds":60}}'
        )
    return await add_dispatched_task(connection, workflow_policy=policy)


async def run_crash(
    database_url: URL,
    amqp_url: str,
    queue_name: str,
    worker: WorkerFacts,
    crash_point: CrashPoint,
) -> None:
    context = multiprocessing.get_context("spawn")
    process = context.Process(
        target=crash_worker_entry,
        args=(
            ChildConfiguration(
                database_url.render_as_string(hide_password=False),
                amqp_url,
                queue_name,
                worker.authenticated,
                worker.session_id,
                crash_point,
            ),
        ),
    )
    process.start()
    try:
        await wait_for_process(process)
    finally:
        if process.is_alive():
            process.terminate()
            await asyncio.to_thread(process.join, 5)


async def exercise(database_url: URL, amqp_url: str) -> None:
    setup = await asyncpg.connect(asyncpg_dsn(database_url))
    await setup.execute(
        "CREATE TABLE crash_test_handler_invocations "
        "(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
        "task_run_id uuid NOT NULL, task_attempt_id uuid NOT NULL)"
    )
    await setup.execute(
        "CREATE TABLE crash_test_effects (task_run_id uuid PRIMARY KEY)"
    )
    engine = create_async_engine(database_url.set(drivername="postgresql+asyncpg"))
    sessions = build_session_factory(engine)
    connection = await aio_pika.connect(amqp_url)
    suffix = uuid4().hex
    exchange_name = f"taskforge.m13.crash.exchange.{suffix}"
    queue_name = f"taskforge.m13.crash.queue.{suffix}"
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)
    exchange = await channel.declare_exchange(
        exchange_name, aio_pika.ExchangeType.DIRECT, durable=True, auto_delete=False
    )
    queue = await channel.declare_queue(queue_name, durable=True, auto_delete=False)
    await queue.bind(exchange, routing_key="capability.test-capability")
    try:
        # Before claim: no lease exists, and normal redelivery completes directly.
        before = await prepare_task(setup, retry=False)
        crashed_worker = await add_worker(setup)
        replacement = await add_worker(setup)
        await publish_unpublished(sessions, exchange)
        await run_crash(
            database_url, amqp_url, queue_name, crashed_worker, CrashPoint.BEFORE_CLAIM
        )
        invocations = 0

        async def success_handler(context: TaskContext) -> object:
            nonlocal invocations
            await record_invocation(
                database_url.render_as_string(hide_password=False), context
            )
            invocations += 1
            return {"ok": True}

        observed = await consume_until(
            queue,
            execution_consumer(sessions, replacement, success_handler),
            expected_deliveries=1,
        )
        assert observed[0].actions == ["ack"]
        assert observed[0].delivery.redelivered
        assert invocations == 1
        assert not await setup.fetchval(
            "SELECT EXISTS (SELECT FROM task_attempt_claims WHERE "
            "task_attempt_id = $1 AND generation > 1)",
            before.task_attempt_id,
        )

        # After claim: a fresh session ACKs the redelivery as already authoritative.
        after_claim = await prepare_task(setup, retry=True)
        crashed_worker = await add_worker(setup)
        replacement = await add_worker(setup)
        await publish_unpublished(sessions, exchange)
        await run_crash(
            database_url, amqp_url, queue_name, crashed_worker, CrashPoint.AFTER_CLAIM
        )
        stale = await consume_until(
            queue,
            execution_consumer(sessions, replacement, success_handler),
            expected_deliveries=1,
        )
        assert stale[0].attempt_number == 1
        assert stale[0].actions == ["ack"]
        assert stale[0].delivery.redelivered
        assert invocations == 1
        assert (
            await setup.fetchval(
                "SELECT status::text FROM task_runs WHERE id = $1",
                after_claim.task_run_id,
            )
            == "claimed"
        )
        await wait_for_expiry(setup, after_claim.task_attempt_id)
        replacement_id = await recover_and_dispatch(
            sessions, setup, exchange, after_claim.task_attempt_id
        )
        completed = await consume_until(
            queue,
            execution_consumer(sessions, replacement, success_handler),
            expected_deliveries=1,
        )
        assert completed[0].attempt_number == 2
        assert completed[0].actions == ["ack"]
        assert invocations == 2
        assert (
            await setup.fetchval(
                "SELECT failure_kind::text FROM task_attempt_results WHERE "
                "task_attempt_id = $1",
                after_claim.task_attempt_id,
            )
            == "claim_expired"
        )
        assert (
            await setup.fetchval(
                "SELECT result_kind::text FROM task_attempt_results WHERE "
                "task_attempt_id = $1",
                replacement_id,
            )
            == "success"
        )

        # After side effect: retain stale delivery until attempt 2 is published.
        side_effect = await prepare_task(setup, retry=True)
        crashed_worker = await add_worker(setup)
        replacement = await add_worker(setup)
        await publish_unpublished(sessions, exchange)
        await run_crash(
            database_url,
            amqp_url,
            queue_name,
            crashed_worker,
            CrashPoint.AFTER_SIDE_EFFECT,
        )
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM crash_test_effects WHERE task_run_id = $1",
                side_effect.task_run_id,
            )
            == 1
        )
        assert not await setup.fetchval(
            "SELECT EXISTS (SELECT FROM task_attempt_results WHERE "
            "task_attempt_id = $1)",
            side_effect.task_attempt_id,
        )
        await wait_for_expiry(setup, side_effect.task_attempt_id)
        await recover_and_dispatch(
            sessions, setup, exchange, side_effect.task_attempt_id
        )

        async def idempotent_handler(context: TaskContext) -> object:
            await record_effect(
                database_url.render_as_string(hide_password=False), context
            )
            return {"ok": True}

        deliveries = await consume_until(
            queue,
            execution_consumer(sessions, replacement, idempotent_handler),
            expected_deliveries=2,
        )
        stale_delivery = next(item for item in deliveries if item.attempt_number == 1)
        retry_delivery = next(item for item in deliveries if item.attempt_number == 2)
        assert stale_delivery.actions == ["ack"]
        assert stale_delivery.delivery.redelivered
        assert retry_delivery.actions == ["ack"]
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM crash_test_handler_invocations WHERE "
                "task_run_id = $1",
                side_effect.task_run_id,
            )
            == 2
        )
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM crash_test_effects WHERE task_run_id = $1",
                side_effect.task_run_id,
            )
            == 1
        )

        # After result commit: redelivery is obsolete and ACKed without execution.
        after_result = await prepare_task(setup, retry=False)
        crashed_worker = await add_worker(setup)
        replacement = await add_worker(setup)
        await publish_unpublished(sessions, exchange)
        await run_crash(
            database_url,
            amqp_url,
            queue_name,
            crashed_worker,
            CrashPoint.AFTER_RESULT_COMMIT,
        )
        before_redelivery_invocations = invocations
        obsolete = await consume_until(
            queue,
            execution_consumer(sessions, replacement, success_handler),
            expected_deliveries=1,
        )
        assert obsolete[0].attempt_number == 1
        assert obsolete[0].actions == ["ack"]
        assert obsolete[0].delivery.redelivered
        assert invocations == before_redelivery_invocations
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM crash_test_handler_invocations WHERE "
                "task_run_id = $1",
                after_result.task_run_id,
            )
            == 1
        )
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM task_attempt_results WHERE task_attempt_id = $1",
                after_result.task_attempt_id,
            )
            == 1
        )
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM task_result_events WHERE task_attempt_id = $1 "
                "AND event_type = 'result_accepted'",
                after_result.task_attempt_id,
            )
            == 1
        )
        assert not (
            await RecoveryCandidateScanner(
                SQLAlchemyRecoveryCandidateRepository(sessions),
                worker_stale_after_seconds=30,
            ).scan_expired_claims(limit=100)
        ).items
    finally:
        if not connection.is_closed:
            cleanup = await connection.channel()
            await cleanup.queue_delete(queue_name, if_unused=False, if_empty=False)
            await cleanup.exchange_delete(exchange_name)
            await connection.close()
        await setup.close()
        await engine.dispose()


def test_real_worker_process_crash_boundaries() -> None:
    amqp_url = os.getenv("TASKFORGE_BROKER_TEST_AMQP_URL")
    if not amqp_url:
        pytest.fail("TASKFORGE_BROKER_TEST_AMQP_URL is required")
    with temporary_database(
        "TASKFORGE_RECOVERY_TEST_DATABASE_URL", "taskforge_m13_crash"
    ) as database_url:
        configuration = Config("alembic.ini")
        with migration_database_url(database_url.render_as_string(hide_password=False)):
            command.upgrade(configuration, "head")
        asyncio.run(exercise(database_url, amqp_url))
