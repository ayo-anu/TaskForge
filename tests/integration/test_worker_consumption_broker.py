"""Real RabbitMQ manual consumption and unacknowledged redelivery test."""

from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import aio_pika
import pytest

from taskforge.broker.consumer import RabbitMQDispatchConsumer
from taskforge.worker.lifecycle import WorkerDispatchRuntime

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_BROKER_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_BROKER_INTEGRATION=1 explicitly",
    ),
]


async def exercise(amqp_url: str) -> None:
    suffix = uuid4().hex
    queue_name = f"taskforge.worker.task1.{suffix}"
    connection = await aio_pika.connect(amqp_url)
    queue = None
    try:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        queue = await channel.declare_queue(queue_name, durable=True, auto_delete=False)
        await channel.default_exchange.publish(
            aio_pika.Message(b"valid-task-1-delivery"), routing_key=queue_name
        )
        received = asyncio.Event()

        async def leave_unacknowledged(delivery: object) -> None:
            del delivery
            received.set()

        consumer = RabbitMQDispatchConsumer(queue)
        await consumer.consume(leave_unacknowledged)
        await asyncio.wait_for(received.wait(), timeout=3)
        await channel.close()

        redelivery_channel = await connection.channel()
        redelivery_queue = await redelivery_channel.get_queue(queue_name)
        redelivered = await redelivery_queue.get(timeout=3)
        assert redelivered is not None
        assert redelivered.body == b"valid-task-1-delivery"
        assert redelivered.redelivered
        await redelivered.ack()
    finally:
        if not connection.is_closed:
            cleanup = await connection.channel()
            await cleanup.queue_delete(queue_name, if_unused=False, if_empty=False)
            await connection.close()


def test_valid_delivery_remains_unacknowledged_and_redelivers() -> None:
    amqp_url = os.getenv("TASKFORGE_BROKER_TEST_AMQP_URL")
    if not amqp_url:
        pytest.fail("TASKFORGE_BROKER_TEST_AMQP_URL is required")
    asyncio.run(exercise(amqp_url))


async def exercise_paused_admission(amqp_url: str) -> None:
    suffix = uuid4().hex
    queue_name = f"taskforge.worker.paused.{suffix}"
    connection = await aio_pika.connect(amqp_url)
    try:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        queue = await channel.declare_queue(queue_name, durable=True, auto_delete=False)
        await channel.default_exchange.publish(
            aio_pika.Message(b"gated-delivery"), routing_key=queue_name
        )
        handled = False

        async def handle(delivery: object) -> None:
            nonlocal handled
            del delivery
            handled = True

        runtime = WorkerDispatchRuntime(RabbitMQDispatchConsumer(queue), handle)
        await runtime.start(paused=True)
        for _ in range(100):
            if runtime.in_flight == 1:
                break
            await asyncio.sleep(0.01)
        assert runtime.in_flight == 1
        assert not handled

        await runtime.shutdown()
        assert not handled
        await channel.close()

        redelivery_channel = await connection.channel()
        redelivery_queue = await redelivery_channel.get_queue(queue_name)
        redelivered = await redelivery_queue.get(timeout=3)
        assert redelivered is not None
        assert redelivered.redelivered
        assert redelivered.body == b"gated-delivery"
        await redelivered.ack()
        await redelivery_channel.close()
    finally:
        if not connection.is_closed:
            cleanup = await connection.channel()
            await cleanup.queue_delete(queue_name, if_unused=False, if_empty=False)
            await connection.close()


def test_paused_admission_does_not_claim_and_requeues_on_startup_failure() -> None:
    amqp_url = os.getenv("TASKFORGE_BROKER_TEST_AMQP_URL")
    if not amqp_url:
        pytest.fail("TASKFORGE_BROKER_TEST_AMQP_URL is required")
    asyncio.run(exercise_paused_admission(amqp_url))
