"""Opt-in RabbitMQ 4.3 dispatch topology and malformed quarantine tests."""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from uuid import uuid4

import aio_pika
import pytest
from aio_pika.abc import AbstractIncomingMessage, AbstractQueue
from aiormq.exceptions import ChannelPreconditionFailed, DeliveryError

from taskforge.broker.malformed import reject_permanently_malformed
from taskforge.broker.topology import (
    RabbitMQTopologyConfiguration,
    declare_dispatch_topology,
)
from taskforge.dispatch.envelope import (
    create_dispatch_envelope,
    serialize_dispatch_envelope,
)
from taskforge.dispatch.transport import (
    DispatchTransportMetadata,
    MalformedDispatchTransport,
    ValidatedDispatchTransport,
    validate_dispatch_transport,
)
from taskforge.workflows.task_types import (
    JSONMapping,
    TaskTypeDefinition,
    TaskTypeRegistry,
    WorkflowValidationIssue,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_BROKER_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_BROKER_INTEGRATION=1 explicitly",
    ),
]


@dataclass(frozen=True)
class AcceptParameters:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        del parameters
        return ()


def registry() -> TaskTypeRegistry:
    return TaskTypeRegistry(
        (
            TaskTypeDefinition(
                "document.extract", "document-workers", AcceptParameters()
            ),
            TaskTypeDefinition(
                "notify.email", "notification-workers", AcceptParameters()
            ),
        )
    )


def message_metadata(message: AbstractIncomingMessage) -> DispatchTransportMetadata:
    assert message.routing_key is not None
    return DispatchTransportMetadata(
        message.message_id,
        message.routing_key,
        message.content_type,
        message.content_encoding,
    )


def first_dead_letter_event(
    message: AbstractIncomingMessage,
) -> Mapping[str, object]:
    history = message.headers.get("x-death")
    assert isinstance(history, list)
    assert len(history) == 1
    event = history[0]
    assert isinstance(event, dict)
    return event


async def poll_message(
    queue: AbstractQueue, *, attempts: int = 30
) -> AbstractIncomingMessage:
    for _ in range(attempts):
        message = await queue.get(fail=False, timeout=0.25)
        if message is not None:
            return message
        await asyncio.sleep(0.1)
    raise AssertionError("message did not arrive within bounded polling interval")


async def verify_topology(amqp_url: str) -> None:
    suffix = uuid4().hex
    configuration = RabbitMQTopologyConfiguration(
        dispatch_exchange_name=f"taskforge.dispatch.v1.{suffix}",
        malformed_exchange_name=f"taskforge.dispatch.malformed.v1.{suffix}",
        timeout_seconds=3,
    )
    connection = await aio_pika.connect(amqp_url)
    topology = None
    try:
        channel = await connection.channel(
            publisher_confirms=True, on_return_raises=True
        )
        topology = await declare_dispatch_topology(channel, registry(), configuration)
        restarted = await declare_dispatch_topology(channel, registry(), configuration)
        assert restarted.exchange.name == topology.exchange.name
        assert set(restarted.capability_queues) == {
            "document-workers",
            "notification-workers",
        }

        envelope = create_dispatch_envelope(
            dispatch_id=uuid4(),
            task_attempt_id=uuid4(),
            task_run_id=uuid4(),
            workflow_run_id=uuid4(),
            attempt_number=1,
            task_type="document.extract",
            required_capability="document-workers",
            task_payload={},
            references={},
        )
        body = serialize_dispatch_envelope(envelope)
        await topology.exchange.publish(
            aio_pika.Message(
                body,
                content_type="application/json",
                content_encoding="utf-8",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                message_id=str(envelope.dispatch_id),
            ),
            routing_key=envelope.route,
            mandatory=True,
            timeout=3,
        )
        valid = await poll_message(topology.capability_queues["document-workers"])
        assert isinstance(
            validate_dispatch_transport(valid.body, message_metadata(valid)),
            ValidatedDispatchTransport,
        )
        await valid.ack()
        assert (
            await topology.capability_queues["notification-workers"].get(
                fail=False, timeout=0.25
            )
            is None
        )
        with pytest.raises(DeliveryError):
            await topology.exchange.publish(
                aio_pika.Message(b"{}"),
                routing_key="capability.unregistered-workers",
                mandatory=True,
                timeout=3,
            )

        malformed_value = json.loads(body)
        malformed_value["schema_version"] = 4
        malformed_body = json.dumps(malformed_value, separators=(",", ":")).encode()
        await topology.exchange.publish(
            aio_pika.Message(
                malformed_body,
                content_type="application/json",
                content_encoding="utf-8",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                message_id=str(envelope.dispatch_id),
            ),
            routing_key=envelope.route,
            mandatory=True,
            timeout=3,
        )
        malformed = await poll_message(topology.capability_queues["document-workers"])
        classification = validate_dispatch_transport(
            malformed.body, message_metadata(malformed)
        )
        assert classification == MalformedDispatchTransport(
            "unsupported_schema_version"
        )
        await reject_permanently_malformed(malformed)
        quarantined = await poll_message(topology.quarantine_queue)
        assert quarantined.body == malformed_body
        dead_letter = first_dead_letter_event(quarantined)
        assert dead_letter["reason"] == "rejected"
        assert dead_letter["count"] == 1
        assert (
            dead_letter["queue"] == topology.capability_queues["document-workers"].name
        )
        assert dead_letter["exchange"] == topology.exchange.name
        assert dead_letter["routing-keys"] == [envelope.route]
        await quarantined.reject(requeue=False)

        await channel.close()
        channel = await connection.channel(
            publisher_confirms=True, on_return_raises=True
        )
        topology = await declare_dispatch_topology(channel, registry(), configuration)
        assert (
            await topology.capability_queues["document-workers"].get(
                fail=False, timeout=0.25
            )
            is None
        )
        assert await topology.quarantine_queue.get(fail=False, timeout=0.25) is None

        await topology.exchange.publish(
            aio_pika.Message(
                body,
                content_type="application/json",
                content_encoding="utf-8",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                message_id=str(envelope.dispatch_id),
            ),
            routing_key=envelope.route,
            mandatory=True,
            timeout=3,
        )
        transient = await poll_message(topology.capability_queues["document-workers"])
        assert not transient.redelivered
        await channel.close()

        channel = await connection.channel(
            publisher_confirms=True, on_return_raises=True
        )
        topology = await declare_dispatch_topology(channel, registry(), configuration)
        redelivered = await poll_message(topology.capability_queues["document-workers"])
        assert redelivered.redelivered
        assert isinstance(
            validate_dispatch_transport(
                redelivered.body, message_metadata(redelivered)
            ),
            ValidatedDispatchTransport,
        )
        await redelivered.ack()
        assert await topology.quarantine_queue.get(fail=False, timeout=0.25) is None

        mismatch_channel = await connection.channel()
        with pytest.raises(ChannelPreconditionFailed):
            await mismatch_channel.declare_exchange(
                configuration.dispatch_exchange_name,
                aio_pika.ExchangeType.FANOUT,
                durable=True,
                timeout=3,
            )
    finally:
        if topology is not None:
            cleanup_channel = await connection.channel()
            for queue in topology.capability_queues.values():
                await cleanup_channel.queue_delete(queue.name)
            await cleanup_channel.queue_delete(topology.quarantine_queue.name)
            await cleanup_channel.exchange_delete(topology.exchange.name)
            await cleanup_channel.exchange_delete(topology.malformed_exchange.name)
        await connection.close()


async def verify_incompatible_topology_fails_fast(amqp_url: str) -> None:
    suffix = uuid4().hex
    configuration = RabbitMQTopologyConfiguration(
        dispatch_exchange_name=f"taskforge.dispatch.v1.incompatible.{suffix}",
        malformed_exchange_name=f"taskforge.dispatch.malformed.v1.incompatible.{suffix}",
        timeout_seconds=3,
    )
    connection = await aio_pika.connect(amqp_url)
    try:
        setup_channel = await connection.channel()
        await setup_channel.declare_exchange(
            configuration.dispatch_exchange_name,
            aio_pika.ExchangeType.FANOUT,
            durable=True,
            timeout=3,
        )
        declaration_channel = await connection.channel()
        with pytest.raises(ChannelPreconditionFailed):
            await declare_dispatch_topology(
                declaration_channel, registry(), configuration
            )
    finally:
        cleanup_channel = await connection.channel()
        quarantine_name = f"{configuration.malformed_exchange_name}.quarantine"
        await cleanup_channel.queue_delete(
            quarantine_name, if_unused=False, if_empty=False
        )
        await cleanup_channel.exchange_delete(configuration.dispatch_exchange_name)
        await cleanup_channel.exchange_delete(configuration.malformed_exchange_name)
        await connection.close()


def test_rabbitmq_dispatch_topology_routing_and_malformed_quarantine() -> None:
    amqp_url = os.getenv("TASKFORGE_BROKER_TEST_AMQP_URL")
    if not amqp_url:
        pytest.fail("TASKFORGE_BROKER_TEST_AMQP_URL is required")
    asyncio.run(verify_topology(amqp_url))


def test_incompatible_existing_topology_fails_without_replacement() -> None:
    amqp_url = os.getenv("TASKFORGE_BROKER_TEST_AMQP_URL")
    if not amqp_url:
        pytest.fail("TASKFORGE_BROKER_TEST_AMQP_URL is required")
    asyncio.run(verify_incompatible_topology_fails_fast(amqp_url))
