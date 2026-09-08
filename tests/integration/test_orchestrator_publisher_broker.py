"""Real RabbitMQ evidence for orchestrator-owned ordinary publication resources."""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from urllib.parse import unquote, urlparse
from uuid import uuid4

import aio_pika
import pytest
from aio_pika.robust_connection import RobustConnection

from taskforge.broker.rabbitmq import RabbitMQDispatchPublisher
from taskforge.dispatch.publisher_ports import BrokerDispatchPublication
from taskforge.orchestrator.application import OrchestratorApplication
from taskforge.settings import OrchestratorSettings
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


def broker_settings(amqp_url: str, suffix: str) -> OrchestratorSettings:
    parsed = urlparse(amqp_url)
    assert parsed.hostname and parsed.port and parsed.username and parsed.password
    return OrchestratorSettings(
        postgres_password="unused-postgres-secret",
        rabbitmq_host=parsed.hostname,
        rabbitmq_port=parsed.port,
        rabbitmq_user=unquote(parsed.username),
        rabbitmq_password=unquote(parsed.password),
        rabbitmq_vhost=unquote(parsed.path.lstrip("/")) or "/",
        rabbitmq_dispatch_exchange_name=f"taskforge.dispatch.orchestrator.{suffix}",
        rabbitmq_malformed_exchange_name=(
            f"taskforge.dispatch.orchestrator.malformed.{suffix}"
        ),
        rabbitmq_topology_timeout_seconds=3,
        orchestrator_publication_timeout_seconds=3,
    )


async def verify_orchestrator_broker(amqp_url: str) -> None:
    suffix = uuid4().hex
    settings = broker_settings(amqp_url, suffix)
    catalog = TaskTypeRegistry(
        (TaskTypeDefinition("test.task", "test-capability", AcceptParameters()),)
    )
    application = OrchestratorApplication(settings)
    try:
        await application._connect_broker(catalog)
        connection = application._connection
        channel = application._publisher_channel
        exchange = application._dispatch_exchange
        assert connection is not None and channel is not None and exchange is not None
        assert not isinstance(connection, RobustConnection)

        dispatch_id = uuid4()
        await RabbitMQDispatchPublisher(exchange, timeout_seconds=3).publish(
            BrokerDispatchPublication(
                dispatch_id,
                "capability.test-capability",
                b'{"orchestrator":"persistent-mandatory-confirmed"}',
            )
        )
        queue_name = (
            f"{settings.rabbitmq_dispatch_exchange_name}.capability.test-capability"
        )
        queue = await channel.declare_queue(queue_name, passive=True, timeout=3)
        message = await queue.get(fail=True, timeout=3)
        assert message is not None
        assert message.message_id == str(dispatch_id)
        assert message.delivery_mode is aio_pika.DeliveryMode.PERSISTENT
        await message.ack()

        await channel.queue_delete(queue_name, timeout=3)
        await channel.queue_delete(
            f"{settings.rabbitmq_malformed_exchange_name}.quarantine", timeout=3
        )
        await channel.exchange_delete(
            settings.rabbitmq_dispatch_exchange_name, timeout=3
        )
        await channel.exchange_delete(
            settings.rabbitmq_malformed_exchange_name, timeout=3
        )
    finally:
        await application.close()


def test_orchestrator_owns_ordinary_confirm_publication_channel() -> None:
    amqp_url = os.getenv("TASKFORGE_BROKER_TEST_AMQP_URL")
    if not amqp_url:
        pytest.fail("TASKFORGE_BROKER_TEST_AMQP_URL is required")
    asyncio.run(verify_orchestrator_broker(amqp_url))
