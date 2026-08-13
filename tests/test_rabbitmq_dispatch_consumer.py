from __future__ import annotations

import asyncio
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

from aio_pika.abc import AbstractIncomingMessage, AbstractQueue

from taskforge.broker.consumer import (
    RabbitMQDispatchConsumer,
    RabbitMQDispatchDelivery,
)


def message() -> Any:
    value = Mock()
    value.body = b"payload"
    value.message_id = "message-id"
    value.routing_key = "capability.test"
    value.content_type = "application/json"
    value.content_encoding = "utf-8"
    value.redelivered = True
    value.ack = AsyncMock()
    value.reject = AsyncMock()
    return value


def test_delivery_retains_only_broker_neutral_metadata_and_disposes_manually() -> None:
    incoming = message()
    delivery = RabbitMQDispatchDelivery(
        cast(AbstractIncomingMessage, cast(object, incoming))
    )

    assert delivery.delivery.body == b"payload"
    assert delivery.delivery.metadata.message_id == "message-id"
    assert delivery.delivery.redelivered
    asyncio.run(delivery.acknowledge())
    asyncio.run(delivery.reject(requeue=False))

    incoming.ack.assert_awaited_once_with()
    incoming.reject.assert_awaited_once_with(requeue=False)


def test_consumer_uses_manual_acknowledgement_and_supports_cancel() -> None:
    queue = AsyncMock()
    queue.consume.return_value = "consumer-tag"
    adapter = RabbitMQDispatchConsumer(cast(AbstractQueue, cast(object, queue)))

    async def handler(delivery: Any) -> None:
        del delivery

    tag = asyncio.run(adapter.consume(handler))
    asyncio.run(adapter.cancel(tag))

    assert tag == "consumer-tag"
    queue.consume.assert_awaited_once()
    assert queue.consume.await_args.kwargs["no_ack"] is False
    queue.cancel.assert_awaited_once_with("consumer-tag")
