"""RabbitMQ adapter for broker-neutral manual dispatch consumption."""

from __future__ import annotations

from aio_pika.abc import AbstractIncomingMessage, AbstractQueue
from aiormq.exceptions import AMQPError, ChannelInvalidStateError

from taskforge.dispatch.transport import DispatchTransportMetadata
from taskforge.worker.consumer_ports import (
    BrokerConsumerUnavailable,
    BrokerDispatchDelivery,
    DispatchDeliveryControl,
    DispatchDeliveryHandler,
)


class RabbitMQDispatchDelivery(DispatchDeliveryControl):
    def __init__(self, message: AbstractIncomingMessage) -> None:
        self._message = message
        routing_key = message.routing_key
        self._delivery = BrokerDispatchDelivery(
            message.body,
            DispatchTransportMetadata(
                message.message_id if isinstance(message.message_id, str) else None,
                routing_key if isinstance(routing_key, str) else "",
                message.content_type,
                message.content_encoding,
            ),
            bool(message.redelivered),
        )

    @property
    def delivery(self) -> BrokerDispatchDelivery:
        return self._delivery

    async def acknowledge(self) -> None:
        try:
            await self._message.ack()
        except (AMQPError, ChannelInvalidStateError, ConnectionError, OSError) as error:
            raise BrokerConsumerUnavailable from error

    async def reject(self, *, requeue: bool) -> None:
        try:
            await self._message.reject(requeue=requeue)
        except (AMQPError, ChannelInvalidStateError, ConnectionError, OSError) as error:
            raise BrokerConsumerUnavailable from error


class RabbitMQDispatchConsumer:
    def __init__(self, queue: AbstractQueue) -> None:
        self._queue = queue

    async def consume(self, handler: DispatchDeliveryHandler) -> str:
        async def deliver(message: AbstractIncomingMessage) -> None:
            await handler(RabbitMQDispatchDelivery(message))

        try:
            return await self._queue.consume(deliver, no_ack=False)
        except (AMQPError, ChannelInvalidStateError, ConnectionError, OSError) as error:
            raise BrokerConsumerUnavailable from error

    async def cancel(self, consumer_tag: str) -> None:
        try:
            await self._queue.cancel(consumer_tag)
        except (AMQPError, ChannelInvalidStateError, ConnectionError, OSError) as error:
            raise BrokerConsumerUnavailable from error
