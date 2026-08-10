"""RabbitMQ publisher-confirm adapter for broker-neutral dispatch messages."""

from __future__ import annotations

import aio_pika
from aio_pika.abc import AbstractExchange
from aiormq.exceptions import AMQPError, ChannelInvalidStateError, DeliveryError
from pamqp.commands import Basic

from taskforge.dispatch.publisher_ports import (
    BrokerDispatchPublication,
    BrokerPublicationRejected,
    BrokerPublicationTimeout,
    BrokerUnavailable,
)


class RabbitMQDispatchPublisher:
    def __init__(self, exchange: AbstractExchange, *, timeout_seconds: float) -> None:
        if timeout_seconds <= 0:
            raise ValueError("publication timeout must be positive")
        self._exchange = exchange
        self._timeout_seconds = timeout_seconds

    async def publish(self, publication: BrokerDispatchPublication) -> None:
        message = aio_pika.Message(
            publication.body,
            content_type="application/json",
            content_encoding="utf-8",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=str(publication.dispatch_id),
        )
        try:
            confirmation = await self._exchange.publish(
                message,
                routing_key=publication.route,
                mandatory=True,
                timeout=self._timeout_seconds,
            )
        except TimeoutError as error:
            raise BrokerPublicationTimeout from error
        except DeliveryError as error:
            raise BrokerPublicationRejected from error
        except (AMQPError, ChannelInvalidStateError, ConnectionError, OSError) as error:
            raise BrokerUnavailable from error
        if not isinstance(confirmation, Basic.Ack):
            raise BrokerPublicationRejected
