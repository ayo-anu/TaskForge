"""RabbitMQ dispatch publisher-confirm adapter tests."""

from __future__ import annotations

import asyncio
from typing import cast
from uuid import uuid4

import pytest
from aio_pika import Message
from aio_pika.abc import AbstractExchange
from aiormq.exceptions import AMQPConnectionError, DeliveryError
from pamqp.commands import Basic

from taskforge.broker.rabbitmq import RabbitMQDispatchPublisher
from taskforge.dispatch.publisher_ports import (
    BrokerDispatchPublication,
    BrokerPublicationRejected,
    BrokerPublicationTimeout,
    BrokerUnavailable,
)


class FakeExchange:
    def __init__(self, outcome: object) -> None:
        self.outcome = outcome
        self.calls: list[tuple[Message, str, bool, float | None]] = []

    async def publish(
        self,
        message: Message,
        routing_key: str,
        *,
        mandatory: bool = True,
        immediate: bool = False,
        timeout: float | None = None,
    ) -> object:
        del immediate
        self.calls.append((message, routing_key, mandatory, timeout))
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return self.outcome


def publication() -> BrokerDispatchPublication:
    return BrokerDispatchPublication(uuid4(), "capability.test", b'{"value":1}')


def test_rabbitmq_adapter_publishes_persistent_confirmed_message() -> None:
    exchange = FakeExchange(Basic.Ack(delivery_tag=1))
    item = publication()

    asyncio.run(
        RabbitMQDispatchPublisher(
            cast(AbstractExchange, exchange), timeout_seconds=2
        ).publish(item)
    )

    message, route, mandatory, timeout = exchange.calls[0]
    assert message.body == item.body
    assert message.message_id == str(item.dispatch_id)
    assert message.content_type == "application/json"
    assert message.content_encoding == "utf-8"
    assert message.delivery_mode == 2
    assert (route, mandatory, timeout) == (item.route, True, 2)


@pytest.mark.parametrize("outcome", (Basic.Nack(delivery_tag=1), None))
def test_rabbitmq_adapter_rejects_non_ack_confirmation(outcome: object) -> None:
    with pytest.raises(BrokerPublicationRejected):
        asyncio.run(
            RabbitMQDispatchPublisher(
                cast(AbstractExchange, FakeExchange(outcome)), timeout_seconds=2
            ).publish(publication())
        )


@pytest.mark.parametrize(
    ("failure", "expected"),
    (
        (TimeoutError(), BrokerPublicationTimeout),
        (DeliveryError(None, Basic.Nack(delivery_tag=1)), BrokerPublicationRejected),
        (AMQPConnectionError(), BrokerUnavailable),
        (OSError(), BrokerUnavailable),
    ),
)
def test_rabbitmq_adapter_translates_safe_failure_categories(
    failure: BaseException, expected: type[Exception]
) -> None:
    with pytest.raises(expected):
        asyncio.run(
            RabbitMQDispatchPublisher(
                cast(AbstractExchange, FakeExchange(failure)), timeout_seconds=2
            ).publish(publication())
        )
