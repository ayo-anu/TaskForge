"""RabbitMQ dispatch topology declaration unit tests."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, cast

import pytest
from aio_pika.abc import AbstractChannel
from pamqp import decode, encode

from taskforge.broker.topology import (
    _UNLIMITED_DELIVERY_COUNT,
    DISPATCH_TOPOLOGY_GENERATION,
    SUPPORTED_DISPATCH_ENVELOPE_VERSION,
    RabbitMQTopologyConfiguration,
    declare_dispatch_topology,
)
from taskforge.dispatch.envelope import DISPATCH_ENVELOPE_VERSION
from taskforge.workflows.task_types import (
    JSONMapping,
    TaskTypeDefinition,
    TaskTypeRegistry,
    WorkflowValidationIssue,
)


@dataclass(frozen=True)
class AcceptParameters:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        del parameters
        return ()


@dataclass
class FakeExchange:
    name: str


@dataclass
class FakeQueue:
    name: str
    bindings: list[tuple[str, str | None, float | None]] = field(default_factory=list)

    async def bind(
        self,
        exchange: FakeExchange,
        routing_key: str | None = None,
        *,
        timeout: float | None = None,
    ) -> None:
        self.bindings.append((exchange.name, routing_key, timeout))


class FakeChannel:
    def __init__(self) -> None:
        self.exchanges: list[tuple[str, object, dict[str, object]]] = []
        self.queues: list[tuple[str, dict[str, object], FakeQueue]] = []

    async def declare_exchange(
        self, name: str, kind: object, **kwargs: object
    ) -> FakeExchange:
        self.exchanges.append((name, kind, kwargs))
        return FakeExchange(name)

    async def declare_queue(self, name: str, **kwargs: object) -> FakeQueue:
        queue = FakeQueue(name)
        self.queues.append((name, kwargs, queue))
        return queue


def registry() -> TaskTypeRegistry:
    return TaskTypeRegistry(
        (
            TaskTypeDefinition(
                "document.extract", "document-workers", AcceptParameters()
            ),
            TaskTypeDefinition(
                "document.index", "document-workers", AcceptParameters()
            ),
            TaskTypeDefinition(
                "notify.email", "notification-workers", AcceptParameters()
            ),
        )
    )


def configuration(**changes: object) -> RabbitMQTopologyConfiguration:
    values: dict[str, object] = {
        "dispatch_exchange_name": "taskforge.dispatch.v1",
        "malformed_exchange_name": "taskforge.dispatch.malformed.v1",
        "timeout_seconds": 5.0,
    }
    values.update(changes)
    return RabbitMQTopologyConfiguration(**values)  # type: ignore[arg-type]


def test_declares_generation_one_quorum_topology_once_per_capability() -> None:
    channel = FakeChannel()

    topology = asyncio.run(
        declare_dispatch_topology(
            cast(AbstractChannel, cast(Any, channel)), registry(), configuration()
        )
    )

    assert DISPATCH_TOPOLOGY_GENERATION == 1
    assert SUPPORTED_DISPATCH_ENVELOPE_VERSION == DISPATCH_ENVELOPE_VERSION == 2
    assert [item[0] for item in channel.exchanges] == [
        "taskforge.dispatch.malformed.v1",
        "taskforge.dispatch.v1",
    ]
    assert len(topology.capability_queues) == 2
    source_arguments = channel.queues[1][1]["arguments"]
    assert source_arguments == {
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": "taskforge.dispatch.malformed.v1",
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
        "x-delivery-limit": -1,
    }
    assert channel.queues[0][1]["arguments"] == {"x-queue-type": "quorum"}
    assert channel.queues[1][2].bindings == [
        ("taskforge.dispatch.v1", "capability.document-workers", 5.0)
    ]
    assert channel.queues[2][2].bindings == [
        ("taskforge.dispatch.v1", "capability.notification-workers", 5.0)
    ]


def test_unlimited_delivery_count_round_trips_through_pamqp_field_table() -> None:
    encoded = encode.field_table({"x-delivery-limit": _UNLIMITED_DELIVERY_COUNT})

    consumed, decoded = decode.field_table(encoded)

    assert consumed == len(encoded)
    assert type(decoded["x-delivery-limit"]) is int
    assert decoded["x-delivery-limit"] == -1


def test_validates_all_derived_names_before_declaration() -> None:
    channel = FakeChannel()

    with pytest.raises(ValueError, match="too long"):
        asyncio.run(
            declare_dispatch_topology(
                cast(AbstractChannel, cast(Any, channel)),
                registry(),
                configuration(dispatch_exchange_name="x" * 250),
            )
        )

    assert channel.exchanges == []
    assert channel.queues == []


def test_rejects_ambiguous_configuration_before_declaration() -> None:
    channel = FakeChannel()

    with pytest.raises(ValueError, match="distinct"):
        asyncio.run(
            declare_dispatch_topology(
                cast(AbstractChannel, cast(Any, channel)),
                registry(),
                configuration(malformed_exchange_name="taskforge.dispatch.v1"),
            )
        )

    assert channel.exchanges == []
