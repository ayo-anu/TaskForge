"""RabbitMQ dispatch topology generation 1 declaration."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractExchange, AbstractQueue

from taskforge.dispatch.envelope import DISPATCH_ENVELOPE_VERSION, dispatch_route
from taskforge.workflows.task_types import TaskTypeRegistry

DISPATCH_TOPOLOGY_GENERATION = 1
SUPPORTED_DISPATCH_ENVELOPE_VERSION = 3
MAX_RABBITMQ_NAME_BYTES = 255
_TOPOLOGY_NAME = re.compile(r"\A[a-z][a-z0-9._-]{0,254}\Z")


class _AMQPSignedLong(int):
    """Force pamqp 3.x to encode a small negative value as signed long-int."""

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, int):
            return NotImplemented
        if int(self) < 0 and other in {-32768, -128, 0}:
            return False
        return super().__ge__(other)


_UNLIMITED_DELIVERY_COUNT = _AMQPSignedLong(-1)


@dataclass(frozen=True)
class RabbitMQTopologyConfiguration:
    dispatch_exchange_name: str
    malformed_exchange_name: str
    timeout_seconds: float


@dataclass(frozen=True)
class RabbitMQDispatchTopology:
    exchange: AbstractExchange
    capability_queues: Mapping[str, AbstractQueue]
    malformed_exchange: AbstractExchange
    quarantine_queue: AbstractQueue


async def declare_dispatch_topology(
    channel: AbstractChannel,
    registry: TaskTypeRegistry,
    configuration: RabbitMQTopologyConfiguration,
) -> RabbitMQDispatchTopology:
    """Idempotently declare generation 1 topology for envelope schemas v1-v3."""
    if DISPATCH_ENVELOPE_VERSION != SUPPORTED_DISPATCH_ENVELOPE_VERSION:
        raise RuntimeError("dispatch topology and envelope versions are incompatible")
    names = _topology_names(registry, configuration)
    timeout = configuration.timeout_seconds

    malformed_exchange = await channel.declare_exchange(
        configuration.malformed_exchange_name,
        aio_pika.ExchangeType.FANOUT,
        durable=True,
        auto_delete=False,
        internal=False,
        timeout=timeout,
    )
    quarantine_queue = await channel.declare_queue(
        names.quarantine_queue,
        durable=True,
        exclusive=False,
        auto_delete=False,
        arguments={"x-queue-type": "quorum"},
        timeout=timeout,
    )
    await quarantine_queue.bind(malformed_exchange, timeout=timeout)

    exchange = await channel.declare_exchange(
        configuration.dispatch_exchange_name,
        aio_pika.ExchangeType.DIRECT,
        durable=True,
        auto_delete=False,
        internal=False,
        timeout=timeout,
    )
    queues: dict[str, AbstractQueue] = {}
    for capability, queue_name in names.capability_queues:
        queue = await channel.declare_queue(
            queue_name,
            durable=True,
            exclusive=False,
            auto_delete=False,
            arguments={
                "x-queue-type": "quorum",
                "x-dead-letter-exchange": configuration.malformed_exchange_name,
                "x-dead-letter-strategy": "at-least-once",
                "x-overflow": "reject-publish",
                "x-delivery-limit": _UNLIMITED_DELIVERY_COUNT,
            },
            timeout=timeout,
        )
        await queue.bind(
            exchange, routing_key=dispatch_route(capability), timeout=timeout
        )
        queues[capability] = queue
    return RabbitMQDispatchTopology(
        exchange,
        MappingProxyType(queues),
        malformed_exchange,
        quarantine_queue,
    )


@dataclass(frozen=True)
class _TopologyNames:
    quarantine_queue: str
    capability_queues: tuple[tuple[str, str], ...]


def _topology_names(
    registry: TaskTypeRegistry, configuration: RabbitMQTopologyConfiguration
) -> _TopologyNames:
    if not 0 < configuration.timeout_seconds <= 30:
        raise ValueError("topology timeout must be positive and bounded")
    exchange_names = (
        configuration.dispatch_exchange_name,
        configuration.malformed_exchange_name,
    )
    if any(
        _TOPOLOGY_NAME.fullmatch(name) is None or name.startswith("amq.")
        for name in exchange_names
    ):
        raise ValueError("invalid RabbitMQ topology exchange name")
    if configuration.dispatch_exchange_name == configuration.malformed_exchange_name:
        raise ValueError("dispatch and malformed exchanges must be distinct")
    capability_queues = tuple(
        (
            capability,
            f"{configuration.dispatch_exchange_name}.{dispatch_route(capability)}",
        )
        for capability in sorted(registry.required_capabilities)
    )
    quarantine_queue = f"{configuration.malformed_exchange_name}.quarantine"
    for name in (
        *exchange_names,
        quarantine_queue,
        *(queue for _, queue in capability_queues),
    ):
        if len(name.encode("utf-8")) > MAX_RABBITMQ_NAME_BYTES:
            raise ValueError("derived RabbitMQ topology name is too long")
    return _TopologyNames(quarantine_queue, capability_queues)
