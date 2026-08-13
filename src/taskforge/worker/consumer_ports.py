"""Broker-neutral manual delivery-consumption contracts."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from taskforge.dispatch.transport import DispatchTransportMetadata


class BrokerConsumerUnavailable(Exception):
    """The broker consumer or delivery disposition is unavailable."""


@dataclass(frozen=True, repr=False)
class BrokerDispatchDelivery:
    body: bytes
    metadata: DispatchTransportMetadata
    redelivered: bool

    def __repr__(self) -> str:
        return "BrokerDispatchDelivery(<redacted>)"


class DispatchDeliveryControl(Protocol):
    @property
    def delivery(self) -> BrokerDispatchDelivery: ...

    async def acknowledge(self) -> None: ...

    async def reject(self, *, requeue: bool) -> None: ...


type DispatchDeliveryHandler = Callable[[DispatchDeliveryControl], Awaitable[None]]


class DispatchConsumer(Protocol):
    async def consume(self, handler: DispatchDeliveryHandler) -> str: ...

    async def cancel(self, consumer_tag: str) -> None: ...
