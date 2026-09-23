"""Broker-neutral ports for durable dispatch-outbox publication."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Protocol
from uuid import UUID


class DispatchOutboxPersistenceUnavailable(Exception):
    """Dispatch-outbox persistence is operationally unavailable."""


class DispatchPublicationInvariantConflict(Exception):
    """Durable dispatch state no longer matches the published snapshot."""


class DispatchAcknowledgementPersistenceFailure(Exception):
    """An accepted publication could not be durably acknowledged."""


class BrokerUnavailable(Exception):
    """The broker cannot currently accept publication work."""


class BrokerPublicationTimeout(Exception):
    """Broker acceptance was not confirmed before the deadline."""


class BrokerPublicationRejected(Exception):
    """The broker definitively rejected or returned the publication."""


@dataclass(frozen=True)
class UnpublishedDispatchCursor:
    created_at: datetime
    dispatch_id: UUID

    def __post_init__(self) -> None:
        if self.created_at.tzinfo is None:
            raise ValueError("dispatch cursor timestamp must be timezone-aware")
        object.__setattr__(self, "created_at", self.created_at.astimezone(UTC))


@dataclass(frozen=True, repr=False)
class StoredDispatch:
    dispatch_id: UUID
    task_attempt_id: UUID
    route: str
    payload: dict[str, object]
    created_at: datetime

    def __post_init__(self) -> None:
        if self.created_at.tzinfo is None:
            raise ValueError("stored dispatch timestamp must be timezone-aware")
        object.__setattr__(self, "created_at", self.created_at.astimezone(UTC))

    @property
    def cursor(self) -> UnpublishedDispatchCursor:
        return UnpublishedDispatchCursor(self.created_at, self.dispatch_id)

    def __repr__(self) -> str:
        return (
            "StoredDispatch("
            f"dispatch_id={self.dispatch_id!r}, "
            f"task_attempt_id={self.task_attempt_id!r}, "
            "route=<redacted>, payload=<redacted>, "
            f"created_at={self.created_at!r})"
        )


@dataclass(frozen=True)
class OutboxBacklogObservation:
    """A bounded unpublished count plus an exact indexed oldest timestamp."""

    pending: int
    saturated: bool
    oldest_created_at: datetime | None
    observed_at: datetime


@dataclass(frozen=True, repr=False)
class BrokerDispatchPublication:
    dispatch_id: UUID
    route: str
    body: bytes

    def __repr__(self) -> str:
        return (
            "BrokerDispatchPublication("
            f"dispatch_id={self.dispatch_id!r}, "
            "route=<redacted>, body=<redacted>)"
        )


class PublicationAcknowledgement(Enum):
    RECORDED = "recorded"
    ALREADY_RECORDED = "already_recorded"


@dataclass(frozen=True)
class StartupReplayPage:
    """One bounded page from a process-start outbox replay sweep."""

    records: tuple[StoredDispatch, ...]
    next_cursor: UnpublishedDispatchCursor | None


@dataclass(frozen=True)
class StartupReplayHighWater:
    """Finite outbox key and publication-time boundary captured at process start."""

    cursor: UnpublishedDispatchCursor
    captured_at: datetime


class DispatchOutboxRepository(Protocol):
    async def capture_startup_replay_high_water(
        self,
    ) -> StartupReplayHighWater | None: ...

    async def list_startup_replay_page(
        self,
        *,
        high_water: StartupReplayHighWater,
        after: UnpublishedDispatchCursor | None,
        limit: int,
    ) -> StartupReplayPage: ...

    async def list_unpublished_page(
        self,
        *,
        after: UnpublishedDispatchCursor | None,
        limit: int,
    ) -> tuple[StoredDispatch, ...]: ...

    async def record_accepted_publication(
        self, expected: StoredDispatch
    ) -> PublicationAcknowledgement: ...

    async def observe_unpublished_backlog(
        self, *, limit: int
    ) -> OutboxBacklogObservation: ...


class DispatchBrokerPublisher(Protocol):
    async def publish(self, publication: BrokerDispatchPublication) -> None: ...
