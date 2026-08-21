"""Cooperative, advisory cancellation delivery for trusted task handlers."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol
from uuid import UUID

from taskforge.identity.authentication import AuthenticatedWorker


class TaskCancellationObservationOutcome(StrEnum):
    ACTIVE = "active"
    CANCELLATION_REQUESTED = "cancellation_requested"
    NO_LONGER_AUTHORITATIVE = "no_longer_authoritative"


@dataclass(frozen=True)
class TaskCancellationObservation:
    outcome: TaskCancellationObservationOutcome
    requested_at: datetime | None = None

    def __post_init__(self) -> None:
        requested = (
            self.outcome is TaskCancellationObservationOutcome.CANCELLATION_REQUESTED
        )
        if requested is (self.requested_at is None):
            raise ValueError("cancellation observation and timestamp disagree")
        if self.requested_at is not None:
            if (
                self.requested_at.tzinfo is None
                or self.requested_at.utcoffset() is None
            ):
                raise ValueError("cancellation observation time must be aware")
            object.__setattr__(self, "requested_at", self.requested_at.astimezone(UTC))


class TaskCancellationObservationInvariantError(Exception): ...


class TaskCancellationObservationUnavailable(Exception): ...


class TaskCancellationObserver(Protocol):
    async def observe_cancellation(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        workflow_run_id: UUID,
        task_run_id: UUID,
        task_attempt_id: UUID,
        claim_generation: int,
    ) -> TaskCancellationObservation: ...


class TaskCancellationToken:
    """A monotonic in-process view of durable workflow cancellation intent."""

    def __init__(self) -> None:
        self._event = asyncio.Event()
        self._requested_at: datetime | None = None

    @property
    def is_cancellation_requested(self) -> bool:
        return self._event.is_set()

    @property
    def requested_at(self) -> datetime | None:
        return self._requested_at

    async def wait(self) -> datetime:
        await self._event.wait()
        assert self._requested_at is not None
        return self._requested_at

    def _request(self, requested_at: datetime) -> None:
        if requested_at.tzinfo is None or requested_at.utcoffset() is None:
            raise ValueError("cancellation request time must be aware")
        if self._event.is_set():
            return
        self._requested_at = requested_at.astimezone(UTC)
        self._event.set()
