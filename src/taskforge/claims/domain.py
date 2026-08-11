"""Domain results for task-attempt claim acquisition and replay."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID


class TaskClaimOutcome(StrEnum):
    ACQUIRED_ACTIVE = "acquired_active"
    REPLAYED_ACTIVE = "replayed_active"
    REPLAYED_EXPIRED = "replayed_expired"


@dataclass(frozen=True)
class TaskClaimLease:
    task_attempt_id: UUID
    generation: int
    worker_session_id: UUID
    acquired_at: datetime
    lease_expires_at: datetime

    def __post_init__(self) -> None:
        if self.generation <= 0:
            raise ValueError("claim generation must be positive")
        for field in ("acquired_at", "lease_expires_at"):
            value = getattr(self, field)
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("claim timestamps must be timezone-aware")
            object.__setattr__(self, field, value.astimezone(UTC))
        if self.lease_expires_at <= self.acquired_at:
            raise ValueError("claim lease must expire after acquisition")


@dataclass(frozen=True)
class TaskClaimResult:
    outcome: TaskClaimOutcome
    claim: TaskClaimLease
