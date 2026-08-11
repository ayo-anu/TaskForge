"""Domain results for task-attempt claim acquisition and replay."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID

_RESULT_AUTHORITY_PATTERN = re.compile(
    r"tf_claim_result_v1\.[A-Za-z0-9_-]{43}", re.ASCII
)


class TaskClaimOutcome(StrEnum):
    ACQUIRED_ACTIVE = "acquired_active"
    REPLAYED_ACTIVE = "replayed_active"
    REPLAYED_EXPIRED = "replayed_expired"


class TaskClaimRenewalOutcome(StrEnum):
    RENEWED = "renewed"
    ACTIVE_UNCHANGED = "active_unchanged"
    REPLAYED = "replayed"


@dataclass(frozen=True, repr=False)
class TaskClaimResultAuthority:
    presented_value: str

    def __post_init__(self) -> None:
        if _RESULT_AUTHORITY_PATTERN.fullmatch(self.presented_value) is None:
            raise ValueError("invalid claim result authority")

    def __repr__(self) -> str:
        return "TaskClaimResultAuthority(presented_value=<redacted>)"

    def __str__(self) -> str:
        return "<redacted claim result authority>"


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


@dataclass(frozen=True)
class IssuedTaskClaim:
    outcome: TaskClaimOutcome
    claim: TaskClaimLease
    result_authority: TaskClaimResultAuthority | None

    def __post_init__(self) -> None:
        active = self.outcome in (
            TaskClaimOutcome.ACQUIRED_ACTIVE,
            TaskClaimOutcome.REPLAYED_ACTIVE,
        )
        if active is (self.result_authority is None):
            raise ValueError("active claim outcomes require result authority")


@dataclass(frozen=True)
class TaskClaimRenewalRequest:
    task_attempt_id: UUID
    generation: int
    worker_session_id: UUID
    expected_lease_expires_at: datetime

    def __post_init__(self) -> None:
        if self.generation <= 0:
            raise ValueError("claim generation must be positive")
        value = self.expected_lease_expires_at
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("expected lease expiry must be timezone-aware")
        object.__setattr__(self, "expected_lease_expires_at", value.astimezone(UTC))


@dataclass(frozen=True)
class TaskClaimRenewalResult:
    outcome: TaskClaimRenewalOutcome
    claim: TaskClaimLease
