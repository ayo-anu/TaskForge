"""Process-local orchestration discovery and supervision values."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID


@dataclass(frozen=True)
class DiscoveryHighWater:
    """The greatest immutable source key visible when one sweep begins."""

    created_at: datetime
    entity_id: UUID

    def __post_init__(self) -> None:
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise ValueError("discovery high-water timestamp must be timezone-aware")
        object.__setattr__(self, "created_at", self.created_at.astimezone(UTC))


@dataclass(frozen=True)
class DiscoveryCursor:
    """One process-local strict keyset cursor under a fixed high-water."""

    high_created_at: datetime
    high_id: UUID
    last_created_at: datetime
    last_id: UUID

    def __post_init__(self) -> None:
        for field in ("high_created_at", "last_created_at"):
            value = getattr(self, field)
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("discovery cursor timestamps must be timezone-aware")
            object.__setattr__(self, field, value.astimezone(UTC))
        if (self.last_created_at, self.last_id) > (
            self.high_created_at,
            self.high_id,
        ):
            raise ValueError("discovery cursor cannot pass its high-water")

    @property
    def high_water(self) -> DiscoveryHighWater:
        return DiscoveryHighWater(self.high_created_at, self.high_id)


@dataclass(frozen=True)
class ActiveWorkflowRunCandidate:
    workflow_run_id: UUID
    created_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "created_at", _utc(self.created_at))


@dataclass(frozen=True)
class RunnableTaskCandidate:
    workflow_run_id: UUID
    task_run_id: UUID
    created_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "created_at", _utc(self.created_at))


@dataclass(frozen=True)
class RetryPendingTaskCandidate:
    task_run_id: UUID
    created_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "created_at", _utc(self.created_at))


@dataclass(frozen=True)
class DiscoveryPage[T]:
    items: tuple[T, ...]
    next_cursor: DiscoveryCursor | None


@dataclass(frozen=True)
class WorkloadPassResult:
    candidates: int
    transitions: int
    has_more: bool

    def __post_init__(self) -> None:
        if self.candidates < 0 or self.transitions < 0:
            raise ValueError("workload pass counts cannot be negative")


class LoopExit(StrEnum):
    STOP_REQUESTED = "stop_requested"


class OrchestratorProcessFailure(RuntimeError):
    """A required production orchestrator workload or dependency failed."""


class OrchestratorPersistenceUnavailable(Exception):
    """Read-only orchestration candidate discovery is unavailable."""


class OrchestratorPersistenceInvariantError(Exception):
    """Candidate discovery returned structurally invalid durable data."""


class OrchestratorWorkloadInvariantError(Exception):
    """A bounded pass found durable state it cannot safely interpret."""


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("candidate timestamp must be timezone-aware")
    return value.astimezone(UTC)
