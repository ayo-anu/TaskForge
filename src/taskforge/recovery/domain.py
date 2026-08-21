"""Read-only observations used to revalidate crash-recovery candidates."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from taskforge.runs.domain import TaskRunStatus

MAX_RECOVERY_SCAN_BATCH_SIZE = 100

type JSONValue = (
    bool | int | float | str | list[JSONValue] | dict[str, JSONValue] | None
)
type JSONMapping = dict[str, JSONValue]


@dataclass(frozen=True)
class PreparedExpiredClaimRecovery:
    task_attempt_id: UUID
    task_run_id: UUID
    workflow_run_id: UUID
    previous_task_status: TaskRunStatus
    attempt_number: int
    generation: int
    worker_session_id: UUID
    dispatch_id: UUID
    lease_expires_at: datetime
    recovered_at: datetime
    workflow_execution_policy: JSONMapping | None
    step_execution_policy: JSONMapping | None

    def __post_init__(self) -> None:
        if self.attempt_number <= 0 or self.generation <= 0:
            raise ValueError("prepared recovery numbers must be positive")
        lease_expires_at = _utc(self.lease_expires_at, field="prepared lease expiry")
        recovered_at = _utc(self.recovered_at, field="recovery time")
        if lease_expires_at > recovered_at:
            raise ValueError("prepared claim must be expired")
        object.__setattr__(self, "lease_expires_at", lease_expires_at)
        object.__setattr__(
            self,
            "recovered_at",
            recovered_at,
        )


@dataclass(frozen=True)
class PreparedCancellationSettlement:
    """An expired authoritative claim that must settle as cancellation."""

    task_attempt_id: UUID
    task_run_id: UUID
    workflow_run_id: UUID
    previous_task_status: TaskRunStatus
    attempt_number: int
    generation: int
    worker_session_id: UUID
    dispatch_id: UUID
    lease_expires_at: datetime
    recovered_at: datetime

    def __post_init__(self) -> None:
        if self.attempt_number <= 0 or self.generation <= 0:
            raise ValueError("prepared settlement numbers must be positive")
        lease_expires_at = _utc(self.lease_expires_at, field="prepared lease expiry")
        recovered_at = _utc(self.recovered_at, field="settlement time")
        if lease_expires_at > recovered_at:
            raise ValueError("prepared claim must be expired")
        object.__setattr__(self, "lease_expires_at", lease_expires_at)
        object.__setattr__(self, "recovered_at", recovered_at)


def _utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


@dataclass(frozen=True)
class ExpiredClaimCandidate:
    task_attempt_id: UUID
    task_run_id: UUID
    workflow_run_id: UUID
    attempt_number: int
    generation: int
    worker_session_id: UUID
    lease_expires_at: datetime
    observed_at: datetime

    def __post_init__(self) -> None:
        if self.attempt_number <= 0:
            raise ValueError("claim candidate attempt number must be positive")
        if self.generation <= 0:
            raise ValueError("claim candidate generation must be positive")
        lease_expires_at = _utc(self.lease_expires_at, field="lease expiry")
        observed_at = _utc(self.observed_at, field="claim observation time")
        if lease_expires_at > observed_at:
            raise ValueError("claim candidate must be expired when observed")
        object.__setattr__(self, "lease_expires_at", lease_expires_at)
        object.__setattr__(self, "observed_at", observed_at)


@dataclass(frozen=True)
class ExpiredClaimScanCursor:
    observed_at: datetime
    lease_expires_at: datetime
    task_attempt_id: UUID
    generation: int

    def __post_init__(self) -> None:
        if self.generation <= 0:
            raise ValueError("claim cursor generation must be positive")
        observed_at = _utc(self.observed_at, field="claim cursor observation time")
        lease_expires_at = _utc(
            self.lease_expires_at, field="claim cursor lease expiry"
        )
        if lease_expires_at > observed_at:
            raise ValueError("claim cursor must identify an expired observation")
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "lease_expires_at", lease_expires_at)


@dataclass(frozen=True)
class ExpiredClaimCandidatePage:
    items: tuple[ExpiredClaimCandidate, ...]
    observed_at: datetime
    next_cursor: ExpiredClaimScanCursor | None

    def __post_init__(self) -> None:
        observed_at = _utc(self.observed_at, field="claim page observation time")
        if any(item.observed_at != observed_at for item in self.items):
            raise ValueError("claim candidates must share the page observation time")
        if self.next_cursor is not None and self.next_cursor.observed_at != observed_at:
            raise ValueError("claim cursor must preserve the page observation time")
        object.__setattr__(self, "observed_at", observed_at)


@dataclass(frozen=True)
class StaleWorkerSessionCandidate:
    worker_session_id: UUID
    worker_identity_id: UUID
    last_sequence: int
    last_seen_at: datetime
    accepting_work: bool
    observed_at: datetime

    def __post_init__(self) -> None:
        if self.last_sequence < 0:
            raise ValueError("stale session sequence must be nonnegative")
        if not isinstance(self.accepting_work, bool):
            raise ValueError("stale session availability must be boolean")
        last_seen_at = _utc(self.last_seen_at, field="session last-seen time")
        observed_at = _utc(self.observed_at, field="session observation time")
        if last_seen_at > observed_at:
            raise ValueError("session cannot be observed before its last-seen time")
        object.__setattr__(self, "last_seen_at", last_seen_at)
        object.__setattr__(self, "observed_at", observed_at)


@dataclass(frozen=True)
class StaleWorkerSessionScanCursor:
    observed_at: datetime
    last_seen_at: datetime
    worker_session_id: UUID
    stale_after_seconds: int

    def __post_init__(self) -> None:
        if not 1 <= self.stale_after_seconds <= 3600:
            raise ValueError("stale-session cursor threshold is out of range")
        observed_at = _utc(self.observed_at, field="session cursor observation time")
        last_seen_at = _utc(self.last_seen_at, field="session cursor last-seen time")
        if last_seen_at > observed_at:
            raise ValueError("session cursor last-seen time cannot be in the future")
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "last_seen_at", last_seen_at)


@dataclass(frozen=True)
class StaleWorkerSessionCandidatePage:
    items: tuple[StaleWorkerSessionCandidate, ...]
    observed_at: datetime
    stale_after_seconds: int
    next_cursor: StaleWorkerSessionScanCursor | None

    def __post_init__(self) -> None:
        if not 1 <= self.stale_after_seconds <= 3600:
            raise ValueError("stale-session page threshold is out of range")
        observed_at = _utc(self.observed_at, field="session page observation time")
        if any(item.observed_at != observed_at for item in self.items):
            raise ValueError("stale sessions must share the page observation time")
        if self.next_cursor is not None and (
            self.next_cursor.observed_at != observed_at
            or self.next_cursor.stale_after_seconds != self.stale_after_seconds
        ):
            raise ValueError("stale-session cursor does not match its page")
        object.__setattr__(self, "observed_at", observed_at)
