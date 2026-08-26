"""Domain results for task-attempt claim acquisition and replay."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID

from taskforge.correlation import is_valid_correlation_id
from taskforge.runs.domain import TaskRunStatus

_RESULT_AUTHORITY_PATTERN = re.compile(
    r"tf_claim_result_v1\.[A-Za-z0-9_-]{43}", re.ASCII
)


class TaskClaimOutcome(StrEnum):
    ACQUIRED_ACTIVE = "acquired_active"
    REPLAYED_ACTIVE = "replayed_active"
    REPLAYED_EXPIRED = "replayed_expired"


class TaskClaimRejectionReason(StrEnum):
    INVALID_DISPATCH = "invalid_dispatch"
    STALE_ATTEMPT = "stale_attempt"
    OBSOLETE_TASK = "obsolete_task"
    WORKER_AUTHORITY_REJECTED = "worker_authority_rejected"
    WORKER_SESSION_UNAVAILABLE = "worker_session_unavailable"
    WORKER_SESSION_INACTIVE = "worker_session_inactive"
    WORKER_UNAVAILABLE = "worker_unavailable"
    CAPABILITY_MISMATCH = "capability_mismatch"
    ALREADY_AUTHORITATIVE = "already_authoritative"


class TaskClaimEventType(StrEnum):
    CLAIM_ACQUIRED = "claim_acquired"
    LEASE_RENEWED = "lease_renewed"


class TaskClaimLeaseStatus(StrEnum):
    UNEXPIRED = "unexpired"
    EXPIRED = "expired"


class TaskClaimRejected(Exception):
    """An expected, identifier-free task acquisition denial."""

    def __init__(self, reason: TaskClaimRejectionReason) -> None:
        self.reason = reason
        super().__init__("task claim acquisition rejected")


class TaskClaimRenewalOutcome(StrEnum):
    RENEWED = "renewed"
    ACTIVE_UNCHANGED = "active_unchanged"
    REPLAYED = "replayed"
    CANCELLATION_REQUESTED = "cancellation_requested"


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
    correlation_id: str | None = None

    def __post_init__(self) -> None:
        if self.generation <= 0:
            raise ValueError("claim generation must be positive")
        value = self.expected_lease_expires_at
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("expected lease expiry must be timezone-aware")
        object.__setattr__(self, "expected_lease_expires_at", value.astimezone(UTC))
        if not is_valid_correlation_id(self.correlation_id):
            raise ValueError("renewal correlation ID is invalid")


@dataclass(frozen=True)
class TaskClaimRenewalResult:
    outcome: TaskClaimRenewalOutcome
    claim: TaskClaimLease
    cancellation_requested_at: datetime | None = None

    def __post_init__(self) -> None:
        requested = self.outcome is TaskClaimRenewalOutcome.CANCELLATION_REQUESTED
        if requested is (self.cancellation_requested_at is None):
            raise ValueError("renewal cancellation outcome and timestamp disagree")
        if self.cancellation_requested_at is not None:
            value = self.cancellation_requested_at
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("cancellation request time must be timezone-aware")
            object.__setattr__(self, "cancellation_requested_at", value.astimezone(UTC))


@dataclass(frozen=True)
class InspectedTaskClaim:
    task_attempt_id: UUID
    task_run_id: UUID
    workflow_run_id: UUID
    attempt_number: int
    generation: int
    worker_identity_id: UUID
    worker_session_id: UUID
    acquired_at: datetime
    lease_expires_at: datetime
    observed_at: datetime
    lease_status: TaskClaimLeaseStatus
    task_status: TaskRunStatus

    def __post_init__(self) -> None:
        if self.attempt_number <= 0 or self.generation <= 0:
            raise ValueError("claim inspection numbers must be positive")
        for field in ("acquired_at", "lease_expires_at", "observed_at"):
            value = getattr(self, field)
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("claim inspection timestamps must be timezone-aware")
            object.__setattr__(self, field, value.astimezone(UTC))
        expected = (
            TaskClaimLeaseStatus.UNEXPIRED
            if self.lease_expires_at > self.observed_at
            else TaskClaimLeaseStatus.EXPIRED
        )
        if self.lease_status is not expected:
            raise ValueError("claim lease status must match observed database time")
