"""Transport-neutral worker registration contracts and validation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID

from taskforge.capabilities import is_valid_capability_name

MAX_WORKER_CAPABILITIES = 128
MAX_HEARTBEAT_SEQUENCE = 9_223_372_036_854_775_807


@dataclass(frozen=True)
class WorkerRegistrationIssue:
    code: str
    path: tuple[str | int, ...]
    message: str


class InvalidWorkerRegistration(ValueError):
    def __init__(self, issues: tuple[WorkerRegistrationIssue, ...]) -> None:
        if not issues:
            raise ValueError("at least one registration issue is required")
        self.issues = issues
        super().__init__("worker registration is invalid")


@dataclass(frozen=True)
class WorkerRegistration:
    capabilities: tuple[str, ...]


@dataclass(frozen=True)
class WorkerCapabilityReplacement:
    """Complete session advertisement affecting future claim checks only."""

    capabilities: tuple[str, ...]


@dataclass(frozen=True)
class ReplacedWorkerCapabilities:
    worker_session_id: UUID
    capabilities: tuple[str, ...]


@dataclass(frozen=True)
class RegisteredWorkerSession:
    id: UUID
    registered_at: datetime
    capabilities: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.registered_at.tzinfo is None or self.registered_at.utcoffset() is None:
            raise ValueError("registration timestamp must be timezone-aware")
        object.__setattr__(self, "registered_at", self.registered_at.astimezone(UTC))


@dataclass(frozen=True)
class WorkerHeartbeat:
    sequence: int
    accepting_work: bool

    def __post_init__(self) -> None:
        if (
            isinstance(self.sequence, bool)
            or self.sequence < 1
            or self.sequence > MAX_HEARTBEAT_SEQUENCE
        ):
            raise ValueError("heartbeat sequence must be a positive BIGINT")
        if not isinstance(self.accepting_work, bool):
            raise ValueError("heartbeat availability must be boolean")


@dataclass(frozen=True)
class WorkerHealthProjection:
    worker_session_id: UUID
    last_sequence: int
    last_seen_at: datetime
    accepting_work: bool
    availability_changed_at: datetime

    def __post_init__(self) -> None:
        if self.last_sequence < 0:
            raise ValueError("health sequence must be nonnegative")
        for field in ("last_seen_at", "availability_changed_at"):
            value = getattr(self, field)
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("health timestamps must be timezone-aware")
            object.__setattr__(self, field, value.astimezone(UTC))


class WorkerSessionHealthStatus(StrEnum):
    HEALTHY = "healthy"
    STALE = "stale"
    OFFLINE = "offline"
    ENDED = "ended"


@dataclass(frozen=True)
class WorkerHealthThresholds:
    stale_after_seconds: int
    offline_after_seconds: int

    def __post_init__(self) -> None:
        if not 1 <= self.stale_after_seconds <= 3600:
            raise ValueError("stale threshold is out of range")
        if not 2 <= self.offline_after_seconds <= 86400:
            raise ValueError("offline threshold is out of range")
        if self.offline_after_seconds <= self.stale_after_seconds:
            raise ValueError("offline threshold must exceed stale threshold")


@dataclass(frozen=True)
class InspectedWorkerIdentity:
    id: UUID
    name: str
    enabled: bool


@dataclass(frozen=True)
class InspectedWorkerHealth:
    status: WorkerSessionHealthStatus
    last_sequence: int
    last_seen_at: datetime
    accepting_work: bool
    availability_changed_at: datetime


@dataclass(frozen=True)
class InspectedWorkerSession:
    id: UUID
    identity: InspectedWorkerIdentity
    registered_at: datetime
    ended_at: datetime | None
    capabilities: tuple[str, ...]
    health: InspectedWorkerHealth


@dataclass(frozen=True)
class WorkerInspectionObservation:
    reference_time: datetime
    thresholds: WorkerHealthThresholds


@dataclass(frozen=True)
class InspectedWorkerSessionResource:
    session: InspectedWorkerSession
    observation: WorkerInspectionObservation


@dataclass(frozen=True)
class WorkerSessionPageCursor:
    reference_time: datetime
    last_seen_at: datetime
    worker_session_id: UUID
    worker_identity_id: UUID | None
    health_status: WorkerSessionHealthStatus | None
    thresholds: WorkerHealthThresholds


@dataclass(frozen=True)
class InspectedWorkerSessionPage:
    items: tuple[InspectedWorkerSession, ...]
    observation: WorkerInspectionObservation
    next_cursor: WorkerSessionPageCursor | None


@dataclass(frozen=True)
class InspectedWorkerHeartbeat:
    sequence: int
    received_at: datetime
    accepting_work: bool


@dataclass(frozen=True)
class InspectedWorkerHeartbeatPage:
    items: tuple[InspectedWorkerHeartbeat, ...]
    next_before_sequence: int | None


def validate_worker_registration(
    capabilities: tuple[str, ...],
    *,
    known_capabilities: frozenset[str],
) -> WorkerRegistration:
    """Validate one registration advertisement through the shared contract."""
    return WorkerRegistration(
        validate_worker_capabilities(
            capabilities, known_capabilities=known_capabilities
        )
    )


def validate_worker_capabilities(
    capabilities: tuple[str, ...],
    *,
    known_capabilities: frozenset[str],
) -> tuple[str, ...]:
    """Validate and deterministically canonicalize one complete capability set."""
    issues: list[WorkerRegistrationIssue] = []
    if len(capabilities) > MAX_WORKER_CAPABILITIES:
        issues.append(
            WorkerRegistrationIssue(
                "too_many_capabilities",
                ("capabilities",),
                f"At most {MAX_WORKER_CAPABILITIES} capabilities may be advertised.",
            )
        )

    first_positions: dict[str, int] = {}
    for index, capability in enumerate(capabilities):
        if not is_valid_capability_name(capability):
            issues.append(
                WorkerRegistrationIssue(
                    "invalid_capability",
                    ("capabilities", index),
                    "Capability name is invalid.",
                )
            )
            continue
        if capability in first_positions:
            issues.append(
                WorkerRegistrationIssue(
                    "duplicate_capability",
                    ("capabilities", index),
                    "Capability is advertised more than once.",
                )
            )
            continue
        first_positions[capability] = index
        if capability not in known_capabilities:
            issues.append(
                WorkerRegistrationIssue(
                    "unknown_capability",
                    ("capabilities", index),
                    "Capability is not registered by this service.",
                )
            )

    if issues:
        raise InvalidWorkerRegistration(tuple(issues))
    return tuple(sorted(first_positions))
