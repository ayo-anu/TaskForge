"""Transport-neutral worker registration contracts and validation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from taskforge.capabilities import is_valid_capability_name

MAX_WORKER_CAPABILITIES = 128


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
class RegisteredWorkerSession:
    id: UUID
    registered_at: datetime
    capabilities: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.registered_at.tzinfo is None or self.registered_at.utcoffset() is None:
            raise ValueError("registration timestamp must be timezone-aware")
        object.__setattr__(self, "registered_at", self.registered_at.astimezone(UTC))


def validate_worker_registration(
    capabilities: tuple[str, ...],
    *,
    known_capabilities: frozenset[str],
) -> WorkerRegistration:
    """Validate and deterministically canonicalize one complete advertisement."""
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
    return WorkerRegistration(tuple(sorted(first_positions)))
