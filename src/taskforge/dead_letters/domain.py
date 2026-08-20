"""Dead-letter inspection and operator-transition domain values."""

from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID

from taskforge.retries.domain import RetryNotScheduledReason
from taskforge.worker.results import TaskExecutionFailureKind, TaskExecutionResultKind


class DeadLetterReason(StrEnum):
    PERMANENT_FAILURE = "permanent_failure"
    RETRY_EXHAUSTED = "retry_exhausted"


class DeadLetterStatus(StrEnum):
    OPEN = "open"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"


class DeadLetterActionType(StrEnum):
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"


class InvalidDeadLetterRedriveIdempotencyKey(ValueError):
    """A redrive idempotency key is not a bounded opaque token."""


class DeadLetterRedriveIdempotencyConflict(Exception):
    """A scoped redrive key was reused for a different request."""


@dataclass(frozen=True, repr=False)
class DeadLetterRedriveIdempotency:
    key_digest: str
    request_fingerprint: str

    def __repr__(self) -> str:
        return (
            "DeadLetterRedriveIdempotency(key_digest=<redacted>, "
            "request_fingerprint=<redacted>)"
        )


@dataclass(frozen=True)
class DeadLetterRedriveSummary:
    id: UUID
    target_workflow_run_id: UUID
    requested_by_principal_id: UUID
    reason: str | None
    requested_at: datetime
    target_workflow_run_status: str


@dataclass(frozen=True)
class CreatedDeadLetterRedrive:
    id: UUID
    dead_letter_item_id: UUID
    source_workflow_run_id: UUID
    source_task_run_id: UUID
    source_task_attempt_id: UUID
    target_workflow_run_id: UUID
    workflow_definition_id: UUID
    workflow_version_id: UUID
    requested_by_principal_id: UUID
    reason: str | None
    requested_at: datetime


@dataclass(frozen=True)
class DeadLetterFilters:
    status: DeadLetterStatus | None = None
    reason: DeadLetterReason | None = None
    task_run_id: UUID | None = None
    workflow_run_id: UUID | None = None
    source_task_attempt_id: UUID | None = None
    created_after: datetime | None = None
    created_before: datetime | None = None

    def __post_init__(self) -> None:
        for field in ("created_after", "created_before"):
            value = getattr(self, field)
            if value is not None:
                if value.tzinfo is None:
                    raise ValueError("dead-letter filter timestamps must be aware")
                object.__setattr__(self, field, value.astimezone(UTC))
        if (
            self.created_after is not None
            and self.created_before is not None
            and self.created_after >= self.created_before
        ):
            raise ValueError("created_after must precede created_before")


@dataclass(frozen=True)
class DeadLetterCursor:
    created_at: datetime
    item_id: UUID

    def __post_init__(self) -> None:
        if self.created_at.tzinfo is None:
            raise ValueError("dead-letter cursor timestamp must be aware")
        object.__setattr__(self, "created_at", self.created_at.astimezone(UTC))


@dataclass(frozen=True)
class DeadLetterActionCursor:
    occurred_at: datetime
    action_id: UUID

    def __post_init__(self) -> None:
        if self.occurred_at.tzinfo is None:
            raise ValueError("dead-letter action cursor timestamp must be aware")
        object.__setattr__(self, "occurred_at", self.occurred_at.astimezone(UTC))


@dataclass(frozen=True)
class DeadLetterSummary:
    id: UUID
    task_run_id: UUID
    source_task_attempt_id: UUID
    workflow_run_id: UUID
    reason: DeadLetterReason
    status: DeadLetterStatus
    created_at: datetime
    status_updated_at: datetime
    source_attempt_number: int


@dataclass(frozen=True)
class DeadLetterDetail(DeadLetterSummary):
    workflow_definition_id: UUID
    workflow_version_id: UUID
    step_identifier: str
    result_kind: TaskExecutionResultKind
    failure_kind: TaskExecutionFailureKind | None
    retry_decision_reason: RetryNotScheduledReason | None
    redrive: DeadLetterRedriveSummary | None = None


@dataclass(frozen=True)
class DeadLetterPage:
    items: tuple[DeadLetterSummary, ...]
    next_cursor: DeadLetterCursor | None


@dataclass(frozen=True)
class DeadLetterOperatorAction:
    id: UUID
    dead_letter_item_id: UUID
    operator_principal_id: UUID
    action_type: DeadLetterActionType
    previous_status: DeadLetterStatus
    new_status: DeadLetterStatus
    reason: str | None
    correlation_id: UUID | None
    occurred_at: datetime


@dataclass(frozen=True)
class DeadLetterActionPage:
    items: tuple[DeadLetterOperatorAction, ...]
    next_cursor: DeadLetterActionCursor | None


def create_dead_letter_redrive_idempotency(
    key: object,
    *,
    dead_letter_item_id: UUID,
    requested_by_principal_id: UUID,
    reason: str | None,
) -> DeadLetterRedriveIdempotency:
    """Validate an opaque key and fingerprint one normalized redrive command."""
    if (
        not isinstance(key, str)
        or not 16 <= len(key) <= 128
        or any(not 0x21 <= ord(character) <= 0x7E for character in key)
    ):
        raise InvalidDeadLetterRedriveIdempotencyKey
    normalized_reason = reason.strip() if reason is not None else None
    normalized = {
        "dead_letter_item_id": str(dead_letter_item_id),
        "operation": "dead_letter_redrive",
        "reason": normalized_reason,
        "requested_by_principal_id": str(requested_by_principal_id),
        "schema_version": 1,
    }
    encoded = json.dumps(
        normalized,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return DeadLetterRedriveIdempotency(
        key_digest=_sha256(b"taskforge:dead-letter-redrive-key:v1\0" + key.encode()),
        request_fingerprint=_sha256(
            b"taskforge:dead-letter-redrive-request:v1\0" + encoded
        ),
    )


def redrive_fingerprints_match(left: str, right: str) -> bool:
    return hmac.compare_digest(left, right)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()
