"""Stable filtering, ordering, and cursor contracts for history reads."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from taskforge.audit.domain import AuditAction, AuditActorKind, AuditOutcome
from taskforge.correlation import is_valid_correlation_id

MAX_CURSOR_LENGTH = 2048
MAX_CURSOR_BYTES = 1536
CURSOR_VERSION = 1


class HistoryRecordType(StrEnum):
    EXECUTION_EVENT = "execution_event"
    CLAIM_EVENT = "claim_event"
    RESULT_EVENT = "result_event"
    RETRY_EVENT = "retry_event"
    CANCELLATION_REQUESTED = "cancellation_requested"
    REPLAY_CREATED = "replay_created"
    DEAD_LETTER_CREATED = "dead_letter_created"
    DEAD_LETTER_ACTION = "dead_letter_action"
    DEAD_LETTER_REDRIVE = "dead_letter_redrive"
    HEARTBEAT = "heartbeat"
    AUDIT_RECORD = "audit_record"


SOURCE_RANKS = {
    HistoryRecordType.EXECUTION_EVENT: 110,
    HistoryRecordType.CLAIM_EVENT: 100,
    HistoryRecordType.RESULT_EVENT: 90,
    HistoryRecordType.RETRY_EVENT: 80,
    HistoryRecordType.CANCELLATION_REQUESTED: 70,
    HistoryRecordType.REPLAY_CREATED: 60,
    HistoryRecordType.DEAD_LETTER_CREATED: 50,
    HistoryRecordType.DEAD_LETTER_ACTION: 40,
    HistoryRecordType.DEAD_LETTER_REDRIVE: 30,
    HistoryRecordType.HEARTBEAT: 20,
    HistoryRecordType.AUDIT_RECORD: 10,
}

if len(SOURCE_RANKS) != len(HistoryRecordType) or len(
    set(SOURCE_RANKS.values())
) != len(SOURCE_RANKS):
    raise RuntimeError("history source ranks must be complete and unique")


def uuid_source_key(value: UUID) -> str:
    return str(value)


def heartbeat_source_key(session_id: UUID, sequence: int) -> str:
    if not 1 <= sequence <= 9_223_372_036_854_775_807:
        raise ValueError("heartbeat sequence is invalid")
    return f"{session_id}:{sequence:020d}"


@dataclass(frozen=True)
class HistoryFilters:
    """Closed, normalized filters supported by authorized history queries."""

    record_type: HistoryRecordType | None = None
    resource_type: str | None = None
    resource_id: UUID | None = None
    action: AuditAction | None = None
    outcome: AuditOutcome | None = None
    actor_kind: AuditActorKind | None = None
    actor_id: UUID | None = None
    system_component: str | None = None
    correlation_id: str | None = None
    reason_code: str | None = None
    occurred_from: datetime | None = None
    occurred_to: datetime | None = None

    def __post_init__(self) -> None:
        if self.resource_id is not None and self.resource_type is None:
            raise ValueError("resource_id requires resource_type")
        if self.resource_type is not None and (
            not self.resource_type
            or len(self.resource_type) > 64
            or any(
                character not in "abcdefghijklmnopqrstuvwxyz0123456789_.-"
                for character in self.resource_type
            )
        ):
            raise ValueError("resource_type is invalid")
        if self.actor_id is not None and self.actor_kind not in {
            AuditActorKind.API_PRINCIPAL,
            AuditActorKind.WORKER,
        }:
            raise ValueError("actor_id requires an API-principal or worker actor kind")
        if (
            self.system_component is not None
            and self.actor_kind is not AuditActorKind.SYSTEM
        ):
            raise ValueError("system_component requires the system actor kind")
        if self.system_component is not None and (
            not self.system_component
            or len(self.system_component) > 32
            or any(
                character not in "abcdefghijklmnopqrstuvwxyz0123456789_.-"
                for character in self.system_component
            )
        ):
            raise ValueError("system_component is invalid")
        if self.correlation_id is not None and not is_valid_correlation_id(
            self.correlation_id
        ):
            raise ValueError("correlation_id is invalid")
        if self.reason_code is not None and (
            not self.reason_code
            or len(self.reason_code) > 128
            or any(
                character not in "abcdefghijklmnopqrstuvwxyz0123456789_.-"
                for character in self.reason_code
            )
        ):
            raise ValueError("reason_code is invalid")
        for value in (self.occurred_from, self.occurred_to):
            if value is not None and (
                value.tzinfo is None or value.utcoffset() is None
            ):
                raise ValueError("history timestamp bounds must be timezone-aware")
        if (
            self.occurred_from is not None
            and self.occurred_to is not None
            and self.occurred_from > self.occurred_to
        ):
            raise ValueError("occurred_from must not follow occurred_to")

    def normalized(self) -> dict[str, object]:
        return {
            "record_type": self.record_type.value if self.record_type else None,
            "resource_type": self.resource_type,
            "resource_id": str(self.resource_id) if self.resource_id else None,
            "action": self.action.value if self.action else None,
            "outcome": self.outcome.value if self.outcome else None,
            "actor_kind": self.actor_kind.value if self.actor_kind else None,
            "actor_id": str(self.actor_id) if self.actor_id else None,
            "system_component": self.system_component,
            "correlation_id": self.correlation_id,
            "reason_code": self.reason_code,
            "occurred_from": _canonical_time(self.occurred_from),
            "occurred_to": _canonical_time(self.occurred_to),
        }


def _canonical_time(value: datetime | None) -> str | None:
    if value is None:
        return None
    return (
        value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    )


def validate_source_key(record_type: HistoryRecordType, value: str) -> None:
    if record_type is HistoryRecordType.HEARTBEAT:
        if len(value) != 57 or value[36] != ":" or not value[37:].isdigit():
            raise ValueError("invalid heartbeat source key")
        session = UUID(value[:36])
        sequence = int(value[37:])
        if heartbeat_source_key(session, sequence) != value:
            raise ValueError("noncanonical heartbeat source key")
    else:
        parsed = UUID(value)
        if str(parsed) != value:
            raise ValueError("noncanonical UUID source key")


@dataclass(frozen=True)
class HistoryCursor:
    scope_type: str
    scope_id: UUID | None
    filter_fingerprint: str
    occurred_at: datetime
    record_type: HistoryRecordType
    source_rank: int
    source_key: str

    def __post_init__(self) -> None:
        if self.occurred_at.tzinfo is None or self.occurred_at.utcoffset() is None:
            raise ValueError("cursor timestamp must be timezone-aware")
        if SOURCE_RANKS[self.record_type] != self.source_rank:
            raise ValueError("cursor source rank does not match record type")
        validate_source_key(self.record_type, self.source_key)


def filter_fingerprint(values: dict[str, object]) -> str:
    encoded = json.dumps(values, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode()).hexdigest()


def encode_cursor(cursor: HistoryCursor) -> str:
    payload = {
        "v": CURSOR_VERSION,
        "scope_type": cursor.scope_type,
        "scope_id": str(cursor.scope_id) if cursor.scope_id else None,
        "filter": cursor.filter_fingerprint,
        "occurred_at": cursor.occurred_at.astimezone(UTC)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z"),
        "record_type": cursor.record_type.value,
        "source_rank": cursor.source_rank,
        "source_key": cursor.source_key,
    }
    return (
        base64.urlsafe_b64encode(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        )
        .rstrip(b"=")
        .decode("ascii")
    )


def decode_cursor(
    value: str, *, scope_type: str, scope_id: UUID | None, fingerprint: str
) -> HistoryCursor:
    if not value or len(value) > MAX_CURSOR_LENGTH or not value.isascii():
        raise ValueError("invalid cursor")
    try:
        raw = base64.b64decode(
            value + "=" * (-len(value) % 4), altchars=b"-_", validate=True
        )
        if len(raw) > MAX_CURSOR_BYTES:
            raise ValueError("invalid cursor")
        payload = json.loads(raw)
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("invalid cursor") from error
    keys = {
        "v",
        "scope_type",
        "scope_id",
        "filter",
        "occurred_at",
        "record_type",
        "source_rank",
        "source_key",
    }
    if (
        not isinstance(payload, dict)
        or set(payload) != keys
        or type(payload["v"]) is not int
        or payload["v"] != CURSOR_VERSION
    ):
        raise ValueError("invalid cursor")
    if type(payload["source_rank"]) is not int or not all(
        isinstance(payload[k], str)
        for k in ("scope_type", "filter", "occurred_at", "record_type", "source_key")
    ):
        raise ValueError("invalid cursor")
    if payload["scope_id"] is not None and not isinstance(payload["scope_id"], str):
        raise ValueError("invalid cursor")
    try:
        parsed_scope = UUID(payload["scope_id"]) if payload["scope_id"] else None
        if parsed_scope is not None and str(parsed_scope) != payload["scope_id"]:
            raise ValueError("noncanonical cursor scope")
        occurred = datetime.fromisoformat(payload["occurred_at"].replace("Z", "+00:00"))
    except (ValueError, TypeError) as error:
        raise ValueError("invalid cursor") from error
    if (
        occurred.astimezone(UTC)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
        != payload["occurred_at"]
    ):
        raise ValueError("noncanonical cursor timestamp")
    cursor = HistoryCursor(
        payload["scope_type"],
        parsed_scope,
        payload["filter"],
        occurred,
        HistoryRecordType(payload["record_type"]),
        payload["source_rank"],
        payload["source_key"],
    )
    if (cursor.scope_type, cursor.scope_id, cursor.filter_fingerprint) != (
        scope_type,
        scope_id,
        fingerprint,
    ):
        raise ValueError("cursor scope does not match request")
    return cursor


@dataclass(frozen=True)
class HistoryItem:
    record_type: HistoryRecordType
    occurred_at: datetime
    source_rank: int
    source_key: str
    correlation_id: str | None
    data: dict[str, Any]


@dataclass(frozen=True)
class HistoryPage:
    items: tuple[HistoryItem, ...]
    next_cursor: HistoryCursor | None
