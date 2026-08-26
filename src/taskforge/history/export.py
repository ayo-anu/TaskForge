"""Stable, redacted TaskForge history-export envelope contracts."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from taskforge.history.domain import HistoryCursor, HistoryFilters
from taskforge.identity.authorization import OwnerFilter

EXPORT_SCHEMA_VERSION = "taskforge.history-export.v1"
REDACTION_PROFILE = "taskforge.history-redacted.v1"
EMPTY_RECORDS_SHA256 = hashlib.sha256(b"").hexdigest()
EXPORT_PAGE_SIZE = 100


@dataclass(frozen=True)
class ExportInitialization:
    generated_at: datetime
    high_water: HistoryCursor | None


@dataclass(frozen=True)
class ExportState:
    scope_type: str
    scope_id: UUID | None
    filters: HistoryFilters
    owner_filter: OwnerFilter
    filter_fingerprint: str
    generated_at: datetime
    high_water: HistoryCursor | None
    audit_record_id: UUID


def canonical_timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("export timestamp must be timezone-aware")
    return (
        value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    )


def manifest(state: ExportState) -> dict[str, Any]:
    scope_name = "audit" if state.scope_type == "audit" else "workflow_run"
    high_water = state.high_water
    return {
        "kind": "manifest",
        "schema_version": EXPORT_SCHEMA_VERSION,
        "generated_at": canonical_timestamp(state.generated_at),
        "scope": {
            "type": scope_name,
            "id": str(state.scope_id) if state.scope_id else None,
        },
        "filter_fingerprint": state.filter_fingerprint,
        "ordering": ["occurred_at_desc", "source_rank_desc", "source_key_desc"],
        "high_water": (
            {
                "occurred_at": canonical_timestamp(high_water.occurred_at),
                "source_rank": high_water.source_rank,
                "source_key": high_water.source_key,
            }
            if high_water
            else None
        ),
        "redaction_profile": REDACTION_PROFILE,
    }


def completion(record_count: int, digest: str) -> dict[str, Any]:
    return {
        "kind": "completion",
        "schema_version": EXPORT_SCHEMA_VERSION,
        "record_count": record_count,
        "records_sha256": digest,
    }


def ndjson_line(value: dict[str, Any]) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
