"""Deterministic Task-5 NDJSON envelope contracts."""

import hashlib
import json
from datetime import UTC, datetime
from uuid import UUID, uuid4

from taskforge.history.domain import HistoryCursor, HistoryFilters, HistoryRecordType
from taskforge.history.export import (
    EMPTY_RECORDS_SHA256,
    EXPORT_SCHEMA_VERSION,
    ExportState,
    canonical_timestamp,
    completion,
    manifest,
    ndjson_line,
)
from taskforge.identity.authorization import OwnerFilter


def _state(high_water: HistoryCursor | None) -> ExportState:
    return ExportState(
        "audit",
        None,
        HistoryFilters(),
        OwnerFilter.all_owners(),
        "a" * 64,
        datetime(2026, 8, 26, 12, 34, 56, 123456, tzinfo=UTC),
        high_water,
        uuid4(),
    )


def test_manifest_is_canonical_and_does_not_expose_audit_id() -> None:
    boundary = HistoryCursor(
        "audit",
        None,
        "",
        datetime(2026, 8, 26, 12, tzinfo=UTC),
        HistoryRecordType.AUDIT_RECORD,
        10,
        str(UUID("ffffffff-ffff-4fff-8fff-ffffffffffff")),
    )
    state = _state(boundary)
    value = manifest(state)
    assert value["generated_at"] == "2026-08-26T12:34:56.123456Z"
    assert value["high_water"] == {
        "occurred_at": "2026-08-26T12:00:00.000000Z",
        "source_rank": 10,
        "source_key": "ffffffff-ffff-4fff-8fff-ffffffffffff",
    }
    assert str(state.audit_record_id) not in ndjson_line(value).decode()


def test_empty_completion_digest_and_exact_record_bytes() -> None:
    assert EMPTY_RECORDS_SHA256 == (
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )
    assert completion(0, EMPTY_RECORDS_SHA256)["records_sha256"] == EMPTY_RECORDS_SHA256
    line = ndjson_line({"schema_version": EXPORT_SCHEMA_VERSION, "kind": "record"})
    assert line.endswith(b"\n")
    assert (
        hashlib.sha256(line).hexdigest()
        == hashlib.sha256(
            (
                json.dumps(json.loads(line), sort_keys=True, separators=(",", ":"))
                + "\n"
            ).encode()
        ).hexdigest()
    )


def test_timestamp_uses_exact_utc_microsecond_z_form() -> None:
    assert canonical_timestamp(datetime(2026, 8, 26, tzinfo=UTC)) == (
        "2026-08-26T00:00:00.000000Z"
    )
