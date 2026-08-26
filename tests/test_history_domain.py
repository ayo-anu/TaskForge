"""Closed history filtering, ordering, and cursor contracts."""

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from taskforge.audit.domain import AuditAction, AuditActorKind
from taskforge.history.domain import (
    SOURCE_RANKS,
    HistoryCursor,
    HistoryFilters,
    HistoryRecordType,
    decode_cursor,
    encode_cursor,
    filter_fingerprint,
    heartbeat_source_key,
    uuid_source_key,
)


def test_source_ranks_are_fixed_complete_and_unique() -> None:
    assert SOURCE_RANKS == {
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
    assert len(set(SOURCE_RANKS.values())) == len(HistoryRecordType)


def test_source_keys_are_canonical_text() -> None:
    identifier = UUID("ABCDEFAB-CDEF-4ABC-8DEF-ABCDEFABCDEF")
    assert uuid_source_key(identifier) == "abcdefab-cdef-4abc-8def-abcdefabcdef"
    assert (
        heartbeat_source_key(identifier, 42)
        == "abcdefab-cdef-4abc-8def-abcdefabcdef:00000000000000000042"
    )


def test_cursor_round_trip_binds_scope_and_normalized_filters() -> None:
    scope_id = uuid4()
    filters = HistoryFilters(
        action=AuditAction.WORKFLOW_PUBLISH,
        actor_kind=AuditActorKind.API_PRINCIPAL,
        actor_id=uuid4(),
    )
    fingerprint = filter_fingerprint(filters.normalized())
    cursor = HistoryCursor(
        "workflow",
        scope_id,
        fingerprint,
        datetime(2026, 1, 2, 3, 4, 5, 6, tzinfo=UTC),
        HistoryRecordType.AUDIT_RECORD,
        10,
        str(uuid4()),
    )
    encoded = encode_cursor(cursor)
    assert (
        decode_cursor(
            encoded, scope_type="workflow", scope_id=scope_id, fingerprint=fingerprint
        )
        == cursor
    )
    with pytest.raises(ValueError, match="scope"):
        decode_cursor(
            encoded,
            scope_type="workflow",
            scope_id=scope_id,
            fingerprint=filter_fingerprint({}),
        )


def test_cursor_does_not_compare_persisted_time_to_wall_clock() -> None:
    future = datetime.now(UTC) + timedelta(days=36500)
    cursor = HistoryCursor(
        "audit",
        None,
        filter_fingerprint({}),
        future,
        HistoryRecordType.AUDIT_RECORD,
        10,
        str(uuid4()),
    )
    encoded = encode_cursor(cursor)
    assert (
        decode_cursor(
            encoded,
            scope_type="audit",
            scope_id=None,
            fingerprint=filter_fingerprint({}),
        ).occurred_at
        == future
    )


@pytest.mark.parametrize(
    "mutation",
    [
        lambda payload: payload.update(extra=True),
        lambda payload: payload.update(v=True),
        lambda payload: payload.update(source_rank=True),
        lambda payload: payload.update(source_key=str(uuid4()).upper()),
        lambda payload: payload.update(occurred_at="2026-01-01T00:00:00+00:00"),
    ],
)
def test_cursor_rejects_noncanonical_shapes(mutation: object) -> None:
    import base64
    import json

    cursor = HistoryCursor(
        "audit",
        None,
        "0" * 64,
        datetime(2026, 1, 1, tzinfo=UTC),
        HistoryRecordType.AUDIT_RECORD,
        10,
        str(uuid4()),
    )
    raw = base64.urlsafe_b64decode(encode_cursor(cursor) + "==")
    payload = json.loads(raw)
    mutation(payload)  # type: ignore[operator]
    encoded = (
        base64.urlsafe_b64encode(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        )
        .rstrip(b"=")
        .decode()
    )
    with pytest.raises(ValueError):
        decode_cursor(encoded, scope_type="audit", scope_id=None, fingerprint="0" * 64)


def test_filter_validation_rejects_ambiguous_actor_and_invalid_ranges() -> None:
    with pytest.raises(ValueError):
        HistoryFilters(actor_id=uuid4())
    with pytest.raises(ValueError):
        HistoryFilters(
            occurred_from=datetime(2026, 1, 2, tzinfo=UTC),
            occurred_to=datetime(2026, 1, 1, tzinfo=UTC),
        )
