"""Domain validation for advisory recovery observations and cursors."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from taskforge.recovery.domain import (
    ExpiredClaimCandidate,
    ExpiredClaimCandidatePage,
    ExpiredClaimScanCursor,
    StaleWorkerSessionCandidate,
    StaleWorkerSessionCandidatePage,
    StaleWorkerSessionScanCursor,
)

NOW = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)


def test_claim_page_requires_one_authoritative_observation_time() -> None:
    candidate = ExpiredClaimCandidate(
        uuid4(), uuid4(), uuid4(), 1, 2, uuid4(), NOW, NOW
    )
    cursor = ExpiredClaimScanCursor(
        NOW, candidate.lease_expires_at, candidate.task_attempt_id, 2
    )
    page = ExpiredClaimCandidatePage((candidate,), NOW, cursor)
    assert page.observed_at is NOW

    with pytest.raises(ValueError):
        ExpiredClaimCandidatePage((candidate,), NOW + timedelta(seconds=1), cursor)


def test_claim_candidate_rejects_nonexpired_or_invalid_identity() -> None:
    values = (uuid4(), uuid4(), uuid4())
    with pytest.raises(ValueError):
        ExpiredClaimCandidate(
            *values, 1, 1, uuid4(), NOW + timedelta(microseconds=1), NOW
        )
    with pytest.raises(ValueError):
        ExpiredClaimCandidate(*values, 0, 1, uuid4(), NOW, NOW)
    with pytest.raises(ValueError):
        ExpiredClaimCandidate(*values, 1, 0, uuid4(), NOW, NOW)


def test_stale_session_supports_registration_projection_without_heartbeat() -> None:
    candidate = StaleWorkerSessionCandidate(
        uuid4(), uuid4(), 0, NOW - timedelta(seconds=30), False, NOW
    )
    cursor = StaleWorkerSessionScanCursor(
        NOW, candidate.last_seen_at, candidate.worker_session_id, 30
    )
    page = StaleWorkerSessionCandidatePage((candidate,), NOW, 30, cursor)
    assert page.items[0].last_sequence == 0
    assert page.items[0].accepting_work is False


def test_stale_session_cursor_and_page_thresholds_must_match() -> None:
    cursor = StaleWorkerSessionScanCursor(NOW, NOW - timedelta(seconds=30), uuid4(), 30)
    with pytest.raises(ValueError):
        StaleWorkerSessionCandidatePage((), NOW, 31, cursor)


@pytest.mark.parametrize(
    "value",
    (
        datetime(2026, 8, 14, 12, 0),
        NOW + timedelta(seconds=1),
    ),
)
def test_stale_session_rejects_invalid_last_seen_time(value: datetime) -> None:
    with pytest.raises(ValueError):
        StaleWorkerSessionCandidate(uuid4(), uuid4(), 0, value, True, NOW)
