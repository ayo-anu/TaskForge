"""Task claim acquisition result invariants."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from taskforge.claims.domain import TaskClaimLease, TaskClaimOutcome, TaskClaimResult


def test_claim_lease_normalizes_timestamps_and_preserves_explicit_outcome() -> None:
    acquired = datetime.now(UTC)
    lease = TaskClaimLease(
        uuid4(), 2, uuid4(), acquired, acquired + timedelta(seconds=1)
    )
    result = TaskClaimResult(TaskClaimOutcome.REPLAYED_EXPIRED, lease)
    assert result.outcome is TaskClaimOutcome.REPLAYED_EXPIRED
    assert result.claim.generation == 2
    assert result.claim.acquired_at.tzinfo is UTC


@pytest.mark.parametrize("generation", [0, -1])
def test_claim_generation_must_be_positive(generation: int) -> None:
    acquired = datetime.now(UTC)
    with pytest.raises(ValueError, match="generation"):
        TaskClaimLease(
            uuid4(), generation, uuid4(), acquired, acquired + timedelta(seconds=1)
        )


def test_claim_lease_requires_ordered_aware_timestamps() -> None:
    acquired = datetime.now(UTC)
    with pytest.raises(ValueError, match="expire"):
        TaskClaimLease(uuid4(), 1, uuid4(), acquired, acquired)
    with pytest.raises(ValueError, match="timezone-aware"):
        TaskClaimLease(uuid4(), 1, uuid4(), acquired.replace(tzinfo=None), acquired)
