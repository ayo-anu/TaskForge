"""Focused tests for advisory stale-session application."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest

from taskforge.recovery.domain import StaleWorkerSessionCandidate
from taskforge.recovery.persistence_ports import (
    EndedStaleWorkerSession,
    StaleWorkerSessionRecoveryNoOpReason,
    StaleWorkerSessionRecoveryPersistenceInvariantViolation,
    StaleWorkerSessionRecoveryPersistenceUnavailable,
    StaleWorkerSessionRecoveryResult,
)
from taskforge.recovery.service import (
    StaleWorkerSessionRecoveryInvariantError,
    StaleWorkerSessionRecoveryOutcome,
    StaleWorkerSessionRecoveryService,
    StaleWorkerSessionRecoveryServiceUnavailable,
)

OBSERVED_AT = datetime(2026, 8, 14, 12, tzinfo=UTC)


def candidate() -> StaleWorkerSessionCandidate:
    from uuid import uuid4

    return StaleWorkerSessionCandidate(
        uuid4(),
        uuid4(),
        3,
        OBSERVED_AT - timedelta(seconds=30),
        True,
        OBSERVED_AT,
    )


@dataclass
class FakeRepository:
    result: StaleWorkerSessionRecoveryResult | None = None
    failure: Exception | None = None
    calls: list[tuple[StaleWorkerSessionCandidate, int]] | None = None

    async def end_stale_session(
        self,
        value: StaleWorkerSessionCandidate,
        *,
        stale_after_seconds: int,
    ) -> StaleWorkerSessionRecoveryResult:
        if self.calls is None:
            self.calls = []
        self.calls.append((value, stale_after_seconds))
        if self.failure is not None:
            raise self.failure
        assert self.result is not None
        return self.result


def test_stale_session_is_ended_with_repository_issued_time() -> None:
    value = candidate()
    ended_at = OBSERVED_AT + timedelta(seconds=1)
    repository = FakeRepository(EndedStaleWorkerSession(ended_at))

    receipt = asyncio.run(
        StaleWorkerSessionRecoveryService(repository).end_stale_session(
            value, stale_after_seconds=30
        )
    )

    assert receipt.outcome is StaleWorkerSessionRecoveryOutcome.SESSION_ENDED
    assert receipt.worker_session_id == value.worker_session_id
    assert receipt.ended_at == ended_at
    assert repository.calls == [(value, 30)]


@pytest.mark.parametrize("threshold", [0, -1, 3601, True])
def test_invalid_threshold_is_rejected_before_persistence(threshold: int) -> None:
    repository = FakeRepository()

    with pytest.raises(ValueError, match="threshold"):
        asyncio.run(
            StaleWorkerSessionRecoveryService(repository).end_stale_session(
                candidate(), stale_after_seconds=threshold
            )
        )

    assert repository.calls is None


@pytest.mark.parametrize(
    ("result", "outcome"),
    [
        (
            StaleWorkerSessionRecoveryNoOpReason.CANDIDATE_REFRESHED,
            StaleWorkerSessionRecoveryOutcome.CANDIDATE_REFRESHED,
        ),
        (
            StaleWorkerSessionRecoveryNoOpReason.SESSION_ALREADY_ENDED,
            StaleWorkerSessionRecoveryOutcome.SESSION_ALREADY_ENDED,
        ),
    ],
)
def test_expected_concurrent_invalidation_is_a_typed_noop(
    result: StaleWorkerSessionRecoveryNoOpReason,
    outcome: StaleWorkerSessionRecoveryOutcome,
) -> None:
    receipt = asyncio.run(
        StaleWorkerSessionRecoveryService(FakeRepository(result)).end_stale_session(
            candidate(), stale_after_seconds=30
        )
    )

    assert receipt.outcome is outcome
    assert receipt.ended_at is None


@pytest.mark.parametrize(
    ("failure", "expected"),
    [
        (
            StaleWorkerSessionRecoveryPersistenceInvariantViolation(),
            StaleWorkerSessionRecoveryInvariantError,
        ),
        (
            StaleWorkerSessionRecoveryPersistenceUnavailable(),
            StaleWorkerSessionRecoveryServiceUnavailable,
        ),
    ],
)
def test_persistence_failures_preserve_the_service_boundary(
    failure: Exception,
    expected: type[Exception],
) -> None:
    with pytest.raises(expected):
        asyncio.run(
            StaleWorkerSessionRecoveryService(
                FakeRepository(failure=failure)
            ).end_stale_session(candidate(), stale_after_seconds=30)
        )
