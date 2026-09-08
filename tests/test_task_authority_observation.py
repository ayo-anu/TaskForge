"""Reason-specific task authority observation classification tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.persistence.task_cancellation import _classify
from taskforge.worker.cancellation import (
    TaskCancellationObservationInvariantError,
    TaskCancellationObservationOutcome,
)


def facts() -> tuple[dict[str, object], AuthenticatedWorker, UUID, UUID, UUID]:
    now = datetime.now(UTC)
    worker = AuthenticatedWorker(uuid4(), uuid4())
    session_id, workflow_id, task_id = uuid4(), uuid4(), uuid4()
    values: dict[str, object] = {
        "observed_at": now,
        "identity_exists": True,
        "identity_disabled_at": None,
        "credential_exists": True,
        "credential_identity_id": worker.worker_identity_id,
        "credential_revoked_at": None,
        "credential_expires_at": now + timedelta(hours=1),
        "session_exists": True,
        "session_identity_id": worker.worker_identity_id,
        "session_ended_at": None,
        "workflow_status": "running",
        "task_workflow_run_id": workflow_id,
        "task_status": "running",
        "attempt_task_run_id": task_id,
        "attempt_number": 1,
        "latest_attempt_number": 1,
        "claim_exists": True,
        "claim_session_id": session_id,
        "lease_expires_at": now + timedelta(minutes=1),
        "claim_terminated_at": None,
        "open_claim_generation": 2,
        "result_generation": None,
        "recovered": False,
        "cancellation_requested_at": None,
    }
    return values, worker, session_id, workflow_id, task_id


def classify(values: dict[str, object]) -> TaskCancellationObservationOutcome:
    _, worker, session_id, workflow_id, task_id = facts()
    # Replace generated identity facts while retaining the caller's state changes.
    base, worker, session_id, workflow_id, task_id = facts()
    base.update(values)
    base["credential_identity_id"] = worker.worker_identity_id
    base["session_identity_id"] = worker.worker_identity_id
    base["claim_session_id"] = session_id
    base["task_workflow_run_id"] = workflow_id
    base["attempt_task_run_id"] = task_id
    return _classify(base, worker, session_id, workflow_id, task_id, 2).outcome


def test_active_cancellation_and_expiry_are_distinct() -> None:
    now = datetime.now(UTC)
    assert classify({}) is TaskCancellationObservationOutcome.ACTIVE
    assert (
        classify(
            {
                "workflow_status": "cancelling",
                "cancellation_requested_at": now,
            }
        )
        is TaskCancellationObservationOutcome.CANCELLATION_REQUESTED
    )
    assert (
        classify({"lease_expires_at": now - timedelta(seconds=1)})
        is TaskCancellationObservationOutcome.CLAIM_EXPIRED_AWAITING_RECOVERY
    )


def test_only_positive_local_evidence_produces_ack_safe_outcomes() -> None:
    now = datetime.now(UTC)
    settled = {
        "claim_terminated_at": now,
        "open_claim_generation": None,
        "result_generation": 2,
    }
    assert (
        classify({**settled, "recovered": True})
        is TaskCancellationObservationOutcome.CLAIM_RECOVERED
    )
    assert (
        classify({**settled, "latest_attempt_number": 2})
        is TaskCancellationObservationOutcome.ATTEMPT_OR_GENERATION_OBSOLETE
    )
    assert (
        classify({**settled, "task_status": "succeeded"})
        is TaskCancellationObservationOutcome.TASK_INACTIVE
    )


def test_worker_and_session_failures_win_over_local_recovery() -> None:
    now = datetime.now(UTC)
    recovered = {
        "claim_terminated_at": now,
        "open_claim_generation": None,
        "result_generation": 2,
        "recovered": True,
    }
    assert (
        classify({**recovered, "credential_revoked_at": now})
        is TaskCancellationObservationOutcome.WORKER_AUTHORITY_REJECTED
    )
    assert (
        classify({**recovered, "session_ended_at": now})
        is TaskCancellationObservationOutcome.WORKER_SESSION_INACTIVE
    )


@pytest.mark.parametrize(
    "change",
    (
        {"claim_terminated_at": datetime.now(UTC), "open_claim_generation": None},
        {"task_status": "succeeded"},
        {"claim_exists": False},
    ),
)
def test_structural_uncertainty_is_never_ack_safe(
    change: dict[str, object],
) -> None:
    base, worker, session_id, workflow_id, task_id = facts()
    base.update(change)
    with pytest.raises(TaskCancellationObservationInvariantError):
        _classify(base, worker, session_id, workflow_id, task_id, 2)
