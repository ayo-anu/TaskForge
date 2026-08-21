"""Focused workflow-run cancellation domain and service tests."""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from taskforge.identity.authorization import OwnerFilter
from taskforge.runs.domain import (
    AcceptedWorkflowRunCancellation,
    InvalidWorkflowRunCancellationIdempotencyKey,
    InvalidWorkflowRunCancellationReason,
    WorkflowRunCancellationCommand,
    WorkflowRunCancellationIdempotencyConflict,
    WorkflowRunCancellationOutcome,
    WorkflowRunStatus,
    create_workflow_run_cancellation_command,
)
from taskforge.runs.persistence_ports import (
    PersistedCancellationOutcome,
    PersistedWorkflowRunCancellation,
    WorkflowRunCancellationPersistenceInvariantViolation,
    WorkflowRunPersistenceUnavailable,
)
from taskforge.runs.service import (
    WorkflowRunCancellationInvariantError,
    WorkflowRunNotFound,
    WorkflowRunService,
    WorkflowRunServiceUnavailable,
)

KEY = "abcdefghijklmnop"


def command(
    *, key: object = KEY, reason: object = " maintenance "
) -> WorkflowRunCancellationCommand:
    return create_workflow_run_cancellation_command(
        uuid4(), uuid4(), idempotency_key=key, reason=reason
    )


@pytest.mark.parametrize("key", ("a" * 16, "z" * 128))
def test_key_boundaries_and_reason_normalization(key: str) -> None:
    created = command(key=key)
    assert created.reason == "maintenance"
    assert len(created.idempotency.key_digest) == 64
    assert len(created.idempotency.request_fingerprint) == 64


@pytest.mark.parametrize(
    "key", (None, 1, "a" * 15, "a" * 129, "with space key!!!", "é" * 16)
)
def test_invalid_cancellation_keys_are_rejected(key: object) -> None:
    with pytest.raises(InvalidWorkflowRunCancellationIdempotencyKey):
        command(key=key)


@pytest.mark.parametrize("reason", ("", "   ", "x" * 2001, 1, []))
def test_invalid_reasons_are_rejected(reason: object) -> None:
    with pytest.raises(InvalidWorkflowRunCancellationReason):
        command(reason=reason)


def test_reason_boundaries_and_none_are_accepted() -> None:
    assert command(reason=None).reason is None
    assert command(reason="x").reason == "x"
    assert command(reason="x" * 2000).reason == "x" * 2000


def test_fingerprint_is_exact_versioned_canonical_json() -> None:
    run_id, requester = uuid4(), uuid4()
    created = create_workflow_run_cancellation_command(
        run_id, requester, idempotency_key=KEY, reason=" reason "
    )
    normalized = {
        "operation": "workflow_run_cancel",
        "reason": "reason",
        "requested_by_principal_id": str(requester),
        "schema_version": 1,
        "workflow_run_id": str(run_id),
    }
    encoded = json.dumps(
        normalized,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    assert (
        created.idempotency.key_digest
        == hashlib.sha256(
            b"taskforge:workflow-run-cancellation-key:v1\0" + KEY.encode()
        ).hexdigest()
    )
    assert (
        created.idempotency.request_fingerprint
        == hashlib.sha256(
            b"taskforge:workflow-run-cancellation-request:v1\0" + encoded
        ).hexdigest()
    )
    assert "abcdefghijklmnop" not in repr(created.idempotency)


class CancellationRepository:
    def __init__(self, result: PersistedWorkflowRunCancellation | None) -> None:
        self.result = result
        self.error: Exception | None = None
        self.call: tuple[object, ...] | None = None

    async def cancel_run(
        self,
        run_id: object,
        owner_filter: object,
        cancellation_command: object,
    ) -> PersistedWorkflowRunCancellation | None:
        self.call = (run_id, owner_filter, cancellation_command)
        if self.error is not None:
            raise self.error
        return self.result


def persisted(
    outcome: PersistedCancellationOutcome,
) -> PersistedWorkflowRunCancellation:
    run_id, requester = uuid4(), uuid4()
    accepted = AcceptedWorkflowRunCancellation(requester, "reason", datetime.now(UTC))
    status = (
        WorkflowRunStatus.CANCELLED
        if outcome is PersistedCancellationOutcome.ALREADY_CANCELLED
        else WorkflowRunStatus.CANCELLING
    )
    return PersistedWorkflowRunCancellation(run_id, outcome, status, accepted)


def test_service_passes_only_normalized_hashed_command() -> None:
    record = persisted(PersistedCancellationOutcome.NEWLY_ACCEPTED)
    repository = CancellationRepository(record)
    service = WorkflowRunService(repository)  # type: ignore[arg-type]
    owner = OwnerFilter.only(uuid4())
    assert record.canonical_request is not None
    result = asyncio.run(
        service.cancel_run(
            record.workflow_run_id,
            owner,
            requested_by_principal_id=record.canonical_request.requested_by_principal_id,
            idempotency_key=KEY,
            reason=" reason ",
        )
    )
    assert result.outcome is WorkflowRunCancellationOutcome.NEWLY_ACCEPTED
    assert result.accepted_request == record.canonical_request
    assert repository.call is not None
    sent = repository.call[2]
    assert isinstance(sent, WorkflowRunCancellationCommand)
    assert sent.reason == "reason"
    assert KEY not in repr(sent)


@pytest.mark.parametrize(
    "outcome",
    (
        PersistedCancellationOutcome.ALREADY_CANCELLING,
        PersistedCancellationOutcome.ALREADY_CANCELLED,
        PersistedCancellationOutcome.TERMINAL_STATE_WON,
    ),
)
def test_service_conceals_canonical_metadata_for_non_retries(
    outcome: PersistedCancellationOutcome,
) -> None:
    record = persisted(outcome)
    repository = CancellationRepository(record)
    result = asyncio.run(
        WorkflowRunService(repository).cancel_run(  # type: ignore[arg-type]
            record.workflow_run_id,
            OwnerFilter.all_owners(),
            requested_by_principal_id=uuid4(),
            idempotency_key=KEY,
            reason=None,
        )
    )
    assert result.accepted_request is None


def test_same_key_in_different_principal_scope_is_concealed_already_outcome() -> None:
    record = persisted(PersistedCancellationOutcome.ALREADY_CANCELLING)
    repository = CancellationRepository(record)
    result = asyncio.run(
        WorkflowRunService(repository).cancel_run(  # type: ignore[arg-type]
            record.workflow_run_id,
            OwnerFilter.all_owners(),
            requested_by_principal_id=uuid4(),
            idempotency_key=KEY,
            reason="different semantic request",
        )
    )

    assert result.outcome is WorkflowRunCancellationOutcome.ALREADY_CANCELLING
    assert result.accepted_request is None


def test_service_maps_conflict_missing_invariant_and_unavailable() -> None:
    cases = (
        (
            CancellationRepository(
                persisted(PersistedCancellationOutcome.IDEMPOTENCY_CONFLICT)
            ),
            WorkflowRunCancellationIdempotencyConflict,
        ),
        (CancellationRepository(None), WorkflowRunNotFound),
    )
    for repository, expected in cases:
        with pytest.raises(expected):
            asyncio.run(
                WorkflowRunService(repository).cancel_run(  # type: ignore[arg-type]
                    uuid4(),
                    OwnerFilter.only(uuid4()),
                    requested_by_principal_id=uuid4(),
                    idempotency_key=KEY,
                    reason=None,
                )
            )
    failure_cases: tuple[tuple[Exception, type[Exception]], ...] = (
        (
            WorkflowRunCancellationPersistenceInvariantViolation(),
            WorkflowRunCancellationInvariantError,
        ),
        (WorkflowRunPersistenceUnavailable(), WorkflowRunServiceUnavailable),
    )
    for failure, expected_failure in failure_cases:
        repository = CancellationRepository(None)
        repository.error = failure
        with pytest.raises(expected_failure):
            asyncio.run(
                WorkflowRunService(repository).cancel_run(  # type: ignore[arg-type]
                    uuid4(),
                    OwnerFilter.only(uuid4()),
                    requested_by_principal_id=uuid4(),
                    idempotency_key=KEY,
                    reason=None,
                )
            )
