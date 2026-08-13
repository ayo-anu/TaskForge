"""Focused task claim application service tests."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest

from taskforge.claims.authority import TaskClaimResultAuthorityIssuer
from taskforge.claims.domain import (
    InspectedTaskClaim,
    TaskClaimLease,
    TaskClaimLeaseStatus,
    TaskClaimOutcome,
    TaskClaimRejected,
    TaskClaimRejectionReason,
    TaskClaimRenewalOutcome,
    TaskClaimRenewalRequest,
    TaskClaimRenewalResult,
    TaskClaimResult,
)
from taskforge.claims.persistence_ports import (
    TaskClaimAlreadyOwned,
    TaskClaimAttemptStale,
    TaskClaimAuthorityRejected,
    TaskClaimCapabilityMismatch,
    TaskClaimDispatchRejected,
    TaskClaimInvariantViolation,
    TaskClaimNotEligible,
    TaskClaimPersistenceUnavailable,
    TaskClaimSessionInactive,
    TaskClaimSessionUnavailable,
    TaskClaimWorkerUnavailable,
)
from taskforge.claims.service import (
    TaskClaimInspectionService,
    TaskClaimService,
    TaskClaimServiceInvariantError,
    TaskClaimServiceUnavailable,
)
from taskforge.dispatch.envelope import create_dispatch_envelope
from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.identity.authorization import OwnerFilter
from taskforge.runs.domain import TaskRunStatus


class FakeRepository:
    def __init__(
        self,
        result: TaskClaimResult,
        renewal_result: TaskClaimRenewalResult | None = None,
        acquisition_error: Exception | None = None,
    ) -> None:
        self.result = result
        self.renewal_result = renewal_result
        self.acquisition_error = acquisition_error
        self.call: tuple[Any, ...] | None = None

    async def acquire_claim(self, *args: Any, **kwargs: Any) -> TaskClaimResult:
        self.call = (*args, kwargs)
        if self.acquisition_error is not None:
            raise self.acquisition_error
        return self.result

    async def renew_claim(self, *args: Any, **kwargs: Any) -> TaskClaimRenewalResult:
        self.call = (*args, kwargs)
        assert self.renewal_result is not None
        return self.renewal_result


def test_service_passes_authenticated_context_and_server_policy() -> None:
    acquired = datetime.now(UTC)
    result = TaskClaimResult(
        TaskClaimOutcome.ACQUIRED_ACTIVE,
        TaskClaimLease(uuid4(), 1, uuid4(), acquired, acquired + timedelta(seconds=60)),
    )
    repository = FakeRepository(result)
    service = TaskClaimService(
        repository, TaskClaimResultAuthorityIssuer(b"a" * 32), lease_seconds=60
    )
    worker = AuthenticatedWorker(uuid4(), uuid4())
    envelope = create_dispatch_envelope(
        dispatch_id=uuid4(),
        task_attempt_id=result.claim.task_attempt_id,
        task_run_id=uuid4(),
        workflow_run_id=uuid4(),
        attempt_number=1,
        task_type="test.task",
        required_capability="test-capability",
        task_payload={},
        references={},
    )
    issued = asyncio.run(
        service.claim_task(worker, result.claim.worker_session_id, envelope)
    )
    assert issued.outcome is result.outcome
    assert issued.claim is result.claim
    assert issued.result_authority is not None
    assert repository.call == (
        worker,
        result.claim.worker_session_id,
        envelope,
        {"lease_seconds": 60},
    )


def test_service_rejects_nonpositive_policy() -> None:
    acquired = datetime.now(UTC)
    result = TaskClaimResult(
        TaskClaimOutcome.ACQUIRED_ACTIVE,
        TaskClaimLease(uuid4(), 1, uuid4(), acquired, acquired + timedelta(seconds=1)),
    )
    with pytest.raises(ValueError, match="positive"):
        TaskClaimService(
            FakeRepository(result),
            TaskClaimResultAuthorityIssuer(b"a" * 32),
            lease_seconds=0,
        )


def test_service_omits_authority_for_expired_replay() -> None:
    acquired = datetime.now(UTC)
    result = TaskClaimResult(
        TaskClaimOutcome.REPLAYED_EXPIRED,
        TaskClaimLease(uuid4(), 1, uuid4(), acquired, acquired + timedelta(seconds=1)),
    )
    service = TaskClaimService(
        FakeRepository(result),
        TaskClaimResultAuthorityIssuer(b"a" * 32),
        lease_seconds=60,
    )
    worker = AuthenticatedWorker(uuid4(), uuid4())
    envelope = create_dispatch_envelope(
        dispatch_id=uuid4(),
        task_attempt_id=result.claim.task_attempt_id,
        task_run_id=uuid4(),
        workflow_run_id=uuid4(),
        attempt_number=1,
        task_type="test.task",
        required_capability="test-capability",
        task_payload={},
        references={},
    )
    issued = asyncio.run(
        service.claim_task(worker, result.claim.worker_session_id, envelope)
    )
    assert issued.result_authority is None


def test_service_forwards_renewal_without_result_authority() -> None:
    acquired = datetime.now(UTC)
    lease = TaskClaimLease(
        uuid4(), 2, uuid4(), acquired, acquired + timedelta(seconds=60)
    )
    renewal = TaskClaimRenewalResult(TaskClaimRenewalOutcome.RENEWED, lease)
    repository = FakeRepository(
        TaskClaimResult(TaskClaimOutcome.ACQUIRED_ACTIVE, lease), renewal
    )
    service = TaskClaimService(
        repository, TaskClaimResultAuthorityIssuer(b"a" * 32), lease_seconds=60
    )
    worker = AuthenticatedWorker(uuid4(), uuid4())
    request = TaskClaimRenewalRequest(
        lease.task_attempt_id,
        lease.generation,
        lease.worker_session_id,
        lease.lease_expires_at,
    )
    assert asyncio.run(service.renew_claim(worker, request)) is renewal
    assert repository.call == (worker, request, {"lease_seconds": 60})


@pytest.mark.parametrize(
    ("repository_error", "expected_reason"),
    (
        (
            TaskClaimDispatchRejected(),
            TaskClaimRejectionReason.INVALID_DISPATCH,
        ),
        (
            TaskClaimAttemptStale(),
            TaskClaimRejectionReason.STALE_ATTEMPT,
        ),
        (TaskClaimNotEligible(), TaskClaimRejectionReason.OBSOLETE_TASK),
        (
            TaskClaimAuthorityRejected(),
            TaskClaimRejectionReason.WORKER_AUTHORITY_REJECTED,
        ),
        (
            TaskClaimSessionUnavailable(),
            TaskClaimRejectionReason.WORKER_SESSION_UNAVAILABLE,
        ),
        (
            TaskClaimSessionInactive(),
            TaskClaimRejectionReason.WORKER_SESSION_INACTIVE,
        ),
        (
            TaskClaimWorkerUnavailable(),
            TaskClaimRejectionReason.WORKER_UNAVAILABLE,
        ),
        (
            TaskClaimCapabilityMismatch(),
            TaskClaimRejectionReason.CAPABILITY_MISMATCH,
        ),
        (
            TaskClaimAlreadyOwned(),
            TaskClaimRejectionReason.ALREADY_AUTHORITATIVE,
        ),
    ),
)
def test_service_maps_expected_acquisition_denials_to_safe_rejections(
    repository_error: Exception,
    expected_reason: TaskClaimRejectionReason,
) -> None:
    acquired = datetime.now(UTC)
    result = TaskClaimResult(
        TaskClaimOutcome.ACQUIRED_ACTIVE,
        TaskClaimLease(uuid4(), 1, uuid4(), acquired, acquired + timedelta(seconds=60)),
    )
    repository = FakeRepository(result, acquisition_error=repository_error)
    service = TaskClaimService(
        repository, TaskClaimResultAuthorityIssuer(b"a" * 32), lease_seconds=60
    )
    worker = AuthenticatedWorker(uuid4(), uuid4())
    envelope = create_dispatch_envelope(
        dispatch_id=uuid4(),
        task_attempt_id=result.claim.task_attempt_id,
        task_run_id=uuid4(),
        workflow_run_id=uuid4(),
        attempt_number=1,
        task_type="test.task",
        required_capability="secret-capability",
        task_payload={},
        references={},
    )

    with pytest.raises(TaskClaimRejected) as raised:
        asyncio.run(
            service.claim_task(worker, result.claim.worker_session_id, envelope)
        )

    assert raised.value.reason is expected_reason
    assert str(raised.value) == "task claim acquisition rejected"
    assert raised.value.__cause__ is repository_error
    rendered = str(raised.value)
    assert str(worker.worker_identity_id) not in rendered
    assert str(result.claim.task_attempt_id) not in rendered
    assert "secret-capability" not in rendered


@pytest.mark.parametrize(
    ("repository_error", "service_error"),
    (
        (TaskClaimInvariantViolation(), TaskClaimServiceInvariantError),
        (TaskClaimPersistenceUnavailable(), TaskClaimServiceUnavailable),
    ),
)
def test_service_translates_internal_acquisition_failures(
    repository_error: Exception, service_error: type[Exception]
) -> None:
    acquired = datetime.now(UTC)
    result = TaskClaimResult(
        TaskClaimOutcome.ACQUIRED_ACTIVE,
        TaskClaimLease(uuid4(), 1, uuid4(), acquired, acquired + timedelta(seconds=60)),
    )
    repository = FakeRepository(result, acquisition_error=repository_error)
    service = TaskClaimService(
        repository, TaskClaimResultAuthorityIssuer(b"a" * 32), lease_seconds=60
    )
    worker = AuthenticatedWorker(uuid4(), uuid4())
    envelope = create_dispatch_envelope(
        dispatch_id=uuid4(),
        task_attempt_id=result.claim.task_attempt_id,
        task_run_id=uuid4(),
        workflow_run_id=uuid4(),
        attempt_number=1,
        task_type="test.task",
        required_capability="test-capability",
        task_payload={},
        references={},
    )

    with pytest.raises(service_error) as raised:
        asyncio.run(
            service.claim_task(worker, result.claim.worker_session_id, envelope)
        )

    assert not isinstance(raised.value, TaskClaimRejected)
    assert raised.value.__cause__ is repository_error


class FakeInspectionRepository:
    def __init__(self, claim: InspectedTaskClaim) -> None:
        self.claim = claim
        self.call: tuple[object, ...] | None = None

    async def get_current_claim(
        self, task_attempt_id: object, owner_filter: OwnerFilter
    ) -> InspectedTaskClaim:
        self.call = (task_attempt_id, owner_filter)
        return self.claim


def test_inspection_service_passes_exact_attempt_and_owner_filter() -> None:
    observed = datetime.now(UTC)
    claim = InspectedTaskClaim(
        uuid4(),
        uuid4(),
        uuid4(),
        1,
        1,
        uuid4(),
        uuid4(),
        observed - timedelta(seconds=1),
        observed + timedelta(seconds=60),
        observed,
        TaskClaimLeaseStatus.UNEXPIRED,
        TaskRunStatus.CLAIMED,
    )
    repository = FakeInspectionRepository(claim)
    service = TaskClaimInspectionService(repository)
    owner_filter = OwnerFilter.only(uuid4())

    result = asyncio.run(service.get_current_claim(claim.task_attempt_id, owner_filter))

    assert result is claim
    assert repository.call == (claim.task_attempt_id, owner_filter)
