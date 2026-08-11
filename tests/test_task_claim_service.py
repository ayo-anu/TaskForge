"""Focused task claim application service tests."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest

from taskforge.claims.authority import TaskClaimResultAuthorityIssuer
from taskforge.claims.domain import (
    TaskClaimLease,
    TaskClaimOutcome,
    TaskClaimRenewalOutcome,
    TaskClaimRenewalRequest,
    TaskClaimRenewalResult,
    TaskClaimResult,
)
from taskforge.claims.service import TaskClaimService
from taskforge.dispatch.envelope import create_dispatch_envelope
from taskforge.identity.authentication import AuthenticatedWorker


class FakeRepository:
    def __init__(
        self,
        result: TaskClaimResult,
        renewal_result: TaskClaimRenewalResult | None = None,
    ) -> None:
        self.result = result
        self.renewal_result = renewal_result
        self.call: tuple[Any, ...] | None = None

    async def acquire_claim(self, *args: Any, **kwargs: Any) -> TaskClaimResult:
        self.call = (*args, kwargs)
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
