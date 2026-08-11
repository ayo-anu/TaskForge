"""Focused task claim application service tests."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest

from taskforge.claims.domain import TaskClaimLease, TaskClaimOutcome, TaskClaimResult
from taskforge.claims.service import TaskClaimService
from taskforge.dispatch.envelope import create_dispatch_envelope
from taskforge.identity.authentication import AuthenticatedWorker


class FakeRepository:
    def __init__(self, result: TaskClaimResult) -> None:
        self.result = result
        self.call: tuple[Any, ...] | None = None

    async def acquire_claim(self, *args: Any, **kwargs: Any) -> TaskClaimResult:
        self.call = (*args, kwargs)
        return self.result


def test_service_passes_authenticated_context_and_server_policy() -> None:
    acquired = datetime.now(UTC)
    result = TaskClaimResult(
        TaskClaimOutcome.ACQUIRED_ACTIVE,
        TaskClaimLease(uuid4(), 1, uuid4(), acquired, acquired + timedelta(seconds=60)),
    )
    repository = FakeRepository(result)
    service = TaskClaimService(repository, lease_seconds=60)
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
    assert (
        asyncio.run(
            service.claim_task(worker, result.claim.worker_session_id, envelope)
        )
        is result
    )
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
        TaskClaimService(FakeRepository(result), lease_seconds=0)
