"""Tests for per-delivery claim renewal and quiescence."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest

from taskforge.claims.domain import (
    TaskClaimLease,
    TaskClaimRenewalOutcome,
    TaskClaimRenewalRejected,
    TaskClaimRenewalRejectionReason,
    TaskClaimRenewalRequest,
    TaskClaimRenewalResult,
)
from taskforge.claims.service import TaskClaimServiceUnavailable
from taskforge.dispatch.envelope import create_dispatch_envelope
from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.worker.cancellation import (
    TaskCancellationObservation,
    TaskCancellationObservationOutcome,
    TaskCancellationToken,
)
from taskforge.worker.claim_renewal import (
    ClaimRenewalSupervisor,
    DeliveryAuthorityObsolete,
)
from taskforge.worker.runtime_errors import WorkerProcessFailure


class Renewer:
    def __init__(self, outcomes: list[object], lease: TaskClaimLease) -> None:
        self.outcomes = outcomes
        self.lease = lease
        self.requests: list[TaskClaimRenewalRequest] = []

    async def renew_claim(
        self, worker: Any, request: TaskClaimRenewalRequest
    ) -> TaskClaimRenewalResult:
        del worker
        self.requests.append(request)
        outcome = self.outcomes.pop(0) if self.outcomes else "renewed"
        if isinstance(outcome, Exception):
            raise outcome
        renewed = TaskClaimLease(
            self.lease.task_attempt_id,
            self.lease.generation,
            self.lease.worker_session_id,
            self.lease.acquired_at,
            self.lease.lease_expires_at + timedelta(seconds=len(self.requests)),
        )
        renewal_outcome = (
            outcome
            if isinstance(outcome, TaskClaimRenewalOutcome)
            else TaskClaimRenewalOutcome.RENEWED
        )
        return TaskClaimRenewalResult(renewal_outcome, renewed)


class Observer:
    def __init__(self, outcomes: list[TaskCancellationObservationOutcome]) -> None:
        self.outcomes = outcomes
        self.calls = 0

    async def observe_cancellation(self, *args: Any) -> TaskCancellationObservation:
        self.calls += 1
        outcome = self.outcomes.pop(0) if len(self.outcomes) > 1 else self.outcomes[0]
        return TaskCancellationObservation(outcome)


def fixture() -> tuple[AuthenticatedWorker, TaskClaimLease, Any]:
    worker = AuthenticatedWorker(uuid4(), uuid4())
    acquired = datetime.now(UTC)
    lease = TaskClaimLease(
        uuid4(), 1, uuid4(), acquired, acquired + timedelta(seconds=1)
    )
    envelope = create_dispatch_envelope(
        dispatch_id=uuid4(),
        task_attempt_id=lease.task_attempt_id,
        task_run_id=uuid4(),
        workflow_run_id=uuid4(),
        attempt_number=1,
        task_type="test.task",
        required_capability="test",
        task_payload={},
        references={},
    )
    return worker, lease, envelope


def supervisor(
    renewer: Renewer,
    observer: Observer,
    worker: AuthenticatedWorker,
    lease: TaskClaimLease,
) -> ClaimRenewalSupervisor:
    return ClaimRenewalSupervisor(
        renewer,
        observer,
        worker,
        lease.worker_session_id,
        lease_seconds=0.003,  # type: ignore[arg-type]
        operation_timeout_seconds=0.0001,
        observation_poll_seconds=0.0001,
    )


def test_unknown_renewal_replays_identical_request() -> None:
    async def scenario() -> None:
        worker, lease, envelope = fixture()
        renewer = Renewer([TaskClaimServiceUnavailable(), "renewed"], lease)
        guard = supervisor(
            renewer,
            Observer([TaskCancellationObservationOutcome.ACTIVE]),
            worker,
            lease,
        ).guard(envelope, lease, TaskCancellationToken())
        guard.start()
        await guard.protect(asyncio.sleep(0.01))
        await guard.close()
        assert len(renewer.requests) >= 2
        assert renewer.requests[0] == renewer.requests[1]

    asyncio.run(scenario())


def test_replayed_renewal_is_followed_immediately_using_database_expiry() -> None:
    async def scenario() -> None:
        worker, lease, envelope = fixture()
        renewer = Renewer(
            [TaskClaimRenewalOutcome.REPLAYED, TaskClaimRenewalOutcome.RENEWED],
            lease,
        )
        guard = supervisor(
            renewer,
            Observer([TaskCancellationObservationOutcome.ACTIVE]),
            worker,
            lease,
        ).guard(envelope, lease, TaskCancellationToken())
        guard.start()
        await guard.protect(asyncio.sleep(0.01))
        await guard.close()

        assert len(renewer.requests) >= 2
        assert renewer.requests[1].expected_lease_expires_at == (
            lease.lease_expires_at + timedelta(seconds=1)
        )

    asyncio.run(scenario())


def test_expiry_quiesces_until_exact_delivery_is_recovered() -> None:
    async def scenario() -> None:
        worker, lease, envelope = fixture()
        expired = TaskClaimRenewalRejected(TaskClaimRenewalRejectionReason.EXPIRED)
        observer = Observer(
            [
                TaskCancellationObservationOutcome.CLAIM_EXPIRED_AWAITING_RECOVERY,
                TaskCancellationObservationOutcome.CLAIM_RECOVERED,
            ]
        )
        guard = supervisor(Renewer([expired], lease), observer, worker, lease).guard(
            envelope, lease, TaskCancellationToken()
        )
        guard.start()
        with pytest.raises(DeliveryAuthorityObsolete):
            await guard.protect(asyncio.sleep(1))
        assert observer.calls == 2
        await guard.close()

    asyncio.run(scenario())


def test_process_authority_loss_is_not_delivery_local() -> None:
    async def scenario() -> None:
        worker, lease, envelope = fixture()
        rejected = TaskClaimRenewalRejected(
            TaskClaimRenewalRejectionReason.WORKER_AUTHORITY_REJECTED
        )
        guard = supervisor(
            Renewer([rejected], lease),
            Observer([TaskCancellationObservationOutcome.CLAIM_RECOVERED]),
            worker,
            lease,
        ).guard(envelope, lease, TaskCancellationToken())
        guard.start()
        with pytest.raises(WorkerProcessFailure):
            await guard.protect(asyncio.sleep(1))
        await guard.close()

    asyncio.run(scenario())


def test_session_loss_while_quiesced_never_becomes_ack_safe() -> None:
    async def scenario() -> None:
        worker, lease, envelope = fixture()
        expired = TaskClaimRenewalRejected(TaskClaimRenewalRejectionReason.EXPIRED)
        guard = supervisor(
            Renewer([expired], lease),
            Observer(
                [
                    TaskCancellationObservationOutcome.CLAIM_EXPIRED_AWAITING_RECOVERY,
                    TaskCancellationObservationOutcome.WORKER_SESSION_INACTIVE,
                ]
            ),
            worker,
            lease,
        ).guard(envelope, lease, TaskCancellationToken())
        guard.start()

        with pytest.raises(WorkerProcessFailure):
            await guard.protect(asyncio.sleep(1))
        await guard.close()

    asyncio.run(scenario())


def test_delivery_local_obsolescence_does_not_stop_independent_execution() -> None:
    async def scenario() -> None:
        local_worker, local_lease, local_envelope = fixture()
        healthy_worker, healthy_lease, healthy_envelope = fixture()
        local = supervisor(
            Renewer(
                [TaskClaimRenewalRejected(TaskClaimRenewalRejectionReason.STALE)],
                local_lease,
            ),
            Observer([TaskCancellationObservationOutcome.CLAIM_RECOVERED]),
            local_worker,
            local_lease,
        ).guard(local_envelope, local_lease, TaskCancellationToken())
        healthy = supervisor(
            Renewer([], healthy_lease),
            Observer([TaskCancellationObservationOutcome.ACTIVE]),
            healthy_worker,
            healthy_lease,
        ).guard(healthy_envelope, healthy_lease, TaskCancellationToken())
        local.start()
        healthy.start()

        local_result, healthy_result = await asyncio.gather(
            local.protect(asyncio.sleep(1)),
            healthy.protect(asyncio.sleep(0.01, result="completed")),
            return_exceptions=True,
        )

        assert isinstance(local_result, DeliveryAuthorityObsolete)
        assert healthy_result == "completed"
        await local.close()
        await healthy.close()

    asyncio.run(scenario())


def test_cancelled_protection_retains_renewal_until_handler_physically_exits() -> None:
    async def scenario() -> None:
        worker, lease, envelope = fixture()
        renewer = Renewer([], lease)
        guard = ClaimRenewalSupervisor(
            renewer,
            Observer([TaskCancellationObservationOutcome.ACTIVE]),
            worker,
            lease.worker_session_id,
            lease_seconds=60,
            operation_timeout_seconds=1,
            observation_poll_seconds=1,
        ).guard(envelope, lease, TaskCancellationToken())
        entered = asyncio.Event()
        cancellation_observed = asyncio.Event()
        release = asyncio.Event()

        async def cancellation_resistant_handler() -> None:
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancellation_observed.set()
                await release.wait()

        guard.start()
        protected = asyncio.create_task(guard.protect(cancellation_resistant_handler()))
        await entered.wait()
        protected.cancel()
        await cancellation_observed.wait()
        assert not protected.done()
        assert guard._task is not None and not guard._task.done()
        protected.cancel()
        await asyncio.sleep(0)
        assert not protected.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await protected
        await guard.close()
        assert renewer.requests

    asyncio.run(scenario())
