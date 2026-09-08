"""Per-delivery supervision of database-authoritative claim renewal."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from time import monotonic
from typing import Any, Protocol, TypeVar
from uuid import UUID

from taskforge.claims.domain import (
    TaskClaimLease,
    TaskClaimRenewalOutcome,
    TaskClaimRenewalRejected,
    TaskClaimRenewalRejectionReason,
    TaskClaimRenewalRequest,
    TaskClaimRenewalResult,
)
from taskforge.claims.service import (
    TaskClaimServiceInvariantError,
    TaskClaimServiceUnavailable,
)
from taskforge.dispatch.envelope import DispatchEnvelope
from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.worker.cancellation import (
    TaskCancellationObservationInvariantError,
    TaskCancellationObservationOutcome,
    TaskCancellationObservationUnavailable,
    TaskCancellationObserver,
    TaskCancellationToken,
)
from taskforge.worker.runtime_errors import WorkerProcessFailure

T = TypeVar("T")

_ACK_SAFE = frozenset(
    {
        TaskCancellationObservationOutcome.CLAIM_RECOVERED,
        TaskCancellationObservationOutcome.ATTEMPT_OR_GENERATION_OBSOLETE,
        TaskCancellationObservationOutcome.TASK_INACTIVE,
    }
)
_PROCESS_WIDE = frozenset(
    {
        TaskCancellationObservationOutcome.WORKER_AUTHORITY_REJECTED,
        TaskCancellationObservationOutcome.WORKER_SESSION_INACTIVE,
    }
)
_LOCAL_RENEWAL_LOSS = frozenset(
    {
        TaskClaimRenewalRejectionReason.EXPIRED,
        TaskClaimRenewalRejectionReason.RECOVERED,
        TaskClaimRenewalRejectionReason.STALE,
        TaskClaimRenewalRejectionReason.TASK_INACTIVE,
    }
)
_PROCESS_RENEWAL_LOSS = frozenset(
    {
        TaskClaimRenewalRejectionReason.WORKER_AUTHORITY_REJECTED,
        TaskClaimRenewalRejectionReason.WORKER_SESSION_UNAVAILABLE,
        TaskClaimRenewalRejectionReason.WORKER_SESSION_INACTIVE,
    }
)


class TaskClaimRenewer(Protocol):
    async def renew_claim(
        self,
        authenticated_worker: AuthenticatedWorker,
        request: TaskClaimRenewalRequest,
    ) -> TaskClaimRenewalResult: ...


class DeliveryAuthorityObsolete(Exception):
    """Durable evidence proves this exact physical delivery is disposable."""


class ClaimRenewalSupervisor:
    """Create independent renewal guards over shared stateless services."""

    def __init__(
        self,
        renewer: TaskClaimRenewer,
        observer: TaskCancellationObserver,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        *,
        lease_seconds: int,
        operation_timeout_seconds: float,
        observation_poll_seconds: float,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        self._renewer = renewer
        self._observer = observer
        self._authenticated_worker = authenticated_worker
        self._worker_session_id = worker_session_id
        self._lease_seconds = lease_seconds
        self._operation_timeout_seconds = operation_timeout_seconds
        self._observation_poll_seconds = observation_poll_seconds
        self._clock = clock

    def guard(
        self,
        envelope: DispatchEnvelope,
        claim: TaskClaimLease,
        cancellation_token: TaskCancellationToken,
    ) -> ClaimRenewalGuard:
        if claim.worker_session_id != self._worker_session_id:
            raise WorkerProcessFailure("claim session does not match worker session")
        return ClaimRenewalGuard(
            self._renewer,
            self._observer,
            self._authenticated_worker,
            self._worker_session_id,
            envelope,
            claim,
            cancellation_token,
            lease_seconds=self._lease_seconds,
            operation_timeout_seconds=self._operation_timeout_seconds,
            observation_poll_seconds=self._observation_poll_seconds,
            clock=self._clock,
        )


class ClaimRenewalGuard:
    """Keep one claimed delivery authoritative until settlement or obsolescence."""

    def __init__(
        self,
        renewer: TaskClaimRenewer,
        observer: TaskCancellationObserver,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        envelope: DispatchEnvelope,
        claim: TaskClaimLease,
        cancellation_token: TaskCancellationToken,
        *,
        lease_seconds: int,
        operation_timeout_seconds: float,
        observation_poll_seconds: float,
        clock: Callable[[], float],
    ) -> None:
        self._renewer = renewer
        self._observer = observer
        self._worker = authenticated_worker
        self._worker_session_id = worker_session_id
        self._envelope = envelope
        self._claim = claim
        self._token = cancellation_token
        self._lease_seconds = lease_seconds
        self._operation_timeout_seconds = operation_timeout_seconds
        self._poll_seconds = observation_poll_seconds
        self._clock = clock
        self._task: asyncio.Task[None] | None = None
        self._closed = False

    def start(self) -> None:
        if self._task is not None or self._closed:
            raise RuntimeError("claim renewal guard cannot be started")
        self._task = asyncio.create_task(
            self._run(),
            name=f"taskforge-claim-renewal-{self._claim.task_attempt_id}",
        )

    async def protect(self, operation: Awaitable[T]) -> T:
        """Abort local work when renewal proves or must assume authority loss."""
        task = self._task
        if task is None:
            raise RuntimeError("claim renewal guard is not started")
        operation_task = asyncio.ensure_future(operation)
        done, _ = await asyncio.wait(
            (operation_task, task), return_when=asyncio.FIRST_COMPLETED
        )
        if task in done:
            self._task = None
            await _cancel_and_wait(operation_task)
            await task
            raise DeliveryAuthorityObsolete
        try:
            result = await operation_task
        except BaseException:
            raise
        if task.done():
            self._task = None
            await task
            raise DeliveryAuthorityObsolete
        return result

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        task, self._task = self._task, None
        if task is not None:
            await _cancel_and_wait(task)

    async def quiesce(self) -> None:
        """Stop renewing and wait for positive durable delivery obsolescence."""
        task, self._task = self._task, None
        if task is not None:
            await _cancel_and_wait(task)
        await self._wait_until_ack_safe()

    async def _run(self) -> None:
        request = TaskClaimRenewalRequest(
            self._claim.task_attempt_id,
            self._claim.generation,
            self._claim.worker_session_id,
            self._claim.lease_expires_at,
            self._envelope.correlation_id,
        )
        # The claim may be an active replay already close to its database expiry.
        # No local time budget is trusted until this process confirms a renewal.
        confirmed_until = self._clock()
        cadence = self._lease_seconds / 3
        uncertain_attempts = 0
        while True:
            try:
                async with asyncio.timeout(self._operation_timeout_seconds):
                    result = await self._renewer.renew_claim(self._worker, request)
            except asyncio.CancelledError:
                raise
            except TaskClaimRenewalRejected as error:
                if error.reason in _PROCESS_RENEWAL_LOSS:
                    raise WorkerProcessFailure(
                        "worker claim authority is invalid"
                    ) from error
                if error.reason not in _LOCAL_RENEWAL_LOSS:
                    raise WorkerProcessFailure(
                        "unclassified claim renewal denial"
                    ) from error
                await self._wait_until_ack_safe()
                return
            except TaskClaimServiceInvariantError as error:
                raise WorkerProcessFailure("claim renewal invariant failed") from error
            except (TaskClaimServiceUnavailable, TimeoutError):
                uncertain_attempts += 1
                if uncertain_attempts > 1 and self._clock() >= confirmed_until:
                    await self._wait_until_ack_safe()
                    return
                await asyncio.sleep(min(self._operation_timeout_seconds, cadence))
                continue
            uncertain_attempts = 0
            if result.claim.task_attempt_id != self._claim.task_attempt_id:
                raise WorkerProcessFailure("claim renewal returned a different attempt")
            if (
                result.claim.generation != self._claim.generation
                or result.claim.worker_session_id != self._worker_session_id
            ):
                raise WorkerProcessFailure("claim renewal returned different authority")
            returned_expiry = result.claim.lease_expires_at
            expected_expiry = request.expected_lease_expires_at
            if returned_expiry < expected_expiry:
                raise WorkerProcessFailure("claim renewal moved lease backwards")
            if (
                result.outcome
                in (TaskClaimRenewalOutcome.RENEWED, TaskClaimRenewalOutcome.REPLAYED)
                and returned_expiry <= expected_expiry
            ) or (
                result.outcome
                in (
                    TaskClaimRenewalOutcome.ACTIVE_UNCHANGED,
                    TaskClaimRenewalOutcome.CANCELLATION_REQUESTED,
                )
                and returned_expiry != expected_expiry
            ):
                raise WorkerProcessFailure("claim renewal outcome is inconsistent")
            if result.outcome is TaskClaimRenewalOutcome.CANCELLATION_REQUESTED:
                assert result.cancellation_requested_at is not None
                self._token._request(result.cancellation_requested_at)
                if self._clock() >= confirmed_until:
                    await self._wait_until_ack_safe()
                    return
            request = TaskClaimRenewalRequest(
                result.claim.task_attempt_id,
                result.claim.generation,
                result.claim.worker_session_id,
                result.claim.lease_expires_at,
                self._envelope.correlation_id,
            )
            if result.outcome in (
                TaskClaimRenewalOutcome.RENEWED,
                TaskClaimRenewalOutcome.ACTIVE_UNCHANGED,
            ):
                confirmed_until = (
                    self._clock()
                    + self._lease_seconds
                    - self._operation_timeout_seconds
                )
            if result.outcome is TaskClaimRenewalOutcome.REPLAYED:
                # A replay proves what an earlier uncertain request committed,
                # but that lease may now be old. Renew its returned expiry
                # immediately before granting a fresh local timing window.
                continue
            await asyncio.sleep(cadence)

    async def _wait_until_ack_safe(self) -> None:
        while True:
            try:
                async with asyncio.timeout(self._operation_timeout_seconds):
                    observation = await self._observer.observe_cancellation(
                        self._worker,
                        self._worker_session_id,
                        self._envelope.workflow_run_id,
                        self._envelope.task_run_id,
                        self._claim.task_attempt_id,
                        self._claim.generation,
                    )
            except asyncio.CancelledError:
                raise
            except (TaskCancellationObservationUnavailable, TimeoutError):
                await asyncio.sleep(self._poll_seconds)
                continue
            except TaskCancellationObservationInvariantError as error:
                raise WorkerProcessFailure(
                    "claim authority observation invariant failed"
                ) from error
            if observation.outcome in _PROCESS_WIDE:
                raise WorkerProcessFailure("worker execution authority is invalid")
            if observation.outcome in _ACK_SAFE:
                return
            if (
                observation.outcome
                is TaskCancellationObservationOutcome.CANCELLATION_REQUESTED
            ):
                assert observation.requested_at is not None
                self._token._request(observation.requested_at)
            await asyncio.sleep(self._poll_seconds)


async def _cancel_and_wait(task: asyncio.Future[Any]) -> None:
    if not task.done():
        task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
