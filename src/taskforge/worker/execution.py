"""Worker delivery validation, claim, start, and trusted handler dispatch."""

from __future__ import annotations

from typing import Protocol
from uuid import UUID

from taskforge.claims.domain import (
    IssuedTaskClaim,
    TaskClaimOutcome,
    TaskClaimRejected,
    TaskClaimRejectionReason,
)
from taskforge.claims.service import (
    TaskClaimServiceInvariantError,
    TaskClaimServiceUnavailable,
)
from taskforge.dispatch.envelope import DispatchEnvelope
from taskforge.dispatch.transport import (
    MalformedDispatchTransport,
    validate_dispatch_transport,
)
from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.worker.consumer_ports import DispatchDeliveryControl
from taskforge.worker.handlers import TaskHandlerInvocation, TaskHandlerRegistry
from taskforge.worker.start import (
    TaskStartInvariantError,
    TaskStartOutcome,
    TaskStartRejected,
    TaskStartRequest,
    TaskStartServiceUnavailable,
)


class WorkerConsumptionPaused(Exception):
    """Consumption must pause while preserving the current valid delivery."""


class HandlerDispatchFailed(Exception):
    """A trusted registered handler raised during dispatch."""


class TaskClaimAcquirer(Protocol):
    async def claim_task(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        dispatch: DispatchEnvelope,
    ) -> IssuedTaskClaim: ...


class TaskStarter(Protocol):
    async def start_task(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        request: TaskStartRequest,
    ) -> TaskStartOutcome: ...


_ACKNOWLEDGED_REJECTIONS = frozenset(
    {
        TaskClaimRejectionReason.STALE_ATTEMPT,
        TaskClaimRejectionReason.OBSOLETE_TASK,
        TaskClaimRejectionReason.ALREADY_AUTHORITATIVE,
    }
)
_PAUSED_REJECTIONS = frozenset(
    {
        TaskClaimRejectionReason.WORKER_AUTHORITY_REJECTED,
        TaskClaimRejectionReason.WORKER_SESSION_UNAVAILABLE,
        TaskClaimRejectionReason.WORKER_SESSION_INACTIVE,
        TaskClaimRejectionReason.WORKER_UNAVAILABLE,
        TaskClaimRejectionReason.CAPABILITY_MISMATCH,
    }
)


class WorkerExecutionConsumer:
    def __init__(
        self,
        claim_service: TaskClaimAcquirer,
        start_service: TaskStarter,
        handlers: TaskHandlerRegistry,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
    ) -> None:
        self._claim_service = claim_service
        self._start_service = start_service
        self._handlers = handlers
        self._authenticated_worker = authenticated_worker
        self._worker_session_id = worker_session_id

    async def consume(self, control: DispatchDeliveryControl) -> None:
        transport = validate_dispatch_transport(
            control.delivery.body, control.delivery.metadata
        )
        if isinstance(transport, MalformedDispatchTransport):
            await control.reject(requeue=False)
            return
        envelope = transport.envelope
        definition = self._handlers.definition(envelope.task_type)
        if (
            definition is None
            or definition.required_capability != envelope.required_capability
        ):
            raise WorkerConsumptionPaused("local handler registration drift")

        try:
            issued = await self._claim_service.claim_task(
                self._authenticated_worker, self._worker_session_id, envelope
            )
        except TaskClaimRejected as error:
            if error.reason is TaskClaimRejectionReason.INVALID_DISPATCH:
                await control.reject(requeue=False)
                return
            if error.reason in _ACKNOWLEDGED_REJECTIONS:
                await control.acknowledge()
                return
            if error.reason in _PAUSED_REJECTIONS:
                raise WorkerConsumptionPaused(
                    "worker cannot claim valid delivery"
                ) from error
            raise WorkerConsumptionPaused("unclassified claim rejection") from error
        except (TaskClaimServiceInvariantError, TaskClaimServiceUnavailable) as error:
            raise WorkerConsumptionPaused("claim persistence failed closed") from error

        if issued.outcome is TaskClaimOutcome.REPLAYED_EXPIRED:
            raise WorkerConsumptionPaused("expired claim requires recovery")
        try:
            await self._start_service.start_task(
                self._authenticated_worker,
                self._worker_session_id,
                TaskStartRequest(
                    envelope.task_run_id,
                    envelope.task_attempt_id,
                    issued.claim.generation,
                ),
            )
        except (
            TaskStartRejected,
            TaskStartInvariantError,
            TaskStartServiceUnavailable,
        ) as error:
            raise WorkerConsumptionPaused("task start failed closed") from error

        invocation = TaskHandlerInvocation(
            envelope.dispatch_id,
            envelope.workflow_run_id,
            envelope.task_run_id,
            envelope.task_attempt_id,
            envelope.attempt_number,
            issued.claim.generation,
            self._worker_session_id,
            envelope.task_type,
            envelope.task_payload,
            envelope.references,
            envelope.correlation_id,
        )
        try:
            await definition.handler(invocation)
        except Exception as error:
            raise HandlerDispatchFailed from error
