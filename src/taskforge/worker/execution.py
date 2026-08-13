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
from taskforge.worker.handlers import (
    TaskDeadline,
    TaskHandlerRegistry,
    create_task_context,
)
from taskforge.worker.start import (
    TaskStartInvariantError,
    TaskStartReceipt,
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
    ) -> TaskStartReceipt: ...


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
            start = await self._start_service.start_task(
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

        context = create_task_context(
            dispatch_id=envelope.dispatch_id,
            workflow_run_id=envelope.workflow_run_id,
            task_run_id=envelope.task_run_id,
            task_attempt_id=envelope.task_attempt_id,
            attempt_number=envelope.attempt_number,
            task_type=envelope.task_type,
            parameters=envelope.task_payload,
            references=envelope.references,
            correlation_id=envelope.correlation_id,
            trace_context=envelope.trace_context,
            cancellation_requested_at_start=start.cancellation_requested_at_start,
            deadline=(
                TaskDeadline(envelope.deadline_at)
                if envelope.deadline_at is not None
                else None
            ),
        )
        try:
            await definition.handler(context)
        except Exception as error:
            raise HandlerDispatchFailed from error
