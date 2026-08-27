"""Worker delivery validation, claim, start, and trusted handler dispatch."""

from __future__ import annotations

import asyncio
import logging
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
from taskforge.logging import bind_log_context, log_event
from taskforge.worker.cancellation import (
    TaskCancellationObservationOutcome,
    TaskCancellationObserver,
    TaskCancellationToken,
)
from taskforge.worker.consumer_ports import DispatchDeliveryControl
from taskforge.worker.handlers import (
    TaskContext,
    TaskDeadline,
    TaskHandler,
    TaskHandlerDefinition,
    TaskHandlerRegistry,
    create_task_context,
)
from taskforge.worker.result_submission import (
    TaskResultAuthorityRejected,
    TaskResultConflict,
    TaskResultInvalidOutput,
    TaskResultInvalidState,
    TaskResultInvariantError,
    TaskResultNotFound,
    TaskResultServiceUnavailable,
    TaskResultStale,
    TaskResultSubmissionOutcome,
    TaskResultSubmissionReceipt,
    TaskResultSubmissionRequest,
)
from taskforge.worker.results import (
    TaskCancellation,
    TaskExecutionResult,
    TaskPermanentFailure,
    TaskRetryableFailure,
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


logger = logging.getLogger(__name__)


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


class TaskResultSubmitter(Protocol):
    async def submit_result(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        request: TaskResultSubmissionRequest,
    ) -> TaskResultSubmissionReceipt: ...


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
        result_service: TaskResultSubmitter,
        handlers: TaskHandlerRegistry,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        cancellation_observer: TaskCancellationObserver | None = None,
        *,
        cancellation_poll_seconds: float = 1.0,
    ) -> None:
        if cancellation_poll_seconds <= 0:
            raise ValueError("cancellation poll interval must be positive")
        self._claim_service = claim_service
        self._start_service = start_service
        self._result_service = result_service
        self._handlers = handlers
        self._authenticated_worker = authenticated_worker
        self._worker_session_id = worker_session_id
        self._cancellation_observer = cancellation_observer
        self._cancellation_poll_seconds = cancellation_poll_seconds

    async def consume(self, control: DispatchDeliveryControl) -> None:
        transport = validate_dispatch_transport(
            control.delivery.body, control.delivery.metadata
        )
        if isinstance(transport, MalformedDispatchTransport):
            log_event(
                logger,
                logging.WARNING,
                "broker.delivery.malformed",
                {
                    "reason.code": transport.code,
                    "broker.redelivered": control.delivery.redelivered,
                },
            )
            await control.reject(requeue=False)
            return
        envelope = transport.envelope
        fields: dict[str, object] = {
            "dispatch.id": envelope.dispatch_id,
            "workflow.run.id": envelope.workflow_run_id,
            "task.run.id": envelope.task_run_id,
            "task.attempt.id": envelope.task_attempt_id,
            "task.attempt.number": envelope.attempt_number,
            "task.type": envelope.task_type,
            "worker.id": self._authenticated_worker.worker_identity_id,
            "worker.session.id": self._worker_session_id,
            "broker.redelivered": control.delivery.redelivered,
        }
        if envelope.correlation_id is not None:
            fields["correlation.id"] = envelope.correlation_id
        with bind_log_context(**fields):
            log_event(logger, logging.INFO, "worker.delivery.validated")
            await self._consume_validated(control, envelope)

    async def _consume_validated(
        self, control: DispatchDeliveryControl, envelope: DispatchEnvelope
    ) -> None:
        definition = self._handlers.definition(envelope.task_type)
        if (
            definition is None
            or definition.required_capability != envelope.required_capability
        ):
            log_event(
                logger,
                logging.ERROR,
                "worker.handler.registration_drift",
                {"reason.code": "handler_registration_drift"},
            )
            raise WorkerConsumptionPaused("local handler registration drift")

        try:
            issued = await self._claim_service.claim_task(
                self._authenticated_worker, self._worker_session_id, envelope
            )
        except TaskClaimRejected as error:
            log_event(
                logger,
                (
                    logging.INFO
                    if error.reason in _ACKNOWLEDGED_REJECTIONS
                    else logging.WARNING
                ),
                "worker.claim.rejected",
                {"reason.code": error.reason.value, "outcome": "rejected"},
                error=error,
            )
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
            log_event(
                logger,
                logging.ERROR,
                "worker.claim.failed",
                {"error.category": "claim_service_failure", "outcome": "paused"},
                error=error,
            )
            raise WorkerConsumptionPaused("claim persistence failed closed") from error

        with bind_log_context(**{"claim.generation": issued.claim.generation}):
            log_event(
                logger,
                logging.INFO,
                "worker.claim.issued",
                {"outcome": issued.outcome.value},
            )
            await self._consume_claimed(control, envelope, definition, issued)

    async def _consume_claimed(
        self,
        control: DispatchDeliveryControl,
        envelope: DispatchEnvelope,
        definition: TaskHandlerDefinition,
        issued: IssuedTaskClaim,
    ) -> None:
        if issued.outcome is TaskClaimOutcome.REPLAYED_EXPIRED:
            log_event(
                logger,
                logging.WARNING,
                "worker.claim.expired",
                {"reason.code": "replayed_expired", "outcome": "paused"},
            )
            raise WorkerConsumptionPaused("expired claim requires recovery")
        try:
            start = await self._start_service.start_task(
                self._authenticated_worker,
                self._worker_session_id,
                TaskStartRequest(
                    envelope.task_run_id,
                    envelope.task_attempt_id,
                    issued.claim.generation,
                    envelope.correlation_id,
                ),
            )
        except (
            TaskStartRejected,
            TaskStartInvariantError,
            TaskStartServiceUnavailable,
        ) as error:
            log_event(
                logger,
                logging.ERROR,
                "worker.start.failed",
                {"error.category": "task_start_failure", "outcome": "paused"},
                error=error,
            )
            raise WorkerConsumptionPaused("task start failed closed") from error
        log_event(logger, logging.INFO, "worker.task.started")

        cancellation_token = TaskCancellationToken()
        initial_observation = TaskCancellationObservationOutcome.ACTIVE
        if self._cancellation_observer is not None:
            initial_observation = await self._observe_cancellation_once(
                envelope, issued, cancellation_token
            )
        if (
            initial_observation
            is TaskCancellationObservationOutcome.NO_LONGER_AUTHORITATIVE
        ):
            # The durable claim/session authority is already gone. As with an
            # obsolete or already-authoritative claim delivery, there is no work
            # left for this delivery to perform and no worker result to author.
            await control.acknowledge()
            log_event(
                logger,
                logging.INFO,
                "worker.delivery.acknowledged",
                {"outcome": "no_longer_authoritative"},
            )
            return
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
            cancellation_token=cancellation_token,
            deadline=(
                TaskDeadline(envelope.deadline_at)
                if envelope.deadline_at is not None
                else None
            ),
        )
        monitor: asyncio.Task[None] | None = None
        if (
            self._cancellation_observer is not None
            and initial_observation is TaskCancellationObservationOutcome.ACTIVE
        ):
            monitor = asyncio.create_task(
                self._monitor_cancellation(envelope, issued, cancellation_token),
                name=f"taskforge-cancellation-{envelope.task_attempt_id}",
            )
        try:
            result = (
                TaskExecutionResult.cancellation()
                if cancellation_token.is_cancellation_requested
                else await _execute_handler_logged(
                    definition.handler, context, envelope.execution_timeout_seconds
                )
            )
        finally:
            if monitor is not None:
                monitor.cancel()
                try:
                    await monitor
                except asyncio.CancelledError:
                    pass
        if issued.result_authority is None:
            raise WorkerConsumptionPaused("active claim lacks result authority")
        try:
            receipt = await self._result_service.submit_result(
                self._authenticated_worker,
                self._worker_session_id,
                TaskResultSubmissionRequest(
                    envelope.dispatch_id,
                    envelope.task_run_id,
                    envelope.task_attempt_id,
                    issued.claim.generation,
                    issued.result_authority,
                    result,
                    envelope.correlation_id,
                ),
            )
        except TaskResultStale as error:
            log_event(
                logger,
                logging.INFO,
                "worker.result.stale",
                {"reason.code": "stale_result", "outcome": "paused"},
                error=error,
            )
            raise WorkerConsumptionPaused(
                "task result persistence failed closed"
            ) from error
        except (
            TaskResultAuthorityRejected,
            TaskResultConflict,
            TaskResultInvalidOutput,
            TaskResultInvalidState,
            TaskResultInvariantError,
            TaskResultNotFound,
            TaskResultServiceUnavailable,
        ) as error:
            log_event(
                logger,
                logging.ERROR,
                "worker.result.failed",
                {"error.category": "result_submission_failure", "outcome": "paused"},
                error=error,
            )
            raise WorkerConsumptionPaused(
                "task result persistence failed closed"
            ) from error
        if (
            receipt.task_attempt_id != envelope.task_attempt_id
            or receipt.outcome
            not in {
                TaskResultSubmissionOutcome.ACCEPTED,
                TaskResultSubmissionOutcome.REPLAYED_IDENTICAL,
            }
        ):
            raise WorkerConsumptionPaused("task result receipt failed closed")
        await control.acknowledge()

        log_event(
            logger,
            logging.INFO,
            "worker.delivery.completed",
            {
                "outcome": receipt.outcome.value,
            },
        )

    async def _observe_cancellation_once(
        self,
        envelope: DispatchEnvelope,
        issued: IssuedTaskClaim,
        token: TaskCancellationToken,
    ) -> TaskCancellationObservationOutcome:
        assert self._cancellation_observer is not None
        try:
            observation = await self._cancellation_observer.observe_cancellation(
                self._authenticated_worker,
                self._worker_session_id,
                envelope.workflow_run_id,
                envelope.task_run_id,
                envelope.task_attempt_id,
                issued.claim.generation,
            )
        except Exception:
            # Observation is advisory but bounded: transient persistence failures
            # never fabricate cancellation and do not permanently stop polling.
            return TaskCancellationObservationOutcome.ACTIVE
        if (
            observation.outcome
            is TaskCancellationObservationOutcome.CANCELLATION_REQUESTED
        ):
            assert observation.requested_at is not None
            token._request(observation.requested_at)
        return observation.outcome

    async def _monitor_cancellation(
        self,
        envelope: DispatchEnvelope,
        issued: IssuedTaskClaim,
        token: TaskCancellationToken,
    ) -> None:
        while True:
            await asyncio.sleep(self._cancellation_poll_seconds)
            outcome = await self._observe_cancellation_once(envelope, issued, token)
            if outcome is not TaskCancellationObservationOutcome.ACTIVE:
                return


async def _execute_handler(
    handler: TaskHandler,
    context: TaskContext,
    execution_timeout_seconds: int | None,
) -> TaskExecutionResult:
    try:
        if execution_timeout_seconds is None:
            raw_result = await handler(context)
        else:
            timeout = asyncio.timeout(execution_timeout_seconds)
            try:
                async with timeout:
                    raw_result = await handler(context)
            except TimeoutError:
                if timeout.expired():
                    return TaskExecutionResult.retryable_execution_timeout()
                return TaskExecutionResult.retryable_handler_exception()
    except Exception:
        return TaskExecutionResult.retryable_handler_exception()
    if isinstance(raw_result, TaskRetryableFailure):
        return TaskExecutionResult.retryable_handler_reported()
    if isinstance(raw_result, TaskPermanentFailure):
        return TaskExecutionResult.permanent_failure()
    if isinstance(raw_result, TaskCancellation):
        return TaskExecutionResult.cancellation()
    return TaskExecutionResult.success(raw_result)


async def _execute_handler_logged(
    handler: TaskHandler,
    context: TaskContext,
    execution_timeout_seconds: int | None,
) -> TaskExecutionResult:
    log_event(logger, logging.INFO, "worker.handler.started")
    result = await _execute_handler(handler, context, execution_timeout_seconds)
    log_event(
        logger,
        logging.INFO,
        "worker.handler.completed",
        {
            "outcome": result.kind.value,
            "reason.code": (
                result.failure_kind.value
                if result.failure_kind is not None
                else "completed"
            ),
        },
    )
    return result
