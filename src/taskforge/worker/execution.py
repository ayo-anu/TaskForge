"""Worker delivery validation, claim, start, and trusted handler dispatch."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from time import perf_counter
from typing import Protocol, TypeVar
from uuid import UUID

from opentelemetry.trace import SpanKind
from opentelemetry.util.types import AttributeValue

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
from taskforge.metrics import add as add_metric
from taskforge.metrics import record as record_metric
from taskforge.tracing import (
    extract_trace_context,
    set_attributes,
    set_error,
    set_error_type,
    span,
)
from taskforge.worker.cancellation import (
    TaskCancellationObservationInvariantError,
    TaskCancellationObservationOutcome,
    TaskCancellationObservationUnavailable,
    TaskCancellationObserver,
    TaskCancellationToken,
)
from taskforge.worker.claim_renewal import (
    ClaimRenewalGuard,
    ClaimRenewalSupervisor,
    DeliveryAuthorityObsolete,
)
from taskforge.worker.consumer_ports import (
    BrokerConsumerUnavailable,
    DispatchDeliveryControl,
)
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
    TaskResultRateLimited,
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
from taskforge.worker.runtime_errors import WorkerProcessFailure
from taskforge.worker.start import (
    TaskStartInvariantError,
    TaskStartReceipt,
    TaskStartRejected,
    TaskStartRejectionReason,
    TaskStartRequest,
    TaskStartServiceUnavailable,
)


class WorkerConsumptionPaused(Exception):
    """Consumption must pause while preserving the current valid delivery."""


logger = logging.getLogger(__name__)
T = TypeVar("T")

_ACKNOWLEDGED_AUTHORITY_OBSERVATIONS = frozenset(
    {
        TaskCancellationObservationOutcome.CLAIM_RECOVERED,
        TaskCancellationObservationOutcome.ATTEMPT_OR_GENERATION_OBSOLETE,
        TaskCancellationObservationOutcome.TASK_INACTIVE,
    }
)
_PROCESS_AUTHORITY_OBSERVATIONS = frozenset(
    {
        TaskCancellationObservationOutcome.WORKER_AUTHORITY_REJECTED,
        TaskCancellationObservationOutcome.WORKER_SESSION_INACTIVE,
    }
)
_DELIVERY_AUTHORITY_LOSS_OBSERVATIONS = frozenset(
    {
        TaskCancellationObservationOutcome.CLAIM_EXPIRED_AWAITING_RECOVERY,
        *_ACKNOWLEDGED_AUTHORITY_OBSERVATIONS,
    }
)


class _DeliveryAuthorityLost(Exception):
    """The running delivery must stop and enter reason-preserving quiescence."""

    def __init__(self, outcome: TaskCancellationObservationOutcome) -> None:
        self.outcome = outcome
        super().__init__("delivery execution authority was lost")


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
        claim_renewal: ClaimRenewalSupervisor | None = None,
        process_failure_known: Callable[[], bool] = lambda: False,
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
        self._claim_renewal = claim_renewal
        self._process_failure_known = process_failure_known
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
            await self._reject(control, None, requeue=False)
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
            parent = extract_trace_context(envelope.trace_context)
            with span(
                "taskforge.worker.process",
                kind=SpanKind.CONSUMER,
                parent=parent,
                attributes={
                    "messaging.system": "rabbitmq",
                    "messaging.destination.name": envelope.route,
                    "messaging.message.id": str(envelope.dispatch_id),
                    "messaging.message.redelivered": control.delivery.redelivered,
                    "taskforge.broker.route": envelope.route,
                },
            ) as process_span:
                try:
                    log_event(logger, logging.INFO, "worker.delivery.validated")
                    await self._consume_validated(control, envelope)
                except WorkerConsumptionPaused as error:
                    if isinstance(
                        error.__cause__,
                        (
                            TaskClaimRejected,
                            TaskStartRejected,
                            TaskResultAuthorityRejected,
                            TaskResultConflict,
                            TaskResultInvalidOutput,
                            TaskResultInvalidState,
                            TaskResultNotFound,
                            TaskResultStale,
                        ),
                    ):
                        set_attributes(process_span, {"taskforge.outcome": "rejected"})
                    else:
                        set_error(process_span, error, "worker_consumption_paused")
                    raise

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
                await self._reject(control, envelope, requeue=False)
                return
            if error.reason in _ACKNOWLEDGED_REJECTIONS:
                await self._acknowledge(control, envelope)
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
        cancellation_token = TaskCancellationToken()
        guard = (
            self._claim_renewal.guard(envelope, issued.claim, cancellation_token)
            if self._claim_renewal is not None
            else None
        )
        if guard is not None:
            guard.start()
        try:
            await self._consume_with_authority(
                control,
                envelope,
                definition,
                issued,
                cancellation_token,
                guard,
            )
        except DeliveryAuthorityObsolete as error:
            if self._process_failure_known():
                raise WorkerProcessFailure(
                    "process authority failed before delivery acknowledgement"
                ) from error
            await self._acknowledge(control, envelope)
            log_event(
                logger,
                logging.INFO,
                "worker.delivery.acknowledged",
                {"outcome": "delivery_authority_obsolete"},
            )
        finally:
            if guard is not None:
                await guard.close()

    async def _consume_with_authority(
        self,
        control: DispatchDeliveryControl,
        envelope: DispatchEnvelope,
        definition: TaskHandlerDefinition,
        issued: IssuedTaskClaim,
        cancellation_token: TaskCancellationToken,
        guard: ClaimRenewalGuard | None,
    ) -> None:
        if issued.outcome is TaskClaimOutcome.REPLAYED_EXPIRED:
            log_event(
                logger,
                logging.WARNING,
                "worker.claim.expired",
                {"reason.code": "replayed_expired", "outcome": "paused"},
            )
            if guard is None:
                raise WorkerConsumptionPaused("expired claim requires recovery")
            await guard.quiesce()
            raise DeliveryAuthorityObsolete
        try:
            start = await self._protect(
                guard,
                self._start_service.start_task(
                    self._authenticated_worker,
                    self._worker_session_id,
                    TaskStartRequest(
                        envelope.task_run_id,
                        envelope.task_attempt_id,
                        issued.claim.generation,
                        envelope.correlation_id,
                    ),
                ),
            )
        except TaskStartRejected as error:
            if (
                error.reason is TaskStartRejectionReason.STALE_CLAIM
                and guard is not None
            ):
                await guard.quiesce()
                raise DeliveryAuthorityObsolete from error
            if guard is None:
                raise WorkerConsumptionPaused(
                    "worker cannot start claimed task"
                ) from error
            raise WorkerProcessFailure("worker cannot start claimed task") from error
        except (TaskStartInvariantError, TaskStartServiceUnavailable) as error:
            log_event(
                logger,
                logging.ERROR,
                "worker.start.failed",
                {"error.category": "task_start_failure", "outcome": "paused"},
                error=error,
            )
            if guard is None:
                raise WorkerConsumptionPaused("task start failed closed") from error
            raise WorkerProcessFailure("task start failed closed") from error
        log_event(logger, logging.INFO, "worker.task.started")

        initial_observation = TaskCancellationObservationOutcome.ACTIVE
        if self._cancellation_observer is not None:
            initial_observation = await self._observe_cancellation_once(
                envelope, issued, cancellation_token
            )
        if initial_observation in _PROCESS_AUTHORITY_OBSERVATIONS:
            raise WorkerProcessFailure("worker execution authority is invalid")
        if initial_observation in _ACKNOWLEDGED_AUTHORITY_OBSERVATIONS:
            if guard is not None:
                await guard.quiesce()
                raise DeliveryAuthorityObsolete
            await self._acknowledge(control, envelope)
            return
        if (
            initial_observation
            is TaskCancellationObservationOutcome.CLAIM_EXPIRED_AWAITING_RECOVERY
        ):
            if guard is None:
                raise WorkerConsumptionPaused("expired claim requires recovery")
            await guard.quiesce()
            raise DeliveryAuthorityObsolete
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
        monitor_authority_loss_handled = False
        try:
            try:
                result = (
                    TaskExecutionResult.cancellation()
                    if cancellation_token.is_cancellation_requested
                    else await self._protect(
                        guard,
                        self._execute_with_cancellation_monitor(
                            definition.handler,
                            context,
                            envelope.execution_timeout_seconds,
                            monitor,
                        ),
                    )
                )
            except _DeliveryAuthorityLost as error:
                monitor_authority_loss_handled = True
                if guard is None:
                    raise WorkerConsumptionPaused(
                        "delivery execution authority was lost"
                    ) from error
                await guard.quiesce()
                raise DeliveryAuthorityObsolete from error
        finally:
            if monitor is not None:
                monitor.cancel()
                try:
                    await monitor
                except asyncio.CancelledError:
                    pass
                except _DeliveryAuthorityLost:
                    if not monitor_authority_loss_handled:
                        raise
        if issued.result_authority is None:
            raise WorkerConsumptionPaused("active claim lacks result authority")
        request = TaskResultSubmissionRequest(
            envelope.dispatch_id,
            envelope.task_run_id,
            envelope.task_attempt_id,
            issued.claim.generation,
            issued.result_authority,
            result,
            envelope.correlation_id,
        )
        while True:
            try:
                receipt = await self._protect(
                    guard,
                    self._result_service.submit_result(
                        self._authenticated_worker,
                        self._worker_session_id,
                        request,
                    ),
                )
                break
            except TaskResultRateLimited as error:
                log_event(
                    logger,
                    logging.WARNING,
                    "worker.result.rate_limited",
                    {"outcome": "rate_limited", "error.retryable": True},
                )
                if guard is None:
                    raise WorkerConsumptionPaused(
                        "task result submission rate limited"
                    ) from error
                await self._protect(
                    guard, asyncio.sleep(max(1, error.retry_after_seconds))
                )
            except TaskResultStale as error:
                log_event(
                    logger,
                    logging.INFO,
                    "worker.result.stale",
                    {"reason.code": "stale_result", "outcome": "paused"},
                    error=error,
                )
                if guard is not None:
                    await guard.quiesce()
                    raise DeliveryAuthorityObsolete from error
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
                if guard is None:
                    raise WorkerConsumptionPaused(
                        "task result persistence failed closed"
                    ) from error
                raise WorkerProcessFailure(
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
        await self._protect(guard, self._acknowledge(control, envelope))

        log_event(
            logger,
            logging.INFO,
            "worker.delivery.completed",
            {
                "outcome": receipt.outcome.value,
            },
        )

    async def _protect(
        self, guard: ClaimRenewalGuard | None, operation: Awaitable[T]
    ) -> T:
        if guard is None:
            return await operation
        return await guard.protect(operation)

    async def _execute_with_cancellation_monitor(
        self,
        handler: TaskHandler,
        context: TaskContext,
        execution_timeout_seconds: int | None,
        monitor: asyncio.Task[None] | None,
    ) -> TaskExecutionResult:
        execution = asyncio.create_task(
            _execute_handler_logged(handler, context, execution_timeout_seconds),
            name=f"taskforge-handler-{context.task_attempt_id}",
        )
        try:
            if monitor is None:
                return await execution
            done, _ = await asyncio.wait(
                (execution, monitor), return_when=asyncio.FIRST_COMPLETED
            )
            if monitor in done:
                error = monitor.exception()
                if error is not None:
                    await _cancel_task(execution)
                    raise error
            return await execution
        finally:
            await _cancel_task(execution)

    async def _acknowledge(
        self, control: DispatchDeliveryControl, envelope: DispatchEnvelope
    ) -> None:
        with span(
            "taskforge.delivery.ack",
            kind=SpanKind.CLIENT,
            attributes={
                "messaging.system": "rabbitmq",
                "messaging.destination.name": envelope.route,
                "messaging.message.id": str(envelope.dispatch_id),
                "taskforge.broker.route": envelope.route,
                "taskforge.delivery.disposition": "ack",
            },
        ) as active_span:
            try:
                await control.acknowledge()
            except BrokerConsumerUnavailable as error:
                set_error(active_span, error, "broker_acknowledgement_failure")
                raise

    async def _reject(
        self,
        control: DispatchDeliveryControl,
        envelope: DispatchEnvelope | None,
        *,
        requeue: bool,
    ) -> None:
        attributes: dict[str, AttributeValue] = {
            "messaging.system": "rabbitmq",
            "taskforge.delivery.disposition": (
                "reject_requeue" if requeue else "reject_drop"
            ),
        }
        if envelope is not None:
            attributes.update(
                {
                    "messaging.destination.name": envelope.route,
                    "messaging.message.id": str(envelope.dispatch_id),
                    "taskforge.broker.route": envelope.route,
                }
            )
        with span(
            "taskforge.delivery.reject",
            kind=SpanKind.CLIENT,
            attributes=attributes,
        ) as active_span:
            try:
                await control.reject(requeue=requeue)
            except BrokerConsumerUnavailable as error:
                set_error(active_span, error, "broker_rejection_failure")
                raise

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
        except TaskCancellationObservationUnavailable:
            # Observation is advisory but bounded: transient persistence failures
            # never fabricate cancellation and do not permanently stop polling.
            return TaskCancellationObservationOutcome.ACTIVE
        except TaskCancellationObservationInvariantError as error:
            raise WorkerProcessFailure(
                "task authority observation invariant failed"
            ) from error
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
            if outcome in _PROCESS_AUTHORITY_OBSERVATIONS:
                raise WorkerProcessFailure("worker execution authority is invalid")
            if outcome in _DELIVERY_AUTHORITY_LOSS_OBSERVATIONS:
                raise _DeliveryAuthorityLost(outcome)
            if outcome is TaskCancellationObservationOutcome.CANCELLATION_REQUESTED:
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
    started = perf_counter()
    with span("taskforge.handler.execute") as active_span:
        log_event(logger, logging.INFO, "worker.handler.started")
        result = await _execute_handler(handler, context, execution_timeout_seconds)
        reason = (
            result.failure_kind.value
            if result.failure_kind is not None
            else "completed"
        )
        set_attributes(
            active_span,
            {"taskforge.outcome": result.kind.value, "taskforge.reason.code": reason},
        )
        if reason in {"handler_exception", "execution_timeout"}:
            set_error_type(active_span, "TaskHandlerFailure", reason)
        log_event(
            logger,
            logging.INFO,
            "worker.handler.completed",
            {"outcome": result.kind.value, "reason.code": reason},
        )
        attributes = {"taskforge.result.kind": result.kind.value}
        if result.failure_kind is not None:
            attributes["taskforge.failure.kind"] = result.failure_kind.value
        add_metric("taskforge.handler.executions", attributes=attributes)
        record_metric(
            "taskforge.handler.duration",
            perf_counter() - started,
            attributes,
        )
        return result


async def _cancel_task(task: asyncio.Task[object]) -> None:
    if not task.done():
        task.cancel()
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            # A second cancellation of the delivery must not detach a handler
            # that can still perform side effects under the live claim.
            continue
        except BaseException:
            break
    if task.done():
        try:
            task.result()
        except asyncio.CancelledError:
            pass
