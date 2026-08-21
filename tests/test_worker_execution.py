from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest

from taskforge.claims.authority import TaskClaimResultAuthorityIssuer
from taskforge.claims.domain import (
    IssuedTaskClaim,
    TaskClaimLease,
    TaskClaimOutcome,
    TaskClaimRejected,
    TaskClaimRejectionReason,
)
from taskforge.dispatch.envelope import (
    TraceContext,
    create_dispatch_envelope,
    serialize_dispatch_envelope,
)
from taskforge.dispatch.transport import DispatchTransportMetadata
from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.worker.consumer_ports import (
    BrokerConsumerUnavailable,
    BrokerDispatchDelivery,
)
from taskforge.worker.execution import (
    WorkerConsumptionPaused,
    WorkerExecutionConsumer,
    _execute_handler,
)
from taskforge.worker.handlers import (
    TaskContext,
    TaskDeadline,
    TaskHandlerDefinition,
    TaskHandlerRegistry,
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
)
from taskforge.worker.results import (
    TaskCancellation,
    TaskExecutionFailureKind,
    TaskExecutionResult,
    TaskExecutionResultKind,
    TaskPermanentFailure,
    TaskRetryableFailure,
)
from taskforge.worker.start import (
    TaskStartInvariantError,
    TaskStartOutcome,
    TaskStartReceipt,
)
from taskforge.workflows.task_types import (
    JSONMapping,
    TaskTypeDefinition,
    TaskTypeRegistry,
    WorkflowValidationIssue,
)


@dataclass(frozen=True)
class AcceptParameters:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        del parameters
        return ()


class Control:
    def __init__(
        self,
        body: bytes,
        metadata: DispatchTransportMetadata,
        *,
        acknowledge_error: Exception | None = None,
    ) -> None:
        self._delivery = BrokerDispatchDelivery(body, metadata, False)
        self.actions: list[str] = []
        self.acknowledge_error = acknowledge_error

    @property
    def delivery(self) -> BrokerDispatchDelivery:
        return self._delivery

    async def acknowledge(self) -> None:
        self.actions.append("ack")
        if self.acknowledge_error is not None:
            raise self.acknowledge_error

    async def reject(self, *, requeue: bool) -> None:
        self.actions.append(f"reject:{requeue}")


class ClaimService:
    def __init__(self, result: IssuedTaskClaim | Exception) -> None:
        self.result = result

    async def claim_task(self, *args: Any) -> IssuedTaskClaim:
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


class StartService:
    def __init__(
        self,
        events: list[str],
        *,
        cancellation_requested: bool = False,
        error: Exception | None = None,
    ) -> None:
        self.events = events
        self.cancellation_requested = cancellation_requested
        self.error = error

    async def start_task(self, *args: Any) -> TaskStartReceipt:
        self.events.append("start")
        if self.error is not None:
            raise self.error
        return TaskStartReceipt(TaskStartOutcome.STARTED, self.cancellation_requested)


class ResultService:
    def __init__(
        self,
        events: list[str] | None = None,
        *,
        outcome: TaskResultSubmissionOutcome = TaskResultSubmissionOutcome.ACCEPTED,
        error: Exception | None = None,
        task_attempt_id: Any = None,
    ) -> None:
        self.events = events
        self.outcome = outcome
        self.error = error
        self.task_attempt_id = task_attempt_id
        self.requests: list[Any] = []

    async def submit_result(self, *args: Any) -> TaskResultSubmissionReceipt:
        request = args[-1]
        self.requests.append(request)
        if self.events is not None:
            self.events.append("result")
        if self.error is not None:
            raise self.error
        return TaskResultSubmissionReceipt(
            self.outcome,
            self.task_attempt_id or request.task_attempt_id,
        )


def fixture(
    *,
    task_attempt_id: Any = None,
    generation: int = 1,
    correlation_id: str | None = None,
    trace_context: TraceContext | None = None,
    deadline_at: datetime | None = None,
    execution_timeout_seconds: int | None = None,
) -> tuple[Any, Control, IssuedTaskClaim, AuthenticatedWorker, Any]:
    task_attempt_id = task_attempt_id or uuid4()
    envelope = create_dispatch_envelope(
        dispatch_id=uuid4(),
        task_attempt_id=task_attempt_id,
        task_run_id=uuid4(),
        workflow_run_id=uuid4(),
        attempt_number=1,
        task_type="test.task",
        required_capability="test-capability",
        task_payload={"secret": "value"},
        references={},
        correlation_id=correlation_id,
        trace_context=trace_context,
        deadline_at=deadline_at,
        execution_timeout_seconds=execution_timeout_seconds,
    )
    metadata = DispatchTransportMetadata(
        str(envelope.dispatch_id), envelope.route, "application/json", "utf-8"
    )
    now = datetime.now(UTC)
    issued = IssuedTaskClaim(
        TaskClaimOutcome.ACQUIRED_ACTIVE,
        TaskClaimLease(
            envelope.task_attempt_id,
            generation,
            uuid4(),
            now,
            now + timedelta(seconds=60),
        ),
        TaskClaimResultAuthorityIssuer(b"a" * 32).issue(
            worker_identity_id=uuid4(),
            worker_session_id=uuid4(),
            task_attempt_id=envelope.task_attempt_id,
            generation=generation,
        ),
    )
    return (
        envelope,
        Control(serialize_dispatch_envelope(envelope), metadata),
        issued,
        AuthenticatedWorker(uuid4(), uuid4()),
        metadata,
    )


def registry(handler: Any) -> TaskHandlerRegistry:
    task_types = TaskTypeRegistry(
        (TaskTypeDefinition("test.task", "test-capability", AcceptParameters()),)
    )
    return TaskHandlerRegistry(
        (TaskHandlerDefinition("test.task", "test-capability", handler),), task_types
    )


def test_valid_delivery_acks_only_after_result_submission() -> None:
    envelope, control, issued, worker, _ = fixture()
    events: list[str] = []

    async def handler(invocation: TaskContext) -> object:
        events.append("handler")
        assert invocation.task_attempt_id == envelope.task_attempt_id
        assert control.actions == []
        return None

    result_service = ResultService(events)
    consumer = WorkerExecutionConsumer(
        ClaimService(issued),
        StartService(events),
        result_service,
        registry(handler),
        worker,
        issued.claim.worker_session_id,
    )
    asyncio.run(consumer.consume(control))

    assert events == ["start", "handler", "result"]
    assert control.actions == ["ack"]
    assert len(result_service.requests) == 1
    assert result_service.requests[0].result.kind is TaskExecutionResultKind.SUCCESS
    assert "result_authority=<redacted>" in repr(result_service.requests[0])


@pytest.mark.parametrize(
    "outcome",
    (
        TaskResultSubmissionOutcome.ACCEPTED,
        TaskResultSubmissionOutcome.REPLAYED_IDENTICAL,
    ),
)
def test_explicit_successful_result_receipts_ack_exactly_once(
    outcome: TaskResultSubmissionOutcome,
) -> None:
    _, control, issued, worker, _ = fixture()
    consumer = WorkerExecutionConsumer(
        ClaimService(issued),
        StartService([]),
        ResultService(outcome=outcome),
        registry(lambda context: asyncio.sleep(0, result=context)),
        worker,
        issued.claim.worker_session_id,
    )

    asyncio.run(consumer.consume(control))

    assert control.actions == ["ack"]


@pytest.mark.parametrize(
    "error",
    (
        TaskResultConflict(),
        TaskResultStale(),
        TaskResultAuthorityRejected(),
        TaskResultInvalidState(),
        TaskResultInvariantError(),
        TaskResultNotFound(),
        TaskResultInvalidOutput(),
        TaskResultServiceUnavailable(),
    ),
)
def test_result_rejections_and_uncertainty_preserve_delivery(error: Exception) -> None:
    _, control, issued, worker, _ = fixture()
    consumer = WorkerExecutionConsumer(
        ClaimService(issued),
        StartService([]),
        ResultService(error=error),
        registry(lambda context: asyncio.sleep(0, result=context)),
        worker,
        issued.claim.worker_session_id,
    )

    with pytest.raises(WorkerConsumptionPaused, match="persistence failed closed"):
        asyncio.run(consumer.consume(control))

    assert control.actions == []


def test_mismatched_result_receipt_preserves_delivery() -> None:
    _, control, issued, worker, _ = fixture()
    consumer = WorkerExecutionConsumer(
        ClaimService(issued),
        StartService([]),
        ResultService(task_attempt_id=uuid4()),
        registry(lambda context: asyncio.sleep(0, result=context)),
        worker,
        issued.claim.worker_session_id,
    )

    with pytest.raises(WorkerConsumptionPaused, match="receipt failed closed"):
        asyncio.run(consumer.consume(control))

    assert control.actions == []


def test_unrecognized_result_receipt_outcome_preserves_delivery() -> None:
    _, control, issued, worker, _ = fixture()
    consumer = WorkerExecutionConsumer(
        ClaimService(issued),
        StartService([]),
        ResultService(outcome="unexpected"),  # type: ignore[arg-type]
        registry(lambda context: asyncio.sleep(0, result=context)),
        worker,
        issued.claim.worker_session_id,
    )

    with pytest.raises(WorkerConsumptionPaused, match="receipt failed closed"):
        asyncio.run(consumer.consume(control))

    assert control.actions == []


def test_ack_failure_occurs_after_result_and_does_not_resubmit() -> None:
    _, original, issued, worker, _ = fixture()
    control = Control(
        original.delivery.body,
        original.delivery.metadata,
        acknowledge_error=BrokerConsumerUnavailable(),
    )
    events: list[str] = []
    results = ResultService(events)
    consumer = WorkerExecutionConsumer(
        ClaimService(issued),
        StartService(events),
        results,
        registry(lambda context: asyncio.sleep(0, result=context)),
        worker,
        issued.claim.worker_session_id,
    )

    with pytest.raises(BrokerConsumerUnavailable):
        asyncio.run(consumer.consume(control))

    assert events == ["start", "result"]
    assert control.actions == ["ack"]
    assert len(results.requests) == 1


def test_external_cancellation_never_submits_or_acks() -> None:
    _, control, issued, worker, _ = fixture()
    results = ResultService()

    async def handler(context: TaskContext) -> object:
        del context
        raise asyncio.CancelledError

    consumer = WorkerExecutionConsumer(
        ClaimService(issued),
        StartService([]),
        results,
        registry(handler),
        worker,
        issued.claim.worker_session_id,
    )

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(consumer.consume(control))

    assert results.requests == []
    assert control.actions == []


def test_external_cancellation_during_result_submission_never_acks() -> None:
    _, control, issued, worker, _ = fixture()

    class CancelledResultService:
        async def submit_result(self, *args: Any) -> TaskResultSubmissionReceipt:
            del args
            raise asyncio.CancelledError

    consumer = WorkerExecutionConsumer(
        ClaimService(issued),
        StartService([]),
        CancelledResultService(),
        registry(lambda context: asyncio.sleep(0, result=context)),
        worker,
        issued.claim.worker_session_id,
    )

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(consumer.consume(control))

    assert control.actions == []


def execute_and_capture_context(
    *,
    task_attempt_id: Any = None,
    generation: int = 1,
    correlation_id: str | None = None,
    trace_context: TraceContext | None = None,
    deadline_at: datetime | None = None,
    cancellation_requested: bool = False,
) -> tuple[TaskContext, Control]:
    _, control, issued, worker, _ = fixture(
        task_attempt_id=task_attempt_id,
        generation=generation,
        correlation_id=correlation_id,
        trace_context=trace_context,
        deadline_at=deadline_at,
    )
    received: list[TaskContext] = []

    async def handler(context: TaskContext) -> object:
        received.append(context)
        return None

    consumer = WorkerExecutionConsumer(
        ClaimService(issued),
        StartService([], cancellation_requested=cancellation_requested),
        ResultService(),
        registry(handler),
        worker,
        issued.claim.worker_session_id,
    )
    asyncio.run(consumer.consume(control))
    return received[0], control


def test_handler_context_carries_stable_attempt_identity_and_observability() -> None:
    attempt_id = uuid4()
    correlation_id = "customer-secret-correlation"
    trace = TraceContext(
        "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
        "vendor=secret",
    )
    deadline_at = datetime(2030, 1, 1, tzinfo=UTC)

    first, first_control = execute_and_capture_context(
        task_attempt_id=attempt_id,
        generation=1,
        correlation_id=correlation_id,
        trace_context=trace,
        deadline_at=deadline_at,
        cancellation_requested=True,
    )
    redelivered, _ = execute_and_capture_context(
        task_attempt_id=attempt_id,
        generation=99,
        correlation_id=correlation_id,
        trace_context=trace,
        deadline_at=deadline_at,
        cancellation_requested=True,
    )
    later_attempt, _ = execute_and_capture_context(task_attempt_id=uuid4())

    expected_key = f"taskforge:task-attempt:{attempt_id}"
    assert first.idempotency_key == expected_key
    assert redelivered.idempotency_key == expected_key
    assert later_attempt.idempotency_key != expected_key
    assert first.correlation_id == correlation_id
    assert first.trace_context == trace
    assert first.deadline == TaskDeadline(deadline_at)
    assert first.cancellation_requested_at_start is True
    assert first_control.actions == ["ack"]


def test_handler_context_has_absent_deadline_and_no_infrastructure_authority() -> None:
    context, _ = execute_and_capture_context()

    assert context.deadline is None
    forbidden = {
        "claim_generation",
        "generation",
        "worker_session_id",
        "result_authority",
        "broker",
        "delivery",
        "control",
        "db",
        "session",
        "repository",
    }
    assert forbidden.isdisjoint(context.__dataclass_fields__)
    assert "execution_timeout_seconds" not in context.__dataclass_fields__


def test_task_context_repr_redacts_handler_data() -> None:
    context, _ = execute_and_capture_context(
        correlation_id="customer-secret-correlation",
        trace_context=TraceContext(
            "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
            "vendor=secret",
        ),
    )

    rendered = repr(context)
    assert context.idempotency_key not in rendered
    assert "customer-secret-correlation" not in rendered
    assert "0123456789abcdef" not in rendered
    assert "secret" not in rendered
    assert "parameters=<redacted>" in rendered
    assert "references=<redacted>" in rendered


def test_start_invariant_failure_never_invokes_handler_or_acknowledges() -> None:
    _, control, issued, worker, _ = fixture()
    events: list[str] = []

    async def handler(context: TaskContext) -> object:
        del context
        events.append("handler")
        return None

    consumer = WorkerExecutionConsumer(
        ClaimService(issued),
        StartService(events, error=TaskStartInvariantError()),
        ResultService(),
        registry(handler),
        worker,
        issued.claim.worker_session_id,
    )

    with pytest.raises(WorkerConsumptionPaused, match="start failed closed"):
        asyncio.run(consumer.consume(control))
    assert events == ["start"]
    assert control.actions == []


def test_expired_claim_replay_preserves_delivery_without_start_or_handler() -> None:
    _, control, issued, worker, _ = fixture()
    expired = IssuedTaskClaim(TaskClaimOutcome.REPLAYED_EXPIRED, issued.claim, None)
    events: list[str] = []

    async def handler(invocation: TaskContext) -> object:
        del invocation
        events.append("handler")
        return None

    consumer = WorkerExecutionConsumer(
        ClaimService(expired),
        StartService(events),
        ResultService(),
        registry(handler),
        worker,
        issued.claim.worker_session_id,
    )

    with pytest.raises(WorkerConsumptionPaused, match="expired claim"):
        asyncio.run(consumer.consume(control))

    assert events == []
    assert control.actions == []


def test_malformed_delivery_is_rejected_without_claiming() -> None:
    _, _, issued, worker, metadata = fixture()
    malformed = Control(b"not-json", metadata)
    consumer = WorkerExecutionConsumer(
        ClaimService(issued),
        StartService([]),
        ResultService(),
        registry(lambda value: value),
        worker,
        issued.claim.worker_session_id,
    )
    asyncio.run(consumer.consume(malformed))
    assert malformed.actions == ["reject:False"]


def test_local_handler_registration_drift_preserves_valid_delivery() -> None:
    _, control, issued, worker, _ = fixture()
    task_types = TaskTypeRegistry(
        (TaskTypeDefinition("test.task", "test-capability", AcceptParameters()),)
    )
    consumer = WorkerExecutionConsumer(
        ClaimService(issued),
        StartService([]),
        ResultService(),
        TaskHandlerRegistry((), task_types),
        worker,
        issued.claim.worker_session_id,
    )

    with pytest.raises(WorkerConsumptionPaused, match="registration drift"):
        asyncio.run(consumer.consume(control))

    assert control.actions == []


@pytest.mark.parametrize(
    ("reason", "action", "paused"),
    (
        (TaskClaimRejectionReason.INVALID_DISPATCH, "reject:False", False),
        (TaskClaimRejectionReason.STALE_ATTEMPT, "ack", False),
        (TaskClaimRejectionReason.OBSOLETE_TASK, "ack", False),
        (TaskClaimRejectionReason.ALREADY_AUTHORITATIVE, "ack", False),
        (TaskClaimRejectionReason.CAPABILITY_MISMATCH, None, True),
        (TaskClaimRejectionReason.WORKER_UNAVAILABLE, None, True),
    ),
)
def test_claim_disposition_is_semantic(
    reason: TaskClaimRejectionReason, action: str | None, paused: bool
) -> None:
    _, control, issued, worker, _ = fixture()
    consumer = WorkerExecutionConsumer(
        ClaimService(TaskClaimRejected(reason)),
        StartService([]),
        ResultService(),
        registry(lambda value: value),
        worker,
        issued.claim.worker_session_id,
    )
    if paused:
        with pytest.raises(WorkerConsumptionPaused):
            asyncio.run(consumer.consume(control))
    else:
        asyncio.run(consumer.consume(control))
    assert control.actions == ([] if action is None else [action])


def context_fixture() -> TaskContext:
    received, _ = execute_and_capture_context()
    return received


@pytest.mark.parametrize(
    ("returned", "kind", "failure_kind"),
    (
        (object(), TaskExecutionResultKind.SUCCESS, None),
        (
            TaskRetryableFailure(),
            TaskExecutionResultKind.RETRYABLE_FAILURE,
            TaskExecutionFailureKind.HANDLER_REPORTED,
        ),
        (
            TaskPermanentFailure(),
            TaskExecutionResultKind.PERMANENT_FAILURE,
            TaskExecutionFailureKind.HANDLER_REPORTED,
        ),
        (TaskCancellation(), TaskExecutionResultKind.CANCELLATION, None),
    ),
)
def test_handler_returns_are_normalized(
    returned: object,
    kind: TaskExecutionResultKind,
    failure_kind: TaskExecutionFailureKind | None,
) -> None:
    async def handler(context: TaskContext) -> object:
        del context
        return returned

    result = asyncio.run(_execute_handler(handler, context_fixture(), None))

    assert result.kind is kind
    assert result.failure_kind is failure_kind
    assert result.value is (
        returned if kind is TaskExecutionResultKind.SUCCESS else None
    )


def test_ordinary_handler_exception_is_retryable_without_sensitive_text() -> None:
    async def handler(context: TaskContext) -> object:
        del context
        raise RuntimeError("secret exception text")

    result = asyncio.run(_execute_handler(handler, context_fixture(), None))

    assert result.kind is TaskExecutionResultKind.RETRYABLE_FAILURE
    assert result.failure_kind is TaskExecutionFailureKind.HANDLER_EXCEPTION
    assert "secret exception text" not in repr(result)


def test_execution_timeout_requests_cancellation_and_normalizes_distinctly() -> None:
    cancelled = asyncio.Event()

    async def handler(context: TaskContext) -> object:
        del context
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise
        return None

    result = asyncio.run(_execute_handler(handler, context_fixture(), 0.001))  # type: ignore[arg-type]

    assert cancelled.is_set()
    assert result.kind is TaskExecutionResultKind.RETRYABLE_FAILURE
    assert result.failure_kind is TaskExecutionFailureKind.EXECUTION_TIMEOUT


def test_bare_cancelled_error_propagates_outside_ordinary_normalization() -> None:
    async def handler(context: TaskContext) -> object:
        del context
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(_execute_handler(handler, context_fixture(), None))


def test_handler_raised_timeout_error_is_not_execution_timeout() -> None:
    async def handler(context: TaskContext) -> object:
        del context
        raise TimeoutError

    result = asyncio.run(_execute_handler(handler, context_fixture(), 10))

    assert result.failure_kind is TaskExecutionFailureKind.HANDLER_EXCEPTION


def test_normalized_success_repr_redacts_output_and_context() -> None:
    secret = "handler-output-secret"

    async def handler(context: TaskContext) -> object:
        del context
        return {"payload": secret}

    result = asyncio.run(_execute_handler(handler, context_fixture(), None))
    rendered = repr(result)

    assert secret not in rendered
    assert "payload" not in rendered
    assert "value=<redacted>" in rendered


@pytest.mark.parametrize("kind", ("success", object(), None))
def test_execution_result_rejects_non_enum_kind(kind: object) -> None:
    with pytest.raises(ValueError, match="supported result kind"):
        TaskExecutionResult(kind)  # type: ignore[arg-type]


def test_execution_result_rejects_non_enum_failure_kind() -> None:
    with pytest.raises(ValueError, match="supported failure kind"):
        TaskExecutionResult(
            TaskExecutionResultKind.RETRYABLE_FAILURE,
            failure_kind="handler_exception",  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    ("kind", "value", "failure_kind"),
    (
        (
            TaskExecutionResultKind.SUCCESS,
            None,
            TaskExecutionFailureKind.HANDLER_REPORTED,
        ),
        (TaskExecutionResultKind.RETRYABLE_FAILURE, object(), None),
        (TaskExecutionResultKind.RETRYABLE_FAILURE, None, None),
        (
            TaskExecutionResultKind.PERMANENT_FAILURE,
            None,
            TaskExecutionFailureKind.HANDLER_EXCEPTION,
        ),
        (TaskExecutionResultKind.PERMANENT_FAILURE, object(), None),
        (TaskExecutionResultKind.CANCELLATION, object(), None),
        (
            TaskExecutionResultKind.CANCELLATION,
            None,
            TaskExecutionFailureKind.HANDLER_REPORTED,
        ),
    ),
)
def test_execution_result_rejects_invalid_closed_model_combinations(
    kind: TaskExecutionResultKind,
    value: object | None,
    failure_kind: TaskExecutionFailureKind | None,
) -> None:
    with pytest.raises(ValueError):
        TaskExecutionResult(kind, value, failure_kind)


def test_execution_result_accepts_success_with_none_value() -> None:
    result = TaskExecutionResult.success(None)

    assert result.kind is TaskExecutionResultKind.SUCCESS
    assert result.value is None
    assert result.failure_kind is None


def test_consumer_uses_durable_timeout_only_for_handler_then_acks() -> None:
    _, control, issued, worker, _ = fixture(execution_timeout_seconds=1)
    events: list[str] = []

    async def handler(context: TaskContext) -> object:
        events.append("handler")
        assert "execution_timeout_seconds" not in context.__dataclass_fields__
        return TaskRetryableFailure()

    consumer = WorkerExecutionConsumer(
        ClaimService(issued),
        StartService(events),
        ResultService(events),
        registry(handler),
        worker,
        issued.claim.worker_session_id,
    )
    asyncio.run(consumer.consume(control))

    assert events == ["start", "handler", "result"]
    assert control.actions == ["ack"]


def test_timeout_boundary_begins_after_durable_start_and_wraps_only_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, control, issued, worker, _ = fixture(execution_timeout_seconds=30)
    events: list[str] = []

    class ObservedTimeout:
        async def __aenter__(self) -> None:
            events.append("timeout-enter")

        async def __aexit__(self, *args: object) -> bool:
            events.append("timeout-exit")
            return False

        def expired(self) -> bool:
            return False

    def observed_timeout(seconds: float | None) -> ObservedTimeout:
        assert seconds == 30
        return ObservedTimeout()

    monkeypatch.setattr("taskforge.worker.execution.asyncio.timeout", observed_timeout)

    async def handler(context: TaskContext) -> object:
        del context
        events.append("handler")
        return None

    consumer = WorkerExecutionConsumer(
        ClaimService(issued),
        StartService(events),
        ResultService(events),
        registry(handler),
        worker,
        issued.claim.worker_session_id,
    )
    asyncio.run(consumer.consume(control))

    assert events == [
        "start",
        "timeout-enter",
        "handler",
        "timeout-exit",
        "result",
    ]
    assert control.actions == ["ack"]
