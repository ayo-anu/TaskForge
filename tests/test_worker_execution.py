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
from taskforge.worker.consumer_ports import BrokerDispatchDelivery
from taskforge.worker.execution import WorkerConsumptionPaused, WorkerExecutionConsumer
from taskforge.worker.handlers import (
    TaskContext,
    TaskDeadline,
    TaskHandlerDefinition,
    TaskHandlerRegistry,
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
    def __init__(self, body: bytes, metadata: DispatchTransportMetadata) -> None:
        self._delivery = BrokerDispatchDelivery(body, metadata, False)
        self.actions: list[str] = []

    @property
    def delivery(self) -> BrokerDispatchDelivery:
        return self._delivery

    async def acknowledge(self) -> None:
        self.actions.append("ack")

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


def fixture(
    *,
    task_attempt_id: Any = None,
    generation: int = 1,
    correlation_id: str | None = None,
    trace_context: TraceContext | None = None,
    deadline_at: datetime | None = None,
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


def test_valid_delivery_starts_then_invokes_without_acknowledging() -> None:
    envelope, control, issued, worker, _ = fixture()
    events: list[str] = []

    async def handler(invocation: TaskContext) -> object:
        events.append("handler")
        assert invocation.task_attempt_id == envelope.task_attempt_id
        assert control.actions == []
        return None

    consumer = WorkerExecutionConsumer(
        ClaimService(issued),
        StartService(events),
        registry(handler),
        worker,
        issued.claim.worker_session_id,
    )
    asyncio.run(consumer.consume(control))

    assert events == ["start", "handler"]
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
    assert first_control.actions == []


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
        registry(lambda value: value),
        worker,
        issued.claim.worker_session_id,
    )
    asyncio.run(consumer.consume(malformed))
    assert malformed.actions == ["reject:False"]


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
