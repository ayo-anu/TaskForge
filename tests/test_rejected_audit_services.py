"""Focused service-boundary tests for required rejected-command auditing."""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest

from taskforge.audit.domain import AuditRecord, AuditRejected
from taskforge.claims.domain import TaskClaimRenewalRequest
from taskforge.claims.persistence_ports import (
    TaskClaimNotEligible,
    TaskClaimRenewalStale,
)
from taskforge.claims.service import TaskClaimService, TaskClaimServiceUnavailable
from taskforge.dead_letters.persistence_ports import DeadLetterTransitionConflict
from taskforge.dead_letters.service import DeadLetterService
from taskforge.dispatch.envelope import DispatchEnvelope
from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.runs.service import (
    WorkflowRunNotFound,
    WorkflowRunService,
    WorkflowRunServiceUnavailable,
)
from taskforge.worker.result_submission import (
    TaskResultAuthorityRejected,
    TaskResultServiceUnavailable,
    TaskResultSubmissionRequest,
    TaskResultSubmissionService,
)
from taskforge.worker.results import TaskExecutionResult
from taskforge.worker.service import WorkerRejectedAuditUnavailable, _worker_rejected
from taskforge.worker.start import (
    TaskStartRejected,
    TaskStartRequest,
    TaskStartService,
    TaskStartServiceUnavailable,
)
from taskforge.worker.start_persistence_ports import TaskStartClaimStale
from taskforge.workflows.service import (
    WorkflowNotFound,
    WorkflowService,
    WorkflowServiceUnavailable,
)


class Recorder:
    def __init__(self, *, fail: bool = False) -> None:
        self.records: list[AuditRecord] = []
        self.fail = fail

    async def record(self, record: AuditRecord) -> None:
        self.records.append(record)
        if self.fail:
            raise AuditRejected


class BoundaryRecorder(Recorder):
    def __init__(self, transaction: object) -> None:
        super().__init__()
        self.transaction = transaction

    async def record(self, record: AuditRecord) -> None:
        assert self.transaction.exited is True
        await super().record(record)


class ClaimRepository:
    def __init__(self, error: Exception) -> None:
        self.error = error

    async def acquire_claim(self, *args: object, **kwargs: object) -> Any:
        raise self.error

    async def renew_claim(self, *args: object, **kwargs: object) -> Any:
        raise self.error


class Issuer:
    def issue(self, **kwargs: object) -> Any:
        raise AssertionError("not reached")

    def verify(self, *args: object, **kwargs: object) -> bool:
        return False


def worker() -> AuthenticatedWorker:
    return AuthenticatedWorker(uuid4(), uuid4())


def test_claim_acquisition_rejection_is_recorded_once() -> None:
    authenticated = worker()
    session_id = uuid4()
    attempt_id = uuid4()
    dispatch = object.__new__(DispatchEnvelope)
    object.__setattr__(dispatch, "task_attempt_id", attempt_id)
    object.__setattr__(dispatch, "attempt_number", 2)
    object.__setattr__(dispatch, "correlation_id", "claim-correlation")
    recorder = Recorder()
    service = TaskClaimService(
        ClaimRepository(TaskClaimNotEligible()),
        Issuer(),  # type: ignore[arg-type]
        lease_seconds=30,
        rejected_audit=recorder,
    )

    with pytest.raises(Exception, match="task claim acquisition rejected"):
        asyncio.run(service.claim_task(authenticated, session_id, dispatch))

    assert len(recorder.records) == 1
    record = recorder.records[0]
    assert (record.action, record.reason_code) == (
        "task_claim.acquire",
        "obsolete_task",
    )
    assert record.actor.worker_identity_id == authenticated.worker_identity_id
    assert record.actor.worker_session_id == session_id


def test_claim_renewal_rejection_is_recorded_after_repository_returns() -> None:
    authenticated = worker()
    recorder = Recorder()
    service = TaskClaimService(
        ClaimRepository(TaskClaimRenewalStale()),
        Issuer(),  # type: ignore[arg-type]
        lease_seconds=30,
        rejected_audit=recorder,
    )
    request = TaskClaimRenewalRequest(
        uuid4(),
        3,
        uuid4(),
        datetime.now(UTC) + timedelta(seconds=30),
        "renew-correlation",
    )

    with pytest.raises(TaskClaimRenewalStale):
        asyncio.run(service.renew_claim(authenticated, request))

    assert len(recorder.records) == 1
    assert recorder.records[0].reason_code == "stale_claim"


def test_claim_rejected_audit_failure_is_fail_closed() -> None:
    service = TaskClaimService(
        ClaimRepository(TaskClaimNotEligible()),
        Issuer(),  # type: ignore[arg-type]
        lease_seconds=30,
        rejected_audit=Recorder(fail=True),
    )
    dispatch = object.__new__(DispatchEnvelope)
    object.__setattr__(dispatch, "task_attempt_id", uuid4())
    object.__setattr__(dispatch, "attempt_number", 1)
    object.__setattr__(dispatch, "correlation_id", None)
    with pytest.raises(TaskClaimServiceUnavailable):
        asyncio.run(service.claim_task(worker(), uuid4(), dispatch))


class StartRepository:
    async def start_task(self, *args: object, **kwargs: object) -> Any:
        raise TaskStartClaimStale


def test_task_start_rejection_and_audit_failure_contract() -> None:
    request = TaskStartRequest(uuid4(), uuid4(), 1, "start-correlation")
    recorder = Recorder()
    with pytest.raises(TaskStartRejected):
        asyncio.run(
            TaskStartService(StartRepository(), recorder).start_task(
                worker(), uuid4(), request
            )
        )
    assert len(recorder.records) == 1
    assert recorder.records[0].reason_code == "stale_claim"

    with pytest.raises(TaskStartServiceUnavailable):
        asyncio.run(
            TaskStartService(StartRepository(), Recorder(fail=True)).start_task(
                worker(), uuid4(), request
            )
        )


def test_result_authority_rejection_and_audit_failure_contract() -> None:
    from taskforge.claims.domain import TaskClaimResultAuthority

    request = TaskResultSubmissionRequest(
        uuid4(),
        uuid4(),
        uuid4(),
        1,
        TaskClaimResultAuthority("tf_claim_result_v1." + "A" * 43),
        TaskExecutionResult.cancellation(),
        "result-correlation",
    )
    recorder = Recorder()
    service = TaskResultSubmissionService(Any, Issuer(), recorder)  # type: ignore[arg-type]
    with pytest.raises(TaskResultAuthorityRejected):
        asyncio.run(service.submit_result(worker(), uuid4(), request))
    assert len(recorder.records) == 1
    assert recorder.records[0].reason_code == "worker_authority_rejected"

    service = TaskResultSubmissionService(Any, Issuer(), Recorder(fail=True))  # type: ignore[arg-type]
    with pytest.raises(TaskResultServiceUnavailable):
        asyncio.run(service.submit_result(worker(), uuid4(), request))


class DeadLetterRepository:
    async def transition(self, *args: object, **kwargs: object) -> Any:
        raise DeadLetterTransitionConflict


def test_dead_letter_transition_rejection_is_recorded_once() -> None:
    recorder = Recorder()
    service = DeadLetterService(DeadLetterRepository(), recorder)  # type: ignore[arg-type]
    with pytest.raises(DeadLetterTransitionConflict):
        asyncio.run(
            service.acknowledge(
                uuid4(),
                Any,  # type: ignore[arg-type]
                operator_principal_id=uuid4(),
                reason=None,
                correlation_id=uuid4(),
            )
        )
    assert len(recorder.records) == 1
    assert recorder.records[0].reason_code == "transition_conflict"


def test_dead_letter_rejected_audit_failure_is_fail_closed() -> None:
    service = DeadLetterService(DeadLetterRepository(), Recorder(fail=True))  # type: ignore[arg-type]
    from taskforge.dead_letters.persistence_ports import (
        DeadLetterPersistenceUnavailable,
    )

    with pytest.raises(DeadLetterPersistenceUnavailable):
        asyncio.run(
            service.resolve(
                uuid4(),
                Any,  # type: ignore[arg-type]
                operator_principal_id=uuid4(),
                reason="safe reason",
                correlation_id=uuid4(),
            )
        )


def test_workflow_and_run_rejected_audit_fail_closed() -> None:
    principal_id = uuid4()
    correlation_id = uuid4()
    workflow_id = uuid4()
    captured = Recorder()
    workflow_capture = WorkflowService(Any, Any, captured)  # type: ignore[arg-type]
    for action in (
        "workflow.create",
        "workflow.publish",
        "workflow.availability_change",
    ):
        asyncio.run(
            workflow_capture._audit_rejection(
                WorkflowNotFound(),
                action=action,
                workflow_id=workflow_id,
                principal_id=principal_id,
                correlation_id=correlation_id,
            )
        )
    assert [record.action for record in captured.records] == [
        "workflow.create",
        "workflow.publish",
        "workflow.availability_change",
    ]

    workflow_service = WorkflowService(Any, Any, Recorder(fail=True))  # type: ignore[arg-type]
    with pytest.raises(WorkflowServiceUnavailable):
        asyncio.run(
            workflow_service._audit_rejection(
                WorkflowNotFound(),
                action="workflow.publish",
                workflow_id=workflow_id,
                principal_id=principal_id,
                correlation_id=correlation_id,
            )
        )

    run_service = WorkflowRunService(Any, Recorder(fail=True))  # type: ignore[arg-type]
    with pytest.raises(WorkflowRunServiceUnavailable):
        asyncio.run(
            run_service._audit_command_rejection(
                WorkflowNotFound(),
                action="workflow_run.cancel",
                resource_id=uuid4(),
                principal_id=principal_id,
                correlation_id=correlation_id,
                reasons={WorkflowNotFound: "workflow_run_not_visible"},
            )
        )

    captured = Recorder()
    run_capture = WorkflowRunService(Any, captured)  # type: ignore[arg-type]
    for action, resource_type in (
        ("workflow_run.cancel", "workflow_run"),
        ("workflow_run.create", "workflow"),
    ):
        asyncio.run(
            run_capture._audit_command_rejection(
                WorkflowNotFound(),
                action=action,
                resource_id=workflow_id,
                principal_id=principal_id,
                correlation_id=correlation_id,
                reasons={WorkflowNotFound: "workflow_run_not_visible"},
                resource_type=resource_type,
            )
        )
    assert [record.action for record in captured.records] == [
        "workflow_run.cancel",
        "workflow_run.create",
    ]


def test_worker_rejected_audit_failure_is_fail_closed() -> None:
    captured = Recorder()
    for action, reason in (
        ("worker_session.register", "registration_conflict"),
        ("worker_session.capabilities_replace", "worker_session_inactive"),
        ("worker_session.heartbeat", "stale_heartbeat"),
    ):
        asyncio.run(
            _worker_rejected(
                captured,
                worker(),
                None,
                action,
                reason,
                uuid4(),
                {},
            )
        )
    assert len(captured.records) == 3

    with pytest.raises(WorkerRejectedAuditUnavailable):
        asyncio.run(
            _worker_rejected(
                Recorder(fail=True),
                worker(),
                None,
                "worker_session.register",
                "registration_conflict",
                uuid4(),
                {"capabilities": {"count": 0, "sha256": "0" * 64}},
            )
        )


class ReplayTransaction:
    def __init__(self) -> None:
        self.exited = False
        self.creation_calls = 0

    async def __aenter__(self) -> "ReplayTransaction":
        return self

    async def __aexit__(self, *args: object) -> None:
        self.exited = True

    async def prepare_full_replay(self, *args: object) -> None:
        return None


class ReplayRepository:
    def __init__(self, transaction: ReplayTransaction) -> None:
        self.transaction_value = transaction

    def creation_transaction(self) -> ReplayTransaction:
        return self.transaction_value


def test_rejected_replay_is_audited_only_after_transaction_exit() -> None:
    transaction = ReplayTransaction()
    recorder = BoundaryRecorder(transaction)
    service = WorkflowRunService(ReplayRepository(transaction), recorder)  # type: ignore[arg-type]

    with pytest.raises(WorkflowRunNotFound):
        asyncio.run(
            service.create_full_replay(
                uuid4(),
                Any,  # type: ignore[arg-type]
                requested_by_principal_id=uuid4(),
                correlation_id=uuid4(),
            )
        )

    assert len(recorder.records) == 1
    assert recorder.records[0].reason_code == "source_not_visible"
    assert recorder.records[0].action == "workflow_run.replay"
    assert transaction.creation_calls == 0
