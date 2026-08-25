"""Application service for durable task start acknowledgement."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID, uuid4

from taskforge.audit.domain import (
    AuditActor,
    AuditActorKind,
    AuditOutcome,
    AuditRecord,
    AuditRejected,
)
from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.persistence.audit import RejectedAuditRecorder
from taskforge.runs.domain import WorkflowRunStatus
from taskforge.worker.start_persistence_ports import (
    PersistedTaskStart,
    TaskStartAuthorityRejected,
    TaskStartClaimStale,
    TaskStartInvariantViolation,
    TaskStartPersistenceUnavailable,
    TaskStartRepository,
    TaskStartSessionRejected,
)


class TaskStartOutcome(StrEnum):
    STARTED = "started"
    REPLAYED_RUNNING = "replayed_running"


class TaskStartRejected(Exception):
    """The worker no longer owns current start authority."""


class TaskStartInvariantError(Exception):
    """Durable task-start state is internally inconsistent."""


class TaskStartServiceUnavailable(Exception):
    """Task-start persistence is operationally unavailable."""


@dataclass(frozen=True)
class TaskStartRequest:
    task_run_id: UUID
    task_attempt_id: UUID
    claim_generation: int
    correlation_id: str | None = None

    def __post_init__(self) -> None:
        if self.claim_generation <= 0:
            raise ValueError("claim generation must be positive")
        if self.correlation_id is not None and not (
            1 <= len(self.correlation_id) <= 128
            and all(32 <= ord(char) <= 126 for char in self.correlation_id)
        ):
            raise ValueError("task start correlation ID is invalid")


@dataclass(frozen=True)
class TaskStartReceipt:
    outcome: TaskStartOutcome
    cancellation_requested_at_start: bool


_START_AUDIT_REASONS: dict[type[Exception], str] = {
    TaskStartAuthorityRejected: "worker_authority_rejected",
    TaskStartSessionRejected: "worker_session_rejected",
    TaskStartClaimStale: "stale_claim",
}
_EXPECTED_START_REJECTIONS = tuple(_START_AUDIT_REASONS)


class TaskStartService:
    def __init__(
        self,
        repository: TaskStartRepository,
        rejected_audit: RejectedAuditRecorder | None = None,
    ) -> None:
        self._repository = repository
        self._rejected_audit = rejected_audit

    async def start_task(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        request: TaskStartRequest,
    ) -> TaskStartReceipt:
        try:
            persisted = await self._repository.start_task(
                authenticated_worker,
                worker_session_id,
                request.task_run_id,
                request.task_attempt_id,
                request.claim_generation,
            )
        except _EXPECTED_START_REJECTIONS as error:
            if self._rejected_audit is not None:
                try:
                    await self._rejected_audit.record(
                        AuditRecord(
                            uuid4(),
                            AuditActor(
                                AuditActorKind.WORKER,
                                worker_identity_id=authenticated_worker.worker_identity_id,
                                worker_session_id=worker_session_id,
                            ),
                            "task_attempt.start",
                            AuditOutcome.REJECTED,
                            "task_attempt",
                            request.task_attempt_id,
                            request.correlation_id,
                            {"claim_generation": request.claim_generation},
                            _START_AUDIT_REASONS[type(error)],
                        )
                    )
                except AuditRejected as audit_error:
                    raise TaskStartServiceUnavailable from audit_error
            raise TaskStartRejected from error
        except TaskStartInvariantViolation as error:
            raise TaskStartInvariantError from error
        except TaskStartPersistenceUnavailable as error:
            raise TaskStartServiceUnavailable from error
        assert isinstance(persisted, PersistedTaskStart)
        return TaskStartReceipt(
            TaskStartOutcome.STARTED
            if persisted.started
            else TaskStartOutcome.REPLAYED_RUNNING,
            persisted.workflow_run_status is WorkflowRunStatus.CANCELLING,
        )
