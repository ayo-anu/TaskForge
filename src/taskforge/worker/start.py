"""Application service for durable task start acknowledgement."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

from taskforge.identity.authentication import AuthenticatedWorker
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

    def __post_init__(self) -> None:
        if self.claim_generation <= 0:
            raise ValueError("claim generation must be positive")


@dataclass(frozen=True)
class TaskStartReceipt:
    outcome: TaskStartOutcome
    cancellation_requested_at_start: bool


class TaskStartService:
    def __init__(self, repository: TaskStartRepository) -> None:
        self._repository = repository

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
        except (
            TaskStartAuthorityRejected,
            TaskStartSessionRejected,
            TaskStartClaimStale,
        ) as error:
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
