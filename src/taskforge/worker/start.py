"""Application service for durable task start acknowledgement."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.worker.start_persistence_ports import (
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


class TaskStartService:
    def __init__(self, repository: TaskStartRepository) -> None:
        self._repository = repository

    async def start_task(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        request: TaskStartRequest,
    ) -> TaskStartOutcome:
        try:
            started = await self._repository.start_task(
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
        return (
            TaskStartOutcome.STARTED if started else TaskStartOutcome.REPLAYED_RUNNING
        )
