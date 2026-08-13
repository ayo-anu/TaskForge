"""Persistence boundary for claim-bound task start acknowledgement."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.runs.domain import WorkflowRunStatus


class TaskStartAuthorityRejected(Exception): ...


class TaskStartSessionRejected(Exception): ...


class TaskStartClaimStale(Exception): ...


class TaskStartInvariantViolation(Exception): ...


class TaskStartPersistenceUnavailable(Exception): ...


@dataclass(frozen=True)
class PersistedTaskStart:
    started: bool
    workflow_run_status: WorkflowRunStatus


class TaskStartRepository(Protocol):
    async def start_task(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        task_run_id: UUID,
        task_attempt_id: UUID,
        claim_generation: int,
    ) -> PersistedTaskStart:
        """Return atomic start and workflow cancellation facts."""
