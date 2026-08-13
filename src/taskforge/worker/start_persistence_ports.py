"""Persistence boundary for claim-bound task start acknowledgement."""

from __future__ import annotations

from typing import Protocol
from uuid import UUID

from taskforge.identity.authentication import AuthenticatedWorker


class TaskStartAuthorityRejected(Exception): ...


class TaskStartSessionRejected(Exception): ...


class TaskStartClaimStale(Exception): ...


class TaskStartInvariantViolation(Exception): ...


class TaskStartPersistenceUnavailable(Exception): ...


class TaskStartRepository(Protocol):
    async def start_task(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        task_run_id: UUID,
        task_attempt_id: UUID,
        claim_generation: int,
    ) -> bool:
        """Return true for a new transition and false for an exact replay."""
