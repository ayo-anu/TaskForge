"""Persistence boundary for atomic worker-session registration."""

from __future__ import annotations

from typing import Protocol
from uuid import UUID

from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.worker.domain import RegisteredWorkerSession, WorkerRegistration


class WorkerRegistrationAuthorityRejected(Exception):
    """Authenticated authority is no longer valid at the write boundary."""


class WorkerRegistrationRecordConflict(Exception):
    """A database constraint rejected the new registration aggregate."""


class WorkerRegistrationPersistenceUnavailable(Exception):
    """Registration persistence was operationally unavailable."""


class WorkerRegistrationRepository(Protocol):
    async def register_session(
        self,
        authenticated_worker: AuthenticatedWorker,
        session_id: UUID,
        registration: WorkerRegistration,
    ) -> RegisteredWorkerSession:
        """Create one complete worker-session aggregate in one transaction."""
