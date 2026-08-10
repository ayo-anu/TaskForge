"""Persistence boundary for atomic worker-session registration."""

from __future__ import annotations

from typing import Protocol
from uuid import UUID

from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.worker.domain import (
    RegisteredWorkerSession,
    WorkerHealthProjection,
    WorkerHeartbeat,
    WorkerRegistration,
)


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


class WorkerHeartbeatAuthorityRejected(Exception):
    """Worker authority is no longer valid at the heartbeat write boundary."""


class WorkerHeartbeatSessionUnavailable(Exception):
    """The session is absent from the authenticated worker's scope."""


class WorkerHeartbeatSessionInactive(Exception):
    """The authenticated session has already ended."""


class WorkerHeartbeatStale(Exception):
    """The heartbeat sequence is lower than the current projection."""


class WorkerHeartbeatSequenceGap(Exception):
    """The heartbeat sequence skips the required next value."""


class WorkerHeartbeatReplayConflict(Exception):
    """The current sequence was replayed with different availability."""


class WorkerHeartbeatInvariantViolation(Exception):
    """Durable heartbeat history and current projection disagree."""


class WorkerHeartbeatPersistenceUnavailable(Exception):
    """Heartbeat persistence was operationally unavailable."""


class WorkerHeartbeatRepository(Protocol):
    async def apply_heartbeat(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        heartbeat: WorkerHeartbeat,
    ) -> WorkerHealthProjection:
        """Apply or replay one heartbeat in a repository-owned transaction."""
