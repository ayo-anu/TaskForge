"""Persistence boundary for atomic worker-session registration."""

from __future__ import annotations

from typing import Protocol
from uuid import UUID

from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.worker.domain import (
    InspectedWorkerHeartbeatPage,
    InspectedWorkerSessionPage,
    InspectedWorkerSessionResource,
    RegisteredWorkerSession,
    ReplacedWorkerCapabilities,
    WorkerCapabilityReplacement,
    WorkerHealthProjection,
    WorkerHealthThresholds,
    WorkerHeartbeat,
    WorkerRegistration,
    WorkerSessionHealthStatus,
    WorkerSessionPageCursor,
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


class WorkerInspectionNotFound(Exception):
    """The inspected worker session does not exist."""


class WorkerInspectionInvariantViolation(Exception):
    """Required durable session and health facts disagree."""


class WorkerInspectionPersistenceUnavailable(Exception):
    """Worker inspection persistence was operationally unavailable."""


class WorkerInspectionRepository(Protocol):
    async def get_session(
        self, worker_session_id: UUID, thresholds: WorkerHealthThresholds
    ) -> InspectedWorkerSessionResource: ...

    async def list_sessions(
        self,
        *,
        worker_identity_id: UUID | None,
        health_status: WorkerSessionHealthStatus | None,
        thresholds: WorkerHealthThresholds,
        limit: int,
        cursor: WorkerSessionPageCursor | None,
    ) -> InspectedWorkerSessionPage: ...

    async def list_heartbeats(
        self,
        worker_session_id: UUID,
        *,
        before_sequence: int | None,
        limit: int,
    ) -> InspectedWorkerHeartbeatPage: ...


class WorkerCapabilityAuthorityRejected(Exception):
    """Worker authority is invalid at the capability write boundary."""


class WorkerCapabilitySessionUnavailable(Exception):
    """The session is absent from the authenticated worker's scope."""


class WorkerCapabilitySessionInactive(Exception):
    """The authenticated worker session has ended."""


class WorkerCapabilityInvariantViolation(Exception):
    """Capability replacement violated an internal persistence invariant."""


class WorkerCapabilityPersistenceUnavailable(Exception):
    """Capability replacement persistence was operationally unavailable."""


class WorkerCapabilityRepository(Protocol):
    async def replace_capabilities(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        replacement: WorkerCapabilityReplacement,
    ) -> ReplacedWorkerCapabilities: ...
