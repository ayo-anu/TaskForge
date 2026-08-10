"""Application service for authenticated worker-session registration."""

from __future__ import annotations

from collections.abc import Callable
from uuid import UUID, uuid4

from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.worker.domain import (
    InspectedWorkerHeartbeatPage,
    InspectedWorkerSessionPage,
    InspectedWorkerSessionResource,
    RegisteredWorkerSession,
    WorkerHealthProjection,
    WorkerHealthThresholds,
    WorkerHeartbeat,
    WorkerSessionHealthStatus,
    WorkerSessionPageCursor,
    validate_worker_registration,
)
from taskforge.worker.persistence_ports import (
    WorkerHeartbeatAuthorityRejected,
    WorkerHeartbeatPersistenceUnavailable,
    WorkerHeartbeatReplayConflict,
    WorkerHeartbeatRepository,
    WorkerHeartbeatSequenceGap,
    WorkerHeartbeatSessionInactive,
    WorkerHeartbeatSessionUnavailable,
    WorkerHeartbeatStale,
    WorkerInspectionInvariantViolation,
    WorkerInspectionNotFound,
    WorkerInspectionPersistenceUnavailable,
    WorkerInspectionRepository,
    WorkerRegistrationAuthorityRejected,
    WorkerRegistrationPersistenceUnavailable,
    WorkerRegistrationRecordConflict,
    WorkerRegistrationRepository,
)
from taskforge.workflows.task_types import TaskTypeRegistry


class WorkerRegistrationRejected(Exception):
    """Worker authority was invalid at the authoritative write boundary."""


class WorkerRegistrationConflict(Exception):
    """A fresh worker session could not be persisted uniquely."""


class WorkerRegistrationServiceUnavailable(Exception):
    """Worker registration persistence was unavailable."""


class WorkerHeartbeatRejected(Exception):
    """Worker authority was invalid at the heartbeat write boundary."""


class WorkerSessionUnavailable(Exception):
    """The session is absent from the authenticated worker's scope."""


class WorkerSessionInactive(Exception):
    """The authenticated worker session has ended."""


class StaleWorkerHeartbeat(Exception):
    """The heartbeat sequence is older than the current projection."""


class WorkerHeartbeatGap(Exception):
    """The heartbeat skips the exact required next sequence."""


class ConflictingWorkerHeartbeatReplay(Exception):
    """The current heartbeat sequence was replayed with a different value."""


class WorkerHeartbeatServiceUnavailable(Exception):
    """Worker heartbeat persistence was unavailable."""


class WorkerInspectionNotFoundError(Exception):
    """The requested worker session does not exist."""


class WorkerInspectionInvariantError(Exception):
    """Required durable worker inspection facts are inconsistent."""


class WorkerInspectionServiceUnavailable(Exception):
    """Worker inspection persistence was unavailable."""


class WorkerRegistrationService:
    def __init__(
        self,
        repository: WorkerRegistrationRepository,
        task_types: TaskTypeRegistry,
        *,
        identifier_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self._repository = repository
        self._task_types = task_types
        self._identifier_factory = identifier_factory

    async def register(
        self,
        authenticated_worker: AuthenticatedWorker,
        capabilities: tuple[str, ...],
    ) -> RegisteredWorkerSession:
        """Validate an advertisement and create one fresh process session."""
        registration = validate_worker_registration(
            capabilities,
            known_capabilities=self._task_types.required_capabilities,
        )
        session_id = self._identifier_factory()
        try:
            return await self._repository.register_session(
                authenticated_worker,
                session_id,
                registration,
            )
        except WorkerRegistrationAuthorityRejected as error:
            raise WorkerRegistrationRejected from error
        except WorkerRegistrationRecordConflict as error:
            raise WorkerRegistrationConflict from error
        except WorkerRegistrationPersistenceUnavailable as error:
            raise WorkerRegistrationServiceUnavailable from error


class WorkerHeartbeatService:
    def __init__(self, repository: WorkerHeartbeatRepository) -> None:
        self._repository = repository

    async def heartbeat(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        *,
        sequence: int,
        accepting_work: bool,
    ) -> WorkerHealthProjection:
        """Apply one strictly ordered liveness and availability command."""
        heartbeat = WorkerHeartbeat(sequence, accepting_work)
        try:
            return await self._repository.apply_heartbeat(
                authenticated_worker,
                worker_session_id,
                heartbeat,
            )
        except WorkerHeartbeatAuthorityRejected as error:
            raise WorkerHeartbeatRejected from error
        except WorkerHeartbeatSessionUnavailable as error:
            raise WorkerSessionUnavailable from error
        except WorkerHeartbeatSessionInactive as error:
            raise WorkerSessionInactive from error
        except WorkerHeartbeatStale as error:
            raise StaleWorkerHeartbeat from error
        except WorkerHeartbeatSequenceGap as error:
            raise WorkerHeartbeatGap from error
        except WorkerHeartbeatReplayConflict as error:
            raise ConflictingWorkerHeartbeatReplay from error
        except WorkerHeartbeatPersistenceUnavailable as error:
            raise WorkerHeartbeatServiceUnavailable from error


class WorkerInspectionService:
    def __init__(
        self,
        repository: WorkerInspectionRepository,
        thresholds: WorkerHealthThresholds,
    ) -> None:
        self._repository = repository
        self._thresholds = thresholds

    @property
    def thresholds(self) -> WorkerHealthThresholds:
        return self._thresholds

    async def get_session(
        self, worker_session_id: UUID
    ) -> InspectedWorkerSessionResource:
        try:
            return await self._repository.get_session(
                worker_session_id, self._thresholds
            )
        except WorkerInspectionNotFound as error:
            raise WorkerInspectionNotFoundError from error
        except WorkerInspectionInvariantViolation as error:
            raise WorkerInspectionInvariantError from error
        except WorkerInspectionPersistenceUnavailable as error:
            raise WorkerInspectionServiceUnavailable from error

    async def list_sessions(
        self,
        *,
        worker_identity_id: UUID | None,
        health_status: WorkerSessionHealthStatus | None,
        limit: int,
        cursor: WorkerSessionPageCursor | None,
    ) -> InspectedWorkerSessionPage:
        try:
            return await self._repository.list_sessions(
                worker_identity_id=worker_identity_id,
                health_status=health_status,
                thresholds=self._thresholds,
                limit=limit,
                cursor=cursor,
            )
        except WorkerInspectionInvariantViolation as error:
            raise WorkerInspectionInvariantError from error
        except WorkerInspectionPersistenceUnavailable as error:
            raise WorkerInspectionServiceUnavailable from error

    async def list_heartbeats(
        self,
        worker_session_id: UUID,
        *,
        before_sequence: int | None,
        limit: int,
    ) -> InspectedWorkerHeartbeatPage:
        try:
            return await self._repository.list_heartbeats(
                worker_session_id,
                before_sequence=before_sequence,
                limit=limit,
            )
        except WorkerInspectionNotFound as error:
            raise WorkerInspectionNotFoundError from error
        except WorkerInspectionPersistenceUnavailable as error:
            raise WorkerInspectionServiceUnavailable from error
