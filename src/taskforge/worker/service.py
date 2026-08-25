"""Application service for authenticated worker-session registration."""

from __future__ import annotations

from collections.abc import Callable
from uuid import UUID, uuid4

from taskforge.audit.domain import (
    AuditActor,
    AuditActorKind,
    AuditOutcome,
    AuditRecord,
    AuditRejected,
    bounded_string_set,
)
from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.persistence.audit import RejectedAuditRecorder
from taskforge.worker.domain import (
    InspectedWorkerHeartbeatPage,
    InspectedWorkerSessionPage,
    InspectedWorkerSessionResource,
    InvalidWorkerRegistration,
    RegisteredWorkerSession,
    ReplacedWorkerCapabilities,
    WorkerCapabilityReplacement,
    WorkerHealthProjection,
    WorkerHealthThresholds,
    WorkerHeartbeat,
    WorkerRegistration,
    WorkerSessionHealthStatus,
    WorkerSessionPageCursor,
    validate_worker_capabilities,
    validate_worker_registration,
)
from taskforge.worker.persistence_ports import (
    WorkerCapabilityAuthorityRejected,
    WorkerCapabilityInvariantViolation,
    WorkerCapabilityPersistenceUnavailable,
    WorkerCapabilityRepository,
    WorkerCapabilitySessionInactive,
    WorkerCapabilitySessionUnavailable,
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

_REGISTRATION_AUDIT_REASONS: dict[type[Exception], str] = {
    InvalidWorkerRegistration: "capability_advertisement_invalid",
    WorkerRegistrationAuthorityRejected: "worker_authority_rejected",
    WorkerRegistrationRecordConflict: "registration_conflict",
}
_HEARTBEAT_AUDIT_REASONS: dict[type[Exception], str] = {
    WorkerHeartbeatAuthorityRejected: "worker_authority_rejected",
    WorkerHeartbeatSessionUnavailable: "worker_session_unavailable",
    WorkerHeartbeatSessionInactive: "worker_session_inactive",
    WorkerHeartbeatStale: "stale_heartbeat",
    WorkerHeartbeatSequenceGap: "heartbeat_sequence_gap",
    WorkerHeartbeatReplayConflict: "heartbeat_replay_conflict",
}
_CAPABILITY_AUDIT_REASONS: dict[type[Exception], str] = {
    InvalidWorkerRegistration: "capability_advertisement_invalid",
    WorkerCapabilityAuthorityRejected: "worker_authority_rejected",
    WorkerCapabilitySessionUnavailable: "worker_session_unavailable",
    WorkerCapabilitySessionInactive: "worker_session_inactive",
}


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


class WorkerCapabilityRejected(Exception):
    """Worker authority was invalid at the capability write boundary."""


class WorkerCapabilitySessionUnavailableError(Exception):
    """The session is absent from the authenticated worker's scope."""


class WorkerCapabilitySessionInactiveError(Exception):
    """The authenticated worker session has ended."""


class WorkerCapabilityInvariantError(Exception):
    """Capability persistence returned an inconsistent result."""


class WorkerCapabilityServiceUnavailable(Exception):
    """Capability replacement persistence was unavailable."""


class WorkerRejectedAuditUnavailable(
    WorkerRegistrationServiceUnavailable,
    WorkerHeartbeatServiceUnavailable,
    WorkerCapabilityServiceUnavailable,
):
    """A required rejected-worker-command audit could not be committed."""


class WorkerRegistrationService:
    def __init__(
        self,
        repository: WorkerRegistrationRepository,
        task_types: TaskTypeRegistry,
        *,
        identifier_factory: Callable[[], UUID] = uuid4,
        rejected_audit: RejectedAuditRecorder | None = None,
    ) -> None:
        self._repository = repository
        self._task_types = task_types
        self._identifier_factory = identifier_factory
        self._rejected_audit = rejected_audit

    async def register(
        self,
        authenticated_worker: AuthenticatedWorker,
        capabilities: tuple[str, ...],
        correlation_id: UUID | None = None,
    ) -> RegisteredWorkerSession:
        """Validate an advertisement and create one fresh process session."""
        try:
            validated_registration = validate_worker_registration(
                capabilities,
                known_capabilities=self._task_types.required_capabilities,
            )
            registration = WorkerRegistration(
                validated_registration.capabilities,
                str(correlation_id) if correlation_id else None,
            )
        except InvalidWorkerRegistration as error:
            await _worker_rejected(
                self._rejected_audit,
                authenticated_worker,
                None,
                "worker_session.register",
                _REGISTRATION_AUDIT_REASONS[type(error)],
                correlation_id,
                {"capabilities": bounded_string_set(capabilities)},
            )
            raise
        session_id = self._identifier_factory()
        try:
            return await self._repository.register_session(
                authenticated_worker,
                session_id,
                registration,
            )
        except WorkerRegistrationAuthorityRejected as error:
            await _worker_rejected(
                self._rejected_audit,
                authenticated_worker,
                None,
                "worker_session.register",
                _REGISTRATION_AUDIT_REASONS[type(error)],
                correlation_id,
                {"capabilities": bounded_string_set(capabilities)},
            )
            raise WorkerRegistrationRejected from error
        except WorkerRegistrationRecordConflict as error:
            await _worker_rejected(
                self._rejected_audit,
                authenticated_worker,
                None,
                "worker_session.register",
                _REGISTRATION_AUDIT_REASONS[type(error)],
                correlation_id,
                {"capabilities": bounded_string_set(capabilities)},
            )
            raise WorkerRegistrationConflict from error
        except WorkerRegistrationPersistenceUnavailable as error:
            raise WorkerRegistrationServiceUnavailable from error


class WorkerHeartbeatService:
    def __init__(
        self,
        repository: WorkerHeartbeatRepository,
        rejected_audit: RejectedAuditRecorder | None = None,
    ) -> None:
        self._repository = repository
        self._rejected_audit = rejected_audit

    async def heartbeat(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        *,
        sequence: int,
        accepting_work: bool,
        correlation_id: UUID | None = None,
    ) -> WorkerHealthProjection:
        """Apply one strictly ordered liveness and availability command."""
        heartbeat = WorkerHeartbeat(
            sequence,
            accepting_work,
            str(correlation_id) if correlation_id else None,
        )
        try:
            return await self._repository.apply_heartbeat(
                authenticated_worker,
                worker_session_id,
                heartbeat,
            )
        except WorkerHeartbeatAuthorityRejected as error:
            await _worker_rejected(
                self._rejected_audit,
                authenticated_worker,
                None,
                "worker_session.heartbeat",
                _HEARTBEAT_AUDIT_REASONS[type(error)],
                correlation_id,
                {"sequence": sequence},
            )
            raise WorkerHeartbeatRejected from error
        except WorkerHeartbeatSessionUnavailable as error:
            await _worker_rejected(
                self._rejected_audit,
                authenticated_worker,
                None,
                "worker_session.heartbeat",
                _HEARTBEAT_AUDIT_REASONS[type(error)],
                correlation_id,
                {"sequence": sequence},
            )
            raise WorkerSessionUnavailable from error
        except WorkerHeartbeatSessionInactive as error:
            await _worker_rejected(
                self._rejected_audit,
                authenticated_worker,
                worker_session_id,
                "worker_session.heartbeat",
                _HEARTBEAT_AUDIT_REASONS[type(error)],
                correlation_id,
                {"sequence": sequence},
            )
            raise WorkerSessionInactive from error
        except WorkerHeartbeatStale as error:
            await _worker_rejected(
                self._rejected_audit,
                authenticated_worker,
                worker_session_id,
                "worker_session.heartbeat",
                _HEARTBEAT_AUDIT_REASONS[type(error)],
                correlation_id,
                {"sequence": sequence},
            )
            raise StaleWorkerHeartbeat from error
        except WorkerHeartbeatSequenceGap as error:
            await _worker_rejected(
                self._rejected_audit,
                authenticated_worker,
                worker_session_id,
                "worker_session.heartbeat",
                _HEARTBEAT_AUDIT_REASONS[type(error)],
                correlation_id,
                {"sequence": sequence},
            )
            raise WorkerHeartbeatGap from error
        except WorkerHeartbeatReplayConflict as error:
            await _worker_rejected(
                self._rejected_audit,
                authenticated_worker,
                worker_session_id,
                "worker_session.heartbeat",
                _HEARTBEAT_AUDIT_REASONS[type(error)],
                correlation_id,
                {"sequence": sequence},
            )
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


class WorkerCapabilityService:
    def __init__(
        self,
        repository: WorkerCapabilityRepository,
        task_types: TaskTypeRegistry,
        rejected_audit: RejectedAuditRecorder | None = None,
    ) -> None:
        self._repository = repository
        self._task_types = task_types
        self._rejected_audit = rejected_audit

    async def replace(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        capabilities: tuple[str, ...],
        correlation_id: UUID | None = None,
    ) -> ReplacedWorkerCapabilities:
        try:
            replacement = WorkerCapabilityReplacement(
                validate_worker_capabilities(
                    capabilities,
                    known_capabilities=self._task_types.required_capabilities,
                ),
                str(correlation_id) if correlation_id else None,
            )
        except InvalidWorkerRegistration as error:
            await _worker_rejected(
                self._rejected_audit,
                authenticated_worker,
                None,
                "worker_session.capabilities_replace",
                _CAPABILITY_AUDIT_REASONS[type(error)],
                correlation_id,
                {"capabilities": bounded_string_set(capabilities)},
            )
            raise
        try:
            return await self._repository.replace_capabilities(
                authenticated_worker,
                worker_session_id,
                replacement,
            )
        except WorkerCapabilityAuthorityRejected as error:
            await _worker_rejected(
                self._rejected_audit,
                authenticated_worker,
                None,
                "worker_session.capabilities_replace",
                _CAPABILITY_AUDIT_REASONS[type(error)],
                correlation_id,
                {"capabilities": bounded_string_set(capabilities)},
            )
            raise WorkerCapabilityRejected from error
        except WorkerCapabilitySessionUnavailable as error:
            await _worker_rejected(
                self._rejected_audit,
                authenticated_worker,
                None,
                "worker_session.capabilities_replace",
                _CAPABILITY_AUDIT_REASONS[type(error)],
                correlation_id,
                {"capabilities": bounded_string_set(capabilities)},
            )
            raise WorkerCapabilitySessionUnavailableError from error
        except WorkerCapabilitySessionInactive as error:
            await _worker_rejected(
                self._rejected_audit,
                authenticated_worker,
                worker_session_id,
                "worker_session.capabilities_replace",
                _CAPABILITY_AUDIT_REASONS[type(error)],
                correlation_id,
                {"capabilities": bounded_string_set(capabilities)},
            )
            raise WorkerCapabilitySessionInactiveError from error
        except WorkerCapabilityInvariantViolation as error:
            raise WorkerCapabilityInvariantError from error
        except WorkerCapabilityPersistenceUnavailable as error:
            raise WorkerCapabilityServiceUnavailable from error


async def _worker_rejected(
    recorder: RejectedAuditRecorder | None,
    worker: AuthenticatedWorker,
    session_id: UUID | None,
    action: str,
    reason: str,
    correlation_id: UUID | None,
    provenance: dict[str, object],
) -> None:
    if recorder is None:
        return
    try:
        await recorder.record(
            AuditRecord(
                uuid4(),
                AuditActor(
                    AuditActorKind.WORKER,
                    worker_identity_id=worker.worker_identity_id,
                    worker_session_id=session_id,
                ),
                action,
                AuditOutcome.REJECTED,
                "worker_session",
                session_id,
                str(correlation_id) if correlation_id else None,
                provenance,
                reason,
            )
        )
    except AuditRejected as error:
        raise WorkerRejectedAuditUnavailable from error
