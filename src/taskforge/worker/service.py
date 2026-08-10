"""Application service for authenticated worker-session registration."""

from __future__ import annotations

from collections.abc import Callable
from uuid import UUID, uuid4

from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.worker.domain import (
    RegisteredWorkerSession,
    validate_worker_registration,
)
from taskforge.worker.persistence_ports import (
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
