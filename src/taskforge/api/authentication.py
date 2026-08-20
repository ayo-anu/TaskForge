"""FastAPI-only Bearer extraction and authentication adaptation."""

from __future__ import annotations

from typing import Annotated, Protocol, cast

from fastapi import HTTPException, Request, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncEngine

from taskforge.claims.service import TaskClaimInspectionService
from taskforge.dead_letters.service import DeadLetterService
from taskforge.identity.authentication import (
    APIAuthenticator,
    AuthenticatedAPIPrincipal,
    AuthenticatedWorker,
    AuthenticationFailure,
    AuthenticationUnavailable,
    WorkerAuthenticator,
)
from taskforge.identity.authorization import AuthorizationService
from taskforge.identity.credentials import (
    CredentialFormatError,
    PresentedCredential,
    parse_presented_credential,
)
from taskforge.identity.principals import PrincipalProfileService
from taskforge.persistence.authentication import (
    SQLAlchemyAPICredentialRepository,
    SQLAlchemyWorkerCredentialRepository,
)
from taskforge.persistence.authorization import SQLAlchemyPrincipalRoleRepository
from taskforge.persistence.claims import SQLAlchemyTaskClaimInspectionRepository
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.dead_letter_operations import SQLAlchemyDeadLetterRepository
from taskforge.persistence.principals import SQLAlchemyPrincipalProfileRepository
from taskforge.persistence.runs import SQLAlchemyWorkflowRunRepository
from taskforge.persistence.workers import (
    SQLAlchemyWorkerCapabilityRepository,
    SQLAlchemyWorkerHeartbeatRepository,
    SQLAlchemyWorkerInspectionRepository,
    SQLAlchemyWorkerRegistrationRepository,
)
from taskforge.persistence.workflows import SQLAlchemyWorkflowRepository
from taskforge.runs.service import WorkflowRunService
from taskforge.settings import Settings
from taskforge.worker.domain import WorkerHealthThresholds
from taskforge.worker.service import (
    WorkerCapabilityService,
    WorkerHeartbeatService,
    WorkerInspectionService,
    WorkerRegistrationService,
)
from taskforge.workflows.service import WorkflowService
from taskforge.workflows.task_types import TaskTypeRegistry

_bearer = HTTPBearer(auto_error=False)


class AuthenticationRuntimeProtocol(Protocol):
    """Authentication resources owned by the API lifespan."""

    api_authenticator: APIAuthenticator
    worker_authenticator: WorkerAuthenticator

    async def close(self) -> None:
        """Release authentication persistence resources."""


class AuthenticationRuntime:
    """Concrete authentication services and their lazy SQLAlchemy engine."""

    def __init__(
        self,
        engine: AsyncEngine,
        api_authenticator: APIAuthenticator,
        worker_authenticator: WorkerAuthenticator,
        authorization_service: AuthorizationService,
        principal_profile_service: PrincipalProfileService,
        workflow_service: WorkflowService,
        workflow_run_service: WorkflowRunService,
        task_type_registry: TaskTypeRegistry,
        worker_registration_service: WorkerRegistrationService,
        worker_heartbeat_service: WorkerHeartbeatService,
        worker_inspection_service: WorkerInspectionService,
        worker_capability_service: WorkerCapabilityService,
        task_claim_inspection_service: TaskClaimInspectionService,
        dead_letter_service: DeadLetterService,
    ) -> None:
        self._engine = engine
        self.api_authenticator = api_authenticator
        self.worker_authenticator = worker_authenticator
        self.authorization_service = authorization_service
        self.principal_profile_service = principal_profile_service
        self.workflow_service = workflow_service
        self.workflow_run_service = workflow_run_service
        self.task_type_registry = task_type_registry
        self.worker_registration_service = worker_registration_service
        self.worker_heartbeat_service = worker_heartbeat_service
        self.worker_inspection_service = worker_inspection_service
        self.worker_capability_service = worker_capability_service
        self.task_claim_inspection_service = task_claim_inspection_service
        self.dead_letter_service = dead_letter_service

    async def close(self) -> None:
        await self._engine.dispose()


def build_authentication_runtime(
    settings: Settings,
    task_type_registry: TaskTypeRegistry,
) -> AuthenticationRuntime:
    """Compose separate API and worker authentication services."""
    engine = build_async_engine(settings)
    sessions = build_session_factory(engine)
    return AuthenticationRuntime(
        engine=engine,
        api_authenticator=APIAuthenticator(
            SQLAlchemyAPICredentialRepository(sessions),
            timeout_seconds=settings.authentication_timeout_seconds,
        ),
        worker_authenticator=WorkerAuthenticator(
            SQLAlchemyWorkerCredentialRepository(sessions),
            timeout_seconds=settings.authentication_timeout_seconds,
        ),
        authorization_service=AuthorizationService(
            SQLAlchemyPrincipalRoleRepository(sessions),
            timeout_seconds=settings.authentication_timeout_seconds,
        ),
        principal_profile_service=PrincipalProfileService(
            SQLAlchemyPrincipalProfileRepository(sessions),
            timeout_seconds=settings.authentication_timeout_seconds,
        ),
        workflow_service=WorkflowService(
            SQLAlchemyWorkflowRepository(sessions),
            task_type_registry,
        ),
        workflow_run_service=WorkflowRunService(
            SQLAlchemyWorkflowRunRepository(sessions)
        ),
        task_type_registry=task_type_registry,
        worker_registration_service=WorkerRegistrationService(
            SQLAlchemyWorkerRegistrationRepository(sessions),
            task_type_registry,
        ),
        worker_heartbeat_service=WorkerHeartbeatService(
            SQLAlchemyWorkerHeartbeatRepository(sessions)
        ),
        worker_inspection_service=WorkerInspectionService(
            SQLAlchemyWorkerInspectionRepository(sessions),
            WorkerHealthThresholds(
                settings.worker_stale_after_seconds,
                settings.worker_offline_after_seconds,
            ),
        ),
        worker_capability_service=WorkerCapabilityService(
            SQLAlchemyWorkerCapabilityRepository(sessions),
            task_type_registry,
        ),
        task_claim_inspection_service=TaskClaimInspectionService(
            SQLAlchemyTaskClaimInspectionRepository(sessions)
        ),
        dead_letter_service=DeadLetterService(SQLAlchemyDeadLetterRepository(sessions)),
    )


async def authenticate_api_principal(
    request: Request,
    authorization: Annotated[HTTPAuthorizationCredentials | None, Security(_bearer)],
) -> AuthenticatedAPIPrincipal:
    """Authenticate an API Bearer value without applying authorization policy."""
    runtime = _runtime(request)
    credential = _extract_credential(authorization)
    try:
        return await runtime.api_authenticator.authenticate(credential)
    except AuthenticationFailure as error:
        raise _credential_rejected() from error
    except AuthenticationUnavailable as error:
        raise _authentication_unavailable() from error


async def authenticate_worker(
    request: Request,
    authorization: Annotated[HTTPAuthorizationCredentials | None, Security(_bearer)],
) -> AuthenticatedWorker:
    """Authenticate a worker Bearer value without trusting a claimed identity."""
    runtime = _runtime(request)
    credential = _extract_credential(authorization)
    try:
        return await runtime.worker_authenticator.authenticate(credential)
    except AuthenticationFailure as error:
        raise _credential_rejected() from error
    except AuthenticationUnavailable as error:
        raise _authentication_unavailable() from error


def _runtime(request: Request) -> AuthenticationRuntimeProtocol:
    return cast(AuthenticationRuntimeProtocol, request.app.state.authentication)


def _extract_credential(
    authorization: HTTPAuthorizationCredentials | None,
) -> PresentedCredential:
    if authorization is None:
        raise _credential_rejected()
    try:
        return parse_presented_credential(authorization.credentials)
    except CredentialFormatError as error:
        raise _credential_rejected() from error


def _credential_rejected() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="authentication required",
        headers={"WWW-Authenticate": "Bearer"},
    )


def _authentication_unavailable() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="authentication unavailable",
    )
