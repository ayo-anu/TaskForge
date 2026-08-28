"""FastAPI-only Bearer extraction and authentication adaptation."""

from __future__ import annotations

from typing import Annotated, Protocol, cast
from uuid import UUID

from fastapi import HTTPException, Request, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncEngine

from taskforge.claims.service import TaskClaimInspectionService
from taskforge.dead_letters.service import DeadLetterService
from taskforge.history.export_service import HistoryExportService
from taskforge.history.service import HistoryService
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
from taskforge.persistence.audit import AuditUnitOfWork, RejectedAuditUnitOfWork
from taskforge.persistence.authentication import (
    SQLAlchemyAPICredentialRepository,
    SQLAlchemyWorkerCredentialRepository,
)
from taskforge.persistence.authorization import SQLAlchemyPrincipalRoleRepository
from taskforge.persistence.claims import SQLAlchemyTaskClaimInspectionRepository
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.dead_letter_operations import SQLAlchemyDeadLetterRepository
from taskforge.persistence.execution_events import (
    SQLAlchemyWorkflowRunExecutionEventRepository,
)
from taskforge.persistence.history import SQLAlchemyHistoryRepository
from taskforge.persistence.principals import SQLAlchemyPrincipalProfileRepository
from taskforge.persistence.rate_limits import SQLAlchemyRateLimitRepository
from taskforge.persistence.runs import SQLAlchemyWorkflowRunRepository
from taskforge.persistence.workers import (
    SQLAlchemyWorkerCapabilityRepository,
    SQLAlchemyWorkerHeartbeatRepository,
    SQLAlchemyWorkerInspectionRepository,
    SQLAlchemyWorkerRegistrationRepository,
)
from taskforge.persistence.workflows import SQLAlchemyWorkflowRepository
from taskforge.rate_limits import (
    BoundedLocalRateLimiter,
    RateLimit,
    RateLimiter,
    RateLimitPolicy,
    rate_limiter_for,
)
from taskforge.runs.persistence_ports import WorkflowRunExecutionEventRepository
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
    rate_limiter: RateLimiter

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
        workflow_run_execution_event_repository: WorkflowRunExecutionEventRepository,
        history_service: HistoryService,
        history_export_service: HistoryExportService,
        rate_limiter: RateLimiter,
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
        self.workflow_run_execution_event_repository = (
            workflow_run_execution_event_repository
        )
        self.history_service = history_service
        self.history_export_service = history_export_service
        self.rate_limiter = rate_limiter

    @property
    def engine(self) -> AsyncEngine:
        """Expose the lifespan-owned engine for API readiness composition."""
        return self._engine

    async def close(self) -> None:
        await self._engine.dispose()


def build_authentication_runtime(
    settings: Settings,
    task_type_registry: TaskTypeRegistry,
) -> AuthenticationRuntime:
    """Compose separate API and worker authentication services."""
    engine = build_async_engine(settings)
    sessions = build_session_factory(engine)
    rate_limits = {
        RateLimitPolicy.API_AUTH_NETWORK: RateLimit(
            settings.api_auth_network_failures_per_minute, 60
        ),
        RateLimitPolicy.API_AUTH_CREDENTIAL: RateLimit(
            settings.api_auth_credential_failures_per_five_minutes, 300
        ),
        RateLimitPolicy.WORKER_AUTH_NETWORK: RateLimit(
            settings.worker_auth_network_failures_per_minute, 60
        ),
        RateLimitPolicy.WORKER_AUTH_CREDENTIAL: RateLimit(
            settings.worker_auth_credential_failures_per_five_minutes, 300
        ),
        RateLimitPolicy.RUN_CREATE: RateLimit(settings.run_creations_per_minute, 60),
        RateLimitPolicy.RUN_REPLAY: RateLimit(settings.run_replays_per_minute, 60),
        RateLimitPolicy.DEAD_LETTER_REDRIVE: RateLimit(
            settings.dead_letter_redrives_per_minute, 60
        ),
        RateLimitPolicy.WORKER_REGISTER: RateLimit(
            settings.worker_registrations_per_minute, 60
        ),
        RateLimitPolicy.WORKER_RESULT: RateLimit(
            settings.worker_results_per_minute, 60
        ),
        RateLimitPolicy.WEBSOCKET_NETWORK: RateLimit(
            settings.websocket_connections_per_network_minute, 60
        ),
        RateLimitPolicy.WEBSOCKET_PRINCIPAL: RateLimit(
            settings.websocket_connections_per_principal_minute, 60
        ),
    }
    rate_limiter = RateLimiter(
        SQLAlchemyRateLimitRepository(
            sessions,
            timeout_seconds=settings.rate_limit_timeout_seconds,
            cleanup_retention_seconds=settings.rate_limit_cleanup_retention_seconds,
        ),
        BoundedLocalRateLimiter(capacity=settings.rate_limit_fallback_capacity),
        rate_limits,
    )
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
            RejectedAuditUnitOfWork(sessions),
        ),
        workflow_run_service=WorkflowRunService(
            SQLAlchemyWorkflowRunRepository(sessions),
            RejectedAuditUnitOfWork(sessions),
        ),
        task_type_registry=task_type_registry,
        worker_registration_service=WorkerRegistrationService(
            SQLAlchemyWorkerRegistrationRepository(sessions),
            task_type_registry,
            rejected_audit=RejectedAuditUnitOfWork(sessions),
        ),
        worker_heartbeat_service=WorkerHeartbeatService(
            SQLAlchemyWorkerHeartbeatRepository(sessions),
            RejectedAuditUnitOfWork(sessions),
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
            rejected_audit=RejectedAuditUnitOfWork(sessions),
        ),
        task_claim_inspection_service=TaskClaimInspectionService(
            SQLAlchemyTaskClaimInspectionRepository(sessions)
        ),
        dead_letter_service=DeadLetterService(
            SQLAlchemyDeadLetterRepository(sessions),
            RejectedAuditUnitOfWork(sessions),
        ),
        workflow_run_execution_event_repository=(
            SQLAlchemyWorkflowRunExecutionEventRepository(sessions)
        ),
        history_service=HistoryService(SQLAlchemyHistoryRepository(sessions)),
        history_export_service=HistoryExportService(
            SQLAlchemyHistoryRepository(sessions), AuditUnitOfWork(sessions)
        ),
        rate_limiter=rate_limiter,
    )


async def authenticate_api_principal(
    request: Request,
    authorization: Annotated[HTTPAuthorizationCredentials | None, Security(_bearer)],
) -> AuthenticatedAPIPrincipal:
    """Authenticate an API Bearer value without applying authorization policy."""
    runtime = _runtime(request)
    source = _network_source(request)
    await _reject_if_blocked(
        runtime, RateLimitPolicy.API_AUTH_NETWORK, "network", source
    )
    try:
        credential = _extract_credential(authorization)
    except HTTPException:
        await _consume_failed_authentication(
            runtime, RateLimitPolicy.API_AUTH_NETWORK, source, None
        )
        raise
    await _reject_if_blocked(
        runtime,
        RateLimitPolicy.API_AUTH_CREDENTIAL,
        "credential",
        credential.credential_id,
    )
    try:
        return await authenticate_api_credential(runtime, credential)
    except AuthenticationFailure as error:
        await _consume_failed_authentication(
            runtime,
            RateLimitPolicy.API_AUTH_NETWORK,
            source,
            (RateLimitPolicy.API_AUTH_CREDENTIAL, credential.credential_id),
        )
        raise _credential_rejected() from error
    except AuthenticationUnavailable as error:
        raise _authentication_unavailable() from error


async def authenticate_api_credential(
    runtime: AuthenticationRuntimeProtocol,
    credential: PresentedCredential,
) -> AuthenticatedAPIPrincipal:
    """Authenticate one parsed API credential for any supported transport."""
    return await runtime.api_authenticator.authenticate(credential)


async def authenticate_worker(
    request: Request,
    authorization: Annotated[HTTPAuthorizationCredentials | None, Security(_bearer)],
) -> AuthenticatedWorker:
    """Authenticate a worker Bearer value without trusting a claimed identity."""
    runtime = _runtime(request)
    source = _network_source(request)
    await _reject_if_blocked(
        runtime, RateLimitPolicy.WORKER_AUTH_NETWORK, "network", source
    )
    try:
        credential = _extract_credential(authorization)
    except HTTPException:
        await _consume_failed_authentication(
            runtime, RateLimitPolicy.WORKER_AUTH_NETWORK, source, None
        )
        raise
    await _reject_if_blocked(
        runtime,
        RateLimitPolicy.WORKER_AUTH_CREDENTIAL,
        "credential",
        credential.credential_id,
    )
    try:
        return await runtime.worker_authenticator.authenticate(credential)
    except AuthenticationFailure as error:
        await _consume_failed_authentication(
            runtime,
            RateLimitPolicy.WORKER_AUTH_NETWORK,
            source,
            (RateLimitPolicy.WORKER_AUTH_CREDENTIAL, credential.credential_id),
        )
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


def _network_source(request: Request) -> str:
    client = request.client
    return client.host if client is not None else "unknown"


async def _reject_if_blocked(
    runtime: AuthenticationRuntimeProtocol,
    policy: RateLimitPolicy,
    kind: str,
    value: str | UUID,
) -> None:
    decision = await rate_limiter_for(runtime).check(policy, kind, value)
    if not decision.allowed:
        raise _rate_limited(decision.retry_after_seconds)


async def _consume_failed_authentication(
    runtime: AuthenticationRuntimeProtocol,
    network_policy: RateLimitPolicy,
    source: str,
    credential: tuple[RateLimitPolicy, UUID] | None,
) -> None:
    limiter = rate_limiter_for(runtime)
    decisions = [await limiter.consume(network_policy, "network", source)]
    if credential is not None:
        decisions.append(
            await limiter.consume(credential[0], "credential", credential[1])
        )
    rejected = [decision for decision in decisions if not decision.allowed]
    if rejected:
        raise _rate_limited(max(item.retry_after_seconds for item in rejected))


def _rate_limited(retry_after_seconds: int) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail="request rate limit exceeded",
        headers={"Retry-After": str(max(1, retry_after_seconds))},
    )


def _authentication_unavailable() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="authentication unavailable",
    )
