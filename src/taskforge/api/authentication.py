"""FastAPI-only Bearer extraction and authentication adaptation."""

from __future__ import annotations

from typing import Annotated, Protocol, cast

from fastapi import HTTPException, Request, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncEngine

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
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.principals import SQLAlchemyPrincipalProfileRepository
from taskforge.settings import Settings

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
    ) -> None:
        self._engine = engine
        self.api_authenticator = api_authenticator
        self.worker_authenticator = worker_authenticator
        self.authorization_service = authorization_service
        self.principal_profile_service = principal_profile_service

    async def close(self) -> None:
        await self._engine.dispose()


def build_authentication_runtime(settings: Settings) -> AuthenticationRuntime:
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
