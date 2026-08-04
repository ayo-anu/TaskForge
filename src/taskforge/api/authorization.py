"""FastAPI adaptation for transport-independent authorization policy."""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import Annotated, Protocol, cast

from fastapi import Depends, HTTPException, Request, status

from taskforge.api.authentication import authenticate_api_principal
from taskforge.identity.authentication import AuthenticatedAPIPrincipal
from taskforge.identity.authorization import (
    AuthorizationContext,
    AuthorizationDenied,
    AuthorizationService,
    AuthorizationUnavailable,
    Permission,
)


class AuthorizationRuntimeProtocol(Protocol):
    authorization_service: AuthorizationService


async def get_authorization_context(
    request: Request,
    identity: Annotated[
        AuthenticatedAPIPrincipal,
        Depends(authenticate_api_principal),
    ],
) -> AuthorizationContext:
    runtime = cast(AuthorizationRuntimeProtocol, request.app.state.authentication)
    try:
        return await runtime.authorization_service.context_for(identity)
    except AuthorizationDenied as error:
        raise _authorization_denied() from error
    except AuthorizationUnavailable as error:
        raise _authorization_unavailable() from error


def require_permission(
    permission: Permission,
) -> Callable[..., Coroutine[None, None, AuthorizationContext]]:
    """Build a dependency without coupling domain policy to FastAPI."""

    async def dependency(
        context: Annotated[
            AuthorizationContext,
            Depends(get_authorization_context),
        ],
    ) -> AuthorizationContext:
        try:
            context.require(permission)
        except AuthorizationDenied as error:
            raise _authorization_denied() from error
        return context

    return dependency


def _authorization_denied() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="access denied",
    )


def _authorization_unavailable() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="authorization unavailable",
    )
