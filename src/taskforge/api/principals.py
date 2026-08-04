"""Protected API-principal profile route."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Protocol, cast
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel

from taskforge.api.authorization import require_permission
from taskforge.api.errors import ErrorResponse
from taskforge.identity.authorization import AuthorizationContext, Permission
from taskforge.identity.principals import (
    PrincipalNotFound,
    PrincipalProfileService,
    PrincipalServiceUnavailable,
)


class PrincipalResponse(BaseModel):
    id: UUID
    name: str
    created_at: datetime


class PrincipalRuntimeProtocol(Protocol):
    principal_profile_service: PrincipalProfileService


router = APIRouter(prefix="/api/v1/principals", tags=["principals"])


@router.get(
    "/{principal_id}",
    response_model=PrincipalResponse,
    responses={
        status.HTTP_401_UNAUTHORIZED: {"model": ErrorResponse},
        status.HTTP_403_FORBIDDEN: {"model": ErrorResponse},
        status.HTTP_404_NOT_FOUND: {"model": ErrorResponse},
        status.HTTP_422_UNPROCESSABLE_CONTENT: {"model": ErrorResponse},
        status.HTTP_503_SERVICE_UNAVAILABLE: {"model": ErrorResponse},
    },
)
async def get_principal(
    principal_id: UUID,
    request: Request,
    context: Annotated[
        AuthorizationContext,
        Depends(require_permission(Permission.VIEW)),
    ],
) -> PrincipalResponse:
    runtime = cast(PrincipalRuntimeProtocol, request.app.state.authentication)
    try:
        profile = await runtime.principal_profile_service.get(principal_id, context)
    except PrincipalNotFound as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND) from error
    except PrincipalServiceUnavailable as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE) from error
    return PrincipalResponse(
        id=profile.id,
        name=profile.name,
        created_at=profile.created_at,
    )
