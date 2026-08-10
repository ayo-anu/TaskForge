"""Authenticated worker-facing session registration route."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Protocol, cast
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel, ConfigDict, Field

from taskforge.api.authentication import authenticate_worker
from taskforge.api.errors import ErrorDetail, ErrorResponse, error_response
from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.worker.domain import (
    MAX_WORKER_CAPABILITIES,
    InvalidWorkerRegistration,
)
from taskforge.worker.service import (
    WorkerRegistrationConflict,
    WorkerRegistrationRejected,
    WorkerRegistrationService,
    WorkerRegistrationServiceUnavailable,
)


class RegisterWorkerSessionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    capabilities: list[str] = Field(max_length=MAX_WORKER_CAPABILITIES)


class RegisteredWorkerSessionResponse(BaseModel):
    id: UUID
    registered_at: datetime
    capabilities: list[str]


class WorkerRegistrationRuntimeProtocol(Protocol):
    worker_registration_service: WorkerRegistrationService


router = APIRouter(tags=["worker-sessions"])

RESPONSES: dict[int | str, dict[str, Any]] = {
    status.HTTP_401_UNAUTHORIZED: {"model": ErrorResponse},
    status.HTTP_409_CONFLICT: {"model": ErrorResponse},
    status.HTTP_422_UNPROCESSABLE_CONTENT: {"model": ErrorResponse},
    status.HTTP_503_SERVICE_UNAVAILABLE: {"model": ErrorResponse},
}


@router.post(
    "/api/v1/worker-sessions",
    response_model=RegisteredWorkerSessionResponse,
    status_code=status.HTTP_201_CREATED,
    responses=RESPONSES,
)
async def register_worker_session(
    body: RegisterWorkerSessionRequest,
    request: Request,
    response: Response,
    authenticated_worker: Annotated[
        AuthenticatedWorker,
        Depends(authenticate_worker),
    ],
) -> RegisteredWorkerSessionResponse | Response:
    try:
        registered = await _runtime(request).worker_registration_service.register(
            authenticated_worker,
            tuple(body.capabilities),
        )
    except InvalidWorkerRegistration as error:
        return error_response(
            request,
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            code="validation_failed",
            message="The request is invalid.",
            details=tuple(
                ErrorDetail(
                    code=issue.code, path=list(issue.path), message=issue.message
                )
                for issue in error.issues
            ),
        )
    except WorkerRegistrationRejected as error:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            headers={"WWW-Authenticate": "Bearer"},
        ) from error
    except WorkerRegistrationConflict as error:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT) from error
    except WorkerRegistrationServiceUnavailable as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE) from error

    response.headers["Location"] = f"/api/v1/worker-sessions/{registered.id}"
    return RegisteredWorkerSessionResponse(
        id=registered.id,
        registered_at=registered.registered_at,
        capabilities=list(registered.capabilities),
    )


def _runtime(request: Request) -> WorkerRegistrationRuntimeProtocol:
    return cast(WorkerRegistrationRuntimeProtocol, request.app.state.authentication)
