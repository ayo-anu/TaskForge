"""Authenticated worker-facing session registration route."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Protocol, cast
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt

from taskforge.api.authentication import authenticate_worker
from taskforge.api.errors import ErrorDetail, ErrorResponse, error_response
from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.worker.domain import (
    MAX_HEARTBEAT_SEQUENCE,
    MAX_WORKER_CAPABILITIES,
    InvalidWorkerRegistration,
)
from taskforge.worker.service import (
    ConflictingWorkerHeartbeatReplay,
    StaleWorkerHeartbeat,
    WorkerHeartbeatGap,
    WorkerHeartbeatRejected,
    WorkerHeartbeatService,
    WorkerHeartbeatServiceUnavailable,
    WorkerRegistrationConflict,
    WorkerRegistrationRejected,
    WorkerRegistrationService,
    WorkerRegistrationServiceUnavailable,
    WorkerSessionInactive,
    WorkerSessionUnavailable,
)


class RegisterWorkerSessionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    capabilities: list[str] = Field(max_length=MAX_WORKER_CAPABILITIES)


class RegisteredWorkerSessionResponse(BaseModel):
    id: UUID
    registered_at: datetime
    capabilities: list[str]


class WorkerHeartbeatRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sequence: Annotated[StrictInt, Field(ge=1, le=MAX_HEARTBEAT_SEQUENCE)]
    accepting_work: StrictBool


class WorkerHealthResponse(BaseModel):
    worker_session_id: UUID
    last_sequence: int
    last_seen_at: datetime
    accepting_work: bool
    availability_changed_at: datetime


class WorkerRegistrationRuntimeProtocol(Protocol):
    worker_registration_service: WorkerRegistrationService
    worker_heartbeat_service: WorkerHeartbeatService


router = APIRouter(tags=["worker-sessions"])

RESPONSES: dict[int | str, dict[str, Any]] = {
    status.HTTP_401_UNAUTHORIZED: {"model": ErrorResponse},
    status.HTTP_404_NOT_FOUND: {"model": ErrorResponse},
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


@router.post(
    "/api/v1/worker-sessions/{worker_session_id}/heartbeats",
    response_model=WorkerHealthResponse,
    responses=RESPONSES,
)
async def heartbeat_worker_session(
    worker_session_id: UUID,
    body: WorkerHeartbeatRequest,
    request: Request,
    authenticated_worker: Annotated[
        AuthenticatedWorker,
        Depends(authenticate_worker),
    ],
) -> WorkerHealthResponse | Response:
    try:
        health = await _runtime(request).worker_heartbeat_service.heartbeat(
            authenticated_worker,
            worker_session_id,
            sequence=body.sequence,
            accepting_work=body.accepting_work,
        )
    except WorkerHeartbeatRejected as error:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            headers={"WWW-Authenticate": "Bearer"},
        ) from error
    except WorkerSessionUnavailable as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND) from error
    except WorkerSessionInactive:
        return _conflict_response(
            request,
            "worker_session_inactive",
            "The worker session is inactive.",
        )
    except StaleWorkerHeartbeat:
        return _conflict_response(
            request,
            "stale_heartbeat",
            "The heartbeat sequence is stale.",
        )
    except WorkerHeartbeatGap:
        return _conflict_response(
            request,
            "heartbeat_sequence_gap",
            "The heartbeat sequence skips the required next value.",
        )
    except ConflictingWorkerHeartbeatReplay:
        return _conflict_response(
            request,
            "heartbeat_replay_conflict",
            "The heartbeat conflicts with the accepted sequence.",
        )
    except WorkerHeartbeatServiceUnavailable as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE) from error
    return WorkerHealthResponse(
        worker_session_id=health.worker_session_id,
        last_sequence=health.last_sequence,
        last_seen_at=health.last_seen_at,
        accepting_work=health.accepting_work,
        availability_changed_at=health.availability_changed_at,
    )


def _conflict_response(request: Request, code: str, message: str) -> Response:
    return error_response(
        request,
        status_code=status.HTTP_409_CONFLICT,
        code=code,
        message=message,
    )


def _runtime(request: Request) -> WorkerRegistrationRuntimeProtocol:
    return cast(WorkerRegistrationRuntimeProtocol, request.app.state.authentication)
