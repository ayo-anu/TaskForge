"""Authenticated worker-facing session registration route."""

from __future__ import annotations

import base64
import binascii
import json
from datetime import UTC, datetime
from typing import Annotated, Any, Protocol, cast
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr

from taskforge.api.authentication import authenticate_worker
from taskforge.api.authorization import require_permission
from taskforge.api.errors import ErrorDetail, ErrorResponse, error_response
from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.identity.authorization import AuthorizationContext, Permission
from taskforge.worker.domain import (
    MAX_HEARTBEAT_SEQUENCE,
    MAX_WORKER_CAPABILITIES,
    InspectedWorkerHeartbeat,
    InspectedWorkerSession,
    InspectedWorkerSessionPage,
    InspectedWorkerSessionResource,
    InvalidWorkerRegistration,
    WorkerHealthThresholds,
    WorkerSessionHealthStatus,
    WorkerSessionPageCursor,
)
from taskforge.worker.service import (
    ConflictingWorkerHeartbeatReplay,
    StaleWorkerHeartbeat,
    WorkerCapabilityInvariantError,
    WorkerCapabilityRejected,
    WorkerCapabilityService,
    WorkerCapabilityServiceUnavailable,
    WorkerCapabilitySessionInactiveError,
    WorkerCapabilitySessionUnavailableError,
    WorkerHeartbeatGap,
    WorkerHeartbeatRejected,
    WorkerHeartbeatService,
    WorkerHeartbeatServiceUnavailable,
    WorkerInspectionInvariantError,
    WorkerInspectionNotFoundError,
    WorkerInspectionService,
    WorkerInspectionServiceUnavailable,
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


class ReplaceWorkerCapabilitiesRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    capabilities: list[StrictStr] = Field(max_length=MAX_WORKER_CAPABILITIES)


class ReplacedWorkerCapabilitiesResponse(BaseModel):
    worker_session_id: UUID
    capabilities: list[str]


class InspectedWorkerIdentityResponse(BaseModel):
    id: UUID
    name: str
    enabled: bool


class InspectedWorkerHealthResponse(BaseModel):
    status: WorkerSessionHealthStatus
    last_sequence: int
    last_seen_at: datetime
    accepting_work: bool
    availability_changed_at: datetime


class WorkerObservationResponse(BaseModel):
    reference_time: datetime
    stale_after_seconds: int
    offline_after_seconds: int


class InspectedWorkerSessionResponse(BaseModel):
    id: UUID
    worker_identity: InspectedWorkerIdentityResponse
    registered_at: datetime
    ended_at: datetime | None
    capabilities: list[str]
    health: InspectedWorkerHealthResponse


class InspectedWorkerSessionResourceResponse(InspectedWorkerSessionResponse):
    observation: WorkerObservationResponse


class WorkerSessionPageMetadataResponse(BaseModel):
    limit: int
    next_cursor: str | None


class WorkerSessionListResponse(BaseModel):
    items: list[InspectedWorkerSessionResponse]
    observation: WorkerObservationResponse
    page: WorkerSessionPageMetadataResponse


class InspectedWorkerHeartbeatResponse(BaseModel):
    sequence: int
    received_at: datetime
    accepting_work: bool


class WorkerHeartbeatHistoryPageResponse(BaseModel):
    limit: int
    next_before_sequence: int | None


class WorkerHeartbeatHistoryResponse(BaseModel):
    items: list[InspectedWorkerHeartbeatResponse]
    page: WorkerHeartbeatHistoryPageResponse


class WorkerRegistrationRuntimeProtocol(Protocol):
    worker_registration_service: WorkerRegistrationService
    worker_heartbeat_service: WorkerHeartbeatService
    worker_inspection_service: WorkerInspectionService
    worker_capability_service: WorkerCapabilityService


router = APIRouter(tags=["worker-sessions"])

RESPONSES: dict[int | str, dict[str, Any]] = {
    status.HTTP_401_UNAUTHORIZED: {"model": ErrorResponse},
    status.HTTP_404_NOT_FOUND: {"model": ErrorResponse},
    status.HTTP_409_CONFLICT: {"model": ErrorResponse},
    status.HTTP_422_UNPROCESSABLE_CONTENT: {"model": ErrorResponse},
    status.HTTP_503_SERVICE_UNAVAILABLE: {"model": ErrorResponse},
}

DEFAULT_INSPECTION_PAGE_SIZE = 50
MAX_INSPECTION_PAGE_SIZE = 100
MAX_INSPECTION_CURSOR_LENGTH = 768
MAX_INSPECTION_CURSOR_BYTES = 512
_INSPECTION_CURSOR_VERSION = 1


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


@router.put(
    "/api/v1/worker-sessions/{worker_session_id}/capabilities",
    response_model=ReplacedWorkerCapabilitiesResponse,
    responses=RESPONSES,
)
async def replace_worker_session_capabilities(
    worker_session_id: UUID,
    body: ReplaceWorkerCapabilitiesRequest,
    request: Request,
    authenticated_worker: Annotated[
        AuthenticatedWorker,
        Depends(authenticate_worker),
    ],
) -> ReplacedWorkerCapabilitiesResponse | Response:
    try:
        replaced = await _runtime(request).worker_capability_service.replace(
            authenticated_worker,
            worker_session_id,
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
    except WorkerCapabilityRejected as error:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            headers={"WWW-Authenticate": "Bearer"},
        ) from error
    except WorkerCapabilitySessionUnavailableError as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND) from error
    except WorkerCapabilitySessionInactiveError:
        return _conflict_response(
            request,
            "worker_session_inactive",
            "The worker session is inactive.",
        )
    except WorkerCapabilityInvariantError as error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        ) from error
    except WorkerCapabilityServiceUnavailable as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE) from error
    return ReplacedWorkerCapabilitiesResponse(
        worker_session_id=replaced.worker_session_id,
        capabilities=list(replaced.capabilities),
    )


@router.get(
    "/api/v1/worker-sessions/{worker_session_id}",
    response_model=InspectedWorkerSessionResourceResponse,
    responses=RESPONSES,
)
async def inspect_worker_session(
    worker_session_id: UUID,
    request: Request,
    context: Annotated[
        AuthorizationContext,
        Depends(require_permission(Permission.VIEW)),
    ],
) -> InspectedWorkerSessionResourceResponse:
    del context
    try:
        resource = await _runtime(request).worker_inspection_service.get_session(
            worker_session_id
        )
    except WorkerInspectionNotFoundError as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND) from error
    except WorkerInspectionInvariantError as error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        ) from error
    except WorkerInspectionServiceUnavailable as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE) from error
    return _session_resource_response(resource)


@router.get(
    "/api/v1/worker-sessions",
    response_model=WorkerSessionListResponse,
    responses=RESPONSES,
)
async def list_worker_sessions(
    request: Request,
    context: Annotated[
        AuthorizationContext,
        Depends(require_permission(Permission.VIEW)),
    ],
    worker_identity_id: UUID | None = None,
    health_status: WorkerSessionHealthStatus | None = None,
    limit: Annotated[int, Query(ge=1, le=MAX_INSPECTION_PAGE_SIZE)] = (
        DEFAULT_INSPECTION_PAGE_SIZE
    ),
    cursor: Annotated[
        str | None, Query(max_length=MAX_INSPECTION_CURSOR_LENGTH)
    ] = None,
) -> WorkerSessionListResponse | Response:
    del context
    service = _runtime(request).worker_inspection_service
    try:
        decoded = (
            _decode_inspection_cursor(
                cursor,
                worker_identity_id=worker_identity_id,
                health_status=health_status,
                thresholds=service.thresholds,
            )
            if cursor is not None
            else None
        )
    except ValueError:
        return error_response(
            request,
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            code="validation_failed",
            message="The request is invalid.",
            details=(
                ErrorDetail(
                    code="invalid_cursor",
                    path=["query", "cursor"],
                    message="Cursor is invalid.",
                ),
            ),
        )
    try:
        page = await service.list_sessions(
            worker_identity_id=worker_identity_id,
            health_status=health_status,
            limit=limit,
            cursor=decoded,
        )
    except WorkerInspectionInvariantError as error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        ) from error
    except WorkerInspectionServiceUnavailable as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE) from error
    return _session_list_response(page, limit)


@router.get(
    "/api/v1/worker-sessions/{worker_session_id}/heartbeats",
    response_model=WorkerHeartbeatHistoryResponse,
    responses=RESPONSES,
)
async def inspect_worker_heartbeats(
    worker_session_id: UUID,
    request: Request,
    context: Annotated[
        AuthorizationContext,
        Depends(require_permission(Permission.VIEW)),
    ],
    limit: Annotated[int, Query(ge=1, le=MAX_INSPECTION_PAGE_SIZE)] = (
        DEFAULT_INSPECTION_PAGE_SIZE
    ),
    before_sequence: Annotated[
        StrictInt | None, Query(ge=1, le=MAX_HEARTBEAT_SEQUENCE)
    ] = None,
) -> WorkerHeartbeatHistoryResponse:
    del context
    try:
        page = await _runtime(request).worker_inspection_service.list_heartbeats(
            worker_session_id,
            before_sequence=before_sequence,
            limit=limit,
        )
    except WorkerInspectionNotFoundError as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND) from error
    except WorkerInspectionServiceUnavailable as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE) from error
    return WorkerHeartbeatHistoryResponse(
        items=[_heartbeat_history_response(item) for item in page.items],
        page=WorkerHeartbeatHistoryPageResponse(
            limit=limit,
            next_before_sequence=page.next_before_sequence,
        ),
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


def _session_response(
    session: InspectedWorkerSession,
) -> InspectedWorkerSessionResponse:
    return InspectedWorkerSessionResponse(
        id=session.id,
        worker_identity=InspectedWorkerIdentityResponse(
            id=session.identity.id,
            name=session.identity.name,
            enabled=session.identity.enabled,
        ),
        registered_at=session.registered_at,
        ended_at=session.ended_at,
        capabilities=list(session.capabilities),
        health=InspectedWorkerHealthResponse(
            status=session.health.status,
            last_sequence=session.health.last_sequence,
            last_seen_at=session.health.last_seen_at,
            accepting_work=session.health.accepting_work,
            availability_changed_at=session.health.availability_changed_at,
        ),
    )


def _observation_response(
    reference_time: datetime, thresholds: WorkerHealthThresholds
) -> WorkerObservationResponse:
    return WorkerObservationResponse(
        reference_time=reference_time,
        stale_after_seconds=thresholds.stale_after_seconds,
        offline_after_seconds=thresholds.offline_after_seconds,
    )


def _session_resource_response(
    resource: InspectedWorkerSessionResource,
) -> InspectedWorkerSessionResourceResponse:
    session = _session_response(resource.session)
    return InspectedWorkerSessionResourceResponse(
        **session.model_dump(),
        observation=_observation_response(
            resource.observation.reference_time,
            resource.observation.thresholds,
        ),
    )


def _session_list_response(
    page: InspectedWorkerSessionPage, limit: int
) -> WorkerSessionListResponse:
    return WorkerSessionListResponse(
        items=[_session_response(item) for item in page.items],
        observation=_observation_response(
            page.observation.reference_time, page.observation.thresholds
        ),
        page=WorkerSessionPageMetadataResponse(
            limit=limit,
            next_cursor=(
                _encode_inspection_cursor(page.next_cursor)
                if page.next_cursor is not None
                else None
            ),
        ),
    )


def _heartbeat_history_response(
    item: InspectedWorkerHeartbeat,
) -> InspectedWorkerHeartbeatResponse:
    return InspectedWorkerHeartbeatResponse(
        sequence=item.sequence,
        received_at=item.received_at,
        accepting_work=item.accepting_work,
    )


def _encode_inspection_cursor(cursor: WorkerSessionPageCursor) -> str:
    payload = json.dumps(
        {
            "v": _INSPECTION_CURSOR_VERSION,
            "rt": _cursor_timestamp(cursor.reference_time),
            "ls": _cursor_timestamp(cursor.last_seen_at),
            "sid": str(cursor.worker_session_id),
            "wid": (
                str(cursor.worker_identity_id)
                if cursor.worker_identity_id is not None
                else None
            ),
            "hs": cursor.health_status.value if cursor.health_status else None,
            "st": cursor.thresholds.stale_after_seconds,
            "ot": cursor.thresholds.offline_after_seconds,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")


def _decode_inspection_cursor(
    value: str,
    *,
    worker_identity_id: UUID | None,
    health_status: WorkerSessionHealthStatus | None,
    thresholds: WorkerHealthThresholds,
) -> WorkerSessionPageCursor:
    if not value or len(value) > MAX_INSPECTION_CURSOR_LENGTH or not value.isascii():
        raise ValueError("invalid cursor")
    try:
        decoded = base64.b64decode(
            value + "=" * (-len(value) % 4), altchars=b"-_", validate=True
        )
        if len(decoded) > MAX_INSPECTION_CURSOR_BYTES:
            raise ValueError("invalid cursor")
        payload = json.loads(decoded)
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("invalid cursor") from error
    expected = {"v", "rt", "ls", "sid", "wid", "hs", "st", "ot"}
    if not isinstance(payload, dict) or set(payload) != expected:
        raise ValueError("invalid cursor")
    if type(payload["v"]) is not int or payload["v"] != _INSPECTION_CURSOR_VERSION:
        raise ValueError("invalid cursor")
    if type(payload["st"]) is not int or type(payload["ot"]) is not int:
        raise ValueError("invalid cursor")
    if not all(isinstance(payload[field], str) for field in ("rt", "ls", "sid")):
        raise ValueError("invalid cursor")
    if payload["wid"] is not None and not isinstance(payload["wid"], str):
        raise ValueError("invalid cursor")
    if payload["hs"] is not None and not isinstance(payload["hs"], str):
        raise ValueError("invalid cursor")
    try:
        decoded_thresholds = WorkerHealthThresholds(payload["st"], payload["ot"])
        cursor = WorkerSessionPageCursor(
            _parse_cursor_timestamp(payload["rt"]),
            _parse_cursor_timestamp(payload["ls"]),
            UUID(payload["sid"]),
            UUID(payload["wid"]) if payload["wid"] is not None else None,
            (
                WorkerSessionHealthStatus(payload["hs"])
                if payload["hs"] is not None
                else None
            ),
            decoded_thresholds,
        )
    except (TypeError, ValueError) as error:
        raise ValueError("invalid cursor") from error
    if (
        cursor.worker_identity_id != worker_identity_id
        or cursor.health_status != health_status
        or cursor.thresholds != thresholds
    ):
        raise ValueError("invalid cursor")
    return cursor


def _cursor_timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("cursor timestamp must be timezone-aware")
    return (
        value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    )


def _parse_cursor_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("invalid cursor timestamp")
    return parsed.astimezone(UTC)
