"""Authorized dead-letter inspection and operator command routes."""

from __future__ import annotations

import base64
import binascii
import json
from datetime import UTC, datetime
from typing import Annotated, Any, Protocol, cast
from uuid import UUID

from fastapi import (
    APIRouter,
    Depends,
    Header,
    HTTPException,
    Query,
    Request,
    Response,
    status,
)
from pydantic import BaseModel, ConfigDict, Field, field_validator

from taskforge.api.authorization import require_permission
from taskforge.api.errors import ErrorDetail, ErrorResponse, error_response
from taskforge.dead_letters.domain import (
    CreatedDeadLetterRedrive,
    DeadLetterActionCursor,
    DeadLetterActionPage,
    DeadLetterActionType,
    DeadLetterCursor,
    DeadLetterDetail,
    DeadLetterFilters,
    DeadLetterOperatorAction,
    DeadLetterPage,
    DeadLetterReason,
    DeadLetterRedriveIdempotencyConflict,
    DeadLetterStatus,
    DeadLetterSummary,
    InvalidDeadLetterRedriveIdempotencyKey,
)
from taskforge.dead_letters.persistence_ports import (
    DeadLetterPersistenceInvariantViolation,
    DeadLetterPersistenceUnavailable,
    DeadLetterRedriveLimitExceeded,
    DeadLetterRedriveNotEligible,
    DeadLetterTransitionConflict,
)
from taskforge.dead_letters.service import DeadLetterNotFound, DeadLetterService
from taskforge.identity.authorization import AuthorizationContext, Permission
from taskforge.retries.domain import RetryNotScheduledReason
from taskforge.worker.results import TaskExecutionFailureKind, TaskExecutionResultKind

DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 100
MAX_CURSOR_LENGTH = 2048
MAX_CURSOR_BYTES = 1536
CURSOR_VERSION = 1


class PageMetadataResponse(BaseModel):
    limit: int
    next_cursor: str | None


class DeadLetterSummaryResponse(BaseModel):
    id: UUID
    task_run_id: UUID
    source_task_attempt_id: UUID
    workflow_run_id: UUID
    reason: DeadLetterReason
    status: DeadLetterStatus
    created_at: datetime
    status_updated_at: datetime
    source_attempt_number: int


class DeadLetterRedriveSummaryResponse(BaseModel):
    id: UUID
    target_workflow_run_id: UUID
    requested_by_principal_id: UUID
    reason: str | None
    requested_at: datetime
    target_workflow_run_status: str


class DeadLetterDetailResponse(DeadLetterSummaryResponse):
    workflow_definition_id: UUID
    workflow_version_id: UUID
    step_identifier: str
    result_kind: TaskExecutionResultKind
    failure_kind: TaskExecutionFailureKind | None
    retry_decision_reason: RetryNotScheduledReason | None
    redrive: DeadLetterRedriveSummaryResponse | None = None


class DeadLetterListResponse(BaseModel):
    items: list[DeadLetterSummaryResponse]
    page: PageMetadataResponse


class DeadLetterActionResponse(BaseModel):
    id: UUID
    dead_letter_item_id: UUID
    operator_principal_id: UUID
    action_type: DeadLetterActionType
    previous_status: DeadLetterStatus
    new_status: DeadLetterStatus
    reason: str | None
    correlation_id: UUID | None
    occurred_at: datetime


class DeadLetterActionListResponse(BaseModel):
    items: list[DeadLetterActionResponse]
    page: PageMetadataResponse


class AcknowledgeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    reason: Annotated[str | None, Field(max_length=2000)] = None

    @field_validator("reason")
    @classmethod
    def reason_not_blank(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("reason must not be blank")
        return value


class ResolveRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    reason: Annotated[str, Field(max_length=2000)]

    @field_validator("reason")
    @classmethod
    def reason_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("reason must not be blank")
        return value


class RedriveRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    reason: Annotated[str | None, Field(max_length=2000)] = None

    @field_validator("reason")
    @classmethod
    def reason_not_blank(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("reason must not be blank")
        return value.strip() if value is not None else None


class DeadLetterRedriveResponse(BaseModel):
    id: UUID
    dead_letter_item_id: UUID
    source_workflow_run_id: UUID
    source_task_run_id: UUID
    source_task_attempt_id: UUID
    target_workflow_run_id: UUID
    workflow_definition_id: UUID
    workflow_version_id: UUID
    requested_by_principal_id: UUID
    reason: str | None
    requested_at: datetime


class DeadLetterRuntimeProtocol(Protocol):
    dead_letter_service: DeadLetterService


router = APIRouter(prefix="/api/v1/dead-letters", tags=["dead-letters"])

COMMON_RESPONSES: dict[int | str, dict[str, Any]] = {
    status.HTTP_401_UNAUTHORIZED: {"model": ErrorResponse},
    status.HTTP_403_FORBIDDEN: {"model": ErrorResponse},
    status.HTTP_404_NOT_FOUND: {"model": ErrorResponse},
    status.HTTP_409_CONFLICT: {"model": ErrorResponse},
    status.HTTP_422_UNPROCESSABLE_CONTENT: {"model": ErrorResponse},
    status.HTTP_500_INTERNAL_SERVER_ERROR: {"model": ErrorResponse},
    status.HTTP_503_SERVICE_UNAVAILABLE: {"model": ErrorResponse},
}


@router.get("", response_model=DeadLetterListResponse, responses=COMMON_RESPONSES)
async def list_dead_letters(
    request: Request,
    context: Annotated[
        AuthorizationContext, Depends(require_permission(Permission.VIEW))
    ],
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = DEFAULT_PAGE_SIZE,
    cursor: Annotated[str | None, Query(max_length=MAX_CURSOR_LENGTH)] = None,
    current_status: Annotated[DeadLetterStatus | None, Query(alias="status")] = None,
    reason: DeadLetterReason | None = None,
    task_run_id: UUID | None = None,
    workflow_run_id: UUID | None = None,
    source_task_attempt_id: UUID | None = None,
    created_after: datetime | None = None,
    created_before: datetime | None = None,
) -> DeadLetterListResponse | Response:
    """Return a live keyset page; mutable filters can change between requests."""
    try:
        filters = DeadLetterFilters(
            status=current_status,
            reason=reason,
            task_run_id=task_run_id,
            workflow_run_id=workflow_run_id,
            source_task_attempt_id=source_task_attempt_id,
            created_after=created_after,
            created_before=created_before,
        )
    except ValueError:
        return _invalid_date_range(request)
    try:
        decoded = _decode_item_cursor(cursor, filters) if cursor else None
    except ValueError:
        return _invalid_cursor(request)
    try:
        page = await _runtime(request).dead_letter_service.list_items(
            context.owner_filter_for(Permission.VIEW),
            filters,
            limit=limit,
            cursor=decoded,
        )
    except DeadLetterPersistenceInvariantViolation as error:
        raise HTTPException(status_code=500) from error
    except DeadLetterPersistenceUnavailable as error:
        raise HTTPException(status_code=503) from error
    return _list_response(page, limit, filters)


@router.get(
    "/{item_id}", response_model=DeadLetterDetailResponse, responses=COMMON_RESPONSES
)
async def get_dead_letter(
    item_id: UUID,
    request: Request,
    context: Annotated[
        AuthorizationContext, Depends(require_permission(Permission.VIEW))
    ],
) -> DeadLetterDetailResponse:
    try:
        item = await _runtime(request).dead_letter_service.get_item(
            item_id, context.owner_filter_for(Permission.VIEW)
        )
    except DeadLetterNotFound as error:
        raise HTTPException(status_code=404) from error
    except DeadLetterPersistenceInvariantViolation as error:
        raise HTTPException(status_code=500) from error
    except DeadLetterPersistenceUnavailable as error:
        raise HTTPException(status_code=503) from error
    return _detail_response(item)


@router.get(
    "/{item_id}/actions",
    response_model=DeadLetterActionListResponse,
    responses=COMMON_RESPONSES,
)
async def list_dead_letter_actions(
    item_id: UUID,
    request: Request,
    context: Annotated[
        AuthorizationContext, Depends(require_permission(Permission.VIEW))
    ],
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = DEFAULT_PAGE_SIZE,
    cursor: Annotated[str | None, Query(max_length=MAX_CURSOR_LENGTH)] = None,
) -> DeadLetterActionListResponse | Response:
    try:
        decoded = _decode_action_cursor(cursor, item_id) if cursor else None
    except ValueError:
        return _invalid_cursor(request)
    try:
        page = await _runtime(request).dead_letter_service.list_actions(
            item_id,
            context.owner_filter_for(Permission.VIEW),
            limit=limit,
            cursor=decoded,
        )
    except DeadLetterNotFound as error:
        raise HTTPException(status_code=404) from error
    except DeadLetterPersistenceInvariantViolation as error:
        raise HTTPException(status_code=500) from error
    except DeadLetterPersistenceUnavailable as error:
        raise HTTPException(status_code=503) from error
    return _actions_response(page, limit, item_id)


@router.post(
    "/{item_id}/acknowledge",
    response_model=DeadLetterDetailResponse,
    responses=COMMON_RESPONSES,
)
async def acknowledge_dead_letter(
    item_id: UUID,
    body: AcknowledgeRequest,
    request: Request,
    context: Annotated[
        AuthorizationContext, Depends(require_permission(Permission.OPERATE_WORKFLOW))
    ],
) -> DeadLetterDetailResponse:
    return await _command(item_id, body.reason, False, request, context)


@router.post(
    "/{item_id}/resolve",
    response_model=DeadLetterDetailResponse,
    responses=COMMON_RESPONSES,
)
async def resolve_dead_letter(
    item_id: UUID,
    body: ResolveRequest,
    request: Request,
    context: Annotated[
        AuthorizationContext, Depends(require_permission(Permission.OPERATE_WORKFLOW))
    ],
) -> DeadLetterDetailResponse:
    return await _command(item_id, body.reason, True, request, context)


@router.post(
    "/{item_id}/redrive",
    response_model=DeadLetterRedriveResponse,
    status_code=status.HTTP_201_CREATED,
    responses=COMMON_RESPONSES,
)
async def redrive_dead_letter(
    item_id: UUID,
    body: RedriveRequest,
    request: Request,
    response: Response,
    context: Annotated[
        AuthorizationContext, Depends(require_permission(Permission.OPERATE_WORKFLOW))
    ],
    idempotency_key: Annotated[
        str | None, Header(alias="Idempotency-Key", max_length=128)
    ] = None,
) -> DeadLetterRedriveResponse | Response:
    try:
        redrive = await _runtime(request).dead_letter_service.redrive(
            item_id,
            context.owner_filter_for(Permission.OPERATE_WORKFLOW),
            operator_principal_id=context.principal_id,
            idempotency_key=idempotency_key,
            reason=body.reason,
            correlation_id=cast(UUID, request.state.request_id),
        )
    except InvalidDeadLetterRedriveIdempotencyKey:
        return error_response(
            request,
            status_code=422,
            code="validation_failed",
            message="The request is invalid.",
            details=(
                ErrorDetail(
                    code="invalid_idempotency_key",
                    path=["header", "Idempotency-Key"],
                    message="Idempotency-Key must be 16-128 printable ASCII characters.",
                ),
            ),
        )
    except DeadLetterNotFound as error:
        raise HTTPException(status_code=404) from error
    except DeadLetterRedriveIdempotencyConflict:
        return error_response(
            request,
            status_code=409,
            code="idempotency_conflict",
            message="The idempotency key conflicts with an earlier request.",
        )
    except DeadLetterRedriveNotEligible:
        return error_response(
            request,
            status_code=409,
            code="dead_letter_not_redrivable",
            message="The dead-letter item is not eligible for redrive.",
        )
    except DeadLetterRedriveLimitExceeded:
        return error_response(
            request,
            status_code=409,
            code="redrive_limit_exceeded",
            message="The dead-letter item already has a redrive.",
        )
    except DeadLetterPersistenceInvariantViolation as error:
        raise HTTPException(status_code=500) from error
    except DeadLetterPersistenceUnavailable as error:
        raise HTTPException(status_code=503) from error
    response.headers["Location"] = (
        f"/api/v1/workflow-runs/{redrive.target_workflow_run_id}"
    )
    return _redrive_response(redrive)


async def _command(
    item_id: UUID,
    reason: str | None,
    resolve: bool,
    request: Request,
    context: AuthorizationContext,
) -> DeadLetterDetailResponse:
    service = _runtime(request).dead_letter_service
    owner_filter = context.owner_filter_for(Permission.OPERATE_WORKFLOW)
    correlation_id = cast(UUID, request.state.request_id)
    try:
        if resolve:
            assert reason is not None
            item = await service.resolve(
                item_id,
                owner_filter,
                operator_principal_id=context.principal_id,
                reason=reason,
                correlation_id=correlation_id,
            )
        else:
            item = await service.acknowledge(
                item_id,
                owner_filter,
                operator_principal_id=context.principal_id,
                reason=reason,
                correlation_id=correlation_id,
            )
    except DeadLetterNotFound as error:
        raise HTTPException(status_code=404) from error
    except DeadLetterTransitionConflict as error:
        raise HTTPException(status_code=409) from error
    except DeadLetterPersistenceInvariantViolation as error:
        raise HTTPException(status_code=500) from error
    except DeadLetterPersistenceUnavailable as error:
        raise HTTPException(status_code=503) from error
    return _detail_response(item)


def _runtime(request: Request) -> DeadLetterRuntimeProtocol:
    return cast(DeadLetterRuntimeProtocol, request.app.state.authentication)


def _summary_response(item: DeadLetterSummary) -> DeadLetterSummaryResponse:
    return DeadLetterSummaryResponse(**item.__dict__)


def _detail_response(item: DeadLetterDetail) -> DeadLetterDetailResponse:
    return DeadLetterDetailResponse(**item.__dict__)


def _action_response(item: DeadLetterOperatorAction) -> DeadLetterActionResponse:
    return DeadLetterActionResponse(**item.__dict__)


def _redrive_response(item: CreatedDeadLetterRedrive) -> DeadLetterRedriveResponse:
    return DeadLetterRedriveResponse(**item.__dict__)


def _list_response(
    page: DeadLetterPage, limit: int, filters: DeadLetterFilters
) -> DeadLetterListResponse:
    return DeadLetterListResponse(
        items=[_summary_response(item) for item in page.items],
        page=PageMetadataResponse(
            limit=limit,
            next_cursor=(
                _encode_item_cursor(page.next_cursor, filters)
                if page.next_cursor is not None
                else None
            ),
        ),
    )


def _actions_response(
    page: DeadLetterActionPage, limit: int, item_id: UUID
) -> DeadLetterActionListResponse:
    return DeadLetterActionListResponse(
        items=[_action_response(item) for item in page.items],
        page=PageMetadataResponse(
            limit=limit,
            next_cursor=(
                _encode_action_cursor(page.next_cursor, item_id)
                if page.next_cursor is not None
                else None
            ),
        ),
    )


def _filter_payload(filters: DeadLetterFilters) -> dict[str, str | None]:
    return {
        "status": filters.status.value if filters.status else None,
        "reason": filters.reason.value if filters.reason else None,
        "task_run_id": str(filters.task_run_id) if filters.task_run_id else None,
        "workflow_run_id": (
            str(filters.workflow_run_id) if filters.workflow_run_id else None
        ),
        "source_task_attempt_id": (
            str(filters.source_task_attempt_id)
            if filters.source_task_attempt_id
            else None
        ),
        "created_after": _timestamp(filters.created_after),
        "created_before": _timestamp(filters.created_before),
    }


def _encode_item_cursor(cursor: DeadLetterCursor, filters: DeadLetterFilters) -> str:
    return _encode(
        {
            "v": CURSOR_VERSION,
            "created_at": _timestamp(cursor.created_at),
            "item_id": str(cursor.item_id),
            "filters": _filter_payload(filters),
        }
    )


def _decode_item_cursor(value: str, filters: DeadLetterFilters) -> DeadLetterCursor:
    payload = _decode(value)
    if set(payload) != {"v", "created_at", "item_id", "filters"}:
        raise ValueError("invalid cursor")
    if payload["v"] != CURSOR_VERSION or payload["filters"] != _filter_payload(filters):
        raise ValueError("invalid cursor")
    try:
        return DeadLetterCursor(
            datetime.fromisoformat(str(payload["created_at"]).replace("Z", "+00:00")),
            UUID(str(payload["item_id"])),
        )
    except (TypeError, ValueError) as error:
        raise ValueError("invalid cursor") from error


def _encode_action_cursor(cursor: DeadLetterActionCursor, item_id: UUID) -> str:
    return _encode(
        {
            "v": CURSOR_VERSION,
            "item_id": str(item_id),
            "occurred_at": _timestamp(cursor.occurred_at),
            "action_id": str(cursor.action_id),
        }
    )


def _decode_action_cursor(value: str, item_id: UUID) -> DeadLetterActionCursor:
    payload = _decode(value)
    if set(payload) != {"v", "item_id", "occurred_at", "action_id"}:
        raise ValueError("invalid cursor")
    if payload["v"] != CURSOR_VERSION or payload["item_id"] != str(item_id):
        raise ValueError("invalid cursor")
    try:
        return DeadLetterActionCursor(
            datetime.fromisoformat(str(payload["occurred_at"]).replace("Z", "+00:00")),
            UUID(str(payload["action_id"])),
        )
    except (TypeError, ValueError) as error:
        raise ValueError("invalid cursor") from error


def _encode(payload: dict[str, object]) -> str:
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _decode(value: str) -> dict[str, object]:
    if not value or len(value) > MAX_CURSOR_LENGTH or not value.isascii():
        raise ValueError("invalid cursor")
    try:
        raw = base64.b64decode(
            value + "=" * (-len(value) % 4), altchars=b"-_", validate=True
        )
        if len(raw) > MAX_CURSOR_BYTES:
            raise ValueError("invalid cursor")
        payload = json.loads(raw)
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("invalid cursor") from error
    if not isinstance(payload, dict):
        raise ValueError("invalid cursor")
    return payload


def _timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return (
        value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    )


def _invalid_cursor(request: Request) -> Response:
    return error_response(
        request,
        status_code=422,
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


def _invalid_date_range(request: Request) -> Response:
    return error_response(
        request,
        status_code=422,
        code="validation_failed",
        message="The request is invalid.",
        details=(
            ErrorDetail(
                code="invalid_date_range",
                path=["query"],
                message="created_after must precede created_before.",
            ),
        ),
    )
