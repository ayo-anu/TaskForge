"""Authorized, redacted immutable audit and operational-history routes."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Protocol, cast
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel, ConfigDict

from taskforge.api.authorization import require_permission
from taskforge.api.errors import ErrorDetail, error_response
from taskforge.audit.domain import AuditAction, AuditActorKind, AuditOutcome
from taskforge.history.domain import (
    HistoryCursor,
    HistoryFilters,
    HistoryItem,
    HistoryRecordType,
    decode_cursor,
    encode_cursor,
    filter_fingerprint,
)
from taskforge.history.service import (
    HistoryNotFound,
    HistoryService,
    HistoryUnavailable,
)
from taskforge.identity.authorization import AuthorizationContext, Permission

DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 100


class _ClosedResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AuditDataResponse(_ClosedResponse):
    id: UUID
    actor_kind: str
    api_principal_id: UUID | None
    worker_identity_id: UUID | None
    worker_session_id: UUID | None
    system_component: str | None
    action: str
    outcome: str
    reason_code: str | None
    resource_type: str
    resource_id: UUID | None
    diagnostic_provenance: dict[str, object]


class ExecutionDataResponse(_ClosedResponse):
    id: UUID
    workflow_run_id: UUID
    task_run_id: UUID | None
    cursor: int
    event_type: str
    payload: dict[str, object]


class ClaimDataResponse(_ClosedResponse):
    id: UUID
    task_attempt_id: UUID
    generation: int
    worker_identity_id: UUID | None
    worker_session_id: UUID | None
    event_type: str
    previous_lease_expires_at: datetime | None
    lease_expires_at: datetime


class ResultDataResponse(_ClosedResponse):
    id: UUID
    task_attempt_id: UUID
    claim_generation: int
    worker_identity_id: UUID | None
    worker_session_id: UUID | None
    actor_component: str | None
    event_type: str
    result_kind: str | None
    failure_kind: str | None


class RetryDataResponse(_ClosedResponse):
    id: UUID
    task_run_id: UUID
    event_type: str
    actor_component: str | None
    failed_attempt_number: int | None
    retry_attempt_number: int | None
    next_eligible_at: datetime | None
    decision_reason: str | None


class CancellationDataResponse(_ClosedResponse):
    workflow_run_id: UUID
    requested_by_principal_id: UUID
    reason_present: bool


class ReplayDataResponse(_ClosedResponse):
    workflow_run_id: UUID
    source_workflow_run_id: UUID
    mode: str
    requested_scope: dict[str, object]


class DeadLetterCreatedDataResponse(_ClosedResponse):
    id: UUID
    task_run_id: UUID
    source_task_attempt_id: UUID
    reason: str


class DeadLetterActionDataResponse(_ClosedResponse):
    id: UUID
    dead_letter_item_id: UUID
    operator_principal_id: UUID
    action_type: str
    previous_status: str
    new_status: str
    reason_present: bool


class DeadLetterRedriveDataResponse(_ClosedResponse):
    id: UUID
    dead_letter_item_id: UUID
    requested_by_principal_id: UUID
    target_workflow_run_id: UUID
    reason_present: bool


class HeartbeatDataResponse(_ClosedResponse):
    worker_session_id: UUID
    worker_identity_id: UUID | None
    sequence: int
    accepting_work: bool


HistoryDataResponse = (
    AuditDataResponse
    | ExecutionDataResponse
    | ClaimDataResponse
    | ResultDataResponse
    | RetryDataResponse
    | CancellationDataResponse
    | ReplayDataResponse
    | DeadLetterCreatedDataResponse
    | DeadLetterActionDataResponse
    | DeadLetterRedriveDataResponse
    | HeartbeatDataResponse
)


class HistoryItemResponse(BaseModel):
    record_type: str
    occurred_at: datetime
    correlation_id: str | None
    data: HistoryDataResponse


class HistoryPageMetadataResponse(BaseModel):
    limit: int
    next_cursor: str | None


class HistoryResponse(BaseModel):
    items: list[HistoryItemResponse]
    page: HistoryPageMetadataResponse


class HistoryRuntimeProtocol(Protocol):
    history_service: HistoryService


router = APIRouter(tags=["history"])


class HistoryQueryParameters(BaseModel):
    limit: int
    cursor: str | None
    filters: HistoryFilters


async def history_query_parameters(
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = DEFAULT_PAGE_SIZE,
    cursor: Annotated[str | None, Query(max_length=2048)] = None,
    record_type: HistoryRecordType | None = None,
    resource_type: Annotated[str | None, Query(min_length=1, max_length=64)] = None,
    target_resource_id: Annotated[UUID | None, Query(alias="resource_id")] = None,
    action: AuditAction | None = None,
    outcome: AuditOutcome | None = None,
    actor_kind: AuditActorKind | None = None,
    actor_id: UUID | None = None,
    system_component: Annotated[str | None, Query(min_length=1, max_length=32)] = None,
    correlation_id: Annotated[str | None, Query(min_length=1, max_length=128)] = None,
    reason_code: Annotated[str | None, Query(min_length=1, max_length=128)] = None,
    occurred_from: datetime | None = None,
    occurred_to: datetime | None = None,
) -> HistoryQueryParameters:
    try:
        filters = HistoryFilters(
            record_type,
            resource_type,
            target_resource_id,
            action,
            outcome,
            actor_kind,
            actor_id,
            system_component,
            correlation_id,
            reason_code,
            occurred_from,
            occurred_to,
        )
    except ValueError as error:
        raise HTTPException(status_code=422) from error
    return HistoryQueryParameters(limit=limit, cursor=cursor, filters=filters)


@router.get("/api/v1/audit-records", response_model=HistoryResponse)
async def list_audit_records(
    request: Request,
    context: Annotated[
        AuthorizationContext, Depends(require_permission(Permission.ADMINISTER))
    ],
    query: Annotated[HistoryQueryParameters, Depends(history_query_parameters)],
) -> HistoryResponse | Response:
    return await _list(request, context, "audit", None, query)


@router.get("/api/v1/workflows/{resource_id}/history", response_model=HistoryResponse)
async def list_workflow_history(
    resource_id: UUID,
    request: Request,
    context: Annotated[
        AuthorizationContext, Depends(require_permission(Permission.VIEW))
    ],
    query: Annotated[HistoryQueryParameters, Depends(history_query_parameters)],
) -> HistoryResponse | Response:
    return await _list(request, context, "workflow", resource_id, query)


@router.get(
    "/api/v1/workflow-runs/{resource_id}/history", response_model=HistoryResponse
)
async def list_run_history(
    resource_id: UUID,
    request: Request,
    context: Annotated[
        AuthorizationContext, Depends(require_permission(Permission.VIEW))
    ],
    query: Annotated[HistoryQueryParameters, Depends(history_query_parameters)],
) -> HistoryResponse | Response:
    """Inspect execution/control-plane history; this is not state reconstruction."""
    return await _list(request, context, "run", resource_id, query)


@router.get("/api/v1/task-runs/{resource_id}/history", response_model=HistoryResponse)
async def list_task_history(
    resource_id: UUID,
    request: Request,
    context: Annotated[
        AuthorizationContext, Depends(require_permission(Permission.VIEW))
    ],
    query: Annotated[HistoryQueryParameters, Depends(history_query_parameters)],
) -> HistoryResponse | Response:
    return await _list(request, context, "task", resource_id, query)


@router.get("/api/v1/workers/{resource_id}/history", response_model=HistoryResponse)
async def list_worker_history(
    resource_id: UUID,
    request: Request,
    context: Annotated[
        AuthorizationContext, Depends(require_permission(Permission.VIEW))
    ],
    query: Annotated[HistoryQueryParameters, Depends(history_query_parameters)],
) -> HistoryResponse | Response:
    return await _list(request, context, "worker", resource_id, query)


@router.get(
    "/api/v1/dead-letters/{resource_id}/history", response_model=HistoryResponse
)
async def list_dead_letter_history(
    resource_id: UUID,
    request: Request,
    context: Annotated[
        AuthorizationContext, Depends(require_permission(Permission.VIEW))
    ],
    query: Annotated[HistoryQueryParameters, Depends(history_query_parameters)],
) -> HistoryResponse | Response:
    return await _list(request, context, "dead_letter", resource_id, query)


async def _list(
    request: Request,
    context: AuthorizationContext,
    scope_type: str,
    scope_id: UUID | None,
    query: HistoryQueryParameters,
) -> HistoryResponse | Response:
    fingerprint = filter_fingerprint(query.filters.normalized())
    try:
        cursor = (
            decode_cursor(
                query.cursor,
                scope_type=scope_type,
                scope_id=scope_id,
                fingerprint=fingerprint,
            )
            if query.cursor
            else None
        )
    except ValueError:
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
    try:
        page = await cast(
            HistoryRuntimeProtocol, request.app.state.authentication
        ).history_service.list(
            scope_type,
            scope_id,
            context.owner_filter_for(
                Permission.ADMINISTER if scope_type == "audit" else Permission.VIEW
            ),
            limit=query.limit,
            cursor=cursor,
            filters=query.filters,
        )
    except HistoryNotFound as error:
        raise HTTPException(status_code=404) from error
    except HistoryUnavailable as error:
        raise HTTPException(status_code=503) from error
    next_cursor = page.next_cursor
    if next_cursor is not None:
        next_cursor = HistoryCursor(
            next_cursor.scope_type,
            next_cursor.scope_id,
            fingerprint,
            next_cursor.occurred_at,
            next_cursor.record_type,
            next_cursor.source_rank,
            next_cursor.source_key,
        )
    return HistoryResponse(
        items=[_response(item) for item in page.items],
        page=HistoryPageMetadataResponse(
            limit=query.limit,
            next_cursor=encode_cursor(next_cursor) if next_cursor else None,
        ),
    )


def _response(item: HistoryItem) -> HistoryItemResponse:
    return HistoryItemResponse(
        record_type=item.record_type.value,
        occurred_at=item.occurred_at,
        correlation_id=item.correlation_id,
        data=cast(HistoryDataResponse, item.data),
    )
