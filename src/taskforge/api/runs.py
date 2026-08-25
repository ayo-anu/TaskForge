"""Authorized workflow-run creation, cancellation, and inspection routes."""

from __future__ import annotations

import base64
import binascii
import json
from datetime import UTC, datetime
from typing import Annotated, Any, Literal, Protocol, cast
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
from pydantic import BaseModel, ConfigDict, Field, StrictInt

from taskforge.api.authorization import require_permission
from taskforge.api.errors import ErrorDetail, ErrorResponse, error_response
from taskforge.identity.authorization import AuthorizationContext, Permission
from taskforge.retries.domain import (
    InspectedRetryEvent,
    InspectedRetryEventPage,
    RetryEventCursor,
    RetryEventType,
    RetryNotScheduledReason,
)
from taskforge.runs.domain import (
    CreatedFailedSubgraphWorkflowReplay,
    CreatedFullWorkflowReplay,
    CreatedWorkflowRun,
    ExplicitWorkflowVersion,
    FailedSubgraphReplaySelectionInvalid,
    InspectedTaskRun,
    InspectedWorkflowRun,
    InvalidFailedSubgraphReplayRequest,
    InvalidWorkflowRunCancellationIdempotencyKey,
    InvalidWorkflowRunCancellationReason,
    InvalidWorkflowRunIdempotencyKey,
    InvalidWorkflowRunInput,
    LatestWorkflowVersion,
    RunFailureReason,
    TaskRunStatus,
    WorkflowReplayIdempotencyConflict,
    WorkflowReplayMode,
    WorkflowRunCancellationCaveat,
    WorkflowRunCancellationIdempotencyConflict,
    WorkflowRunCancellationOutcome,
    WorkflowRunCancellationResult,
    WorkflowRunIdempotencyConflict,
    WorkflowRunReplayNotEligible,
    WorkflowRunStatus,
    WorkflowRunTargetUnavailable,
    create_workflow_run_input,
)
from taskforge.runs.service import (
    TaskRunNotFound,
    WorkflowRunCancellationInvariantError,
    WorkflowRunInspectionInvariantError,
    WorkflowRunNotFound,
    WorkflowRunPersistenceConflict,
    WorkflowRunReplayInvariantError,
    WorkflowRunService,
    WorkflowRunServiceUnavailable,
    WorkflowRunTargetNotFound,
    WorkflowVersionUnavailable,
)
from taskforge.worker.results import TaskExecutionFailureKind
from taskforge.workflows.task_types import JSONValue


class StartWorkflowRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version_number: Annotated[StrictInt, Field(gt=0)] | None = None
    payload: dict[str, JSONValue] = Field(default_factory=dict)
    input_references: dict[str, JSONValue] = Field(default_factory=dict)


class CancelWorkflowRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: str | None = None


class FullWorkflowReplayRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal[WorkflowReplayMode.FULL]


class FailedSubgraphWorkflowReplayRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal[WorkflowReplayMode.FAILED_SUBGRAPH]
    failed_step_identifiers: Annotated[list[str], Field(min_length=1, max_length=256)]


WorkflowReplayRequest = Annotated[
    FullWorkflowReplayRequest | FailedSubgraphWorkflowReplayRequest,
    Field(discriminator="mode"),
]


class CancelWorkflowRunResponse(BaseModel):
    workflow_run_id: UUID
    outcome: WorkflowRunCancellationOutcome
    status: WorkflowRunStatus
    requested_by_principal_id: UUID | None = None
    reason: str | None = None
    requested_at: datetime | None = None


class StartedWorkflowRunResponse(BaseModel):
    id: UUID
    workflow_definition_id: UUID
    workflow_version_id: UUID
    version_number: int
    requested_by_principal_id: UUID
    status: WorkflowRunStatus
    created_at: datetime


class WorkflowReplayResponse(StartedWorkflowRunResponse):
    source_workflow_run_id: UUID
    mode: WorkflowReplayMode
    failed_step_identifiers: tuple[str, ...] | None = None


class WorkflowRunResponse(StartedWorkflowRunResponse):
    updated_at: datetime
    failure_reason: RunFailureReason | None
    cancellation: WorkflowRunCancellationInspectionResponse | None


class WorkflowRunCancellationInspectionResponse(BaseModel):
    requested_by_principal_id: UUID
    reason: str | None
    requested_at: datetime
    recovered_cancellation_count: int
    caveats: tuple[WorkflowRunCancellationCaveat, ...]


class TaskRunResponse(BaseModel):
    id: UUID
    workflow_run_id: UUID
    workflow_version_id: UUID
    step_identifier: str
    status: TaskRunStatus
    created_at: datetime
    updated_at: datetime
    failure_reason: RunFailureReason | None
    attempt_count: int
    retry_attempt_count: int
    maximum_attempts: int | None
    retry_eligible_at: datetime | None
    latest_failure_kind: TaskExecutionFailureKind | None


class TaskRunListResponse(BaseModel):
    items: list[TaskRunResponse]


class RetryEventResponse(BaseModel):
    id: UUID
    workflow_run_id: UUID
    task_run_id: UUID
    event_type: RetryEventType
    failed_attempt_id: UUID | None
    failed_attempt_number: int | None
    retry_attempt_id: UUID | None
    retry_attempt_number: int | None
    next_eligible_at: datetime | None
    decision_reason: RetryNotScheduledReason | None
    failure_kind: TaskExecutionFailureKind | None
    occurred_at: datetime


class RetryEventPageMetadataResponse(BaseModel):
    limit: int
    next_cursor: str | None


class RetryEventHistoryResponse(BaseModel):
    items: list[RetryEventResponse]
    page: RetryEventPageMetadataResponse


class WorkflowRunRuntimeProtocol(Protocol):
    workflow_run_service: WorkflowRunService


router = APIRouter(tags=["workflow-runs"])

DEFAULT_RETRY_EVENT_PAGE_SIZE = 50
MAX_RETRY_EVENT_PAGE_SIZE = 100
MAX_RETRY_EVENT_CURSOR_LENGTH = 768
MAX_RETRY_EVENT_CURSOR_BYTES = 512
_RETRY_EVENT_CURSOR_VERSION = 1

COMMON_RESPONSES: dict[int | str, dict[str, Any]] = {
    status.HTTP_401_UNAUTHORIZED: {"model": ErrorResponse},
    status.HTTP_403_FORBIDDEN: {"model": ErrorResponse},
    status.HTTP_404_NOT_FOUND: {"model": ErrorResponse},
    status.HTTP_409_CONFLICT: {"model": ErrorResponse},
    status.HTTP_422_UNPROCESSABLE_CONTENT: {"model": ErrorResponse},
    status.HTTP_503_SERVICE_UNAVAILABLE: {"model": ErrorResponse},
}


@router.post(
    "/api/v1/workflows/{workflow_id}/runs",
    response_model=StartedWorkflowRunResponse,
    status_code=status.HTTP_201_CREATED,
    responses=COMMON_RESPONSES,
)
async def start_workflow_run(
    workflow_id: UUID,
    body: StartWorkflowRunRequest,
    request: Request,
    response: Response,
    context: Annotated[
        AuthorizationContext,
        Depends(require_permission(Permission.OPERATE_WORKFLOW)),
    ],
    idempotency_key: Annotated[
        str | None,
        Header(alias="Idempotency-Key", max_length=128),
    ] = None,
) -> StartedWorkflowRunResponse | Response:
    selection = (
        LatestWorkflowVersion()
        if body.version_number is None
        else ExplicitWorkflowVersion(body.version_number)
    )
    try:
        input_snapshot = create_workflow_run_input(
            body.payload,
            body.input_references,
        )
        service = _runtime(request).workflow_run_service
        if idempotency_key is None:
            created = await service.create_run(
                workflow_id,
                owner_principal_id=context.principal_id,
                requested_by_principal_id=context.principal_id,
                selection=selection,
                input_snapshot=input_snapshot,
                correlation_id=cast(UUID, request.state.request_id),
            )
        else:
            created = await service.create_idempotent_run(
                workflow_id,
                owner_principal_id=context.principal_id,
                requested_by_principal_id=context.principal_id,
                selection=selection,
                input_snapshot=input_snapshot,
                idempotency_key=idempotency_key,
                correlation_id=cast(UUID, request.state.request_id),
            )
    except InvalidWorkflowRunInput as error:
        return _input_validation_error(request, error)
    except InvalidWorkflowRunIdempotencyKey:
        return _idempotency_key_validation_error(request)
    except (WorkflowRunTargetNotFound, WorkflowVersionUnavailable) as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND) from error
    except WorkflowRunTargetUnavailable as error:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT) from error
    except WorkflowRunIdempotencyConflict:
        return error_response(
            request,
            status_code=status.HTTP_409_CONFLICT,
            code="idempotency_conflict",
            message="The idempotency key conflicts with an earlier request.",
        )
    except WorkflowRunPersistenceConflict as error:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT) from error
    except WorkflowRunServiceUnavailable as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE) from error
    response.headers["Location"] = f"/api/v1/workflow-runs/{created.id}"
    return _started_run_response(created)


@router.post(
    "/api/v1/workflow-runs/{source_workflow_run_id}/replay",
    response_model=WorkflowReplayResponse,
    status_code=status.HTTP_201_CREATED,
    responses=COMMON_RESPONSES,
)
async def replay_workflow_run(
    source_workflow_run_id: UUID,
    body: WorkflowReplayRequest,
    request: Request,
    response: Response,
    context: Annotated[
        AuthorizationContext,
        Depends(require_permission(Permission.OPERATE_WORKFLOW)),
    ],
    idempotency_key: Annotated[
        str | None,
        Header(alias="Idempotency-Key", max_length=128),
    ] = None,
) -> WorkflowReplayResponse | Response:
    service = _runtime(request).workflow_run_service
    owner_filter = context.owner_filter_for(Permission.OPERATE_WORKFLOW)
    correlation_id = cast(UUID, request.state.request_id)
    replay: CreatedFullWorkflowReplay | CreatedFailedSubgraphWorkflowReplay
    try:
        if isinstance(body, FullWorkflowReplayRequest):
            if idempotency_key is None:
                replay = await service.create_full_replay(
                    source_workflow_run_id,
                    owner_filter,
                    requested_by_principal_id=context.principal_id,
                    correlation_id=correlation_id,
                )
            else:
                replay = await service.create_idempotent_full_replay(
                    source_workflow_run_id,
                    owner_filter,
                    requested_by_principal_id=context.principal_id,
                    idempotency_key=idempotency_key,
                    correlation_id=correlation_id,
                )
        elif idempotency_key is None:
            replay = await service.create_failed_subgraph_replay(
                source_workflow_run_id,
                owner_filter,
                requested_by_principal_id=context.principal_id,
                failed_step_identifiers=body.failed_step_identifiers,
                correlation_id=correlation_id,
            )
        else:
            replay = await service.create_idempotent_failed_subgraph_replay(
                source_workflow_run_id,
                owner_filter,
                requested_by_principal_id=context.principal_id,
                failed_step_identifiers=body.failed_step_identifiers,
                idempotency_key=idempotency_key,
                correlation_id=correlation_id,
            )
    except InvalidWorkflowRunIdempotencyKey:
        return _idempotency_key_validation_error(request)
    except InvalidFailedSubgraphReplayRequest:
        return _failed_subgraph_replay_validation_error(request)
    except WorkflowRunNotFound as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND) from error
    except WorkflowRunReplayNotEligible:
        return _replay_conflict_error(
            request,
            code="workflow_run_not_replayable",
            message="The workflow run is not eligible for the requested replay.",
        )
    except FailedSubgraphReplaySelectionInvalid:
        return _replay_conflict_error(
            request,
            code="failed_subgraph_replay_invalid",
            message="The requested failed-task replay is not dependency-safe.",
        )
    except WorkflowReplayIdempotencyConflict:
        return _replay_conflict_error(
            request,
            code="idempotency_conflict",
            message="The idempotency key conflicts with an earlier request.",
        )
    except WorkflowRunReplayInvariantError as error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        ) from error
    except WorkflowRunPersistenceConflict as error:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT) from error
    except WorkflowRunServiceUnavailable as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE) from error
    response.headers["Location"] = f"/api/v1/workflow-runs/{replay.run.id}"
    return _workflow_replay_response(replay)


@router.post(
    "/api/v1/workflow-runs/{run_id}/cancel",
    response_model=CancelWorkflowRunResponse,
    responses=COMMON_RESPONSES,
)
async def cancel_workflow_run(
    run_id: UUID,
    body: CancelWorkflowRunRequest,
    request: Request,
    context: Annotated[
        AuthorizationContext,
        Depends(require_permission(Permission.OPERATE_WORKFLOW)),
    ],
    idempotency_key: Annotated[
        str | None,
        Header(alias="Idempotency-Key", max_length=128),
    ] = None,
) -> CancelWorkflowRunResponse | Response:
    try:
        result = await _runtime(request).workflow_run_service.cancel_run(
            run_id,
            context.owner_filter_for(Permission.OPERATE_WORKFLOW),
            requested_by_principal_id=context.principal_id,
            idempotency_key=idempotency_key,
            reason=body.reason,
            correlation_id=cast(UUID, request.state.request_id),
        )
    except InvalidWorkflowRunCancellationIdempotencyKey:
        return _cancellation_idempotency_key_validation_error(request)
    except InvalidWorkflowRunCancellationReason:
        return _cancellation_reason_validation_error(request)
    except WorkflowRunNotFound as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND) from error
    except WorkflowRunCancellationIdempotencyConflict:
        return error_response(
            request,
            status_code=status.HTTP_409_CONFLICT,
            code="idempotency_conflict",
            message="The idempotency key conflicts with an earlier request.",
        )
    except WorkflowRunCancellationInvariantError as error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        ) from error
    except WorkflowRunServiceUnavailable as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE) from error
    if result.outcome is WorkflowRunCancellationOutcome.TERMINAL_STATE_WON:
        return error_response(
            request,
            status_code=status.HTTP_409_CONFLICT,
            code="workflow_run_not_cancellable",
            message="The workflow run is already terminal.",
        )
    return _cancellation_response(result)


@router.get(
    "/api/v1/workflow-runs/{run_id}",
    response_model=WorkflowRunResponse,
    responses=COMMON_RESPONSES,
)
async def get_workflow_run(
    run_id: UUID,
    request: Request,
    context: Annotated[
        AuthorizationContext,
        Depends(require_permission(Permission.VIEW)),
    ],
) -> WorkflowRunResponse:
    try:
        run = await _runtime(request).workflow_run_service.get_run(
            run_id,
            owner_principal_id=context.principal_id,
        )
    except WorkflowRunNotFound as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND) from error
    except WorkflowRunInspectionInvariantError as error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        ) from error
    except WorkflowRunServiceUnavailable as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE) from error
    return _workflow_run_response(run)


@router.get(
    "/api/v1/workflow-runs/{run_id}/tasks",
    response_model=TaskRunListResponse,
    responses=COMMON_RESPONSES,
)
async def list_workflow_run_tasks(
    run_id: UUID,
    request: Request,
    context: Annotated[
        AuthorizationContext,
        Depends(require_permission(Permission.VIEW)),
    ],
) -> TaskRunListResponse:
    try:
        tasks = await _runtime(request).workflow_run_service.list_task_runs(
            run_id,
            owner_principal_id=context.principal_id,
        )
    except WorkflowRunNotFound as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND) from error
    except WorkflowRunInspectionInvariantError as error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        ) from error
    except WorkflowRunServiceUnavailable as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE) from error
    return TaskRunListResponse(items=[_task_run_response(task) for task in tasks])


@router.get(
    "/api/v1/task-runs/{task_run_id}",
    response_model=TaskRunResponse,
    responses=COMMON_RESPONSES,
)
async def get_task_run(
    task_run_id: UUID,
    request: Request,
    context: Annotated[
        AuthorizationContext,
        Depends(require_permission(Permission.VIEW)),
    ],
) -> TaskRunResponse:
    try:
        task = await _runtime(request).workflow_run_service.get_task_run(
            task_run_id,
            owner_principal_id=context.principal_id,
        )
    except TaskRunNotFound as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND) from error
    except WorkflowRunInspectionInvariantError as error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        ) from error
    except WorkflowRunServiceUnavailable as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE) from error
    return _task_run_response(task)


@router.get(
    "/api/v1/task-runs/{task_run_id}/retry-events",
    response_model=RetryEventHistoryResponse,
    responses=COMMON_RESPONSES,
)
async def list_task_retry_events(
    task_run_id: UUID,
    request: Request,
    context: Annotated[
        AuthorizationContext,
        Depends(require_permission(Permission.VIEW)),
    ],
    limit: Annotated[int, Query(ge=1, le=MAX_RETRY_EVENT_PAGE_SIZE)] = (
        DEFAULT_RETRY_EVENT_PAGE_SIZE
    ),
    cursor: Annotated[
        str | None, Query(max_length=MAX_RETRY_EVENT_CURSOR_LENGTH)
    ] = None,
) -> RetryEventHistoryResponse | Response:
    try:
        decoded = _decode_retry_event_cursor(cursor, task_run_id) if cursor else None
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
        page = await _runtime(request).workflow_run_service.list_retry_events(
            task_run_id,
            owner_principal_id=context.principal_id,
            limit=limit,
            cursor=decoded,
        )
    except TaskRunNotFound as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND) from error
    except WorkflowRunInspectionInvariantError as error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        ) from error
    except WorkflowRunServiceUnavailable as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE) from error
    return _retry_event_history_response(page, limit)


def _runtime(request: Request) -> WorkflowRunRuntimeProtocol:
    return cast(WorkflowRunRuntimeProtocol, request.app.state.authentication)


def _started_run_response(run: CreatedWorkflowRun) -> StartedWorkflowRunResponse:
    return StartedWorkflowRunResponse(
        id=run.id,
        workflow_definition_id=run.workflow_definition_id,
        workflow_version_id=run.workflow_version_id,
        version_number=run.version_number,
        requested_by_principal_id=run.requested_by_principal_id,
        status=run.status,
        created_at=run.created_at,
    )


def _workflow_replay_response(
    replay: CreatedFullWorkflowReplay | CreatedFailedSubgraphWorkflowReplay,
) -> WorkflowReplayResponse:
    run = replay.run
    failed_step_identifiers = (
        replay.canonical_failed_step_identifiers
        if isinstance(replay, CreatedFailedSubgraphWorkflowReplay)
        else None
    )
    return WorkflowReplayResponse(
        id=run.id,
        workflow_definition_id=run.workflow_definition_id,
        workflow_version_id=run.workflow_version_id,
        version_number=run.version_number,
        requested_by_principal_id=run.requested_by_principal_id,
        status=run.status,
        created_at=run.created_at,
        source_workflow_run_id=replay.source_workflow_run_id,
        mode=replay.mode,
        failed_step_identifiers=failed_step_identifiers,
    )


def _cancellation_response(
    result: WorkflowRunCancellationResult,
) -> CancelWorkflowRunResponse:
    accepted = result.accepted_request
    return CancelWorkflowRunResponse(
        workflow_run_id=result.workflow_run_id,
        outcome=result.outcome,
        status=result.status,
        requested_by_principal_id=(
            accepted.requested_by_principal_id if accepted is not None else None
        ),
        reason=accepted.reason if accepted is not None else None,
        requested_at=accepted.requested_at if accepted is not None else None,
    )


def _workflow_run_response(run: InspectedWorkflowRun) -> WorkflowRunResponse:
    return WorkflowRunResponse(
        id=run.id,
        workflow_definition_id=run.workflow_definition_id,
        workflow_version_id=run.workflow_version_id,
        version_number=run.version_number,
        requested_by_principal_id=run.requested_by_principal_id,
        status=run.status,
        created_at=run.created_at,
        updated_at=run.updated_at,
        failure_reason=run.failure_reason,
        cancellation=(
            WorkflowRunCancellationInspectionResponse(
                requested_by_principal_id=run.cancellation.requested_by_principal_id,
                reason=run.cancellation.reason,
                requested_at=run.cancellation.requested_at,
                recovered_cancellation_count=(
                    run.cancellation.recovered_cancellation_count
                ),
                caveats=run.cancellation.caveats,
            )
            if run.cancellation is not None
            else None
        ),
    )


def _task_run_response(task: InspectedTaskRun) -> TaskRunResponse:
    return TaskRunResponse(
        id=task.id,
        workflow_run_id=task.workflow_run_id,
        workflow_version_id=task.workflow_version_id,
        step_identifier=task.step_identifier,
        status=task.status,
        created_at=task.created_at,
        updated_at=task.updated_at,
        failure_reason=task.failure_reason,
        attempt_count=task.attempt_count,
        retry_attempt_count=task.retry_attempt_count,
        maximum_attempts=task.maximum_attempts,
        retry_eligible_at=task.retry_eligible_at,
        latest_failure_kind=task.latest_failure_kind,
    )


def _retry_event_history_response(
    page: InspectedRetryEventPage, limit: int
) -> RetryEventHistoryResponse:
    return RetryEventHistoryResponse(
        items=[_retry_event_response(item) for item in page.items],
        page=RetryEventPageMetadataResponse(
            limit=limit,
            next_cursor=(
                _encode_retry_event_cursor(page.next_cursor)
                if page.next_cursor is not None
                else None
            ),
        ),
    )


def _retry_event_response(event: InspectedRetryEvent) -> RetryEventResponse:
    return RetryEventResponse(
        id=event.id,
        workflow_run_id=event.workflow_run_id,
        task_run_id=event.task_run_id,
        event_type=event.event_type,
        failed_attempt_id=event.failed_attempt_id,
        failed_attempt_number=event.failed_attempt_number,
        retry_attempt_id=event.retry_attempt_id,
        retry_attempt_number=event.retry_attempt_number,
        next_eligible_at=event.next_eligible_at,
        decision_reason=event.decision_reason,
        failure_kind=event.failure_kind,
        occurred_at=event.occurred_at,
    )


def _encode_retry_event_cursor(cursor: RetryEventCursor) -> str:
    payload = json.dumps(
        {
            "v": _RETRY_EVENT_CURSOR_VERSION,
            "task_run_id": str(cursor.task_run_id),
            "occurred_at": cursor.occurred_at.astimezone(UTC)
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z"),
            "event_id": str(cursor.event_id),
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")


def _decode_retry_event_cursor(value: str, task_run_id: UUID) -> RetryEventCursor:
    if not value or len(value) > MAX_RETRY_EVENT_CURSOR_LENGTH or not value.isascii():
        raise ValueError("invalid cursor")
    try:
        decoded = base64.b64decode(
            value + "=" * (-len(value) % 4), altchars=b"-_", validate=True
        )
        if len(decoded) > MAX_RETRY_EVENT_CURSOR_BYTES:
            raise ValueError("invalid cursor")
        payload = json.loads(decoded)
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("invalid cursor") from error
    if not isinstance(payload, dict) or set(payload) != {
        "v",
        "task_run_id",
        "occurred_at",
        "event_id",
    }:
        raise ValueError("invalid cursor")
    if type(payload["v"]) is not int or payload["v"] != _RETRY_EVENT_CURSOR_VERSION:
        raise ValueError("invalid cursor")
    if not all(
        isinstance(payload[field], str)
        for field in ("task_run_id", "occurred_at", "event_id")
    ):
        raise ValueError("invalid cursor")
    try:
        parsed_task_run_id = UUID(payload["task_run_id"])
        occurred_at = datetime.fromisoformat(
            payload["occurred_at"].replace("Z", "+00:00")
        )
        cursor = RetryEventCursor(
            parsed_task_run_id, occurred_at, UUID(payload["event_id"])
        )
    except (TypeError, ValueError) as error:
        raise ValueError("invalid cursor") from error
    if cursor.task_run_id != task_run_id:
        raise ValueError("invalid cursor")
    return cursor


def _input_validation_error(
    request: Request,
    error: InvalidWorkflowRunInput,
) -> Response:
    return error_response(
        request,
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        code="validation_failed",
        message="The request is invalid.",
        details=tuple(
            ErrorDetail(
                code=issue.code,
                path=["body", *issue.path],
                message=issue.message,
            )
            for issue in error.issues
        ),
    )


def _idempotency_key_validation_error(request: Request) -> Response:
    return error_response(
        request,
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        code="validation_failed",
        message="The request is invalid.",
        details=(
            ErrorDetail(
                code="invalid_idempotency_key",
                path=["header", "Idempotency-Key"],
                message="Idempotency-Key is invalid.",
            ),
        ),
    )


def _failed_subgraph_replay_validation_error(request: Request) -> Response:
    return error_response(
        request,
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        code="validation_failed",
        message="The request is invalid.",
        details=(
            ErrorDetail(
                code="invalid_failed_step_identifiers",
                path=["body", "failed_step_identifiers"],
                message="Failed step identifiers are invalid.",
            ),
        ),
    )


def _replay_conflict_error(request: Request, *, code: str, message: str) -> Response:
    return error_response(
        request,
        status_code=status.HTTP_409_CONFLICT,
        code=code,
        message=message,
    )


def _cancellation_idempotency_key_validation_error(request: Request) -> Response:
    return error_response(
        request,
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        code="validation_failed",
        message="The request is invalid.",
        details=(
            ErrorDetail(
                code="invalid_idempotency_key",
                path=["header", "Idempotency-Key"],
                message="Idempotency-Key is invalid.",
            ),
        ),
    )


def _cancellation_reason_validation_error(request: Request) -> Response:
    return error_response(
        request,
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        code="validation_failed",
        message="The request is invalid.",
        details=(
            ErrorDetail(
                code="invalid_cancellation_reason",
                path=["body", "reason"],
                message="Cancellation reason must be nonblank and at most 2000 characters.",
            ),
        ),
    )
