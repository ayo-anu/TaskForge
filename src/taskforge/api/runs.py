"""Authorized workflow-run creation and read-only inspection routes."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Protocol, cast
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response, status
from pydantic import BaseModel, ConfigDict, Field, StrictInt

from taskforge.api.authorization import require_permission
from taskforge.api.errors import ErrorDetail, ErrorResponse, error_response
from taskforge.identity.authorization import AuthorizationContext, Permission
from taskforge.runs.domain import (
    CreatedWorkflowRun,
    ExplicitWorkflowVersion,
    InspectedTaskRun,
    InspectedWorkflowRun,
    InvalidWorkflowRunIdempotencyKey,
    InvalidWorkflowRunInput,
    LatestWorkflowVersion,
    TaskRunStatus,
    WorkflowRunIdempotencyConflict,
    WorkflowRunStatus,
    WorkflowRunTargetUnavailable,
    create_workflow_run_input,
)
from taskforge.runs.service import (
    TaskRunNotFound,
    WorkflowRunNotFound,
    WorkflowRunPersistenceConflict,
    WorkflowRunService,
    WorkflowRunServiceUnavailable,
    WorkflowRunTargetNotFound,
    WorkflowVersionUnavailable,
)
from taskforge.workflows.task_types import JSONValue


class StartWorkflowRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version_number: Annotated[StrictInt, Field(gt=0)] | None = None
    payload: dict[str, JSONValue] = Field(default_factory=dict)
    input_references: dict[str, JSONValue] = Field(default_factory=dict)


class StartedWorkflowRunResponse(BaseModel):
    id: UUID
    workflow_definition_id: UUID
    workflow_version_id: UUID
    version_number: int
    requested_by_principal_id: UUID
    status: WorkflowRunStatus
    created_at: datetime


class WorkflowRunResponse(StartedWorkflowRunResponse):
    updated_at: datetime


class TaskRunResponse(BaseModel):
    id: UUID
    workflow_run_id: UUID
    workflow_version_id: UUID
    step_identifier: str
    status: TaskRunStatus
    created_at: datetime
    updated_at: datetime


class TaskRunListResponse(BaseModel):
    items: list[TaskRunResponse]


class WorkflowRunRuntimeProtocol(Protocol):
    workflow_run_service: WorkflowRunService


router = APIRouter(tags=["workflow-runs"])

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
            )
        else:
            created = await service.create_idempotent_run(
                workflow_id,
                owner_principal_id=context.principal_id,
                requested_by_principal_id=context.principal_id,
                selection=selection,
                input_snapshot=input_snapshot,
                idempotency_key=idempotency_key,
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
    except WorkflowRunServiceUnavailable as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE) from error
    return _task_run_response(task)


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
    )


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
