"""Authorized current task-claim inspection."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Protocol, cast
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel

from taskforge.api.authorization import require_permission
from taskforge.api.errors import ErrorResponse
from taskforge.claims.domain import TaskClaimLeaseStatus
from taskforge.claims.persistence_ports import (
    TaskClaimInspectionInvariantViolation,
    TaskClaimInspectionNotFound,
    TaskClaimInspectionPersistenceUnavailable,
)
from taskforge.claims.service import TaskClaimInspectionService
from taskforge.identity.authorization import AuthorizationContext, Permission
from taskforge.runs.domain import TaskRunStatus


class CurrentTaskClaimResponse(BaseModel):
    task_attempt_id: UUID
    task_run_id: UUID
    workflow_run_id: UUID
    attempt_number: int
    generation: int
    worker_identity_id: UUID
    worker_session_id: UUID
    acquired_at: datetime
    lease_expires_at: datetime
    observed_at: datetime
    lease_status: TaskClaimLeaseStatus
    task_status: TaskRunStatus


class TaskClaimInspectionRuntimeProtocol(Protocol):
    task_claim_inspection_service: TaskClaimInspectionService


router = APIRouter(tags=["task-claims"])


@router.get(
    "/api/v1/task-attempts/{task_attempt_id}/claim",
    response_model=CurrentTaskClaimResponse,
    responses={
        status.HTTP_401_UNAUTHORIZED: {"model": ErrorResponse},
        status.HTTP_403_FORBIDDEN: {"model": ErrorResponse},
        status.HTTP_404_NOT_FOUND: {"model": ErrorResponse},
        status.HTTP_422_UNPROCESSABLE_CONTENT: {"model": ErrorResponse},
        status.HTTP_503_SERVICE_UNAVAILABLE: {"model": ErrorResponse},
    },
)
async def inspect_current_task_claim(
    task_attempt_id: UUID,
    request: Request,
    context: Annotated[
        AuthorizationContext,
        Depends(require_permission(Permission.VIEW)),
    ],
) -> CurrentTaskClaimResponse:
    try:
        claim = await _runtime(request).task_claim_inspection_service.get_current_claim(
            task_attempt_id,
            context.owner_filter_for(Permission.VIEW),
        )
    except TaskClaimInspectionNotFound as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND) from error
    except TaskClaimInspectionPersistenceUnavailable as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE) from error
    except TaskClaimInspectionInvariantViolation as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE) from error
    return CurrentTaskClaimResponse(**claim.__dict__)


def _runtime(request: Request) -> TaskClaimInspectionRuntimeProtocol:
    return cast(TaskClaimInspectionRuntimeProtocol, request.app.state.authentication)
