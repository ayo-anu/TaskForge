"""Authenticated, owner-scoped workflow-run WebSocket handshakes."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Protocol, cast
from uuid import UUID

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status
from fastapi.security.utils import get_authorization_scheme_param
from pydantic import BaseModel

from taskforge.api.authentication import (
    AuthenticationRuntimeProtocol,
    authenticate_api_credential,
)
from taskforge.identity.authentication import (
    AuthenticationFailure,
    AuthenticationUnavailable,
)
from taskforge.identity.authorization import (
    AuthorizationDenied,
    AuthorizationService,
    AuthorizationUnavailable,
    Permission,
)
from taskforge.identity.credentials import (
    CredentialFormatError,
    PresentedCredential,
    parse_presented_credential,
)
from taskforge.runs.domain import (
    StoredWorkflowRunExecutionEvent,
    WorkflowRunExecutionEventResumeState,
)
from taskforge.runs.persistence_ports import (
    WorkflowRunExecutionEventInvariantViolation,
    WorkflowRunExecutionEventPersistenceUnavailable,
    WorkflowRunExecutionEventRepository,
)
from taskforge.runs.service import (
    WorkflowRunInspectionInvariantError,
    WorkflowRunNotFound,
    WorkflowRunService,
    WorkflowRunServiceUnavailable,
)
from taskforge.workflows.task_types import JSONValue

POLICY_REJECTION_REASON = "connection denied"
SERVICE_REJECTION_REASON = "service unavailable"
INVALID_CURSOR_REASON = "invalid cursor"
SNAPSHOT_REQUIRED_REASON = "snapshot required"
EXECUTION_EVENT_REPLAY_PAGE_SIZE = 100
MAX_POSTGRESQL_BIGINT = 9_223_372_036_854_775_807
MAX_CURSOR_TEXT_LENGTH = len(str(MAX_POSTGRESQL_BIGINT))


class _InvalidRunIdentifier(Exception):
    """Keep identifier parsing failures separate from service failures."""


class _InvalidCursorSyntax(Exception):
    """The supplied cursor is not canonical bounded decimal text."""


class _CursorAhead(Exception):
    """The supplied cursor names a future stream position."""


class _CursorExpired(Exception):
    def __init__(self, earliest_cursor: int, latest_cursor: int) -> None:
        self.earliest_cursor = earliest_cursor
        self.latest_cursor = latest_cursor


class ExecutionStreamErrorMessage(BaseModel):
    version: Literal[1] = 1
    type: Literal["error"] = "error"
    code: Literal["invalid_cursor", "cursor_ahead"]


class ExecutionStreamSnapshotRequiredMessage(BaseModel):
    version: Literal[1] = 1
    type: Literal["snapshot_required"] = "snapshot_required"
    reason: Literal["cursor_expired"] = "cursor_expired"
    workflow_run_id: UUID
    earliest_retained_cursor: int
    latest_cursor: int


class ExecutionStreamEvent(BaseModel):
    id: UUID
    workflow_run_id: UUID
    task_run_id: UUID | None
    cursor: int
    event_type: str
    occurred_at: datetime
    payload: dict[str, JSONValue]


class ExecutionStreamEventMessage(BaseModel):
    version: Literal[1] = 1
    type: Literal["execution_event"] = "execution_event"
    event: ExecutionStreamEvent


class ExecutionStreamRuntimeProtocol(AuthenticationRuntimeProtocol, Protocol):
    authorization_service: AuthorizationService
    workflow_run_service: WorkflowRunService
    workflow_run_execution_event_repository: WorkflowRunExecutionEventRepository


router = APIRouter(tags=["workflow-run-stream"])


@router.websocket("/api/v1/workflow-runs/{raw_run_id}/stream")
async def workflow_run_execution_stream(
    websocket: WebSocket,
    raw_run_id: str,
    cursor: str | None = None,
) -> None:
    """Accept only API principals allowed to inspect the requested owned run."""
    runtime = cast(ExecutionStreamRuntimeProtocol, websocket.app.state.authentication)
    try:
        credential = _bearer_credential(websocket.headers.get("Authorization"))
        identity = await authenticate_api_credential(runtime, credential)
        context = await runtime.authorization_service.context_for(identity)
        context.require(Permission.VIEW)
        run_id = _parse_run_id(raw_run_id)
        await runtime.workflow_run_service.get_run(
            run_id,
            owner_principal_id=context.principal_id,
        )
    except (
        CredentialFormatError,
        AuthenticationFailure,
        AuthorizationDenied,
        WorkflowRunNotFound,
        _InvalidRunIdentifier,
    ):
        await websocket.close(
            code=status.WS_1008_POLICY_VIOLATION,
            reason=POLICY_REJECTION_REASON,
        )
        return
    except (
        AuthenticationUnavailable,
        AuthorizationUnavailable,
        WorkflowRunInspectionInvariantError,
        WorkflowRunServiceUnavailable,
    ):
        await websocket.close(
            code=status.WS_1011_INTERNAL_ERROR,
            reason=SERVICE_REJECTION_REASON,
        )
        return

    try:
        requested_cursor = _parse_cursor(cursor)
    except _InvalidCursorSyntax:
        await websocket.accept()
        await _send_then_close(
            websocket,
            ExecutionStreamErrorMessage(code="invalid_cursor"),
            reason=INVALID_CURSOR_REASON,
        )
        return

    try:
        resume_state = (
            await runtime.workflow_run_execution_event_repository.inspect_resume_cursor(
                run_id, requested_cursor
            )
        )
        last_delivered_cursor = _resume_position(resume_state)
    except _CursorAhead:
        await websocket.accept()
        await _send_then_close(
            websocket,
            ExecutionStreamErrorMessage(code="cursor_ahead"),
            reason=INVALID_CURSOR_REASON,
        )
        return
    except _CursorExpired as error:
        await websocket.accept()
        await _send_then_close(
            websocket,
            ExecutionStreamSnapshotRequiredMessage(
                workflow_run_id=run_id,
                earliest_retained_cursor=error.earliest_cursor,
                latest_cursor=error.latest_cursor,
            ),
            reason=SNAPSHOT_REQUIRED_REASON,
        )
        return
    except (
        WorkflowRunExecutionEventInvariantViolation,
        WorkflowRunExecutionEventPersistenceUnavailable,
    ):
        await websocket.close(
            code=status.WS_1011_INTERNAL_ERROR,
            reason=SERVICE_REJECTION_REASON,
        )
        return

    await websocket.accept()
    if requested_cursor is not None:
        try:
            await _replay_events(websocket, runtime, run_id, last_delivered_cursor)
        except WebSocketDisconnect:
            return
        except (
            WorkflowRunExecutionEventInvariantViolation,
            WorkflowRunExecutionEventPersistenceUnavailable,
            RuntimeError,
            TypeError,
            ValueError,
        ):
            await _close_accepted_for_service_failure(websocket)
            return
    try:
        while True:
            message = await websocket.receive()
            if message["type"] == "websocket.disconnect":
                return
    except WebSocketDisconnect:
        return


def _parse_cursor(value: str | None) -> int | None:
    if value is None:
        return None
    if (
        not value
        or len(value) > MAX_CURSOR_TEXT_LENGTH
        or not value.isascii()
        or (value != "0" and (not value.isdecimal() or value.startswith("0")))
    ):
        raise _InvalidCursorSyntax
    parsed = int(value)
    if parsed > MAX_POSTGRESQL_BIGINT:
        raise _InvalidCursorSyntax
    return parsed


def _resume_position(state: WorkflowRunExecutionEventResumeState) -> int:
    requested = state.requested_cursor
    if requested is None:
        return state.latest_cursor
    if requested > state.latest_cursor:
        raise _CursorAhead
    if state.latest_cursor == 0:
        return 0
    earliest = state.earliest_retained_cursor
    if earliest is None:
        raise WorkflowRunExecutionEventInvariantViolation
    if requested < earliest - 1:
        raise _CursorExpired(earliest, state.latest_cursor)
    if requested == earliest - 1:
        return requested
    if state.requested_cursor_exists is not True:
        raise WorkflowRunExecutionEventInvariantViolation
    return requested


async def _replay_events(
    websocket: WebSocket,
    runtime: ExecutionStreamRuntimeProtocol,
    workflow_run_id: UUID,
    last_delivered_cursor: int,
) -> int:
    while True:
        events = await runtime.workflow_run_execution_event_repository.list_after(
            workflow_run_id,
            last_delivered_cursor,
            EXECUTION_EVENT_REPLAY_PAGE_SIZE,
        )
        if not events:
            return last_delivered_cursor
        for event in events:
            if (
                event.workflow_run_id != workflow_run_id
                or event.cursor != last_delivered_cursor + 1
            ):
                raise WorkflowRunExecutionEventInvariantViolation
            message = _execution_event_message(event)
            await websocket.send_json(message.model_dump(mode="json"))
            last_delivered_cursor = event.cursor


def _execution_event_message(
    event: StoredWorkflowRunExecutionEvent,
) -> ExecutionStreamEventMessage:
    return ExecutionStreamEventMessage(
        event=ExecutionStreamEvent(
            id=event.id,
            workflow_run_id=event.workflow_run_id,
            task_run_id=event.task_run_id,
            cursor=event.cursor,
            event_type=event.event_type,
            occurred_at=event.occurred_at,
            payload=dict(event.payload),
        )
    )


async def _send_then_close(
    websocket: WebSocket,
    message: BaseModel,
    *,
    reason: str,
) -> None:
    try:
        await websocket.send_json(message.model_dump(mode="json"))
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason=reason)
    except (RuntimeError, WebSocketDisconnect):
        return


async def _close_accepted_for_service_failure(websocket: WebSocket) -> None:
    try:
        await websocket.close(
            code=status.WS_1011_INTERNAL_ERROR,
            reason=SERVICE_REJECTION_REASON,
        )
    except (RuntimeError, WebSocketDisconnect):
        return


def _bearer_credential(authorization: str | None) -> PresentedCredential:
    scheme, value = get_authorization_scheme_param(authorization)
    if scheme.lower() != "bearer" or not value:
        raise CredentialFormatError("invalid credential format")
    return parse_presented_credential(value)


def _parse_run_id(raw_run_id: str) -> UUID:
    try:
        return UUID(raw_run_id)
    except ValueError as error:
        raise _InvalidRunIdentifier from error
