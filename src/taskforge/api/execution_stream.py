"""Authenticated, owner-scoped workflow-run WebSocket handshakes."""

from __future__ import annotations

from typing import Protocol, cast
from uuid import UUID

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status
from fastapi.security.utils import get_authorization_scheme_param

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
from taskforge.runs.service import (
    WorkflowRunInspectionInvariantError,
    WorkflowRunNotFound,
    WorkflowRunService,
    WorkflowRunServiceUnavailable,
)

POLICY_REJECTION_REASON = "connection denied"
SERVICE_REJECTION_REASON = "service unavailable"


class _InvalidRunIdentifier(Exception):
    """Keep identifier parsing failures separate from service failures."""


class ExecutionStreamRuntimeProtocol(AuthenticationRuntimeProtocol, Protocol):
    authorization_service: AuthorizationService
    workflow_run_service: WorkflowRunService


router = APIRouter(tags=["workflow-run-stream"])


@router.websocket("/api/v1/workflow-runs/{raw_run_id}/stream")
async def workflow_run_execution_stream(
    websocket: WebSocket,
    raw_run_id: str,
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

    await websocket.accept()
    try:
        while True:
            message = await websocket.receive()
            if message["type"] == "websocket.disconnect":
                return
    except WebSocketDisconnect:
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
