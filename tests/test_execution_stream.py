"""Authenticated workflow-run WebSocket handshake and lifecycle tests."""

from __future__ import annotations

import asyncio
import base64
import secrets
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from pydantic import SecretStr
from starlette.types import Message, Scope

from taskforge.api.application import create_app
from taskforge.api.execution_stream import workflow_run_execution_stream
from taskforge.api.health import ReadinessCoordinator
from taskforge.identity.authentication import APIAuthenticator, WorkerAuthenticator
from taskforge.identity.authorization import AuthorizationService, Role
from taskforge.identity.credentials import (
    DEFAULT_VERIFIER_ALGORITHM,
    DEFAULT_VERIFIERS,
    CredentialScope,
)
from taskforge.identity.ports import CredentialRecord
from taskforge.runs.domain import InspectedWorkflowRun, WorkflowRunStatus
from taskforge.runs.service import (
    WorkflowRunInspectionInvariantError,
    WorkflowRunNotFound,
    WorkflowRunServiceUnavailable,
)
from taskforge.settings import Settings


class AlwaysReady:
    async def start(self) -> None:
        pass

    async def is_ready(self) -> bool:
        return True

    async def close(self) -> None:
        pass


class CredentialRepository:
    def __init__(
        self,
        record: CredentialRecord | None,
        error: Exception | None = None,
    ) -> None:
        self.record = record
        self.error = error

    async def find_api_credential(self, credential_id: UUID) -> CredentialRecord | None:
        if self.error is not None:
            raise self.error
        if self.record is not None and self.record.credential_id == credential_id:
            return self.record
        return None

    async def find_worker_credential(
        self, credential_id: UUID
    ) -> CredentialRecord | None:
        del credential_id
        return None


class RoleRepository:
    def __init__(
        self,
        roles: frozenset[str],
        error: Exception | None = None,
    ) -> None:
        self.roles = roles
        self.error = error

    async def find_role_names(self, principal_id: UUID) -> frozenset[str]:
        del principal_id
        if self.error is not None:
            raise self.error
        return self.roles


class RunServiceStub:
    def __init__(self, principal_id: UUID, run_id: UUID) -> None:
        self.calls: list[tuple[UUID, UUID]] = []
        self.error: Exception | None = None
        now = datetime.now(UTC)
        self.run = InspectedWorkflowRun(
            run_id,
            uuid4(),
            uuid4(),
            1,
            principal_id,
            WorkflowRunStatus.RUNNING,
            now,
            now,
        )

    async def get_run(
        self, run_id: UUID, *, owner_principal_id: UUID
    ) -> InspectedWorkflowRun:
        self.calls.append((run_id, owner_principal_id))
        if self.error is not None:
            raise self.error
        return self.run


class Runtime:
    def __init__(
        self,
        api_authenticator: APIAuthenticator,
        authorization_service: AuthorizationService,
        run_service: RunServiceStub,
    ) -> None:
        self.api_authenticator = api_authenticator
        self.worker_authenticator = WorkerAuthenticator(
            CredentialRepository(None), timeout_seconds=0.05
        )
        self.authorization_service = authorization_service
        self.workflow_run_service = run_service

    async def close(self) -> None:
        pass


def credential_value(scope: CredentialScope, credential_id: UUID, secret: bytes) -> str:
    prefix = "tf_api_v1" if scope is CredentialScope.API else "tf_worker_v1"
    encoded = base64.urlsafe_b64encode(secret).rstrip(b"=").decode("ascii")
    return f"{prefix}.{credential_id}.{encoded}"


def make_app(
    *,
    roles: frozenset[str] = frozenset({Role.VIEWER.value}),
    credential_error: Exception | None = None,
    role_error: Exception | None = None,
    revoked: bool = False,
    expired: bool = False,
    disabled: bool = False,
) -> tuple[FastAPI, Runtime, str, UUID, UUID]:
    principal_id, credential_id, run_id = uuid4(), uuid4(), uuid4()
    secret = secrets.token_bytes(32)
    record = CredentialRecord(
        credential_id,
        principal_id,
        DEFAULT_VERIFIERS.encode(
            secret,
            algorithm=DEFAULT_VERIFIER_ALGORITHM,
        ),
        revoked,
        expired,
        disabled,
    )
    run_service = RunServiceStub(principal_id, run_id)
    runtime = Runtime(
        APIAuthenticator(
            CredentialRepository(record, credential_error), timeout_seconds=0.05
        ),
        AuthorizationService(RoleRepository(roles, role_error), timeout_seconds=0.05),
        run_service,
    )
    app = create_app(
        settings=Settings(
            postgres_password=SecretStr("postgres-test-secret"),
            rabbitmq_password=SecretStr("rabbitmq-test-secret"),
        ),
        readiness=ReadinessCoordinator((AlwaysReady(),), timeout_seconds=0.05),
        authentication=runtime,
    )
    app.state.authentication = runtime
    return (
        app,
        runtime,
        credential_value(CredentialScope.API, credential_id, secret),
        principal_id,
        run_id,
    )


async def websocket_exchange(
    app: FastAPI,
    run_id: object,
    credential: str | None,
    *messages: dict[str, Any],
) -> list[dict[str, Any]]:
    inbound = iter(({"type": "websocket.connect"}, *messages))
    outbound: list[dict[str, Any]] = []
    headers = (
        [(b"authorization", f"Bearer {credential}".encode())] if credential else []
    )
    path = f"/api/v1/workflow-runs/{run_id}/stream"
    scope: Scope = {
        "type": "websocket",
        "asgi": {"version": "3.0", "spec_version": "2.4"},
        "http_version": "1.1",
        "scheme": "ws",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": headers,
        "client": ("testclient", 50000),
        "server": ("testserver", 80),
        "subprotocols": [],
        "state": {},
        "extensions": {"websocket.http.response": {}},
    }

    async def receive() -> Message:
        return cast(Message, next(inbound))

    async def send(message: Message) -> None:
        outbound.append(dict(message))

    await app(scope, receive, send)
    return outbound


def assert_denied(
    app: FastAPI,
    run_id: object,
    credential: str | None,
    *,
    code: int,
    reason: str,
) -> None:
    outbound = asyncio.run(websocket_exchange(app, run_id, credential))
    assert outbound == [{"type": "websocket.close", "code": code, "reason": reason}]


def test_authenticated_owner_handshake_and_clean_disconnect() -> None:
    app, runtime, credential, principal_id, run_id = make_app()

    outbound = asyncio.run(
        websocket_exchange(
            app,
            run_id,
            credential,
            {"type": "websocket.disconnect", "code": 1000, "reason": ""},
        )
    )

    assert outbound == [
        {"type": "websocket.accept", "subprotocol": None, "headers": []}
    ]
    assert runtime.workflow_run_service.calls == [(run_id, principal_id)]


@pytest.mark.parametrize("credential", [None, "malformed", "Basic secret"])
def test_missing_and_invalid_credentials_are_uniformly_denied(
    credential: str | None,
) -> None:
    app, runtime, _, _, run_id = make_app()

    assert_denied(app, run_id, credential, code=1008, reason="connection denied")
    assert runtime.workflow_run_service.calls == []


def test_wrong_scope_and_invalid_secret_are_denied() -> None:
    app, runtime, credential, _, run_id = make_app()
    _, raw_id, _ = credential.split(".")
    wrong_scope = credential_value(
        CredentialScope.WORKER, UUID(raw_id), secrets.token_bytes(32)
    )
    invalid_secret = credential_value(
        CredentialScope.API, UUID(raw_id), secrets.token_bytes(32)
    )

    for rejected in (wrong_scope, invalid_secret):
        assert_denied(app, run_id, rejected, code=1008, reason="connection denied")
    assert runtime.workflow_run_service.calls == []


def test_missing_view_permission_is_denied_before_run_lookup() -> None:
    app, runtime, credential, _, run_id = make_app(roles=frozenset())

    assert_denied(app, run_id, credential, code=1008, reason="connection denied")
    assert runtime.workflow_run_service.calls == []


def test_malformed_run_id_is_denied_without_leaking_or_lookup() -> None:
    app, runtime, credential, _, _ = make_app()

    assert_denied(
        app,
        "not-a-private-run-id",
        credential,
        code=1008,
        reason="connection denied",
    )
    assert runtime.workflow_run_service.calls == []


def test_nonexistent_and_cross_owner_runs_are_observationally_identical() -> None:
    observations: list[tuple[int, str]] = []
    for _case in ("nonexistent", "cross-owner"):
        app, runtime, credential, _, run_id = make_app()
        runtime.workflow_run_service.error = WorkflowRunNotFound()
        outbound = asyncio.run(websocket_exchange(app, run_id, credential))
        observations.append((outbound[0]["code"], outbound[0]["reason"]))

    assert observations == [
        (1008, "connection denied"),
        (1008, "connection denied"),
    ]


@pytest.mark.parametrize(
    ("failure_source", "error"),
    [
        ("credential", RuntimeError("database credential detail")),
        ("role", RuntimeError("database role detail")),
        ("run", WorkflowRunServiceUnavailable("database run detail")),
        ("run", WorkflowRunInspectionInvariantError("corrupt run detail")),
    ],
)
def test_service_failures_are_safe_and_rejected_before_accept(
    failure_source: str,
    error: Exception,
) -> None:
    options: dict[str, Any] = {}
    if failure_source == "credential":
        options["credential_error"] = error
    if failure_source == "role":
        options["role_error"] = error
    app, runtime, credential, _, run_id = make_app(**options)
    if failure_source == "run":
        runtime.workflow_run_service.error = error

    assert_denied(app, run_id, credential, code=1011, reason="service unavailable")


class ReceiveOnlyWebSocket:
    def __init__(self, runtime: Runtime, messages: Sequence[dict[str, Any]]) -> None:
        self.app = cast(Any, type("App", (), {"state": type("State", (), {})()})())
        self.app.state.authentication = runtime
        self.headers = {"Authorization": "Bearer unused"}
        self._messages = iter(messages)
        self.accepted = False
        self.received = 0

    async def accept(self) -> None:
        assert runtime_run_service(self).calls
        self.accepted = True

    async def receive(self) -> dict[str, Any]:
        assert self.accepted
        self.received += 1
        return next(self._messages)


def runtime_run_service(websocket: ReceiveOnlyWebSocket) -> RunServiceStub:
    return cast(Runtime, websocket.app.state.authentication).workflow_run_service


def test_accept_follows_authorization_and_inbound_messages_are_discarded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, runtime, _, principal_id, run_id = make_app()
    del app
    socket = ReceiveOnlyWebSocket(
        runtime,
        (
            {"type": "websocket.receive", "text": "ignored"},
            {"type": "websocket.receive", "bytes": b"ignored"},
            {"type": "websocket.disconnect", "code": 1000},
        ),
    )

    async def authenticate(*args: object) -> object:
        del args
        return type(
            "Identity", (), {"principal_id": principal_id, "credential_id": uuid4()}
        )()

    monkeypatch.setattr(
        "taskforge.api.execution_stream._bearer_credential", lambda value: value
    )
    monkeypatch.setattr(
        "taskforge.api.execution_stream.authenticate_api_credential", authenticate
    )

    asyncio.run(workflow_run_execution_stream(cast(Any, socket), str(run_id)))

    assert socket.accepted is True
    assert socket.received == 3
    assert runtime.workflow_run_service.calls == [(run_id, principal_id)]
