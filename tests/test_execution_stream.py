"""Authenticated workflow-run WebSocket handshake and lifecycle tests."""

from __future__ import annotations

import asyncio
import base64
import json
import secrets
from collections.abc import Sequence
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI, WebSocketDisconnect
from pydantic import SecretStr
from starlette.types import Message, Scope

from taskforge.api.application import create_app
from taskforge.api.execution_stream import workflow_run_execution_stream
from taskforge.api.health import ReadinessCoordinator
from taskforge.identity.authentication import APIAuthenticator, WorkerAuthenticator
from taskforge.identity.authorization import AuthorizationService, OwnerFilter, Role
from taskforge.identity.credentials import (
    DEFAULT_VERIFIER_ALGORITHM,
    DEFAULT_VERIFIERS,
    CredentialScope,
)
from taskforge.identity.ports import CredentialRecord
from taskforge.runs.domain import (
    InspectedWorkflowRun,
    StoredWorkflowRunExecutionEvent,
    WorkflowRunExecutionEventResumeState,
    WorkflowRunStatus,
)
from taskforge.runs.persistence_ports import (
    WorkflowRunExecutionEventInvariantViolation,
    WorkflowRunExecutionEventPersistenceUnavailable,
)
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
        self.calls: list[tuple[UUID, OwnerFilter]] = []
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
        self, run_id: UUID, *, owner_filter: OwnerFilter
    ) -> InspectedWorkflowRun:
        self.calls.append((run_id, owner_filter))
        if self.error is not None:
            raise self.error
        return self.run


class ExecutionEventRepositoryStub:
    def __init__(self) -> None:
        self.inspect_calls: list[tuple[UUID, int | None]] = []
        self.list_calls: list[tuple[UUID, int, int]] = []
        self.inspect_error: Exception | None = None
        self.list_error: Exception | None = None
        self.resume_state: WorkflowRunExecutionEventResumeState | None = None
        self.pages: list[tuple[StoredWorkflowRunExecutionEvent, ...]] = []

    async def inspect_resume_cursor(
        self, workflow_run_id: UUID, requested_cursor: int | None
    ) -> WorkflowRunExecutionEventResumeState:
        self.inspect_calls.append((workflow_run_id, requested_cursor))
        if self.inspect_error is not None:
            raise self.inspect_error
        if self.resume_state is not None:
            return self.resume_state
        return WorkflowRunExecutionEventResumeState(
            None,
            0,
            requested_cursor,
            None if requested_cursor is None else False,
        )

    async def list_after(
        self, workflow_run_id: UUID, after_cursor: int, limit: int
    ) -> tuple[StoredWorkflowRunExecutionEvent, ...]:
        self.list_calls.append((workflow_run_id, after_cursor, limit))
        if self.list_error is not None:
            raise self.list_error
        return self.pages.pop(0) if self.pages else ()


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
        self.workflow_run_execution_event_repository = ExecutionEventRepositoryStub()

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
        readiness=ReadinessCoordinator(AlwaysReady(), timeout_seconds=0.05),
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
    cursor: str | None = None,
    send_failure: Exception | None = None,
    send_failure_after: int = 0,
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
        "query_string": (b"" if cursor is None else f"cursor={cursor}".encode()),
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
        if message["type"] == "websocket.send" and send_failure is not None:
            sent_count = sum(item["type"] == "websocket.send" for item in outbound)
            if sent_count >= send_failure_after:
                raise send_failure
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
    outbound = asyncio.run(
        websocket_exchange(app, run_id, credential, {"type": "websocket.disconnect"})
    )
    assert outbound == [{"type": "websocket.close", "code": code, "reason": reason}]


def protocol_payloads(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        cast(dict[str, Any], json.loads(message["text"]))
        for message in messages
        if message["type"] == "websocket.send"
    ]


def stored_event(run_id: UUID, cursor: int) -> StoredWorkflowRunExecutionEvent:
    return StoredWorkflowRunExecutionEvent(
        uuid4(),
        run_id,
        cursor,
        uuid4(),
        "task_run.status_changed",
        {"previous_status": "running", "status": "succeeded"},
        datetime.now(UTC),
    )


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
    assert runtime.workflow_run_service.calls == [
        (run_id, OwnerFilter.only(principal_id))
    ]


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
    assert runtime.workflow_run_service.calls == [
        (run_id, OwnerFilter.only(principal_id))
    ]


def test_no_cursor_baselines_at_latest_without_replay() -> None:
    app, runtime, credential, _, run_id = make_app()
    runtime.workflow_run_execution_event_repository.resume_state = (
        WorkflowRunExecutionEventResumeState(1, 7, None, None)
    )

    outbound = asyncio.run(
        websocket_exchange(
            app,
            run_id,
            credential,
            {"type": "websocket.disconnect", "code": 1000},
        )
    )

    assert outbound == [
        {"type": "websocket.accept", "subprotocol": None, "headers": []}
    ]
    assert runtime.workflow_run_execution_event_repository.inspect_calls == [
        (run_id, None)
    ]
    assert runtime.workflow_run_execution_event_repository.list_calls == []


@pytest.mark.parametrize(
    "cursor",
    ["", "-1", "+1", " 1", "1 ", "01", "1.0", "1e2", "x", "9223372036854775808"],
)
def test_malformed_cursor_is_protocol_error_without_bounds_lookup(cursor: str) -> None:
    app, runtime, credential, _, run_id = make_app()

    outbound = asyncio.run(websocket_exchange(app, run_id, credential, cursor=cursor))

    assert protocol_payloads(outbound) == [
        {"version": 1, "type": "error", "code": "invalid_cursor"}
    ]
    assert outbound[-1] == {
        "type": "websocket.close",
        "code": 1008,
        "reason": "invalid cursor",
    }
    assert runtime.workflow_run_execution_event_repository.inspect_calls == []


def test_administrator_handshake_uses_unrestricted_run_scope() -> None:
    app, runtime, credential, _, run_id = make_app(
        roles=frozenset({Role.ADMINISTRATOR.value})
    )

    outbound = asyncio.run(
        websocket_exchange(app, run_id, credential, {"type": "websocket.disconnect"})
    )

    assert runtime.workflow_run_service.calls == [(run_id, OwnerFilter.all_owners())]
    assert outbound[0]["type"] == "websocket.accept"


def test_cursor_ahead_is_protocol_error_without_disclosing_bounds() -> None:
    app, runtime, credential, _, run_id = make_app()
    runtime.workflow_run_execution_event_repository.resume_state = (
        WorkflowRunExecutionEventResumeState(1, 2, 3, False)
    )

    outbound = asyncio.run(websocket_exchange(app, run_id, credential, cursor="3"))

    assert protocol_payloads(outbound) == [
        {"version": 1, "type": "error", "code": "cursor_ahead"}
    ]
    assert outbound[-1]["code"] == 1008
    assert "earliest" not in outbound[1]["text"]
    assert "latest" not in outbound[1]["text"]


def test_expired_cursor_requires_snapshot_without_recovery_directive() -> None:
    app, runtime, credential, _, run_id = make_app()
    runtime.workflow_run_execution_event_repository.resume_state = (
        WorkflowRunExecutionEventResumeState(3, 8, 0, False)
    )

    outbound = asyncio.run(websocket_exchange(app, run_id, credential, cursor="0"))

    assert protocol_payloads(outbound) == [
        {
            "version": 1,
            "type": "snapshot_required",
            "reason": "cursor_expired",
            "workflow_run_id": str(run_id),
            "earliest_retained_cursor": 3,
            "latest_cursor": 8,
        }
    ]
    assert outbound[-1] == {
        "type": "websocket.close",
        "code": 1008,
        "reason": "snapshot required",
    }
    serialized = outbound[1]["text"]
    assert "snapshot_url" not in serialized
    assert "action" not in serialized
    assert "payload" not in serialized


def test_internal_retained_cursor_gap_is_pre_accept_service_failure() -> None:
    app, runtime, credential, _, run_id = make_app()
    runtime.workflow_run_execution_event_repository.resume_state = cast(
        WorkflowRunExecutionEventResumeState,
        SimpleNamespace(
            earliest_retained_cursor=1,
            latest_cursor=4,
            requested_cursor=2,
            requested_cursor_exists=False,
        ),
    )

    outbound = asyncio.run(websocket_exchange(app, run_id, credential, cursor="2"))

    assert outbound == [
        {"type": "websocket.close", "code": 1011, "reason": "service unavailable"}
    ]


def test_empty_stream_accepts_cursor_zero_and_performs_final_empty_read() -> None:
    app, runtime, credential, _, run_id = make_app()

    outbound = asyncio.run(
        websocket_exchange(
            app,
            run_id,
            credential,
            {"type": "websocket.disconnect", "code": 1000},
            cursor="0",
        )
    )

    assert outbound == [
        {"type": "websocket.accept", "subprotocol": None, "headers": []}
    ]
    assert runtime.workflow_run_execution_event_repository.list_calls == [
        (run_id, 0, 100)
    ]


def test_latest_cursor_is_valid_and_yields_empty_replay() -> None:
    app, runtime, credential, _, run_id = make_app()
    repository = runtime.workflow_run_execution_event_repository
    repository.resume_state = WorkflowRunExecutionEventResumeState(1, 4, 4, True)

    outbound = asyncio.run(
        websocket_exchange(
            app,
            run_id,
            credential,
            {"type": "websocket.disconnect", "code": 1000},
            cursor="4",
        )
    )

    assert protocol_payloads(outbound) == []
    assert repository.list_calls == [(run_id, 4, 100)]


def test_middle_cursor_replays_strictly_after_in_ascending_order() -> None:
    app, runtime, credential, _, run_id = make_app()
    repository = runtime.workflow_run_execution_event_repository
    repository.resume_state = WorkflowRunExecutionEventResumeState(1, 4, 2, True)
    repository.pages = [
        (stored_event(run_id, 3), stored_event(run_id, 4)),
        (),
    ]

    outbound = asyncio.run(
        websocket_exchange(
            app,
            run_id,
            credential,
            {"type": "websocket.disconnect", "code": 1000},
            cursor="2",
        )
    )

    payloads = protocol_payloads(outbound)
    assert [message["event"]["cursor"] for message in payloads] == [3, 4]
    assert payloads[0] == {
        "version": 1,
        "type": "execution_event",
        "event": {
            "id": payloads[0]["event"]["id"],
            "workflow_run_id": str(run_id),
            "task_run_id": payloads[0]["event"]["task_run_id"],
            "cursor": 3,
            "event_type": "task_run.status_changed",
            "occurred_at": payloads[0]["event"]["occurred_at"],
            "payload": {"previous_status": "running", "status": "succeeded"},
        },
    }
    assert repository.list_calls == [(run_id, 2, 100), (run_id, 4, 100)]


def test_cursor_zero_replay_spans_bounded_pages_without_gaps_or_duplicates() -> None:
    app, runtime, credential, _, run_id = make_app()
    repository = runtime.workflow_run_execution_event_repository
    repository.resume_state = WorkflowRunExecutionEventResumeState(1, 101, 0, False)
    repository.pages = [
        tuple(stored_event(run_id, cursor) for cursor in range(1, 101)),
        (stored_event(run_id, 101),),
        (),
    ]

    outbound = asyncio.run(
        websocket_exchange(
            app,
            run_id,
            credential,
            {"type": "websocket.disconnect", "code": 1000},
            cursor="0",
        )
    )

    assert [item["event"]["cursor"] for item in protocol_payloads(outbound)] == list(
        range(1, 102)
    )
    assert repository.list_calls == [
        (run_id, 0, 100),
        (run_id, 100, 100),
        (run_id, 101, 100),
    ]


@pytest.mark.parametrize(
    "error",
    [
        WorkflowRunExecutionEventPersistenceUnavailable(),
        WorkflowRunExecutionEventInvariantViolation(),
    ],
)
def test_cursor_inspection_failure_is_rejected_before_accept(error: Exception) -> None:
    app, runtime, credential, _, run_id = make_app()
    runtime.workflow_run_execution_event_repository.inspect_error = error

    outbound = asyncio.run(websocket_exchange(app, run_id, credential, cursor="0"))

    assert outbound == [
        {"type": "websocket.close", "code": 1011, "reason": "service unavailable"}
    ]


def test_database_failure_during_replay_closes_accepted_socket() -> None:
    app, runtime, credential, _, run_id = make_app()
    repository = runtime.workflow_run_execution_event_repository
    repository.resume_state = WorkflowRunExecutionEventResumeState(1, 1, 0, False)
    repository.list_error = WorkflowRunExecutionEventPersistenceUnavailable()

    outbound = asyncio.run(websocket_exchange(app, run_id, credential, cursor="0"))

    assert outbound == [
        {"type": "websocket.accept", "subprotocol": None, "headers": []},
        {"type": "websocket.close", "code": 1011, "reason": "service unavailable"},
    ]


def test_disconnect_during_replay_terminates_without_service_close() -> None:
    app, runtime, credential, _, run_id = make_app()
    repository = runtime.workflow_run_execution_event_repository
    repository.resume_state = WorkflowRunExecutionEventResumeState(1, 1, 0, False)
    repository.pages = [(stored_event(run_id, 1),)]

    outbound = asyncio.run(
        websocket_exchange(
            app,
            run_id,
            credential,
            cursor="0",
            send_failure=WebSocketDisconnect(1000),
        )
    )

    assert outbound == [
        {"type": "websocket.accept", "subprotocol": None, "headers": []}
    ]
    assert repository.list_calls == [(run_id, 0, 100)]


def test_serialization_failure_during_replay_closes_accepted_socket() -> None:
    app, runtime, credential, _, run_id = make_app()
    repository = runtime.workflow_run_execution_event_repository
    repository.resume_state = WorkflowRunExecutionEventResumeState(1, 1, 0, False)
    repository.pages = [
        (
            cast(
                StoredWorkflowRunExecutionEvent,
                SimpleNamespace(
                    id=uuid4(),
                    workflow_run_id=run_id,
                    task_run_id=None,
                    cursor=1,
                    event_type="workflow_run.status_changed",
                    occurred_at=datetime.now(UTC),
                    payload=object(),
                ),
            ),
        )
    ]

    outbound = asyncio.run(websocket_exchange(app, run_id, credential, cursor="0"))

    assert outbound == [
        {"type": "websocket.accept", "subprotocol": None, "headers": []},
        {"type": "websocket.close", "code": 1011, "reason": "service unavailable"},
    ]


def test_failed_send_does_not_advance_cursor_or_request_a_later_page() -> None:
    app, runtime, credential, _, run_id = make_app()
    repository = runtime.workflow_run_execution_event_repository
    repository.resume_state = WorkflowRunExecutionEventResumeState(1, 2, 0, False)
    repository.pages = [
        (stored_event(run_id, 1),),
        (stored_event(run_id, 2),),
    ]

    outbound = asyncio.run(
        websocket_exchange(
            app,
            run_id,
            credential,
            cursor="0",
            send_failure=RuntimeError("transport failed"),
            send_failure_after=1,
        )
    )

    assert [item["event"]["cursor"] for item in protocol_payloads(outbound)] == [1]
    assert repository.list_calls == [(run_id, 0, 100), (run_id, 1, 100)]
    assert outbound[-1] == {
        "type": "websocket.close",
        "code": 1011,
        "reason": "service unavailable",
    }


def test_malformed_cursor_still_requires_owner_authorization() -> None:
    app, runtime, credential, _, run_id = make_app()
    runtime.workflow_run_service.error = WorkflowRunNotFound()

    outbound = asyncio.run(websocket_exchange(app, run_id, credential, cursor="bad"))

    assert outbound == [
        {"type": "websocket.close", "code": 1008, "reason": "connection denied"}
    ]
    assert runtime.workflow_run_execution_event_repository.inspect_calls == []
