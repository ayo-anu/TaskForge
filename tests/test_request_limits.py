"""Streaming HTTP request-body limit security tests."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Annotated, Any
from uuid import uuid4

import pytest
from fastapi import Body, FastAPI
from pydantic import SecretStr
from starlette.types import Message, Scope

from taskforge.api.application import create_app
from taskforge.api.errors import install_error_handling
from taskforge.api.request_limits import RequestBodyLimitMiddleware, RequestBodyTooLarge
from taskforge.settings import Settings
from taskforge.workflows.dag_validation import (
    MAX_DAG_DEPENDENCIES,
    MAX_DAG_STEPS,
    DAGEdge,
    validate_dag,
)
from taskforge.workflows.domain import (
    MAX_WORKFLOW_DESCRIPTION_LENGTH,
    MAX_WORKFLOW_NAME_LENGTH,
    WorkflowDefinitionStatus,
    create_draft_dependency,
    create_draft_step,
    create_workflow_draft,
)
from taskforge.workflows.task_types import (
    MAX_PARAMETER_BYTES,
    TaskTypeDefinition,
    TaskTypeRegistry,
)

ASGIApp = Callable[
    [Scope, Callable[[], Awaitable[Message]], Callable[[Message], Awaitable[None]]],
    Awaitable[None],
]


class AcceptParameters:
    def validate(self, parameters: dict[str, object]) -> tuple[()]:
        del parameters
        return ()


def _scope(*, headers: list[tuple[bytes, bytes]] | None = None) -> Scope:
    return {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/body",
        "raw_path": b"/body",
        "query_string": b"",
        "headers": headers or [],
        "client": ("127.0.0.1", 1234),
        "server": ("testserver", 80),
        "state": {"request_id": uuid4()},
    }


def _invoke(
    app: ASGIApp, scope: Scope, frames: list[Message]
) -> tuple[list[Message], list[Message]]:
    delivered: list[Message] = []
    sent: list[Message] = []

    async def run() -> None:
        pending = iter(frames)

        async def receive() -> Message:
            return next(pending)

        async def send(message: Message) -> None:
            sent.append(message)

        await app(scope, receive, send)

    asyncio.run(run())
    return delivered, sent


@pytest.mark.parametrize("frame_values", ([b"123", b"456"], [b"12345", b"67890"]))
def test_multiframe_below_and_exact_limit_reach_downstream(
    frame_values: list[bytes],
) -> None:
    received_frames: list[bytes] = []
    executed = False

    async def downstream(scope: Scope, receive: Any, send: Any) -> None:
        nonlocal executed
        del scope
        while True:
            message = await receive()
            received_frames.append(message.get("body", b""))
            if not message.get("more_body", False):
                break
        executed = True
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    messages = [
        {
            "type": "http.request",
            "body": value,
            "more_body": index < len(frame_values) - 1,
        }
        for index, value in enumerate(frame_values)
    ]
    _, sent = _invoke(
        RequestBodyLimitMiddleware(downstream, max_body_bytes=10),
        _scope(),
        messages,
    )
    assert executed
    assert received_frames == frame_values
    assert [
        item["status"] for item in sent if item["type"] == "http.response.start"
    ] == [204]


def test_later_frame_overflow_aborts_without_delivering_frame_or_executing() -> None:
    delivered: list[bytes] = []
    executed = False

    async def downstream(scope: Scope, receive: Any, send: Any) -> None:
        nonlocal executed
        del scope, send
        while True:
            message = await receive()
            delivered.append(message.get("body", b""))
            if not message.get("more_body", False):
                break
        executed = True

    _, sent = _invoke(
        RequestBodyLimitMiddleware(downstream, max_body_bytes=5),
        _scope(),
        [
            {"type": "http.request", "body": b"123", "more_body": True},
            {"type": "http.request", "body": b"456", "more_body": False},
        ],
    )
    starts = [item for item in sent if item["type"] == "http.response.start"]
    bodies = [item for item in sent if item["type"] == "http.response.body"]
    assert delivered == [b"123"]
    assert not executed
    assert len(starts) == 1 and starts[0]["status"] == 413
    assert len(bodies) == 1 and len(bodies[0]["body"]) < 512
    assert json.loads(bodies[0]["body"])["error"]["code"] == "request_too_large"


def test_receive_remains_failed_after_overflow() -> None:
    failures = 0

    async def downstream(scope: Scope, receive: Any, send: Any) -> None:
        nonlocal failures
        del scope, send
        for _ in range(2):
            try:
                await receive()
            except RequestBodyTooLarge:
                failures += 1
        raise RequestBodyTooLarge

    _, sent = _invoke(
        RequestBodyLimitMiddleware(downstream, max_body_bytes=1),
        _scope(),
        [{"type": "http.request", "body": b"12", "more_body": True}],
    )
    assert failures == 2
    assert [
        item["status"] for item in sent if item["type"] == "http.response.start"
    ] == [413]


def test_overflow_after_response_start_never_synthesizes_second_response() -> None:
    async def downstream(scope: Scope, receive: Any, send: Any) -> None:
        del scope
        await send({"type": "http.response.start", "status": 202, "headers": []})
        await receive()

    _, sent = _invoke(
        RequestBodyLimitMiddleware(downstream, max_body_bytes=1),
        _scope(),
        [{"type": "http.request", "body": b"12", "more_body": False}],
    )
    assert [
        item["status"] for item in sent if item["type"] == "http.response.start"
    ] == [202]


def test_content_length_contract() -> None:
    calls = 0

    async def downstream(scope: Scope, receive: Any, send: Any) -> None:
        nonlocal calls
        del scope, receive
        calls += 1
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    cases = (
        ([(b"content-length", b"11")], 413),
        ([(b"content-length", b"-1")], 400),
        ([(b"content-length", b"invalid")], 400),
        ([(b"content-length", b"1"), (b"Content-Length", b"1")], 400),
    )
    for headers, expected in cases:
        _, sent = _invoke(
            RequestBodyLimitMiddleware(downstream, max_body_bytes=10),
            _scope(headers=headers),
            [],
        )
        assert [
            item["status"] for item in sent if item["type"] == "http.response.start"
        ] == [expected]
    assert calls == 0


def test_real_application_preflight_and_stream_overflow_keep_request_ids() -> None:
    settings = Settings(
        postgres_password=SecretStr("postgres-test-secret"),
        rabbitmq_password=SecretStr("rabbitmq-test-secret"),
        api_max_request_body_bytes=5,
    )
    app = create_app(settings=settings)
    for headers, frames in (
        ([(b"content-length", b"6")], []),
        (
            [(b"content-type", b"application/json"), (b"content-length", b"1")],
            [
                {"type": "http.request", "body": b"123", "more_body": True},
                {"type": "http.request", "body": b"456", "more_body": False},
            ],
        ),
    ):
        scope = _scope(headers=headers)
        scope["path"] = "/api/v1/workflows"
        scope["raw_path"] = b"/api/v1/workflows"
        _, sent = _invoke(app, scope, frames)
        start = next(item for item in sent if item["type"] == "http.response.start")
        assert start["status"] == 413
        response_headers = dict(start["headers"])
        body = b"".join(
            item.get("body", b"")
            for item in sent
            if item["type"] == "http.response.body"
        )
        request_id = json.loads(body)["error"]["request_id"].encode("ascii")
        assert response_headers[b"x-request-id"] == request_id
        assert (
            len([item for item in sent if item["type"] == "http.response.start"]) == 1
        )


@pytest.mark.parametrize(
    ("body", "expected_status"),
    ((b"{{{{", 422), (b"{{{{{", 422), (b"{{{{{{", 413)),
)
def test_malformed_json_around_limit_never_executes_endpoint(
    body: bytes, expected_status: int
) -> None:
    app = FastAPI()
    install_error_handling(app, max_request_body_bytes=5)
    executed = False

    @app.post("/json")
    async def parse_json(
        value: Annotated[dict[str, object], Body()],
    ) -> dict[str, object]:
        nonlocal executed
        executed = True
        return value

    scope = _scope(headers=[(b"content-type", b"application/json")])
    scope["path"] = "/json"
    scope["raw_path"] = b"/json"
    _, sent = _invoke(
        app,
        scope,
        [{"type": "http.request", "body": body, "more_body": False}],
    )
    assert (
        next(item["status"] for item in sent if item["type"] == "http.response.start")
        == expected_status
    )
    assert not executed


def test_near_worst_case_valid_workflow_fits_default_transport_limit() -> None:
    registry = TaskTypeRegistry(
        (TaskTypeDefinition("test.task", "test.task", AcceptParameters()),)
    )
    value = "x" * 4_000
    parameters = {"a": value, "b": value, "c": value, "d": "x" * 3_000}
    assert (
        len(json.dumps(parameters, separators=(",", ":")).encode())
        < MAX_PARAMETER_BYTES
    )
    identifiers = [f"step_{index}" for index in range(MAX_DAG_STEPS)]
    steps = tuple(
        create_draft_step(
            step_id=uuid4(),
            identifier=identifier,
            task_type="test.task",
            parameters=parameters,
            task_types=registry,
        )
        for identifier in identifiers
    )
    edge_values = [
        DAGEdge(identifiers[left], identifiers[right])
        for left in range(MAX_DAG_STEPS)
        for right in range(left + 1, MAX_DAG_STEPS)
    ][:MAX_DAG_DEPENDENCIES]
    graph = validate_dag(identifiers, edge_values)
    assert graph.is_valid
    dependencies = tuple(
        create_draft_dependency(
            dependency_id=uuid4(),
            predecessor_identifier=edge.predecessor,
            successor_identifier=edge.successor,
        )
        for edge in edge_values
    )
    create_workflow_draft(
        workflow_id=uuid4(),
        owner_principal_id=uuid4(),
        name="n" * MAX_WORKFLOW_NAME_LENGTH,
        description="d" * MAX_WORKFLOW_DESCRIPTION_LENGTH,
        status=WorkflowDefinitionStatus.DRAFT,
        steps=steps,
        dependencies=dependencies,
    )
    body = json.dumps(
        {
            "name": "n" * MAX_WORKFLOW_NAME_LENGTH,
            "description": "d" * MAX_WORKFLOW_DESCRIPTION_LENGTH,
            "steps": [
                {
                    "identifier": identifier,
                    "task_type": "test.task",
                    "parameters": parameters,
                }
                for identifier in identifiers
            ],
            "dependencies": [
                {"predecessor": edge.predecessor, "successor": edge.successor}
                for edge in edge_values
            ],
        },
        separators=(",", ":"),
    ).encode()
    assert len(body) < 10 * 1024 * 1024

    executed = False

    async def downstream(scope: Scope, receive: Any, send: Any) -> None:
        nonlocal executed
        del scope
        received = b""
        while True:
            message = await receive()
            received += message.get("body", b"")
            if not message.get("more_body", False):
                break
        executed = received == body
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    _, sent = _invoke(
        RequestBodyLimitMiddleware(downstream, max_body_bytes=10 * 1024 * 1024),
        _scope(),
        [{"type": "http.request", "body": body, "more_body": False}],
    )
    assert executed
    assert (
        next(item["status"] for item in sent if item["type"] == "http.response.start")
        == 204
    )
