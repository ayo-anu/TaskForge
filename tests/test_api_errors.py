"""Tests for the common versioned API error contract."""

from __future__ import annotations

import asyncio
import logging
from uuid import UUID

import httpx2
import pytest
from fastapi import FastAPI, HTTPException
from starlette.requests import Request
from starlette.responses import Response

from taskforge.api.errors import (
    REQUEST_ID_HEADER,
    RequestIDMiddleware,
    install_error_handling,
)
from taskforge.api.request_limits import DEFAULT_API_MAX_REQUEST_BODY_BYTES


def make_app() -> FastAPI:
    app = FastAPI()
    install_error_handling(
        app, max_request_body_bytes=DEFAULT_API_MAX_REQUEST_BODY_BYTES
    )

    @app.get("/fail/{value}")
    async def fail(value: int) -> None:
        raise HTTPException(status_code=value)

    @app.get("/validated/{value}")
    async def validated(value: int) -> dict[str, int]:
        return {"value": value}

    @app.get("/unexpected")
    async def unexpected() -> None:
        raise RuntimeError("postgresql://user:secret@internal-host/taskforge")

    return app


def request(
    app: FastAPI,
    path: str,
    *,
    headers: dict[str, str] | None = None,
    raise_app_exceptions: bool = True,
) -> httpx2.Response:
    async def send() -> httpx2.Response:
        transport = httpx2.ASGITransport(
            app=app,
            raise_app_exceptions=raise_app_exceptions,
        )
        async with httpx2.AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            return await client.get(path, headers=headers)

    return asyncio.run(send())


def test_one_server_request_id_is_reused_in_body_and_header() -> None:
    response = request(make_app(), "/fail/401")
    body_request_id = response.json()["error"]["request_id"]

    assert UUID(body_request_id)
    assert response.headers[REQUEST_ID_HEADER] == body_request_id
    assert response.json()["error"] == {
        "version": "1",
        "code": "authentication_required",
        "message": "Authentication is required.",
        "request_id": body_request_id,
    }


def test_server_request_ids_are_unique_and_ignore_client_input() -> None:
    hostile_id = "attacker-controlled-request-id"
    app = make_app()
    first = request(app, "/fail/403", headers={REQUEST_ID_HEADER: hostile_id})
    second = request(app, "/fail/403")

    first_id = first.json()["error"]["request_id"]
    second_id = second.json()["error"]["request_id"]
    assert first_id != hostile_id
    assert first_id != second_id
    assert first.headers[REQUEST_ID_HEADER] == first_id
    assert second.headers[REQUEST_ID_HEADER] == second_id


def test_security_and_resource_failures_use_stable_safe_contracts() -> None:
    app = make_app()
    expected = {
        401: ("authentication_required", "Authentication is required."),
        403: ("forbidden", "Access is forbidden."),
        404: ("resource_not_found", "The requested resource was not found."),
        503: ("service_unavailable", "The service is temporarily unavailable."),
    }

    for status_code, (code, message) in expected.items():
        response = request(app, f"/fail/{status_code}")
        assert response.status_code == status_code
        assert response.json()["error"]["code"] == code
        assert response.json()["error"]["message"] == message


def test_validation_failures_use_the_common_envelope() -> None:
    response = request(make_app(), "/validated/not-an-integer")

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_failed"
    assert response.json()["error"]["message"] == "The request is invalid."
    assert response.json()["error"]["details"] == [
        {
            "code": "invalid_request_value",
            "path": ["path", "value"],
            "message": "Field value is invalid.",
        }
    ]
    assert "not-an-integer" not in response.text


def test_unexpected_failures_have_one_owning_error_event(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="taskforge.api.errors")
    response = request(make_app(), "/unexpected", raise_app_exceptions=False)

    assert response.status_code == 500
    assert response.json()["error"]["code"] == "internal_error"
    assert response.headers[REQUEST_ID_HEADER] == response.json()["error"]["request_id"]
    assert "secret" not in response.text
    errors = [
        record
        for record in caplog.records
        if record.levelno >= logging.ERROR
        and getattr(record, "_event_name", None) == "api.exception.unhandled"
    ]
    assert len(errors) == 1
    fields = errors[0].__dict__["_event_fields"]
    response_request_id = response.headers[REQUEST_ID_HEADER]
    assert fields["request.id"] == response_request_id
    assert fields["correlation.id"] == response_request_id


def test_request_cancellation_is_not_logged_as_application_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    middleware = RequestIDMiddleware(FastAPI())
    request_value = Request({"type": "http", "method": "GET", "path": "/cancel"})

    async def cancel(_request: Request) -> Response:
        raise asyncio.CancelledError

    async def invoke() -> None:
        with pytest.raises(asyncio.CancelledError):
            await middleware.dispatch(request_value, cancel)

    caplog.set_level(logging.INFO, logger="taskforge.api.errors")
    asyncio.run(invoke())
    assert not [record for record in caplog.records if record.levelno >= logging.ERROR]
