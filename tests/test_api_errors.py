"""Tests for the common versioned API error contract."""

from __future__ import annotations

import asyncio
from uuid import UUID

import httpx2
from fastapi import FastAPI, HTTPException

from taskforge.api.errors import REQUEST_ID_HEADER, install_error_handling


def make_app() -> FastAPI:
    app = FastAPI()
    install_error_handling(app)

    @app.get("/fail/{value}")
    async def fail(value: int) -> None:
        raise HTTPException(status_code=value)

    @app.get("/validated/{value}")
    async def validated(value: int) -> dict[str, int]:
        return {"value": value}

    return app


def request(
    app: FastAPI,
    path: str,
    *,
    headers: dict[str, str] | None = None,
) -> httpx2.Response:
    async def send() -> httpx2.Response:
        transport = httpx2.ASGITransport(app=app)
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
    assert "not-an-integer" not in response.text
