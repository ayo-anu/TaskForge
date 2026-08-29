"""Streaming ASGI request-body limits for HTTP requests."""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID, uuid4

from starlette.types import ASGIApp, Message, Receive, Scope, Send

DEFAULT_API_MAX_REQUEST_BODY_BYTES = 10 * 1024 * 1024
REQUEST_ID_HEADER = b"x-request-id"


class RequestBodyTooLarge(Exception):
    """Private control signal raised when streamed request bytes exceed the limit."""


class RequestBodyLimitMiddleware:
    """Reject malformed lengths and stop body parsing at a fixed byte boundary."""

    def __init__(self, app: ASGIApp, *, max_body_bytes: int) -> None:
        if max_body_bytes <= 0:
            raise ValueError("request body limit must be positive")
        self._app = app
        self._max_body_bytes = max_body_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        content_length = _content_length(scope)
        if content_length is None:
            pass
        elif isinstance(content_length, _InvalidContentLength):
            await _send_error(
                scope,
                send,
                status_code=400,
                code="validation_failed",
                message="The request is invalid.",
            )
            return
        elif content_length > self._max_body_bytes:
            await _send_error(
                scope,
                send,
                status_code=413,
                code="request_too_large",
                message="The request body exceeds the allowed size.",
            )
            return

        received = 0
        overflowed = False
        response_started = False

        async def limited_receive() -> Message:
            nonlocal received, overflowed
            if overflowed:
                raise RequestBodyTooLarge
            message = await receive()
            if message["type"] != "http.request":
                return message
            body = message.get("body", b"")
            if not isinstance(body, bytes):
                overflowed = True
                raise RequestBodyTooLarge
            received += len(body)
            if received > self._max_body_bytes:
                overflowed = True
                raise RequestBodyTooLarge
            return message

        async def tracked_send(message: Message) -> None:
            nonlocal response_started
            # Starlette may translate receive failures while parsing a body. Once
            # overflow is authoritative, suppress that not-yet-started response;
            # this middleware emits the single stable 413 after downstream exits.
            if overflowed and not response_started:
                return
            if message["type"] == "http.response.start":
                response_started = True
            await send(message)

        try:
            await self._app(scope, limited_receive, tracked_send)
        except RequestBodyTooLarge:
            pass
        if overflowed and not response_started:
            await _send_error(
                scope,
                send,
                status_code=413,
                code="request_too_large",
                message="The request body exceeds the allowed size.",
            )


class _InvalidContentLength:
    pass


def _content_length(scope: Scope) -> int | _InvalidContentLength | None:
    values = [
        value
        for name, value in scope.get("headers", ())
        if name.lower() == b"content-length"
    ]
    if not values:
        return None
    if len(values) != 1:
        return _InvalidContentLength()
    value = values[0]
    if not value or not value.isascii() or not value.isdigit():
        return _InvalidContentLength()
    try:
        return int(value)
    except ValueError:
        return _InvalidContentLength()


async def _send_error(
    scope: Scope,
    send: Send,
    *,
    status_code: int,
    code: str,
    message: str,
) -> None:
    state = scope.setdefault("state", {})
    raw_request_id: Any = state.get("request_id")
    request_id = raw_request_id if isinstance(raw_request_id, UUID) else uuid4()
    state["request_id"] = request_id
    body = json.dumps(
        {
            "error": {
                "version": "1",
                "code": code,
                "message": message,
                "request_id": str(request_id),
            }
        },
        separators=(",", ":"),
    ).encode("utf-8")
    headers = [
        (b"content-type", b"application/json"),
        (b"content-length", str(len(body)).encode("ascii")),
        (REQUEST_ID_HEADER, str(request_id).encode("ascii")),
    ]
    await send(
        {"type": "http.response.start", "status": status_code, "headers": headers}
    )
    await send({"type": "http.response.body", "body": body})
