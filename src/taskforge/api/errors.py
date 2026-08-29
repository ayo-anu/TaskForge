"""Common safe API error envelopes and per-request identifiers."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from time import perf_counter
from typing import Literal, cast
from uuid import UUID, uuid4

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from opentelemetry.trace import SpanKind
from pydantic import BaseModel
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

from taskforge.api.request_limits import (
    RequestBodyLimitMiddleware,
    RequestBodyTooLarge,
)
from taskforge.logging import bind_log_context, log_event
from taskforge.metrics import (
    add as add_metric,
)
from taskforge.metrics import (
    normalize_http_method,
    normalize_http_route,
    status_class,
)
from taskforge.metrics import (
    record as record_metric,
)
from taskforge.tracing import (
    extract_carrier,
    set_attributes,
    set_error,
    set_error_type,
    span,
    update_name,
)

REQUEST_ID_HEADER = "X-Request-ID"
logger = logging.getLogger(__name__)


class ErrorDetail(BaseModel):
    code: str
    path: list[str | int]
    message: str


class ErrorBody(BaseModel):
    version: Literal["1"] = "1"
    code: str
    message: str
    request_id: UUID
    details: list[ErrorDetail] | None = None


class ErrorResponse(BaseModel):
    error: ErrorBody


ERROR_CONTRACTS = {
    401: ("authentication_required", "Authentication is required."),
    403: ("forbidden", "Access is forbidden."),
    404: ("resource_not_found", "The requested resource was not found."),
    409: ("resource_conflict", "The request conflicts with current state."),
    413: ("request_too_large", "The request body exceeds the allowed size."),
    422: ("validation_failed", "The request is invalid."),
    429: ("rate_limit_exceeded", "The request rate limit was exceeded."),
    500: ("internal_error", "The service encountered an internal error."),
    503: ("service_unavailable", "The service is temporarily unavailable."),
}


class RequestIDMiddleware(BaseHTTPMiddleware):
    """Generate exactly one server-owned request ID for each request."""

    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        request_id = uuid4()
        request.state.request_id = request_id
        started = perf_counter()
        parent = extract_carrier(_trace_headers(request))
        with bind_log_context(
            **{"request.id": request_id, "correlation.id": request_id}
        ):
            with span(
                "taskforge.api.request",
                kind=SpanKind.SERVER,
                parent=parent,
                attributes={"http.request.method": request.method},
                enabled=request.scope.get("path") not in {"/health", "/ready"},
            ) as server_span:
                try:
                    response = await call_next(request)
                except Exception as error:
                    set_error(server_span, error, "api_unhandled_exception")
                    _record_request_metrics(
                        request,
                        started,
                        status_code=None,
                        outcome="unhandled_exception",
                    )
                    raise
                route = _route_template(request)
                update_name(server_span, f"{request.method} {route}")
                set_attributes(
                    server_span,
                    {
                        "http.route": route,
                        "http.response.status_code": response.status_code,
                        "taskforge.correlation.id": str(request_id),
                    },
                )
                if response.status_code >= 500:
                    set_error_type(server_span, "HTTPServerError", "http_5xx")
                response.headers[REQUEST_ID_HEADER] = str(request_id)
                log_event(
                    logger,
                    logging.INFO,
                    "api.request.completed",
                    {
                        "http.method": request.method,
                        "http.route": route,
                        "http.status_code": response.status_code,
                        "duration_ms": round((perf_counter() - started) * 1000, 3),
                        "outcome": "completed",
                    },
                )
                _record_request_metrics(
                    request,
                    started,
                    status_code=response.status_code,
                    outcome="completed",
                )
                return response


def install_error_handling(app: FastAPI, *, max_request_body_bytes: int) -> None:
    """Install one envelope implementation for framework and security failures."""
    # Starlette inserts the last registered middleware outermost. Request IDs must
    # therefore be registered after the body limiter.
    app.add_middleware(
        RequestBodyLimitMiddleware, max_body_bytes=max_request_body_bytes
    )
    app.add_middleware(RequestIDMiddleware)
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(RequestBodyTooLarge, request_body_too_large_handler)
    app.add_exception_handler(Exception, unexpected_exception_handler)


async def request_body_too_large_handler(
    request: Request,
    exception: Exception,
) -> JSONResponse:
    del exception
    code, message = ERROR_CONTRACTS[413]
    return error_response(request, status_code=413, code=code, message=message)


async def http_exception_handler(
    request: Request,
    exception: Exception,
) -> JSONResponse:
    http_exception = cast(StarletteHTTPException, exception)
    code, message = ERROR_CONTRACTS.get(
        http_exception.status_code,
        ("request_failed", "The request could not be completed."),
    )
    return error_response(
        request,
        status_code=http_exception.status_code,
        code=code,
        message=message,
        headers=http_exception.headers,
    )


async def validation_exception_handler(
    request: Request,
    exception: Exception,
) -> JSONResponse:
    validation_error = cast(RequestValidationError, exception)
    code, message = ERROR_CONTRACTS[422]
    return error_response(
        request,
        status_code=422,
        code=code,
        message=message,
        details=tuple(
            _request_validation_detail(error) for error in validation_error.errors()
        ),
    )


async def unexpected_exception_handler(
    request: Request,
    exception: Exception,
) -> JSONResponse:
    request_id = cast(UUID, request.state.request_id)
    with bind_log_context(**{"request.id": request_id, "correlation.id": request_id}):
        log_event(
            logger,
            logging.ERROR,
            "api.exception.unhandled",
            {"error.category": "unexpected"},
            error=exception,
        )
    code, message = ERROR_CONTRACTS[500]
    return error_response(
        request,
        status_code=500,
        code=code,
        message=message,
    )


def error_response(
    request: Request,
    *,
    status_code: int,
    code: str,
    message: str,
    headers: Mapping[str, str] | None = None,
    details: Sequence[ErrorDetail] | None = None,
) -> JSONResponse:
    request_id = cast(UUID, request.state.request_id)
    response_headers = dict(headers or {})
    response_headers[REQUEST_ID_HEADER] = str(request_id)
    body = ErrorResponse(
        error=ErrorBody(
            code=code,
            message=message,
            request_id=request_id,
            details=list(details) if details is not None else None,
        )
    )
    return JSONResponse(
        status_code=status_code,
        content=body.model_dump(mode="json", exclude_none=True),
        headers=response_headers,
    )


def _request_validation_detail(error: Mapping[str, object]) -> ErrorDetail:
    error_type = error.get("type")
    if error_type == "missing":
        code, message = "required_field", "Field is required."
    elif error_type == "extra_forbidden":
        code, message = "unexpected_field", "Field is not allowed."
    else:
        code, message = "invalid_request_value", "Field value is invalid."
    raw_location = error.get("loc", ())
    location = raw_location if isinstance(raw_location, tuple) else ()
    path = [part for part in location if isinstance(part, (str, int))]
    return ErrorDetail(code=code, path=path, message=message)


def _route_template(request: Request) -> str:
    route = request.scope.get("route")
    path = getattr(route, "path", None)
    return normalize_http_route(path)


def _record_request_metrics(
    request: Request,
    started: float,
    *,
    status_code: int | None,
    outcome: Literal["completed", "unhandled_exception"],
) -> None:
    if request.scope.get("path") in {"/health", "/ready"}:
        return
    attributes = {
        "http.request.method": normalize_http_method(request.method),
        "http.route": _route_template(request),
        "http.response.status_class": status_class(status_code),
        "taskforge.outcome": outcome,
    }
    add_metric("taskforge.api.requests", attributes=attributes)
    record_metric(
        "taskforge.api.request.duration", perf_counter() - started, attributes
    )


def _trace_headers(request: Request) -> dict[str, str]:
    carrier: dict[str, str] = {}
    raw_headers = request.scope.get("headers", ())
    if not isinstance(raw_headers, (tuple, list)):
        return carrier
    for item in raw_headers:
        if not (
            isinstance(item, tuple)
            and len(item) == 2
            and isinstance(item[0], bytes)
            and isinstance(item[1], bytes)
        ):
            continue
        name = item[0].lower()
        if name not in {b"traceparent", b"tracestate"}:
            continue
        try:
            carrier[name.decode("ascii")] = item[1].decode("ascii")
        except UnicodeDecodeError:
            continue
    return carrier
