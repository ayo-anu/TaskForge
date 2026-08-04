"""Common safe API error envelopes and per-request identifiers."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from typing import Literal, cast
from uuid import UUID, uuid4

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

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
    422: ("validation_failed", "The request is invalid."),
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
        response = await call_next(request)
        response.headers[REQUEST_ID_HEADER] = str(request_id)
        return response


def install_error_handling(app: FastAPI) -> None:
    """Install one envelope implementation for framework and security failures."""
    app.add_middleware(RequestIDMiddleware)
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(Exception, unexpected_exception_handler)


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
    logger.error(
        "Unhandled API exception type=%s request_id=%s",
        type(exception).__name__,
        request.state.request_id,
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
