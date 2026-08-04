"""Common safe API error envelopes and per-request identifiers."""

from __future__ import annotations

from collections.abc import Mapping
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


class ErrorBody(BaseModel):
    version: Literal["1"] = "1"
    code: str
    message: str
    request_id: UUID


class ErrorResponse(BaseModel):
    error: ErrorBody


ERROR_CONTRACTS = {
    401: ("authentication_required", "Authentication is required."),
    403: ("forbidden", "Access is forbidden."),
    404: ("resource_not_found", "The requested resource was not found."),
    422: ("validation_failed", "The request is invalid."),
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
    del exception
    code, message = ERROR_CONTRACTS[422]
    return error_response(
        request,
        status_code=422,
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
) -> JSONResponse:
    request_id = cast(UUID, request.state.request_id)
    body = ErrorResponse(
        error=ErrorBody(
            code=code,
            message=message,
            request_id=request_id,
        )
    )
    return JSONResponse(
        status_code=status_code,
        content=body.model_dump(mode="json"),
        headers=headers,
    )
