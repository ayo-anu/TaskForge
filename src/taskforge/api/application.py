"""FastAPI application construction for the Taskforge API process."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import cast

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute

from taskforge.api.authentication import (
    AuthenticationRuntime,
    AuthenticationRuntimeProtocol,
    build_authentication_runtime,
)
from taskforge.api.claims import router as claims_router
from taskforge.api.dead_letters import router as dead_letters_router
from taskforge.api.dependencies import build_readiness_coordinator
from taskforge.api.errors import install_error_handling
from taskforge.api.execution_stream import router as execution_stream_router
from taskforge.api.execution_stream import serialize_execution_event
from taskforge.api.execution_stream_runtime import ExecutionStreamRuntime
from taskforge.api.health import (
    LivenessResponse,
    ReadinessCoordinator,
    ReadinessResponse,
)
from taskforge.api.history import router as history_router
from taskforge.api.principals import router as principals_router
from taskforge.api.request_tracking import (
    ActiveHTTPRequestTracker,
    TrackActiveHTTPRequests,
)
from taskforge.api.runs import router as runs_router
from taskforge.api.workers import router as workers_router
from taskforge.api.workflows import router as workflows_router
from taskforge.logging import log_event
from taskforge.metrics import register_http_routes
from taskforge.runtime_provider import load_installed_task_catalog
from taskforge.settings import Settings
from taskforge.shutdown import CooperativeShutdownDeadline
from taskforge.workflows.task_types import TaskTypeRegistry

logger = logging.getLogger(__name__)


def create_app(
    settings: Settings | None = None,
    readiness: ReadinessCoordinator | None = None,
    authentication: AuthenticationRuntimeProtocol | None = None,
    task_types: TaskTypeRegistry | None = None,
) -> FastAPI:
    """Create the API with injectable readiness behavior for focused tests."""
    resolved_settings = settings or Settings()
    request_tracker = ActiveHTTPRequestTracker()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        resolved_task_types = task_types or TaskTypeRegistry(())
        resolved_authentication = authentication or build_authentication_runtime(
            resolved_settings,
            resolved_task_types,
        )
        if readiness is not None:
            resolved_readiness = readiness
        elif isinstance(resolved_authentication, AuthenticationRuntime):
            resolved_readiness = build_readiness_coordinator(
                resolved_settings, resolved_authentication.engine
            )
        else:
            raise TypeError("injected authentication requires injected readiness")
        app.state.readiness = resolved_readiness
        app.state.authentication = resolved_authentication
        resolved_execution_stream: ExecutionStreamRuntime | None = None
        try:
            if isinstance(resolved_authentication, AuthenticationRuntime):
                resolved_execution_stream = ExecutionStreamRuntime(
                    resolved_settings,
                    resolved_authentication.workflow_run_execution_event_repository,
                    serialize_execution_event,
                    availability_changed=resolved_readiness.observe_execution_stream,
                )
                await resolved_execution_stream.start()
                app.state.execution_stream = resolved_execution_stream
            await resolved_readiness.start()
            yield
        finally:
            resolved_readiness.withdraw()
            stream_deadline = CooperativeShutdownDeadline(
                resolved_settings.resource_shutdown_timeout_seconds
            )
            if resolved_execution_stream is not None:
                stream_close = asyncio.create_task(resolved_execution_stream.close())
                try:
                    await stream_deadline.wait(asyncio.shield(stream_close))
                except TimeoutError:
                    log_event(
                        logger,
                        logging.WARNING,
                        "api.shutdown.cancellation_requested",
                        {"reason.code": "resource_shutdown_timeout"},
                    )
                    stream_close.cancel()
                    # The stream may use the authentication engine. Do not close
                    # that shared engine while teardown is still physically live.
                    await asyncio.gather(stream_close, return_exceptions=True)
            # Uvicorn owns admission. This wait protects only resources shared by
            # HTTP handlers and intentionally does not delay stream shutdown.
            await request_tracker.wait_empty()
            shared_resource_deadline = CooperativeShutdownDeadline(
                resolved_settings.resource_shutdown_timeout_seconds
            )
            try:
                await shared_resource_deadline.wait(
                    asyncio.gather(
                        resolved_authentication.close(),
                        resolved_readiness.close(),
                        return_exceptions=True,
                    )
                )
            except TimeoutError:
                log_event(
                    logger,
                    logging.WARNING,
                    "api.shutdown.cancellation_requested",
                    {"reason.code": "resource_shutdown_timeout"},
                )

    app = FastAPI(title="Taskforge API", lifespan=lifespan)
    app.state.settings = resolved_settings
    app.state.active_http_requests = request_tracker
    app.add_middleware(TrackActiveHTTPRequests, tracker=request_tracker)
    install_error_handling(
        app, max_request_body_bytes=resolved_settings.api_max_request_body_bytes
    )
    app.include_router(principals_router)
    app.include_router(claims_router)
    app.include_router(workflows_router)
    app.include_router(runs_router)
    app.include_router(workers_router)
    app.include_router(dead_letters_router)
    app.include_router(execution_stream_router)
    app.include_router(history_router)

    @app.get(
        "/health",
        response_model=LivenessResponse,
        tags=["operations"],
        summary="Unversioned operational liveness probe",
    )
    async def health() -> LivenessResponse:
        """Report process liveness without contacting external dependencies."""
        return LivenessResponse()

    @app.get(
        "/ready",
        response_model=ReadinessResponse,
        responses={status.HTTP_503_SERVICE_UNAVAILABLE: {"model": ReadinessResponse}},
        tags=["operations"],
        summary="Unversioned operational readiness probe",
    )
    async def ready(request: Request) -> ReadinessResponse | JSONResponse:
        """Report whether every currently required API dependency is usable."""
        coordinator = cast(ReadinessCoordinator, request.app.state.readiness)
        snapshot = await coordinator.snapshot()
        response = ReadinessResponse(ready=snapshot.ready, status=snapshot.status)
        if snapshot.ready:
            return response
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content=response.model_dump(),
        )

    register_http_routes(
        route.path for route in app.routes if isinstance(route, APIRoute)
    )

    return app


def create_production_app() -> FastAPI:
    """Create the API from the installed metadata-only trusted task catalog."""
    return create_app(task_types=load_installed_task_catalog())
