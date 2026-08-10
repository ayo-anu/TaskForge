"""FastAPI application construction for the Taskforge API process."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import cast

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from taskforge.api.authentication import (
    AuthenticationRuntimeProtocol,
    build_authentication_runtime,
)
from taskforge.api.dependencies import build_readiness_coordinator
from taskforge.api.errors import install_error_handling
from taskforge.api.health import (
    LivenessResponse,
    ReadinessCoordinator,
    ReadinessResponse,
)
from taskforge.api.principals import router as principals_router
from taskforge.api.runs import router as runs_router
from taskforge.api.workers import router as workers_router
from taskforge.api.workflows import router as workflows_router
from taskforge.settings import Settings
from taskforge.workflows.task_types import TaskTypeRegistry


def create_app(
    settings: Settings | None = None,
    readiness: ReadinessCoordinator | None = None,
    authentication: AuthenticationRuntimeProtocol | None = None,
    task_types: TaskTypeRegistry | None = None,
) -> FastAPI:
    """Create the API with injectable readiness behavior for focused tests."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        resolved_settings = settings or Settings()
        resolved_readiness = readiness or build_readiness_coordinator(resolved_settings)
        resolved_task_types = task_types or TaskTypeRegistry(())
        await resolved_readiness.start()
        try:
            resolved_authentication = authentication or build_authentication_runtime(
                resolved_settings,
                resolved_task_types,
            )
        except BaseException:
            await resolved_readiness.close()
            raise
        app.state.readiness = resolved_readiness
        app.state.authentication = resolved_authentication
        try:
            yield
        finally:
            await asyncio.gather(
                resolved_authentication.close(),
                resolved_readiness.close(),
                return_exceptions=True,
            )

    app = FastAPI(title="Taskforge API", lifespan=lifespan)
    install_error_handling(app)
    app.include_router(principals_router)
    app.include_router(workflows_router)
    app.include_router(runs_router)
    app.include_router(workers_router)

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
        if await coordinator.is_ready():
            return ReadinessResponse(ready=True)
        response = ReadinessResponse(ready=False)
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content=response.model_dump(),
        )

    return app
