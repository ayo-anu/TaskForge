"""Count active HTTP requests without owning server admission."""

from __future__ import annotations

import asyncio

from starlette.types import ASGIApp, Receive, Scope, Send


class ActiveHTTPRequestTracker:
    """Track physical HTTP-handler lifetime for shared-resource teardown."""

    def __init__(self) -> None:
        self._condition = asyncio.Condition()
        self._active = 0

    @property
    def active(self) -> int:
        return self._active

    async def enter(self) -> None:
        async with self._condition:
            self._active += 1

    async def leave(self) -> None:
        async with self._condition:
            if self._active < 1:
                raise RuntimeError("HTTP request tracking underflow")
            self._active -= 1
            if self._active == 0:
                self._condition.notify_all()

    async def wait_empty(self) -> None:
        async with self._condition:
            await self._condition.wait_for(lambda: self._active == 0)


class TrackActiveHTTPRequests:
    """ASGI middleware that observes HTTP work; Uvicorn owns admission."""

    def __init__(self, app: ASGIApp, *, tracker: ActiveHTTPRequestTracker) -> None:
        self._app = app
        self._tracker = tracker

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        await self._tracker.enter()
        try:
            await self._app(scope, receive, send)
        finally:
            await self._tracker.leave()
