"""Protocol-neutral liveness and readiness behavior for the API process."""

from __future__ import annotations

import asyncio
from typing import Literal, Protocol

from pydantic import BaseModel


class LivenessResponse(BaseModel):
    """Safe response body for the unversioned operational liveness endpoint."""

    alive: Literal[True] = True


class ReadinessResponse(BaseModel):
    """Safe response body for the unversioned operational readiness endpoint."""

    ready: bool


class ReadinessAdapter(Protocol):
    """Lifecycle and probe contract for one required API dependency."""

    async def start(self) -> None:
        """Initialize local client resources without requiring connectivity."""

    async def is_ready(self) -> bool:
        """Return whether the dependency can currently serve the API."""

    async def close(self) -> None:
        """Release resources owned by the adapter."""


class ReadinessCoordinator:
    """Run required dependency probes concurrently behind a safe result."""

    def __init__(
        self,
        adapters: tuple[ReadinessAdapter, ...],
        timeout_seconds: float,
    ) -> None:
        self._adapters = adapters
        self._timeout_seconds = timeout_seconds

    async def start(self) -> None:
        """Start every adapter and clean up if initialization is partial."""
        started: list[ReadinessAdapter] = []
        try:
            for adapter in self._adapters:
                await adapter.start()
                started.append(adapter)
        except BaseException:
            await asyncio.gather(
                *(adapter.close() for adapter in reversed(started)),
                return_exceptions=True,
            )
            raise

    async def is_ready(self) -> bool:
        """Return true only when every required dependency is responsive."""
        results = await asyncio.gather(
            *(self._check(adapter) for adapter in self._adapters)
        )
        return all(results)

    async def close(self) -> None:
        """Close every adapter even if another adapter fails to close."""
        await asyncio.gather(
            *(adapter.close() for adapter in reversed(self._adapters)),
            return_exceptions=True,
        )

    async def _check(self, adapter: ReadinessAdapter) -> bool:
        try:
            async with asyncio.timeout(self._timeout_seconds):
                return await adapter.is_ready()
        except asyncio.CancelledError:
            raise
        except Exception:
            return False
