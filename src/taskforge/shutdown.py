"""Shared cooperative shutdown deadlines for process-owned cleanup phases."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import TypeVar

T = TypeVar("T")


class CooperativeShutdownDeadline:
    """One absolute event-loop deadline shared by sequential cleanup operations.

    Cancellation remains cooperative. A coroutine that suppresses cancellation may
    outlive this deadline and ultimately requires the deployment hard stop.
    """

    def __init__(self, timeout_seconds: float) -> None:
        self._expires_at = asyncio.get_running_loop().time() + timeout_seconds

    @property
    def expired(self) -> bool:
        return asyncio.get_running_loop().time() >= self._expires_at

    @property
    def remaining_seconds(self) -> float:
        return max(0.0, self._expires_at - asyncio.get_running_loop().time())

    async def wait(self, operation: Awaitable[T]) -> T:
        async with asyncio.timeout_at(self._expires_at):
            return await operation
