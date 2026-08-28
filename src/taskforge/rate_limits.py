"""Taskforge-specific shared rate controls with bounded degraded fallback."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from time import monotonic
from typing import Protocol, cast
from uuid import UUID

from taskforge.metrics import add as add_metric


class RateLimitPolicy(StrEnum):
    API_AUTH_NETWORK = "api_auth_network"
    API_AUTH_CREDENTIAL = "api_auth_credential"
    WORKER_AUTH_NETWORK = "worker_auth_network"
    WORKER_AUTH_CREDENTIAL = "worker_auth_credential"
    RUN_CREATE = "run_create"
    RUN_REPLAY = "run_replay"
    DEAD_LETTER_REDRIVE = "dead_letter_redrive"
    WORKER_REGISTER = "worker_register"
    WORKER_RESULT = "worker_result"
    WEBSOCKET_NETWORK = "websocket_network"
    WEBSOCKET_PRINCIPAL = "websocket_principal"


@dataclass(frozen=True)
class RateLimit:
    count: int
    window_seconds: int

    def __post_init__(self) -> None:
        if self.count < 1 or self.window_seconds < 1:
            raise ValueError("rate-limit values must be positive")


@dataclass(frozen=True)
class RateLimitDecision:
    allowed: bool
    retry_after_seconds: int
    degraded: bool = False


class RateLimitRepositoryUnavailable(Exception):
    """Shared rate state could not be accessed."""


class RateLimitRepository(Protocol):
    async def consume(
        self, policy: RateLimitPolicy, key_digest: bytes, limit: RateLimit
    ) -> RateLimitDecision: ...

    async def check(
        self, policy: RateLimitPolicy, key_digest: bytes, limit: RateLimit
    ) -> RateLimitDecision: ...


class RateLimitGate(Protocol):
    async def consume(
        self, policy: RateLimitPolicy, kind: str, value: str | UUID
    ) -> RateLimitDecision: ...

    async def check(
        self, policy: RateLimitPolicy, kind: str, value: str | UUID
    ) -> RateLimitDecision: ...


class AllowAllRateLimiter:
    """Compatibility injection for focused runtimes that omit infrastructure."""

    async def consume(
        self, policy: RateLimitPolicy, kind: str, value: str | UUID
    ) -> RateLimitDecision:
        return RateLimitDecision(True, 1)

    async def check(
        self, policy: RateLimitPolicy, kind: str, value: str | UUID
    ) -> RateLimitDecision:
        return RateLimitDecision(True, 1)


def rate_limiter_for(runtime: object) -> RateLimitGate:
    candidate = getattr(runtime, "rate_limiter", None)
    if candidate is None:
        raise RuntimeError("runtime is missing required rate limiter")
    return cast(RateLimitGate, candidate)


def rate_limit_key(policy: RateLimitPolicy, kind: str, value: str | UUID) -> bytes:
    canonical = f"{policy.value}\0{kind}\0{value}"
    return hashlib.sha256(canonical.encode()).digest()


@dataclass
class _LocalWindow:
    started_at: float
    count: int
    window_seconds: int


class BoundedLocalRateLimiter:
    """Degraded process-local defense that never evicts an active counter."""

    def __init__(
        self,
        *,
        capacity: int,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        if capacity < 1:
            raise ValueError("fallback capacity must be positive")
        self._capacity = capacity
        self._clock = clock
        self._windows: dict[tuple[RateLimitPolicy, bytes], _LocalWindow] = {}
        self._lock = asyncio.Lock()

    async def consume(
        self, policy: RateLimitPolicy, key_digest: bytes, limit: RateLimit
    ) -> RateLimitDecision:
        async with self._lock:
            now = self._clock()
            key = (policy, key_digest)
            window = self._windows.get(key)
            if window is not None and now - window.started_at >= limit.window_seconds:
                del self._windows[key]
                window = None
            if window is None:
                self._reclaim_expired(now)
                if len(self._windows) >= self._capacity:
                    return RateLimitDecision(False, limit.window_seconds, True)
                self._windows[key] = _LocalWindow(now, 1, limit.window_seconds)
                return RateLimitDecision(True, limit.window_seconds, True)
            retry_after = max(
                1, int(window.started_at + limit.window_seconds - now + 0.999999)
            )
            if window.count >= limit.count:
                return RateLimitDecision(False, retry_after, True)
            window.count += 1
            return RateLimitDecision(True, retry_after, True)

    async def check(
        self, policy: RateLimitPolicy, key_digest: bytes, limit: RateLimit
    ) -> RateLimitDecision:
        async with self._lock:
            now = self._clock()
            window = self._windows.get((policy, key_digest))
            if window is None or now - window.started_at >= limit.window_seconds:
                return RateLimitDecision(True, limit.window_seconds, True)
            retry_after = max(
                1, int(window.started_at + limit.window_seconds - now + 0.999999)
            )
            return RateLimitDecision(window.count < limit.count, retry_after, True)

    def _reclaim_expired(self, now: float) -> None:
        expired = [
            key
            for key, window in self._windows.items()
            if now - window.started_at >= window.window_seconds
        ]
        for key in expired:
            del self._windows[key]


class RateLimiter:
    """Prefer authoritative shared state and degrade explicitly when unavailable."""

    def __init__(
        self,
        repository: RateLimitRepository,
        fallback: BoundedLocalRateLimiter,
        limits: dict[RateLimitPolicy, RateLimit],
    ) -> None:
        self._repository = repository
        self._fallback = fallback
        self._limits = limits

    async def consume(
        self, policy: RateLimitPolicy, kind: str, value: str | UUID
    ) -> RateLimitDecision:
        return await self._decide("consume", policy, kind, value)

    async def check(
        self, policy: RateLimitPolicy, kind: str, value: str | UUID
    ) -> RateLimitDecision:
        return await self._decide("check", policy, kind, value)

    async def _decide(
        self, operation: str, policy: RateLimitPolicy, kind: str, value: str | UUID
    ) -> RateLimitDecision:
        configured = self._limits[policy]
        digest = rate_limit_key(policy, kind, value)
        try:
            decision = (
                await self._repository.consume(policy, digest, configured)
                if operation == "consume"
                else await self._repository.check(policy, digest, configured)
            )
        except RateLimitRepositoryUnavailable:
            decision = (
                await self._fallback.consume(policy, digest, configured)
                if operation == "consume"
                else await self._fallback.check(policy, digest, configured)
            )
        if operation == "consume":
            add_metric(
                "taskforge.rate_limit.decisions",
                attributes={
                    "taskforge.rate_limit.policy": policy.value,
                    "taskforge.outcome": (
                        "degraded_allowed"
                        if decision.degraded and decision.allowed
                        else "degraded_limited"
                        if decision.degraded
                        else "allowed"
                        if decision.allowed
                        else "limited"
                    ),
                },
            )
        return decision
