"""Explicit HTTP adaptation for Taskforge rate-limit decisions."""

from __future__ import annotations

from typing import Protocol, cast
from uuid import UUID

from fastapi import HTTPException, Request, status

from taskforge.rate_limits import RateLimiter, RateLimitPolicy, rate_limiter_for


class RateLimitRuntime(Protocol):
    rate_limiter: RateLimiter


async def require_rate_limit(
    request: Request,
    policy: RateLimitPolicy,
    kind: str,
    value: str | UUID,
) -> None:
    runtime = cast(RateLimitRuntime, request.app.state.authentication)
    decision = await rate_limiter_for(runtime).consume(policy, kind, value)
    if not decision.allowed:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="request rate limit exceeded",
            headers={"Retry-After": str(max(1, decision.retry_after_seconds))},
        )
