"""Focused fixed-window and degraded-fallback rate-limit tests."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from taskforge.rate_limits import (
    BoundedLocalRateLimiter,
    RateLimit,
    RateLimiter,
    RateLimitPolicy,
    RateLimitRepositoryUnavailable,
    rate_limiter_for,
)


class Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


class UnavailableRepository:
    async def consume(self, *args: object) -> object:
        raise RateLimitRepositoryUnavailable

    async def check(self, *args: object) -> object:
        raise RateLimitRepositoryUnavailable


def test_runtime_must_explicitly_configure_a_rate_limiter() -> None:
    with pytest.raises(RuntimeError, match="missing required rate limiter"):
        rate_limiter_for(object())


def test_local_fallback_exact_threshold_reset_and_identity_isolation() -> None:
    async def exercise() -> None:
        clock = Clock()
        fallback = BoundedLocalRateLimiter(capacity=3, clock=clock)
        policy = RateLimitPolicy.RUN_CREATE
        configured = RateLimit(2, 10)
        first, second, rejected = [
            await fallback.consume(policy, b"a", configured) for _ in range(3)
        ]
        independent = await fallback.consume(policy, b"b", configured)
        assert [first.allowed, second.allowed, rejected.allowed] == [True, True, False]
        assert independent.allowed
        assert all(item.degraded for item in (first, second, rejected, independent))
        clock.now += 10
        assert (await fallback.consume(policy, b"a", configured)).allowed

    asyncio.run(exercise())


def test_full_fallback_never_evicts_active_keys_for_random_churn() -> None:
    async def exercise() -> None:
        clock = Clock()
        fallback = BoundedLocalRateLimiter(capacity=2, clock=clock)
        configured = RateLimit(2, 60)
        assert (
            await fallback.consume(RateLimitPolicy.RUN_CREATE, b"first", configured)
        ).allowed
        assert (
            await fallback.consume(RateLimitPolicy.RUN_CREATE, b"second", configured)
        ).allowed
        unseen = await fallback.consume(
            RateLimitPolicy.RUN_CREATE, b"attacker-new-key", configured
        )
        assert not unseen.allowed and unseen.degraded
        assert (
            await fallback.consume(RateLimitPolicy.RUN_CREATE, b"first", configured)
        ).allowed

    asyncio.run(exercise())


def test_shared_failure_uses_explicit_degraded_fallback() -> None:
    async def exercise() -> None:
        limiter = RateLimiter(
            UnavailableRepository(),  # type: ignore[arg-type]
            BoundedLocalRateLimiter(capacity=2),
            {RateLimitPolicy.WORKER_REGISTER: RateLimit(1, 60)},
        )
        worker_id = uuid4()
        first = await limiter.consume(
            RateLimitPolicy.WORKER_REGISTER, "worker_identity", worker_id
        )
        second = await limiter.consume(
            RateLimitPolicy.WORKER_REGISTER, "worker_identity", worker_id
        )
        assert first.allowed and first.degraded
        assert not second.allowed and second.degraded

    asyncio.run(exercise())
