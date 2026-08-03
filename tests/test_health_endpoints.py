"""Focused tests for the unversioned operational health endpoints."""

from __future__ import annotations

import asyncio

import httpx2
import pytest
from fastapi import FastAPI
from pydantic import SecretStr

from taskforge.api.application import create_app
from taskforge.api.health import ReadinessCoordinator
from taskforge.settings import Settings


class FakeReadinessAdapter:
    """Controllable dependency adapter that never uses external services."""

    def __init__(
        self,
        *,
        result: bool = True,
        error: Exception | None = None,
        delay_seconds: float = 0,
    ) -> None:
        self.result = result
        self.error = error
        self.delay_seconds = delay_seconds
        self.start_count = 0
        self.check_count = 0
        self.close_count = 0

    async def start(self) -> None:
        self.start_count += 1

    async def is_ready(self) -> bool:
        self.check_count += 1
        if self.delay_seconds:
            await asyncio.sleep(self.delay_seconds)
        if self.error is not None:
            raise self.error
        return self.result

    async def close(self) -> None:
        self.close_count += 1


class ProbeBarrier:
    """Prove probes overlap without relying on elapsed wall-clock timing."""

    def __init__(self, participant_count: int) -> None:
        self._participant_count = participant_count
        self._arrived = 0
        self._all_arrived = asyncio.Event()

    async def arrive(self) -> None:
        self._arrived += 1
        if self._arrived == self._participant_count:
            self._all_arrived.set()
        await self._all_arrived.wait()


class CoordinatedAdapter(FakeReadinessAdapter):
    def __init__(self, barrier: ProbeBarrier) -> None:
        super().__init__()
        self._barrier = barrier

    async def is_ready(self) -> bool:
        self.check_count += 1
        await self._barrier.arrive()
        return True


def make_settings() -> Settings:
    return Settings(
        postgres_password=SecretStr("postgres-test-secret"),
        rabbitmq_password=SecretStr("rabbitmq-test-secret"),
    )


def make_app(
    *adapters: FakeReadinessAdapter,
    timeout_seconds: float = 0.05,
) -> FastAPI:
    readiness = ReadinessCoordinator(
        adapters=adapters,
        timeout_seconds=timeout_seconds,
    )
    return create_app(settings=make_settings(), readiness=readiness)


def request(app: FastAPI, path: str) -> httpx2.Response:
    async def send() -> httpx2.Response:
        transport = httpx2.ASGITransport(app=app)
        async with app.router.lifespan_context(app):
            async with httpx2.AsyncClient(
                transport=transport,
                base_url="http://testserver",
            ) as client:
                return await client.get(path)

    return asyncio.run(send())


def test_liveness_is_independent_of_dependencies() -> None:
    first = FakeReadinessAdapter(result=False)
    second = FakeReadinessAdapter(error=RuntimeError("broker-secret-detail"))

    response = request(make_app(first, second), "/health")

    assert response.status_code == 200
    assert response.json() == {"alive": True}
    assert first.check_count == 0
    assert second.check_count == 0
    assert first.start_count == first.close_count == 1
    assert second.start_count == second.close_count == 1


def test_readiness_succeeds_when_all_required_dependencies_are_ready() -> None:
    response = request(
        make_app(FakeReadinessAdapter(), FakeReadinessAdapter()),
        "/ready",
    )

    assert response.status_code == 200
    assert response.json() == {"ready": True}


@pytest.mark.parametrize("failed_adapter_index", (0, 1))
def test_each_required_dependency_can_make_readiness_fail(
    failed_adapter_index: int,
) -> None:
    adapters = [FakeReadinessAdapter(), FakeReadinessAdapter()]
    adapters[failed_adapter_index].result = False

    response = request(make_app(*adapters), "/ready")

    assert response.status_code == 503
    assert response.json() == {"ready": False}


def test_dependency_errors_are_normalized_without_detail_leakage() -> None:
    secret_detail = "postgresql://user:secret@internal-host:5432/taskforge"
    failing = FakeReadinessAdapter(error=RuntimeError(secret_detail))

    response = request(make_app(failing, FakeReadinessAdapter()), "/ready")

    assert response.status_code == 503
    assert response.json() == {"ready": False}
    assert secret_detail not in response.text
    assert "postgres" not in response.text.lower()
    assert "rabbit" not in response.text.lower()


def test_dependency_timeout_makes_readiness_fail() -> None:
    slow = FakeReadinessAdapter(delay_seconds=0.1)

    response = request(
        make_app(slow, FakeReadinessAdapter(), timeout_seconds=0.01),
        "/ready",
    )

    assert response.status_code == 503
    assert response.json() == {"ready": False}


def test_required_dependency_checks_run_concurrently() -> None:
    barrier = ProbeBarrier(participant_count=2)

    response = request(
        make_app(
            CoordinatedAdapter(barrier),
            CoordinatedAdapter(barrier),
            timeout_seconds=0.1,
        ),
        "/ready",
    )

    assert response.status_code == 200
    assert response.json() == {"ready": True}


def test_operational_endpoints_are_intentionally_unversioned() -> None:
    schema = request(
        make_app(FakeReadinessAdapter(), FakeReadinessAdapter()),
        "/openapi.json",
    ).json()

    assert "/health" in schema["paths"]
    assert "/ready" in schema["paths"]
    assert "/api/v1/health" not in schema["paths"]
    assert "/api/v1/ready" not in schema["paths"]
    assert "Unversioned operational" in schema["paths"]["/health"]["get"]["summary"]
    assert "Unversioned operational" in schema["paths"]["/ready"]["get"]["summary"]
