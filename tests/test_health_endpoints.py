"""Focused tests for API-owned operational health and readiness."""

from __future__ import annotations

import asyncio

import httpx2
import pytest
from fastapi import FastAPI
from pydantic import SecretStr

import taskforge.api.health as health_module
from taskforge.api.application import create_app
from taskforge.api.health import ReadinessCoordinator
from taskforge.settings import Settings


class FakePostgreSQLProbe:
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
        self.check_count = 0

    async def is_ready(self) -> bool:
        self.check_count += 1
        if self.delay_seconds:
            await asyncio.sleep(self.delay_seconds)
        if self.error is not None:
            raise self.error
        return self.result


class FakeAuthentication:
    api_authenticator = object()
    worker_authenticator = object()

    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def make_settings() -> Settings:
    return Settings(
        postgres_password=SecretStr("postgres-test-secret"),
        rabbitmq_password=SecretStr("rabbitmq-test-secret"),
    )


def make_app(
    probe: FakePostgreSQLProbe,
    *,
    listener_available: bool | None = None,
    timeout_seconds: float = 0.05,
) -> FastAPI:
    readiness = ReadinessCoordinator(probe, timeout_seconds=timeout_seconds)
    if listener_available is not None:
        readiness.observe_execution_stream(listener_available)
    return create_app(
        settings=make_settings(),
        readiness=readiness,
        authentication=FakeAuthentication(),  # type: ignore[arg-type]
    )


def request(app: FastAPI, path: str) -> httpx2.Response:
    async def send() -> httpx2.Response:
        transport = httpx2.ASGITransport(app=app)
        async with app.router.lifespan_context(app):
            async with httpx2.AsyncClient(
                transport=transport, base_url="http://testserver"
            ) as client:
                return await client.get(path)

    return asyncio.run(send())


def test_liveness_is_no_io_after_startup_and_unchanged() -> None:
    probe = FakePostgreSQLProbe(result=False)
    app = make_app(probe)

    async def exercise() -> tuple[httpx2.Response, int, int]:
        transport = httpx2.ASGITransport(app=app)
        async with app.router.lifespan_context(app):
            before = probe.check_count
            async with httpx2.AsyncClient(
                transport=transport, base_url="http://testserver"
            ) as client:
                response = await client.get("/health")
            return response, before, probe.check_count

    response, before, after = asyncio.run(exercise())
    assert response.status_code == 200
    assert response.json() == {"alive": True}
    assert before == after == 1


def test_ready_is_ready_when_postgresql_and_listener_are_available() -> None:
    response = request(
        make_app(FakePostgreSQLProbe(), listener_available=True), "/ready"
    )
    assert response.status_code == 200
    assert response.json() == {"ready": True, "status": "ready"}


def test_listener_unknown_or_unavailable_is_degraded() -> None:
    for listener in (None, False):
        response = request(
            make_app(FakePostgreSQLProbe(), listener_available=listener), "/ready"
        )
        assert response.status_code == 200
        assert response.json() == {"ready": True, "status": "degraded"}


def test_postgresql_unavailable_is_not_ready() -> None:
    response = request(
        make_app(FakePostgreSQLProbe(result=False), listener_available=True), "/ready"
    )
    assert response.status_code == 503
    assert response.json() == {"ready": False, "status": "not_ready"}


def test_every_ready_request_performs_a_live_probe_and_recovers() -> None:
    probe = FakePostgreSQLProbe(result=False)
    app = make_app(probe, listener_available=True)

    async def exercise() -> tuple[httpx2.Response, httpx2.Response]:
        transport = httpx2.ASGITransport(app=app)
        async with app.router.lifespan_context(app):
            async with httpx2.AsyncClient(
                transport=transport, base_url="http://testserver"
            ) as client:
                first = await client.get("/ready")
                probe.result = True
                second = await client.get("/ready")
                return first, second

    first, second = asyncio.run(exercise())
    assert first.status_code == 503
    assert second.status_code == 200
    assert second.json() == {"ready": True, "status": "ready"}
    assert probe.check_count == 3


def test_dependency_errors_and_timeouts_are_safe() -> None:
    secret = "postgresql://user:sentinel-secret@internal-host/taskforge"
    error_response = request(
        make_app(FakePostgreSQLProbe(error=RuntimeError(secret))), "/ready"
    )
    timeout_response = request(
        make_app(FakePostgreSQLProbe(delay_seconds=0.1), timeout_seconds=0.01),
        "/ready",
    )
    for response in (error_response, timeout_response):
        assert response.status_code == 503
        assert response.json() == {"ready": False, "status": "not_ready"}
        assert secret not in response.text
        assert "postgres" not in response.text.lower()


def test_readiness_is_withdrawn_before_authentication_close() -> None:
    probe = FakePostgreSQLProbe()
    readiness = ReadinessCoordinator(probe, timeout_seconds=0.05)

    class InspectingAuthentication(FakeAuthentication):
        async def close(self) -> None:
            assert (await readiness.snapshot()).status == "not_ready"
            await super().close()

    authentication = InspectingAuthentication()
    app = create_app(
        settings=make_settings(),
        readiness=readiness,
        authentication=authentication,  # type: ignore[arg-type]
    )

    async def exercise() -> None:
        async with app.router.lifespan_context(app):
            pass

    asyncio.run(exercise())
    assert authentication.closed is True


def test_operational_endpoints_are_unversioned() -> None:
    schema = request(make_app(FakePostgreSQLProbe()), "/openapi.json").json()
    assert "/health" in schema["paths"]
    assert "/ready" in schema["paths"]
    assert "/api/v1/health" not in schema["paths"]
    assert "/api/v1/ready" not in schema["paths"]


def test_transition_telemetry_is_bounded_deduplicated_and_initially_meaningful(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logs: list[tuple[str, dict[str, object]]] = []
    metrics: list[tuple[str, dict[str, str | bool] | None]] = []

    def capture_log(
        _logger: object,
        _level: int,
        event: str,
        fields: dict[str, object],
    ) -> None:
        logs.append((event, fields))

    def capture_metric(
        name: str,
        _value: int = 1,
        attributes: dict[str, str | bool] | None = None,
    ) -> None:
        metrics.append((name, attributes))

    monkeypatch.setattr(health_module, "log_event", capture_log)
    monkeypatch.setattr(health_module, "add_metric", capture_metric)
    readiness = ReadinessCoordinator(FakePostgreSQLProbe(), timeout_seconds=0.05)
    readiness.observe_execution_stream(True)

    async def exercise() -> None:
        await readiness.start()
        await readiness.snapshot()
        await readiness.snapshot()

    asyncio.run(exercise())
    readiness.observe_execution_stream(True)

    readiness_statuses = [
        fields["readiness.status"]
        for event, fields in logs
        if event == "process.readiness.changed"
    ]
    assert readiness_statuses == ["ready"]
    assert (
        sum(name == "taskforge.process.readiness.transitions" for name, _ in metrics)
        == 1
    )
    assert all(
        set(fields) <= {"dependency.name", "dependency.state", "reason.code"}
        if event == "dependency.state.changed"
        else set(fields) <= {"readiness.status", "reason.code"}
        for event, fields in logs
    )


def test_telemetry_failures_do_not_change_readiness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("telemetry sentinel secret")

    monkeypatch.setattr(health_module, "log_event", fail)
    monkeypatch.setattr(health_module, "add_metric", fail)
    readiness = ReadinessCoordinator(FakePostgreSQLProbe(), timeout_seconds=0.05)
    readiness.observe_execution_stream(True)

    assert asyncio.run(readiness.start()).status == "ready"
    assert asyncio.run(readiness.snapshot()).status == "ready"


def test_listener_loss_signals_degradation_but_shutdown_teardown_is_silent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logs: list[tuple[str, dict[str, object]]] = []
    metrics: list[tuple[str, dict[str, str | bool] | None]] = []

    def capture_log(
        _logger: object,
        _level: int,
        event: str,
        fields: dict[str, object],
    ) -> None:
        logs.append((event, fields))

    def capture_metric(
        name: str,
        _value: int = 1,
        attributes: dict[str, str | bool] | None = None,
    ) -> None:
        metrics.append((name, attributes))

    monkeypatch.setattr(health_module, "log_event", capture_log)
    monkeypatch.setattr(health_module, "add_metric", capture_metric)
    readiness = ReadinessCoordinator(FakePostgreSQLProbe(), timeout_seconds=0.05)
    readiness.observe_execution_stream(True)
    assert asyncio.run(readiness.start()).status == "ready"

    logs.clear()
    metrics.clear()
    readiness.observe_execution_stream(False)
    assert asyncio.run(readiness.snapshot()).status == "degraded"
    assert logs == [
        (
            "dependency.state.changed",
            {
                "dependency.name": "execution_stream",
                "dependency.state": "unavailable",
                "reason.code": "connection_lost",
            },
        ),
        ("process.readiness.changed", {"readiness.status": "degraded"}),
    ]
    assert metrics == [
        (
            "taskforge.dependency.state.transitions",
            {
                "taskforge.dependency": "execution_stream",
                "taskforge.dependency.state": "unavailable",
            },
        ),
        (
            "taskforge.process.readiness.transitions",
            {"taskforge.readiness.status": "degraded"},
        ),
    ]

    readiness.observe_execution_stream(True)
    logs.clear()
    metrics.clear()
    readiness.withdraw()
    readiness.observe_execution_stream(False)
    assert logs == [
        (
            "process.readiness.changed",
            {"readiness.status": "not_ready", "reason.code": "stopping"},
        )
    ]
    assert metrics == [
        (
            "taskforge.process.readiness.transitions",
            {"taskforge.readiness.status": "not_ready"},
        )
    ]
