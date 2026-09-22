"""Production orchestrator lifecycle and failure-precedence tests."""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any, cast

import pytest
from aio_pika.abc import AbstractChannel, AbstractConnection

import taskforge.orchestrator.application as application_module
from taskforge.orchestrator.application import (
    OrchestratorApplication,
    OrchestratorApplicationState,
    _watch_broker_resource,
)
from taskforge.orchestrator.domain import LoopExit, OrchestratorProcessFailure
from taskforge.runtime_provider import RuntimeProviderError
from taskforge.settings import OrchestratorSettings
from taskforge.workflows.task_types import TaskTypeRegistry


def settings() -> OrchestratorSettings:
    return OrchestratorSettings(
        postgres_password="postgres-secret",
        rabbitmq_password="rabbit-secret",
    )


class Resource:
    def __init__(self, name: str, events: list[str] | None = None) -> None:
        self.name = name
        self.events = events if events is not None else []
        self.is_closed = False
        self._closed = asyncio.Event()

    async def closed(self) -> None:
        await self._closed.wait()

    async def close(self) -> None:
        self.events.append(f"close:{self.name}")
        self.is_closed = True
        self._closed.set()

    def fail(self) -> None:
        self.is_closed = True
        self._closed.set()


class Engine:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def dispose(self) -> None:
        self.events.append("close:engine")


class Telemetry:
    def __init__(self, name: str, events: list[str]) -> None:
        self.name = name
        self.events = events

    def shutdown(self, *, timeout_seconds: float | None = None) -> None:
        del timeout_seconds
        self.events.append(f"close:{self.name}")


async def healthy_loop(application: OrchestratorApplication) -> LoopExit:
    await application._stop_scheduling.wait()
    return LoopExit.STOP_REQUESTED


def install_runtime(
    application: OrchestratorApplication,
    loops: dict[str, Coroutine[Any, Any, LoopExit]],
) -> tuple[Resource, Resource, tuple[asyncio.Task[Any], ...]]:
    connection = Resource("connection")
    channel = Resource("channel")
    application._connection = connection  # type: ignore[assignment]
    application._publisher_channel = channel  # type: ignore[assignment]
    application._loop_tasks = {
        name: asyncio.create_task(loop, name=f"test-{name}")
        for name, loop in loops.items()
    }
    application._resource_watchers = {
        "connection": asyncio.create_task(
            _watch_broker_resource(cast(AbstractConnection, connection), "connection"),
            name="test-connection-watch",
        ),
        "channel": asyncio.create_task(
            _watch_broker_resource(cast(AbstractChannel, channel), "channel"),
            name="test-channel-watch",
        ),
    }
    application.state = OrchestratorApplicationState.RUNNING
    return (
        connection,
        channel,
        (
            *application._loop_tasks.values(),
            *application._resource_watchers.values(),
        ),
    )


def assert_no_orphans(tasks: tuple[asyncio.Task[Any], ...]) -> None:
    assert all(task.done() for task in tasks)


def test_orchestrator_imports_catalog_surface_without_worker_profile_loader() -> None:
    assert hasattr(application_module, "load_installed_task_catalog")
    assert not hasattr(application_module, "load_installed_worker_profile")


def test_broker_composition_uses_ordinary_confirm_channel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        events: list[object] = []
        application = OrchestratorApplication(settings())
        connection = Resource("connection")
        channel = Resource("channel")
        exchange = object()

        async def connect(**kwargs: object) -> Resource:
            events.append(("connect", kwargs))

            async def open_channel(**channel_kwargs: object) -> Resource:
                events.append(("channel", channel_kwargs))
                return channel

            connection.channel = open_channel  # type: ignore[attr-defined]
            return connection

        async def declare(
            selected_channel: object,
            catalog: TaskTypeRegistry,
            configuration: object,
        ) -> object:
            assert selected_channel is channel
            assert isinstance(catalog, TaskTypeRegistry)
            events.append(("topology", configuration))
            return type("Topology", (), {"exchange": exchange})()

        monkeypatch.setattr(
            "taskforge.orchestrator.application.aio_pika.connect", connect
        )
        monkeypatch.setattr(application_module, "declare_dispatch_topology", declare)

        await application._connect_broker(TaskTypeRegistry(()))

        assert events[0][0] == "connect"  # type: ignore[index]
        assert events[1] == (
            "channel",
            {"publisher_confirms": True, "on_return_raises": True},
        )
        assert application._dispatch_exchange is exchange

    asyncio.run(scenario())


def test_stop_only_is_ordinary_success_and_leaves_no_supervision_tasks() -> None:
    async def scenario() -> None:
        application = OrchestratorApplication(settings())
        _connection, _channel, tasks = install_runtime(
            application,
            {"one": healthy_loop(application), "two": healthy_loop(application)},
        )
        supervision = asyncio.create_task(application._supervise())
        await asyncio.sleep(0)
        application.request_stop()
        await supervision

        assert not application.process_failed
        assert application.failure is None
        assert_no_orphans(tasks)

    asyncio.run(scenario())


def test_required_loop_failure_is_process_failure() -> None:
    async def scenario() -> None:
        application = OrchestratorApplication(settings())

        async def fail() -> LoopExit:
            raise RuntimeError("database unavailable")

        _connection, _channel, tasks = install_runtime(
            application,
            {"failure": fail(), "sibling": healthy_loop(application)},
        )
        with pytest.raises(OrchestratorProcessFailure):
            await application._supervise()
        assert application.process_failed
        assert isinstance(application.failure, RuntimeError)
        assert_no_orphans(tasks)

    asyncio.run(scenario())


@pytest.mark.parametrize("resource_name", ("connection", "channel"))
def test_broker_watcher_failure_is_process_failure(resource_name: str) -> None:
    async def scenario() -> None:
        application = OrchestratorApplication(settings())
        connection, channel, tasks = install_runtime(
            application, {"healthy": healthy_loop(application)}
        )
        (connection if resource_name == "connection" else channel).fail()
        with pytest.raises(OrchestratorProcessFailure):
            await application._supervise()
        assert application.process_failed
        assert_no_orphans(tasks)

    asyncio.run(scenario())


def test_stop_and_required_loop_failure_same_turn_gives_failure_precedence() -> None:
    async def scenario() -> None:
        application = OrchestratorApplication(settings())
        release = asyncio.Event()

        async def fail() -> LoopExit:
            await release.wait()
            raise RuntimeError("bounded pass failed")

        _connection, _channel, tasks = install_runtime(
            application,
            {"failure": fail(), "sibling": healthy_loop(application)},
        )
        supervision = asyncio.create_task(application._supervise())
        await asyncio.sleep(0)
        application.request_stop()
        release.set()
        with pytest.raises(OrchestratorProcessFailure):
            await supervision
        assert isinstance(application.failure, RuntimeError)
        assert_no_orphans(tasks)

    asyncio.run(scenario())


@pytest.mark.parametrize("resource_name", ("connection", "channel"))
def test_stop_and_broker_failure_same_turn_gives_failure_precedence(
    resource_name: str,
) -> None:
    async def scenario() -> None:
        application = OrchestratorApplication(settings())
        connection, channel, tasks = install_runtime(
            application, {"healthy": healthy_loop(application)}
        )
        supervision = asyncio.create_task(application._supervise())
        await asyncio.sleep(0)
        application.request_stop()
        (connection if resource_name == "connection" else channel).fail()
        with pytest.raises(OrchestratorProcessFailure):
            await supervision
        assert application.process_failed
        assert_no_orphans(tasks)

    asyncio.run(scenario())


def test_failure_while_ordinary_shutdown_joins_current_pass_wins() -> None:
    async def scenario() -> None:
        application = OrchestratorApplication(settings())
        entered = asyncio.Event()
        release = asyncio.Event()

        async def current_pass() -> LoopExit:
            entered.set()
            await release.wait()
            raise RuntimeError("current bounded pass failed")

        _connection, _channel, tasks = install_runtime(
            application,
            {"current": current_pass(), "sibling": healthy_loop(application)},
        )
        supervision = asyncio.create_task(application._supervise())
        await entered.wait()
        application.request_stop()
        await asyncio.sleep(0)
        release.set()
        with pytest.raises(OrchestratorProcessFailure):
            await supervision
        assert isinstance(application.failure, RuntimeError)
        assert_no_orphans(tasks)

    asyncio.run(scenario())


def test_unexpected_normal_loop_completion_is_process_failure() -> None:
    async def scenario() -> None:
        application = OrchestratorApplication(settings())

        async def return_early() -> LoopExit:
            return LoopExit.STOP_REQUESTED

        _connection, _channel, tasks = install_runtime(
            application, {"early": return_early()}
        )
        with pytest.raises(OrchestratorProcessFailure):
            await application._supervise()
        assert application.process_failed
        assert_no_orphans(tasks)

    asyncio.run(scenario())


def test_critical_loop_failure_during_startup_fails_closed_and_rolls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        events: list[str] = []
        application = OrchestratorApplication(settings())
        connection = Resource("connection", events)
        channel = Resource("channel", events)
        engine = Engine(events)

        class FailedWorkload:
            async def run_once(self) -> object:
                raise RuntimeError("startup pass failed")

        async def connect(catalog: TaskTypeRegistry) -> None:
            del catalog
            application._connection = connection  # type: ignore[assignment]
            application._publisher_channel = channel  # type: ignore[assignment]

        monkeypatch.setattr(application, "_configure_telemetry", lambda: None)
        monkeypatch.setattr(application, "_probe_database", lambda: asyncio.sleep(0))
        monkeypatch.setattr(application, "_connect_broker", connect)
        monkeypatch.setattr(
            application,
            "_compose_workloads",
            lambda sessions, catalog: {"failure": FailedWorkload()},
        )
        monkeypatch.setattr(
            application_module,
            "load_installed_task_catalog",
            lambda: TaskTypeRegistry(()),
        )
        monkeypatch.setattr(
            application_module, "build_async_engine", lambda value: engine
        )
        monkeypatch.setattr(
            application_module, "build_session_factory", lambda value: object()
        )

        with pytest.raises(OrchestratorProcessFailure):
            await application.start()
        assert application.process_failed
        assert application.state is OrchestratorApplicationState.STOPPED
        assert events == ["close:channel", "close:connection", "close:engine"]

    asyncio.run(scenario())


def test_partial_startup_failure_uses_reverse_exact_once_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        events: list[str] = []
        application = OrchestratorApplication(settings())

        def configure() -> None:
            application._metrics = Telemetry("metrics", events)  # type: ignore[assignment]
            application._tracing = Telemetry("tracing", events)  # type: ignore[assignment]

        monkeypatch.setattr(application, "_configure_telemetry", configure)
        monkeypatch.setattr(
            application_module,
            "load_installed_task_catalog",
            lambda: (_ for _ in ()).throw(RuntimeProviderError("missing catalog")),
        )

        with pytest.raises(RuntimeProviderError):
            await application.start()
        await application.close()
        assert application.state is OrchestratorApplicationState.STOPPED
        assert events == ["close:metrics", "close:tracing"]

    asyncio.run(scenario())


def test_incompatible_schema_fails_before_broker_and_workloads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        events: list[str] = []
        application = OrchestratorApplication(settings())
        engine = Engine(events)

        async def reject_schema(value: object) -> None:
            assert value is engine
            events.append("schema:rejected")
            raise RuntimeError("incompatible schema")

        monkeypatch.setattr(application, "_configure_telemetry", lambda: None)
        monkeypatch.setattr(
            application_module,
            "load_installed_task_catalog",
            lambda: TaskTypeRegistry(()),
        )
        monkeypatch.setattr(
            application_module, "build_async_engine", lambda value: engine
        )
        monkeypatch.setattr(
            application_module, "build_session_factory", lambda value: object()
        )
        monkeypatch.setattr(
            application_module, "require_compatible_schema", reject_schema
        )
        monkeypatch.setattr(
            application,
            "_connect_broker",
            lambda *args: pytest.fail("broker must not connect"),
        )

        with pytest.raises(RuntimeError, match="incompatible schema"):
            await application.start()

        assert events == ["schema:rejected", "close:engine"]
        assert application.state is OrchestratorApplicationState.STOPPED

    asyncio.run(scenario())


def test_owned_resources_close_in_reverse_order_exactly_once() -> None:
    async def scenario() -> None:
        events: list[str] = []
        application = OrchestratorApplication(settings())
        application.state = OrchestratorApplicationState.RUNNING
        application._engine = Engine(events)  # type: ignore[assignment]
        application._connection = Resource("connection", events)  # type: ignore[assignment]
        application._publisher_channel = Resource("channel", events)  # type: ignore[assignment]
        application._metrics = Telemetry("metrics", events)  # type: ignore[assignment]
        application._tracing = Telemetry("tracing", events)  # type: ignore[assignment]

        await asyncio.gather(application.close(), application.close())
        await application.close()

        assert events == [
            "close:channel",
            "close:connection",
            "close:engine",
            "close:metrics",
            "close:tracing",
        ]

    asyncio.run(scenario())


def test_cleanup_failure_is_monotonic_process_failure() -> None:
    async def scenario() -> None:
        events: list[str] = []
        application = OrchestratorApplication(settings())
        application.state = OrchestratorApplicationState.RUNNING

        class FailingChannel(Resource):
            async def close(self) -> None:
                self.events.append("close:channel")
                raise RuntimeError("channel cleanup failed")

        application._publisher_channel = FailingChannel(  # type: ignore[assignment]
            "channel", events
        )

        with pytest.raises(ExceptionGroup):
            await application.close()
        assert application.state is OrchestratorApplicationState.STOPPED
        assert application.process_failed
        assert isinstance(application.failure, ExceptionGroup)
        await application.close()
        assert events == ["close:channel"]

    asyncio.run(scenario())
