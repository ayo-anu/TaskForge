"""Production worker composition, supervision, and ownership tests."""

from __future__ import annotations

import asyncio
import os
import signal
from datetime import UTC, datetime
from typing import Any, cast
from uuid import uuid4

import pytest

import taskforge.worker.application as application_module
from taskforge.identity.authentication import (
    AuthenticatedWorker,
    AuthenticationFailure,
    AuthenticationFailureReason,
    AuthenticationUnavailable,
)
from taskforge.identity.credentials import CredentialScope, generate_credential
from taskforge.rate_limits import RateLimiter
from taskforge.runtime_provider import ResolvedWorkerProfile, RuntimeProviderError
from taskforge.settings import WorkerSettings
from taskforge.worker.application import WorkerApplication, WorkerApplicationState
from taskforge.worker.domain import RegisteredWorkerSession
from taskforge.worker.handlers import (
    TaskHandlerDefinition,
    TaskHandlerRegistry,
)
from taskforge.worker.lifecycle import WorkerDispatchRuntime
from taskforge.worker.runtime_errors import WorkerProcessFailure
from taskforge.workflows.task_types import (
    JSONMapping,
    TaskTypeDefinition,
    TaskTypeRegistry,
    WorkflowValidationIssue,
)


class Validator:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        del parameters
        return ()


async def handler(context: Any) -> object:
    del context
    return {"ok": True}


def settings() -> WorkerSettings:
    generated = generate_credential(CredentialScope.WORKER)
    return WorkerSettings(
        postgres_password="postgres-secret",
        rabbitmq_password="rabbit-secret",
        worker_credential=generated.take_presented_value(),
        worker_profile="selected",
    )


def provider() -> tuple[TaskTypeRegistry, ResolvedWorkerProfile]:
    catalog = TaskTypeRegistry(
        (TaskTypeDefinition("test.task", "test-capability", Validator()),)
    )
    handlers = TaskHandlerRegistry(
        (TaskHandlerDefinition("test.task", "test-capability", handler),), catalog
    )
    return catalog, ResolvedWorkerProfile("selected", handlers, ("test-capability",))


class Resource:
    def __init__(self, name: str, events: list[str]) -> None:
        self.name = name
        self.events = events
        self.is_closed = False
        self._closed = asyncio.Event()

    async def close(self) -> None:
        self.events.append(f"close:{self.name}")
        self.is_closed = True
        self._closed.set()

    async def closed(self) -> None:
        await self._closed.wait()


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


class Runtime:
    def __init__(self, name: str, events: list[str]) -> None:
        self.name = name
        self.events = events
        self._failure: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._closed = False

    @property
    def in_flight(self) -> int:
        return 0

    async def activate(self) -> None:
        self.events.append(f"activate:{self.name}")

    async def shutdown(self) -> None:
        if not self._closed:
            self.events.append(f"close:{self.name}")
            self._closed = True

    async def begin_shutdown(self) -> None:
        if not self._closed:
            self.events.append(f"close:{self.name}")
            self._closed = True

    async def wait_drained(self) -> None:
        return

    async def cancel_in_flight(self) -> int:
        return 0

    async def wait_failed(self) -> None:
        await self._failure


class Heartbeat:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self._failure: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    async def send_initial(self) -> None:
        self.events.append("heartbeat:1")

    def start(self) -> None:
        self.events.append("heartbeat:start")

    async def begin_draining(self) -> None:
        self.events.append("heartbeat:draining")

    async def wait_failed(self) -> None:
        await self._failure

    async def close(self) -> None:
        self.events.append("close:heartbeat")


def test_valid_start_uses_paused_admission_then_heartbeat_then_activation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        events: list[str] = []
        catalog, profile = provider()
        application = WorkerApplication(settings())
        engine = Engine(events)
        connection = Resource("connection", events)
        topology = Resource("topology", events)
        channel = Resource("consumer-channel", events)
        runtime = Runtime("runtime", events)
        authenticated = AuthenticatedWorker(uuid4(), uuid4())
        session_id = uuid4()

        def configure() -> None:
            events.append("telemetry")
            application._metrics = Telemetry("metrics", events)  # type: ignore[assignment]
            application._tracing = Telemetry("tracing", events)  # type: ignore[assignment]

        class Authenticator:
            async def authenticate(self, presented: Any) -> AuthenticatedWorker:
                del presented
                events.append("authenticate")
                return authenticated

        class Registration:
            async def register(
                self, worker: AuthenticatedWorker, capabilities: tuple[str, ...]
            ) -> RegisteredWorkerSession:
                assert worker is authenticated
                events.append("register")
                return RegisteredWorkerSession(
                    session_id, datetime.now(UTC), capabilities
                )

        heartbeat = Heartbeat(events)

        async def connect_broker(
            resolved_catalog: TaskTypeRegistry,
            resolved_profile: ResolvedWorkerProfile,
        ) -> None:
            assert resolved_catalog is catalog
            assert resolved_profile is profile
            events.append("broker")
            application._connection = connection  # type: ignore[assignment]
            application._topology_channel = topology  # type: ignore[assignment]
            application._consumer_channels.append(channel)  # type: ignore[arg-type]
            application._consumers.append(object())  # type: ignore[arg-type]

        async def start_consumers(
            resolved_profile: ResolvedWorkerProfile, execution: Any
        ) -> None:
            assert resolved_profile is profile
            assert execution is not None
            assert isinstance(
                cast(Any, execution)._result_service._rate_limiter, RateLimiter
            )
            events.append("consumers:paused")
            application._runtimes.append(runtime)  # type: ignore[arg-type]

        monkeypatch.setattr(application, "_configure_telemetry", configure)
        monkeypatch.setattr(application, "_connect_broker", connect_broker)
        monkeypatch.setattr(application, "_start_consumers", start_consumers)
        monkeypatch.setattr(
            "taskforge.worker.application.load_installed_task_catalog", lambda: catalog
        )
        monkeypatch.setattr(
            "taskforge.worker.application.load_installed_worker_profile",
            lambda name, resolved: profile,
        )
        monkeypatch.setattr(
            "taskforge.worker.application.build_async_engine", lambda value: engine
        )
        monkeypatch.setattr(
            "taskforge.worker.application.build_session_factory", lambda value: object()
        )

        async def accept_schema(value: object) -> None:
            assert value is engine
            events.append("schema")

        monkeypatch.setattr(
            "taskforge.worker.application.require_compatible_schema", accept_schema
        )
        monkeypatch.setattr(
            "taskforge.worker.application.WorkerAuthenticator",
            lambda *args, **kwargs: Authenticator(),
        )
        monkeypatch.setattr(
            "taskforge.worker.application.WorkerRegistrationService",
            lambda *args, **kwargs: Registration(),
        )
        monkeypatch.setattr(
            "taskforge.worker.application.WorkerHeartbeatSupervisor",
            lambda *args, **kwargs: heartbeat,
        )

        await application.start()

        assert application.state is WorkerApplicationState.RUNNING
        assert events[:8] == [
            "telemetry",
            "schema",
            "authenticate",
            "broker",
            "register",
            "consumers:paused",
            "heartbeat:1",
            "heartbeat:start",
        ]
        assert events[8] == "activate:runtime"
        await application.close()

    asyncio.run(scenario())


def test_partial_startup_failure_uses_the_same_reverse_cleanup_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        events: list[str] = []
        application = WorkerApplication(settings())

        def configure() -> None:
            application._metrics = Telemetry("metrics", events)  # type: ignore[assignment]
            application._tracing = Telemetry("tracing", events)  # type: ignore[assignment]

        monkeypatch.setattr(application, "_configure_telemetry", configure)
        monkeypatch.setattr(
            "taskforge.worker.application.load_installed_task_catalog",
            lambda: (_ for _ in ()).throw(RuntimeProviderError("missing")),
        )

        with pytest.raises(RuntimeProviderError):
            await application.start()
        assert application.state is WorkerApplicationState.STOPPED
        assert events == ["close:metrics", "close:tracing"]
        await application.close()
        assert events == ["close:metrics", "close:tracing"]

    asyncio.run(scenario())


def test_incompatible_schema_fails_before_worker_authority_or_broker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        events: list[str] = []
        catalog, profile = provider()
        application = WorkerApplication(settings())
        engine = Engine(events)

        async def reject_schema(value: object) -> None:
            assert value is engine
            events.append("schema:rejected")
            raise RuntimeError("incompatible schema")

        monkeypatch.setattr(application, "_configure_telemetry", lambda: None)
        monkeypatch.setattr(
            "taskforge.worker.application.load_installed_task_catalog", lambda: catalog
        )
        monkeypatch.setattr(
            "taskforge.worker.application.load_installed_worker_profile",
            lambda name, resolved: profile,
        )
        monkeypatch.setattr(
            "taskforge.worker.application.build_async_engine", lambda value: engine
        )
        monkeypatch.setattr(
            "taskforge.worker.application.build_session_factory", lambda value: object()
        )
        monkeypatch.setattr(
            "taskforge.worker.application.require_compatible_schema", reject_schema
        )
        monkeypatch.setattr(
            "taskforge.worker.application.WorkerAuthenticator",
            lambda *args, **kwargs: pytest.fail("authentication must not start"),
        )
        monkeypatch.setattr(
            application,
            "_connect_broker",
            lambda *args: pytest.fail("broker must not connect"),
        )

        with pytest.raises(RuntimeError, match="incompatible schema"):
            await application.start()

        assert events == ["schema:rejected", "close:engine"]
        assert application.state is WorkerApplicationState.STOPPED

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "authentication_error",
    (
        AuthenticationFailure(AuthenticationFailureReason.REVOKED),
        AuthenticationUnavailable(),
    ),
)
def test_identity_or_database_authentication_failure_rolls_back_engine(
    monkeypatch: pytest.MonkeyPatch, authentication_error: Exception
) -> None:
    async def scenario() -> None:
        events: list[str] = []
        catalog, profile = provider()
        application = WorkerApplication(settings())
        engine = Engine(events)

        class Authenticator:
            async def authenticate(self, presented: Any) -> AuthenticatedWorker:
                del presented
                raise authentication_error

        monkeypatch.setattr(application, "_configure_telemetry", lambda: None)
        monkeypatch.setattr(
            "taskforge.worker.application.load_installed_task_catalog", lambda: catalog
        )
        monkeypatch.setattr(
            "taskforge.worker.application.load_installed_worker_profile",
            lambda name, resolved: profile,
        )
        monkeypatch.setattr(
            "taskforge.worker.application.build_async_engine", lambda value: engine
        )
        monkeypatch.setattr(
            "taskforge.worker.application.build_session_factory", lambda value: object()
        )
        monkeypatch.setattr(
            "taskforge.worker.application.require_compatible_schema",
            lambda value: asyncio.sleep(0),
        )
        monkeypatch.setattr(
            "taskforge.worker.application.WorkerAuthenticator",
            lambda *args, **kwargs: Authenticator(),
        )

        with pytest.raises(type(authentication_error)):
            await application.start()
        assert events == ["close:engine"]
        assert application.state is WorkerApplicationState.STOPPED

    asyncio.run(scenario())


def test_broker_startup_failure_closes_partial_broker_and_database_ownership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        events: list[str] = []
        catalog, profile = provider()
        application = WorkerApplication(settings())
        engine = Engine(events)
        connection = Resource("connection", events)
        topology = Resource("topology", events)

        class Authenticator:
            async def authenticate(self, presented: Any) -> AuthenticatedWorker:
                del presented
                return AuthenticatedWorker(uuid4(), uuid4())

        async def fail_broker(
            resolved_catalog: TaskTypeRegistry,
            resolved_profile: ResolvedWorkerProfile,
        ) -> None:
            assert resolved_catalog is catalog
            assert resolved_profile is profile
            application._connection = connection  # type: ignore[assignment]
            application._topology_channel = topology  # type: ignore[assignment]
            raise ConnectionError("broker unavailable")

        monkeypatch.setattr(application, "_configure_telemetry", lambda: None)
        monkeypatch.setattr(application, "_connect_broker", fail_broker)
        monkeypatch.setattr(
            "taskforge.worker.application.load_installed_task_catalog", lambda: catalog
        )
        monkeypatch.setattr(
            "taskforge.worker.application.load_installed_worker_profile",
            lambda name, resolved: profile,
        )
        monkeypatch.setattr(
            "taskforge.worker.application.build_async_engine", lambda value: engine
        )
        monkeypatch.setattr(
            "taskforge.worker.application.build_session_factory", lambda value: object()
        )
        monkeypatch.setattr(
            "taskforge.worker.application.require_compatible_schema",
            lambda value: asyncio.sleep(0),
        )
        monkeypatch.setattr(
            "taskforge.worker.application.WorkerAuthenticator",
            lambda *args, **kwargs: Authenticator(),
        )

        with pytest.raises(ConnectionError):
            await application.start()
        assert events == ["close:topology", "close:connection", "close:engine"]
        assert application.state is WorkerApplicationState.STOPPED

    asyncio.run(scenario())


def test_cleanup_is_reverse_order_exactly_once() -> None:
    async def scenario() -> None:
        events: list[str] = []
        application = WorkerApplication(settings())
        application.state = WorkerApplicationState.RUNNING
        application._runtimes = [
            Runtime("runtime-1", events),  # type: ignore[list-item]
            Runtime("runtime-2", events),  # type: ignore[list-item]
        ]
        application._heartbeat = Heartbeat(events)  # type: ignore[assignment]
        application._consumer_channels = [
            Resource("channel-1", events),  # type: ignore[list-item]
            Resource("channel-2", events),  # type: ignore[list-item]
        ]
        application._topology_channel = Resource("topology", events)  # type: ignore[assignment]
        application._connection = Resource("connection", events)  # type: ignore[assignment]
        application._engine = Engine(events)  # type: ignore[assignment]
        application._metrics = Telemetry("metrics", events)  # type: ignore[assignment]
        application._tracing = Telemetry("tracing", events)  # type: ignore[assignment]

        await asyncio.gather(application.close(), application.close())
        await application.close()

        assert events == [
            "close:runtime-2",
            "close:runtime-1",
            "close:heartbeat",
            "close:channel-2",
            "close:channel-1",
            "close:topology",
            "close:connection",
            "close:engine",
            "close:metrics",
            "close:tracing",
        ]
        assert application.state is WorkerApplicationState.STOPPED

    asyncio.run(scenario())


def test_parent_supervision_distinguishes_stop_from_required_task_failure() -> None:
    async def ordinary_stop() -> None:
        events: list[str] = []
        application = WorkerApplication(settings())
        application._heartbeat = Heartbeat(events)  # type: ignore[assignment]
        application._connection = Resource("connection", events)  # type: ignore[assignment]
        application.request_stop()
        await application._supervise()
        assert not application._process_failed.is_set()

    async def failed_heartbeat() -> None:
        events: list[str] = []
        application = WorkerApplication(settings())
        heartbeat = Heartbeat(events)
        application._heartbeat = heartbeat  # type: ignore[assignment]
        application._connection = Resource("connection", events)  # type: ignore[assignment]
        heartbeat._failure.set_exception(WorkerProcessFailure("authority lost"))
        with pytest.raises(WorkerProcessFailure):
            await application._supervise()
        assert application._process_failed.is_set()

    asyncio.run(ordinary_stop())
    asyncio.run(failed_heartbeat())


def test_sigterm_requests_ordinary_stop_without_cancelling_active_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        events: list[str] = []
        entered = asyncio.Event()
        release = asyncio.Event()
        cancelled = False
        delivery_task: asyncio.Task[None] | None = None

        class Consumer:
            handler: Any = None

            async def consume(self, callback: Any) -> str:
                self.handler = callback
                return "tag"

            async def cancel(self, consumer_tag: str) -> None:
                assert consumer_tag == "tag"
                events.append("consumer:cancel")

        async def handle(control: object) -> None:
            nonlocal cancelled
            del control
            events.append("handler:start")
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled = True
                raise
            events.append("handler:complete")

        consumer = Consumer()
        runtime = WorkerDispatchRuntime(consumer, handle)
        application = WorkerApplication(settings())
        heartbeat = Heartbeat(events)
        connection = Resource("connection", events)
        application._heartbeat = heartbeat  # type: ignore[assignment]
        application._connection = connection  # type: ignore[assignment]

        async def start() -> None:
            nonlocal delivery_task
            await runtime.start()
            application._runtimes.append(runtime)
            delivery_task = asyncio.create_task(consumer.handler(object()))
            await entered.wait()
            loop = asyncio.get_running_loop()
            loop.call_later(0.01, os.kill, os.getpid(), signal.SIGTERM)
            loop.call_later(0.05, release.set)
            application.state = WorkerApplicationState.RUNNING

        monkeypatch.setattr(application, "start", start)
        await application.run()
        assert delivery_task is not None
        await delivery_task

        assert not cancelled
        assert events.index("handler:complete") < events.index("close:heartbeat")
        assert events.count("consumer:cancel") == 1
        assert application.state is WorkerApplicationState.STOPPED
        assert not [
            task
            for task in asyncio.all_tasks()
            if task is not asyncio.current_task()
            and task.get_name().startswith("taskforge-")
            and not task.done()
        ]

    asyncio.run(scenario())


def test_prompt_cancellation_after_drain_timeout_is_not_overrun(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        events: list[str] = []
        logged: list[tuple[str, dict[str, object]]] = []
        metrics: list[str] = []
        entered = asyncio.Event()
        cancellation_seen = asyncio.Event()

        def capture_log(
            _logger: object,
            _level: int,
            event: str,
            fields: dict[str, object] | None = None,
        ) -> None:
            logged.append((event, fields or {}))

        def capture_metric(
            name: str,
            _value: int = 1,
            attributes: dict[str, str] | None = None,
        ) -> None:
            del attributes
            metrics.append(name)

        monkeypatch.setattr(application_module, "log_event", capture_log)
        monkeypatch.setattr(application_module, "add_metric", capture_metric)

        class Consumer:
            handler: Any = None

            async def consume(self, callback: Any) -> str:
                self.handler = callback
                return "tag"

            async def cancel(self, consumer_tag: str) -> None:
                assert consumer_tag == "tag"

        async def handle(control: object) -> None:
            del control
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancellation_seen.set()
                raise

        consumer = Consumer()
        runtime = WorkerDispatchRuntime(consumer, handle)
        await runtime.start()
        delivery = asyncio.create_task(consumer.handler(object()))
        await entered.wait()
        application = WorkerApplication(
            settings().model_copy(
                update={
                    "worker_drain_timeout_seconds": 0.01,
                    "worker_cancellation_grace_seconds": 0.05,
                }
            )
        )
        application.state = WorkerApplicationState.RUNNING
        application._heartbeat_started = True
        application._runtimes = [runtime]
        application._heartbeat = Heartbeat(events)  # type: ignore[assignment]

        await application.close()
        assert cancellation_seen.is_set()
        assert delivery.cancelled()
        assert "worker.shutdown.cancellation_requested" in [
            event for event, _ in logged
        ]
        assert "worker.shutdown.cancellation_completed" in [
            event for event, _ in logged
        ]
        assert "worker.drain.cancellation_overrun" not in [event for event, _ in logged]
        assert "taskforge.worker.drain.timeouts" in metrics
        assert "taskforge.worker.drain.overruns" not in metrics
        assert (
            next(
                fields["outcome"]
                for event, fields in logged
                if event == "worker.shutdown.completed"
            )
            == "drain_timeout"
        )

    asyncio.run(scenario())


def test_drain_timeout_keeps_heartbeat_alive_until_cancelled_handler_exits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        events: list[str] = []
        logged: list[str] = []
        entered = asyncio.Event()
        cancellation_seen = asyncio.Event()
        overrun_seen = asyncio.Event()
        release = asyncio.Event()

        def capture_log(
            _logger: object,
            _level: int,
            event: str,
            fields: dict[str, object] | None = None,
        ) -> None:
            del fields
            logged.append(event)
            if event == "worker.drain.cancellation_overrun":
                overrun_seen.set()

        monkeypatch.setattr(application_module, "log_event", capture_log)

        class Consumer:
            handler: Any = None

            async def consume(self, callback: Any) -> str:
                self.handler = callback
                return "tag"

            async def cancel(self, consumer_tag: str) -> None:
                assert consumer_tag == "tag"
                events.append("consumer:cancel")

        async def handle(control: object) -> None:
            del control
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancellation_seen.set()
                events.append("handler:overrun")
                await release.wait()
            events.append("handler:exit")

        class DrainingHeartbeat(Heartbeat):
            async def begin_draining(self) -> None:
                events.append("heartbeat:draining")

        consumer = Consumer()
        runtime = WorkerDispatchRuntime(consumer, handle)
        await runtime.start()
        delivery = asyncio.create_task(consumer.handler(object()))
        await entered.wait()

        application = WorkerApplication(
            settings().model_copy(
                update={
                    "worker_drain_timeout_seconds": 0.01,
                    "worker_cancellation_grace_seconds": 0.01,
                }
            )
        )
        application.state = WorkerApplicationState.RUNNING
        application._heartbeat_started = True
        application._runtimes = [runtime]
        application._heartbeat = DrainingHeartbeat(events)  # type: ignore[assignment]
        application._engine = Engine(events)  # type: ignore[assignment]
        application._connection = Resource("connection", events)  # type: ignore[assignment]
        application._topology_channel = Resource("topology", events)  # type: ignore[assignment]
        application._consumer_channels = [Resource("consumer-channel", events)]  # type: ignore[list-item]

        closing = asyncio.create_task(application.close())
        await asyncio.wait_for(cancellation_seen.wait(), timeout=1)
        await asyncio.wait_for(overrun_seen.wait(), timeout=1)
        assert not closing.done()
        assert "heartbeat:draining" in events
        assert not any(event.startswith("close:") for event in events)

        release.set()
        await delivery
        await closing
        assert events.index("handler:exit") < events.index("close:heartbeat")
        assert events.index("handler:exit") < events.index("close:connection")
        assert events.index("handler:exit") < events.index("close:engine")
        assert "worker.drain.overrun_completed" in logged
        assert application.state is WorkerApplicationState.STOPPED

    asyncio.run(scenario())
