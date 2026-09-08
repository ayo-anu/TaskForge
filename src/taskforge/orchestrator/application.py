"""Production orchestrator composition and deterministic process ownership."""

from __future__ import annotations

import asyncio
import logging
import signal
from contextlib import suppress
from enum import StrEnum

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractConnection, AbstractExchange
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from taskforge.broker.rabbitmq import RabbitMQDispatchPublisher
from taskforge.broker.topology import (
    RabbitMQTopologyConfiguration,
    declare_dispatch_topology,
)
from taskforge.dispatch.publisher import TaskDispatchPublisher
from taskforge.dispatch.service import TaskDispatchService
from taskforge.logging import configure_logging, log_event
from taskforge.metrics import MetricsRuntime, configure_metrics
from taskforge.orchestrator.domain import LoopExit, OrchestratorProcessFailure
from taskforge.orchestrator.workloads import (
    BoundedWorkload,
    OutboxPublicationWorkload,
    ProgressionDispatchWorkload,
    RecoveryWorkload,
    RetryWorkload,
    run_workload_loop,
)
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.dispatch import (
    SQLAlchemyDispatchOutboxRepository,
    SQLAlchemyTaskDispatchRepository,
)
from taskforge.persistence.orchestrator import (
    SQLAlchemyOrchestratorCandidateRepository,
)
from taskforge.persistence.recovery import (
    SQLAlchemyExpiredClaimRecoveryRepository,
    SQLAlchemyRecoveryCandidateRepository,
    SQLAlchemyStaleWorkerSessionRecoveryRepository,
)
from taskforge.persistence.retries import SQLAlchemyRetryTransitionRepository
from taskforge.persistence.runs import SQLAlchemyWorkflowRunRepository
from taskforge.recovery.progression import ExpiredClaimRecoveryProgressionService
from taskforge.recovery.scanner import RecoveryCandidateScanner
from taskforge.recovery.service import (
    ExpiredClaimRecoveryService,
    StaleWorkerSessionRecoveryService,
)
from taskforge.retries.scanner import DueRetryScanner
from taskforge.retries.service import RetryTransitionService
from taskforge.runs.service import WorkflowRunService
from taskforge.runtime_provider import load_installed_task_catalog
from taskforge.settings import OrchestratorSettings
from taskforge.tracing import TracingRuntime, configure_tracing
from taskforge.workflows.task_types import TaskTypeRegistry

logger = logging.getLogger(__name__)
type OwnedTask = asyncio.Task[LoopExit] | asyncio.Task[None] | asyncio.Task[bool]


class OrchestratorApplicationState(StrEnum):
    NEW = "new"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"


class OrchestratorApplication:
    """Own one production orchestrator and all resources it creates."""

    def __init__(self, settings: OrchestratorSettings) -> None:
        self.settings = settings
        self.state = OrchestratorApplicationState.NEW
        self._ordinary_stop_requested = asyncio.Event()
        self._stop_scheduling = asyncio.Event()
        self._process_failed = asyncio.Event()
        self._failure: Exception | None = None
        self._close_lock = asyncio.Lock()
        self._close_task: asyncio.Task[None] | None = None
        self._engine: AsyncEngine | None = None
        self._connection: AbstractConnection | None = None
        self._publisher_channel: AbstractChannel | None = None
        self._dispatch_exchange: AbstractExchange | None = None
        self._workloads: dict[str, BoundedWorkload] = {}
        self._loop_tasks: dict[str, asyncio.Task[LoopExit]] = {}
        self._resource_watchers: dict[str, asyncio.Task[None]] = {}
        self._stop_waiter: asyncio.Task[bool] | None = None
        self._tracing: TracingRuntime | None = None
        self._metrics: MetricsRuntime | None = None

    @property
    def process_failed(self) -> bool:
        return self._process_failed.is_set()

    @property
    def failure(self) -> BaseException | None:
        return self._failure

    def request_stop(self) -> None:
        """Request ordinary cessation without creating durable drain state."""
        self._ordinary_stop_requested.set()
        self._stop_scheduling.set()

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        installed_signals: list[signal.Signals] = []
        for requested_signal in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(requested_signal, self.request_stop)
            except (NotImplementedError, RuntimeError):
                continue
            installed_signals.append(requested_signal)
        try:
            try:
                await self.start()
                await self._supervise()
            except BaseException:
                with suppress(Exception):
                    await self.close()
                raise
            else:
                await self.close()
        finally:
            for requested_signal in installed_signals:
                with suppress(NotImplementedError, RuntimeError):
                    loop.remove_signal_handler(requested_signal)

    async def start(self) -> None:
        if self.state is not OrchestratorApplicationState.NEW:
            raise RuntimeError("orchestrator application can only start once")
        self.state = OrchestratorApplicationState.STARTING
        try:
            self._configure_telemetry()
            catalog = load_installed_task_catalog()
            self._engine = build_async_engine(self.settings)
            sessions = build_session_factory(self._engine)
            await self._probe_database()
            await self._connect_broker(catalog)
            self._workloads = self._compose_workloads(sessions, catalog)
            self._start_critical_tasks()
            await asyncio.sleep(0)
            self._inspect_critical_tasks()
            self._inspect_broker_state()
            if self.process_failed:
                raise self._process_failure()
            self.state = OrchestratorApplicationState.RUNNING
            log_event(logger, logging.INFO, "orchestrator.started")
        except BaseException:
            with suppress(Exception):
                await self.close()
            raise

    async def close(self) -> None:
        async with self._close_lock:
            if self.state is OrchestratorApplicationState.STOPPED:
                return
            if self._close_task is None:
                self.state = OrchestratorApplicationState.STOPPING
                self._close_task = asyncio.create_task(
                    self._close_owned(), name="taskforge-orchestrator-close"
                )
            operation = self._close_task
        await asyncio.shield(operation)

    def _configure_telemetry(self) -> None:
        settings = self.settings
        configure_logging(
            service_name=settings.application_name,
            environment=settings.environment,
            process_role="orchestrator",
            level=settings.log_level,
        )
        self._tracing = configure_tracing(
            enabled=settings.tracing_enabled,
            exporter=settings.tracing_exporter,
            endpoint=settings.tracing_otlp_endpoint,
            sample_ratio=settings.tracing_sample_ratio,
            export_timeout_seconds=settings.tracing_export_timeout_seconds,
            shutdown_timeout_seconds=settings.tracing_shutdown_timeout_seconds,
            service_name=settings.application_name,
            environment=settings.environment,
            process_role="orchestrator",
        )
        self._metrics = configure_metrics(
            enabled=settings.metrics_enabled,
            exporter=settings.metrics_exporter,
            endpoint=settings.metrics_otlp_endpoint,
            export_interval_seconds=settings.metrics_export_interval_seconds,
            export_timeout_seconds=settings.metrics_export_timeout_seconds,
            shutdown_timeout_seconds=settings.metrics_shutdown_timeout_seconds,
            outbox_staleness_seconds=settings.metrics_outbox_staleness_seconds,
            service_name=settings.application_name,
            environment=settings.environment,
            process_role="orchestrator",
        )

    async def _probe_database(self) -> None:
        if self._engine is None:
            raise OrchestratorProcessFailure("orchestrator database is incomplete")
        async with self._engine.connect() as connection:
            await connection.execute(text("SELECT 1"))

    async def _connect_broker(self, catalog: TaskTypeRegistry) -> None:
        settings = self.settings
        self._connection = await aio_pika.connect(
            host=settings.rabbitmq_host,
            port=settings.rabbitmq_port,
            login=settings.rabbitmq_user,
            password=settings.rabbitmq_password.get_secret_value(),
            virtualhost=settings.rabbitmq_vhost,
            timeout=settings.rabbitmq_topology_timeout_seconds,
        )
        self._publisher_channel = await self._connection.channel(
            publisher_confirms=True,
            on_return_raises=True,
        )
        topology = await declare_dispatch_topology(
            self._publisher_channel,
            catalog,
            RabbitMQTopologyConfiguration(
                settings.rabbitmq_dispatch_exchange_name,
                settings.rabbitmq_malformed_exchange_name,
                settings.rabbitmq_topology_timeout_seconds,
            ),
        )
        self._dispatch_exchange = topology.exchange

    def _compose_workloads(
        self,
        sessions: async_sessionmaker[AsyncSession],
        catalog: TaskTypeRegistry,
    ) -> dict[str, BoundedWorkload]:
        # Session factories are intentionally shared; every existing repository
        # still owns its own short transaction boundary.
        run_repository = SQLAlchemyWorkflowRunRepository(sessions)
        run_service = WorkflowRunService(run_repository)
        candidates = SQLAlchemyOrchestratorCandidateRepository(sessions)
        retry_repository = SQLAlchemyRetryTransitionRepository(sessions)
        recovery_candidates = RecoveryCandidateScanner(
            SQLAlchemyRecoveryCandidateRepository(sessions),
            worker_stale_after_seconds=self.settings.worker_stale_after_seconds,
        )
        batch_size = self.settings.orchestrator_batch_size
        return {
            "progression_dispatch": ProgressionDispatchWorkload(
                candidates,
                run_service,
                TaskDispatchService(
                    SQLAlchemyTaskDispatchRepository(sessions),
                    catalog,
                ),
                batch_size=batch_size,
            ),
            "retry": RetryWorkload(
                candidates,
                RetryTransitionService(retry_repository),
                DueRetryScanner(retry_repository, catalog),
                batch_size=batch_size,
            ),
            "recovery": RecoveryWorkload(
                recovery_candidates,
                ExpiredClaimRecoveryProgressionService(
                    ExpiredClaimRecoveryService(
                        SQLAlchemyExpiredClaimRecoveryRepository(sessions)
                    ),
                    run_service,
                ),
                StaleWorkerSessionRecoveryService(
                    SQLAlchemyStaleWorkerSessionRecoveryRepository(sessions)
                ),
                batch_size=batch_size,
                stale_after_seconds=self.settings.worker_stale_after_seconds,
            ),
            "outbox": OutboxPublicationWorkload(
                TaskDispatchPublisher(
                    SQLAlchemyDispatchOutboxRepository(sessions),
                    RabbitMQDispatchPublisher(
                        self._required_topology_exchange(),
                        timeout_seconds=(
                            self.settings.orchestrator_publication_timeout_seconds
                        ),
                    ),
                ),
                batch_size=batch_size,
            ),
        }

    def _required_topology_exchange(self) -> AbstractExchange:
        if self._dispatch_exchange is None:
            raise OrchestratorProcessFailure("publisher topology is incomplete")
        return self._dispatch_exchange

    def _start_critical_tasks(self) -> None:
        if self._connection is None or self._publisher_channel is None:
            raise OrchestratorProcessFailure("orchestrator broker is incomplete")
        self._loop_tasks = {
            name: asyncio.create_task(
                run_workload_loop(
                    name,
                    workload,
                    self._stop_scheduling,
                    poll_interval_seconds=(
                        self.settings.orchestrator_poll_interval_seconds
                    ),
                ),
                name=f"taskforge-orchestrator-{name}",
            )
            for name, workload in self._workloads.items()
        }
        self._resource_watchers = {
            "rabbitmq_connection": asyncio.create_task(
                _watch_broker_resource(self._connection, "connection"),
                name="taskforge-orchestrator-connection-watch",
            ),
            "rabbitmq_channel": asyncio.create_task(
                _watch_broker_resource(self._publisher_channel, "publisher channel"),
                name="taskforge-orchestrator-channel-watch",
            ),
        }

    async def _supervise(self) -> None:
        if not self._loop_tasks or not self._resource_watchers:
            raise OrchestratorProcessFailure("orchestrator runtime is incomplete")
        self._stop_waiter = asyncio.create_task(
            self._ordinary_stop_requested.wait(),
            name="taskforge-orchestrator-stop-request",
        )
        await asyncio.wait(
            (
                *self._loop_tasks.values(),
                *self._resource_watchers.values(),
                self._stop_waiter,
            ),
            return_when=asyncio.FIRST_COMPLETED,
        )
        self._inspect_critical_tasks()
        if self.process_failed:
            await self._abort_supervision()
            raise self._process_failure()
        if not self._ordinary_stop_requested.is_set():
            self._record_failure(
                OrchestratorProcessFailure("required orchestrator task terminated")
            )
            await self._abort_supervision()
            raise self._process_failure()
        await self._join_ordinary_shutdown()
        if self.process_failed:
            raise self._process_failure()

    async def _join_ordinary_shutdown(self) -> None:
        self._stop_scheduling.set()
        pending = {task for task in self._loop_tasks.values() if not task.done()}
        while pending and not self.process_failed:
            await asyncio.wait(
                (*pending, *self._resource_watchers.values()),
                return_when=asyncio.FIRST_COMPLETED,
            )
            self._inspect_critical_tasks()
            pending = {task for task in pending if not task.done()}
        if self.process_failed:
            await self._abort_supervision()
            return
        self._inspect_critical_tasks()
        if self._inspect_broker_state():
            await self._abort_supervision()
            return
        await self._disarm_resource_watchers()
        self._resource_watchers.clear()
        if self._stop_waiter is not None:
            await self._cancel_and_join((self._stop_waiter,))
            self._stop_waiter = None

    def _inspect_critical_tasks(self) -> None:
        for name, task in self._loop_tasks.items():
            if not task.done():
                continue
            if task.cancelled():
                if not self.process_failed:
                    self._record_failure(
                        OrchestratorProcessFailure(
                            f"required orchestrator loop {name} was cancelled"
                        )
                    )
                continue
            error = task.exception()
            if error is not None:
                self._record_failure(
                    error
                    if isinstance(error, Exception)
                    else OrchestratorProcessFailure(
                        f"required orchestrator loop {name} failed"
                    )
                )
                continue
            if (
                task.result() is not LoopExit.STOP_REQUESTED
                or not self._stop_scheduling.is_set()
            ):
                self._record_failure(
                    OrchestratorProcessFailure(
                        f"required orchestrator loop {name} exited unexpectedly"
                    )
                )
        for watcher in self._resource_watchers.values():
            if not watcher.done() or watcher.cancelled():
                continue
            error = watcher.exception()
            self._record_failure(
                error
                if isinstance(error, Exception)
                else OrchestratorProcessFailure(
                    "required orchestrator broker watcher terminated"
                )
            )

    def _inspect_broker_state(self) -> bool:
        if self._connection is not None and self._connection.is_closed:
            self._record_failure(
                OrchestratorProcessFailure("RabbitMQ connection closed")
            )
        if self._publisher_channel is not None and self._publisher_channel.is_closed:
            self._record_failure(
                OrchestratorProcessFailure("RabbitMQ publisher channel closed")
            )
        return self.process_failed

    def _record_failure(self, error: Exception) -> None:
        if self._failure is None:
            self._failure = error
            self._process_failed.set()
            self._stop_scheduling.set()
            log_event(
                logger,
                logging.ERROR,
                "orchestrator.failed",
                {"error.category": type(error).__name__},
                error=error,
            )

    def _process_failure(self) -> OrchestratorProcessFailure:
        error = self._failure
        failure = OrchestratorProcessFailure("required orchestrator runtime failed")
        if error is not None:
            failure.__cause__ = error
        return failure

    async def _abort_supervision(self) -> None:
        self._stop_scheduling.set()
        await self._cancel_and_join(
            (
                *self._loop_tasks.values(),
                *self._resource_watchers.values(),
                *((self._stop_waiter,) if self._stop_waiter is not None else ()),
            )
        )
        self._resource_watchers.clear()
        self._stop_waiter = None

    async def _disarm_resource_watchers(self) -> None:
        """Join watcher cancellation without hiding a concurrently known failure."""
        watchers = tuple(self._resource_watchers.values())
        for watcher in watchers:
            if not watcher.done():
                watcher.cancel()
        if watchers:
            await asyncio.gather(*watchers, return_exceptions=True)
        # A resource closure that won the race with cancellation leaves its watcher
        # completed with an exception. Inspect after joining so it cannot be converted
        # into ordinary-stop success.
        self._inspect_critical_tasks()

    async def _close_owned(self) -> None:
        errors: list[Exception] = []
        self._stop_scheduling.set()
        try:
            await self._cancel_and_join(
                (
                    *self._loop_tasks.values(),
                    *self._resource_watchers.values(),
                    *((self._stop_waiter,) if self._stop_waiter is not None else ()),
                )
            )
            self._loop_tasks.clear()
            self._resource_watchers.clear()
            self._stop_waiter = None
            if (
                self._publisher_channel is not None
                and not self._publisher_channel.is_closed
            ):
                try:
                    await self._publisher_channel.close()
                except Exception as error:
                    errors.append(error)
            if self._connection is not None and not self._connection.is_closed:
                try:
                    await self._connection.close()
                except Exception as error:
                    errors.append(error)
            if self._engine is not None:
                try:
                    await self._engine.dispose()
                except Exception as error:
                    errors.append(error)
            if self._metrics is not None:
                try:
                    self._metrics.shutdown()
                except Exception as error:
                    errors.append(error)
            if self._tracing is not None:
                try:
                    self._tracing.shutdown()
                except Exception as error:
                    errors.append(error)
        finally:
            self.state = OrchestratorApplicationState.STOPPED
        if errors:
            cleanup = ExceptionGroup("orchestrator resource cleanup failed", errors)
            self._record_failure(cleanup)
            raise cleanup

    @staticmethod
    async def _cancel_and_join(tasks: tuple[OwnedTask, ...]) -> None:
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


async def _watch_broker_resource(
    resource: AbstractConnection | AbstractChannel, name: str
) -> None:
    await resource.closed()
    raise OrchestratorProcessFailure(f"RabbitMQ {name} closed")
