"""Production worker composition and deterministic runtime ownership."""

from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Awaitable
from contextlib import suppress
from enum import StrEnum
from time import perf_counter
from typing import Any

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractConnection
from sqlalchemy.ext.asyncio import AsyncEngine

from taskforge.broker.consumer import RabbitMQDispatchConsumer
from taskforge.broker.topology import (
    RabbitMQTopologyConfiguration,
    declare_dispatch_topology,
)
from taskforge.claims.authority import TaskClaimResultAuthorityIssuer
from taskforge.claims.service import TaskClaimService
from taskforge.identity.authentication import WorkerAuthenticator
from taskforge.identity.credentials import parse_presented_credential
from taskforge.logging import configure_logging, log_event
from taskforge.metrics import MetricsRuntime, configure_metrics
from taskforge.metrics import add as add_metric
from taskforge.metrics import record as record_metric
from taskforge.persistence.audit import RejectedAuditUnitOfWork
from taskforge.persistence.authentication import SQLAlchemyWorkerCredentialRepository
from taskforge.persistence.claims import SQLAlchemyTaskClaimRepository
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.rate_limits import SQLAlchemyRateLimitRepository
from taskforge.persistence.schema_compatibility import require_compatible_schema
from taskforge.persistence.task_cancellation import SQLAlchemyTaskCancellationObserver
from taskforge.persistence.task_results import SQLAlchemyTaskResultRepository
from taskforge.persistence.task_start import SQLAlchemyTaskStartRepository
from taskforge.persistence.workers import (
    SQLAlchemyWorkerHeartbeatRepository,
    SQLAlchemyWorkerRegistrationRepository,
)
from taskforge.rate_limits import (
    BoundedLocalRateLimiter,
    RateLimit,
    RateLimiter,
    RateLimitPolicy,
)
from taskforge.runtime_provider import (
    ResolvedWorkerProfile,
    load_installed_task_catalog,
    load_installed_worker_profile,
)
from taskforge.settings import WorkerSettings
from taskforge.shutdown import CooperativeShutdownDeadline
from taskforge.tracing import TracingRuntime, configure_tracing
from taskforge.worker.claim_renewal import ClaimRenewalSupervisor
from taskforge.worker.execution import WorkerExecutionConsumer
from taskforge.worker.heartbeat import WorkerHeartbeatSupervisor
from taskforge.worker.lifecycle import WorkerDispatchRuntime
from taskforge.worker.result_submission import TaskResultSubmissionService
from taskforge.worker.runtime_errors import WorkerProcessFailure
from taskforge.worker.service import WorkerHeartbeatService, WorkerRegistrationService
from taskforge.worker.start import TaskStartService
from taskforge.workflows.task_types import TaskTypeRegistry

logger = logging.getLogger(__name__)


class WorkerApplicationState(StrEnum):
    NEW = "new"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"


class WorkerApplication:
    """Own one production worker process and every closeable resource it creates."""

    def __init__(self, settings: WorkerSettings) -> None:
        self.settings = settings
        self.state = WorkerApplicationState.NEW
        self._stop_requested = asyncio.Event()
        self._process_failed = asyncio.Event()
        self._close_lock = asyncio.Lock()
        self._close_task: asyncio.Task[None] | None = None
        self._engine: AsyncEngine | None = None
        self._connection: AbstractConnection | None = None
        self._topology_channel: AbstractChannel | None = None
        self._consumer_channels: list[AbstractChannel] = []
        self._consumers: list[RabbitMQDispatchConsumer] = []
        self._runtimes: list[WorkerDispatchRuntime] = []
        self._heartbeat: WorkerHeartbeatSupervisor | None = None
        self._tracing: TracingRuntime | None = None
        self._metrics: MetricsRuntime | None = None
        self._heartbeat_started = False

    def request_stop(self) -> None:
        """Request ordinary termination without authoring durable drain state."""
        self._stop_requested.set()

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
                if not self._stop_requested.is_set():
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
        if self.state is not WorkerApplicationState.NEW:
            raise RuntimeError("worker application can only start once")
        self.state = WorkerApplicationState.STARTING
        try:
            self._configure_telemetry()
            catalog = load_installed_task_catalog()
            profile = load_installed_worker_profile(
                self.settings.worker_profile, catalog
            )
            self._engine = build_async_engine(self.settings)
            sessions = build_session_factory(self._engine)
            await require_compatible_schema(self._engine)
            presented = parse_presented_credential(
                self.settings.worker_credential.get_secret_value()
            )
            worker = await WorkerAuthenticator(
                SQLAlchemyWorkerCredentialRepository(sessions),
                timeout_seconds=self.settings.authentication_timeout_seconds,
            ).authenticate(presented)
            await self._connect_broker(catalog, profile)
            rejected_audit = RejectedAuditUnitOfWork(sessions)
            registration = await WorkerRegistrationService(
                SQLAlchemyWorkerRegistrationRepository(sessions),
                catalog,
                rejected_audit=rejected_audit,
            ).register(worker, profile.capabilities)
            if registration.capabilities != profile.capabilities:
                raise WorkerProcessFailure(
                    "registered worker capabilities do not match profile"
                )
            heartbeat_service = WorkerHeartbeatService(
                SQLAlchemyWorkerHeartbeatRepository(sessions), rejected_audit
            )
            self._heartbeat = WorkerHeartbeatSupervisor(
                heartbeat_service,
                worker,
                registration.id,
                interval_seconds=self.settings.worker_heartbeat_interval_seconds,
                operation_timeout_seconds=(
                    self.settings.worker_control_operation_timeout_seconds
                ),
                stale_after_seconds=self.settings.worker_stale_after_seconds,
            )
            authority_issuer = TaskClaimResultAuthorityIssuer(
                self.settings.task_claim_result_authority_secret.get_secret_value().encode()
            )
            claim_service = TaskClaimService(
                SQLAlchemyTaskClaimRepository(
                    sessions,
                    worker_stale_after_seconds=self.settings.worker_stale_after_seconds,
                ),
                authority_issuer,
                lease_seconds=self.settings.task_claim_lease_seconds,
                rejected_audit=rejected_audit,
            )
            cancellation_observer = SQLAlchemyTaskCancellationObserver(sessions)
            renewal = ClaimRenewalSupervisor(
                claim_service,
                cancellation_observer,
                worker,
                registration.id,
                lease_seconds=self.settings.task_claim_lease_seconds,
                operation_timeout_seconds=(
                    self.settings.worker_control_operation_timeout_seconds
                ),
                observation_poll_seconds=self.settings.task_cancellation_poll_seconds,
            )
            rate_limiter = RateLimiter(
                SQLAlchemyRateLimitRepository(
                    sessions,
                    timeout_seconds=self.settings.rate_limit_timeout_seconds,
                    cleanup_retention_seconds=(
                        self.settings.rate_limit_cleanup_retention_seconds
                    ),
                ),
                BoundedLocalRateLimiter(
                    capacity=self.settings.rate_limit_fallback_capacity
                ),
                {
                    RateLimitPolicy.WORKER_RESULT: RateLimit(
                        self.settings.worker_results_per_minute, 60
                    )
                },
            )
            execution = WorkerExecutionConsumer(
                claim_service,
                TaskStartService(
                    SQLAlchemyTaskStartRepository(sessions), rejected_audit
                ),
                TaskResultSubmissionService(
                    SQLAlchemyTaskResultRepository(sessions),
                    authority_issuer,
                    rejected_audit,
                    rate_limiter=rate_limiter,
                ),
                profile.handlers,
                worker,
                registration.id,
                cancellation_observer,
                claim_renewal=renewal,
                process_failure_known=self._process_failed.is_set,
                cancellation_poll_seconds=self.settings.task_cancellation_poll_seconds,
            )
            await self._start_consumers(profile, execution)
            await self._heartbeat.send_initial()
            self._heartbeat.start()
            self._heartbeat_started = True
            for runtime in self._runtimes:
                if self._stop_requested.is_set():
                    break
                await runtime.activate()
            self.state = WorkerApplicationState.RUNNING
        except BaseException:
            with suppress(Exception):
                await self.close()
            raise

    async def close(self) -> None:
        async with self._close_lock:
            if self.state is WorkerApplicationState.STOPPED:
                return
            if self._close_task is None:
                self.state = WorkerApplicationState.STOPPING
                self._close_task = asyncio.create_task(
                    self._close_owned(), name="taskforge-worker-close"
                )
            operation = self._close_task
        await asyncio.shield(operation)

    def _configure_telemetry(self) -> None:
        settings = self.settings
        configure_logging(
            service_name=settings.application_name,
            environment=settings.environment,
            process_role="worker",
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
            process_role="worker",
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
            process_role="worker",
        )

    async def _connect_broker(
        self, catalog: TaskTypeRegistry, profile: ResolvedWorkerProfile
    ) -> None:
        settings = self.settings
        self._connection = await aio_pika.connect(
            host=settings.rabbitmq_host,
            port=settings.rabbitmq_port,
            login=settings.rabbitmq_user,
            password=settings.rabbitmq_password.get_secret_value(),
            virtualhost=settings.rabbitmq_vhost,
            timeout=settings.rabbitmq_topology_timeout_seconds,
        )
        self._topology_channel = await self._connection.channel()
        configuration = RabbitMQTopologyConfiguration(
            settings.rabbitmq_dispatch_exchange_name,
            settings.rabbitmq_malformed_exchange_name,
            settings.rabbitmq_topology_timeout_seconds,
        )
        topology = await declare_dispatch_topology(
            self._topology_channel, catalog, configuration
        )
        for capability in profile.capabilities:
            channel = await self._connection.channel()
            self._consumer_channels.append(channel)
            await channel.set_qos(prefetch_count=settings.worker_prefetch_count)
            queue = await channel.declare_queue(
                topology.capability_queues[capability].name,
                passive=True,
                timeout=settings.rabbitmq_topology_timeout_seconds,
            )
            self._consumers.append(RabbitMQDispatchConsumer(queue))

    async def _start_consumers(
        self, profile: ResolvedWorkerProfile, execution: WorkerExecutionConsumer
    ) -> None:
        if len(self._consumers) != len(profile.capabilities):
            raise WorkerProcessFailure("worker consumer topology is incomplete")
        for consumer in self._consumers:
            runtime = WorkerDispatchRuntime(
                consumer,
                execution.consume,
                consumer_cancel_timeout_seconds=(
                    self.settings.worker_control_operation_timeout_seconds
                ),
            )
            self._runtimes.append(runtime)
            await runtime.start(paused=True)

    async def _supervise(self) -> None:
        heartbeat = self._heartbeat
        connection = self._connection
        if heartbeat is None or connection is None:
            raise WorkerProcessFailure("worker application is incomplete")
        stop = asyncio.create_task(
            self._stop_requested.wait(), name="taskforge-worker-stop-request"
        )
        watchers: list[asyncio.Task[Any]] = [
            stop,
            asyncio.create_task(
                heartbeat.wait_failed(), name="taskforge-worker-heartbeat-watch"
            ),
            asyncio.create_task(
                _wait_closed(connection), name="taskforge-worker-connection-watch"
            ),
        ]
        watchers.extend(
            asyncio.create_task(
                _wait_closed(channel), name="taskforge-worker-channel-watch"
            )
            for channel in (self._topology_channel, *self._consumer_channels)
            if channel is not None
        )
        watchers.extend(
            asyncio.create_task(
                runtime.wait_failed(), name="taskforge-worker-consumer-watch"
            )
            for runtime in self._runtimes
        )
        try:
            done, _pending = await asyncio.wait(
                watchers, return_when=asyncio.FIRST_COMPLETED
            )
            failures = tuple(task for task in done if task is not stop)
            if not failures and stop in done and self._stop_requested.is_set():
                return
            self._process_failed.set()
            for task in failures:
                error = task.exception()
                if error is not None:
                    raise WorkerProcessFailure(
                        "required worker runtime failed"
                    ) from error
            raise WorkerProcessFailure("required worker runtime terminated")
        finally:
            for task in watchers:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*watchers, return_exceptions=True)

    async def _close_owned(self) -> None:
        started = perf_counter()
        outcome = "completed"
        errors: list[Exception] = []
        deadline: CooperativeShutdownDeadline | None = None
        log_event(
            logger,
            logging.INFO,
            "worker.shutdown.started",
            {"worker.in_flight_count": sum(item.in_flight for item in self._runtimes)},
        )
        try:
            # Local admission closes before broker cancellation is confirmed. The
            # heartbeat transition runs concurrently so neither dependency delays
            # the other safety boundary.
            admission_operations: list[Awaitable[None]] = [
                runtime.begin_shutdown() for runtime in reversed(self._runtimes)
            ]
            if self._heartbeat is not None and self._heartbeat_started:
                admission_operations.append(self._heartbeat.begin_draining())
            admission = asyncio.gather(*admission_operations, return_exceptions=True)
            drain = asyncio.gather(
                *(runtime.wait_drained() for runtime in self._runtimes)
            )
            completed, _ = await asyncio.wait(
                (admission, drain),
                timeout=self.settings.worker_drain_timeout_seconds,
                return_when=asyncio.ALL_COMPLETED,
            )
            if len(completed) != 2:
                outcome = "drain_timeout"
                add_metric("taskforge.worker.drain.timeouts")
                cancelled = sum(
                    await asyncio.gather(
                        *(runtime.cancel_in_flight() for runtime in self._runtimes)
                    )
                )
                log_event(
                    logger,
                    logging.WARNING,
                    "worker.shutdown.cancellation_requested",
                    {
                        "reason.code": "drain_timeout",
                        "drain.timeout_seconds": (
                            self.settings.worker_drain_timeout_seconds
                        ),
                        "worker.in_flight_count": cancelled,
                    },
                )
                await asyncio.wait(
                    (drain,),
                    timeout=self.settings.worker_cancellation_grace_seconds,
                    return_when=asyncio.ALL_COMPLETED,
                )
                active = sum(runtime.in_flight for runtime in self._runtimes)
                if active:
                    outcome = "cancellation_overrun"
                    overrun_started = perf_counter()
                    add_metric(
                        "taskforge.worker.drain.overruns",
                        attributes={"taskforge.outcome": "cancellation_overrun"},
                    )
                    log_event(
                        logger,
                        logging.WARNING,
                        "worker.drain.cancellation_overrun",
                        {"worker.in_flight_count": active},
                    )
                    # Authority-bearing resources intentionally remain live here.
                    # A non-cooperative callback can keep the process alive until
                    # the deployment hard stop.
                    await drain
                    overrun_duration = perf_counter() - overrun_started
                    add_metric(
                        "taskforge.worker.drain.overruns",
                        attributes={"taskforge.outcome": "completed"},
                    )
                    record_metric(
                        "taskforge.worker.drain.overrun.duration", overrun_duration
                    )
                    log_event(
                        logger,
                        logging.INFO,
                        "worker.drain.overrun_completed",
                        {"worker.overrun_duration_seconds": overrun_duration},
                    )
                else:
                    await drain
                    log_event(
                        logger,
                        logging.INFO,
                        "worker.shutdown.cancellation_completed",
                        {"worker.in_flight_count": 0},
                    )
            else:
                await drain

            # Broker cancellation or the durable false heartbeat may itself be
            # non-cooperative. Admission is already closed locally; keep its
            # resources alive until it exits or deployment SIGKILL intervenes.
            admission_results = await admission
            admission_errors = [
                result for result in admission_results if isinstance(result, Exception)
            ]
            errors.extend(admission_errors)
            if not admission_errors:
                for runtime in self._runtimes:
                    await runtime.shutdown()

            deadline = CooperativeShutdownDeadline(
                self.settings.resource_shutdown_timeout_seconds
            )
            if self._heartbeat is not None:
                try:
                    await deadline.wait(self._heartbeat.close())
                except Exception as error:
                    errors.append(error)
            for channel in reversed(self._consumer_channels):
                if not channel.is_closed:
                    try:
                        await deadline.wait(channel.close())
                    except Exception as error:
                        errors.append(error)
            if (
                self._topology_channel is not None
                and not self._topology_channel.is_closed
            ):
                try:
                    await deadline.wait(self._topology_channel.close())
                except Exception as error:
                    errors.append(error)
            if self._connection is not None and not self._connection.is_closed:
                try:
                    await deadline.wait(self._connection.close())
                except Exception as error:
                    errors.append(error)
            if self._engine is not None:
                try:
                    await deadline.wait(self._engine.dispose())
                except Exception as error:
                    errors.append(error)
        finally:
            final_outcome = "cleanup_failed" if errors else outcome
            attributes = {
                "taskforge.process.role": "worker",
                "taskforge.outcome": final_outcome,
            }
            add_metric("taskforge.process.shutdown.operations", attributes=attributes)
            record_metric(
                "taskforge.process.shutdown.duration",
                perf_counter() - started,
                attributes,
            )
            log_event(
                logger,
                logging.INFO if not errors else logging.ERROR,
                "worker.shutdown.completed",
                {
                    "outcome": final_outcome,
                    "duration_ms": (perf_counter() - started) * 1000,
                },
            )
            if deadline is None:
                deadline = CooperativeShutdownDeadline(
                    self.settings.resource_shutdown_timeout_seconds
                )
            if self._metrics is not None:
                try:
                    self._metrics.shutdown(timeout_seconds=deadline.remaining_seconds)
                except Exception as error:
                    errors.append(error)
            if self._tracing is not None:
                try:
                    self._tracing.shutdown(timeout_seconds=deadline.remaining_seconds)
                except Exception as error:
                    errors.append(error)
            self.state = WorkerApplicationState.STOPPED
        if errors:
            raise ExceptionGroup("worker resource cleanup failed", errors)


async def _wait_closed(resource: AbstractConnection | AbstractChannel) -> None:
    await resource.closed()
