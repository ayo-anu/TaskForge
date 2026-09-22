"""Uvicorn runner with shutdown telemetry and unchanged admission semantics."""

from __future__ import annotations

import logging
import socket
from time import perf_counter

import uvicorn

from taskforge.logging import log_event, uvicorn_log_config
from taskforge.metrics import MetricsRuntime
from taskforge.metrics import add as add_metric
from taskforge.metrics import record as record_metric
from taskforge.settings import Settings
from taskforge.shutdown import CooperativeShutdownDeadline
from taskforge.tracing import TracingRuntime

logger = logging.getLogger(__name__)


class InstrumentedServer(uvicorn.Server):
    """Observe Uvicorn shutdown without replacing its signal/admission behavior."""

    def __init__(
        self,
        config: uvicorn.Config,
        *,
        metrics_runtime: MetricsRuntime | None = None,
        tracing_runtime: TracingRuntime | None = None,
        cleanup_timeout_seconds: float = 5.0,
    ) -> None:
        super().__init__(config)
        self._metrics_runtime = metrics_runtime
        self._tracing_runtime = tracing_runtime
        self._cleanup_timeout_seconds = cleanup_timeout_seconds

    async def shutdown(self, sockets: list[socket.socket] | None = None) -> None:
        started = perf_counter()
        outcome = "completed"
        timed_out = False

        class CancellationObserver(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                nonlocal timed_out
                if "timeout graceful shutdown exceeded" in record.getMessage():
                    timed_out = True
                    log_event(
                        logger,
                        logging.WARNING,
                        "api.shutdown.cancellation_requested",
                        {"reason.code": "drain_timeout"},
                    )

        observer = CancellationObserver()
        uvicorn_logger = logging.getLogger("uvicorn.error")
        uvicorn_logger.addHandler(observer)
        log_event(logger, logging.INFO, "api.shutdown.started")
        try:
            await super().shutdown(sockets=sockets)
        except BaseException:
            outcome = "cleanup_failed"
            raise
        finally:
            uvicorn_logger.removeHandler(observer)
            if timed_out and outcome == "completed":
                outcome = "drain_timeout"
            attributes = {
                "taskforge.process.role": "api",
                "taskforge.outcome": outcome,
            }
            add_metric("taskforge.process.shutdown.operations", attributes=attributes)
            record_metric(
                "taskforge.process.shutdown.duration",
                perf_counter() - started,
                attributes,
            )
            log_event(
                logger,
                logging.INFO if outcome == "completed" else logging.ERROR,
                "api.shutdown.completed",
                {"outcome": outcome, "duration_ms": (perf_counter() - started) * 1000},
            )
            # Uvicorn re-raises captured SIGTERM after shutdown, so an outer
            # main() finally block is not a reliable telemetry flush point.
            deadline = CooperativeShutdownDeadline(self._cleanup_timeout_seconds)
            if self._metrics_runtime is not None:
                self._metrics_runtime.shutdown(
                    timeout_seconds=deadline.remaining_seconds
                )
            if self._tracing_runtime is not None:
                self._tracing_runtime.shutdown(
                    timeout_seconds=deadline.remaining_seconds
                )


def run_api_server(
    settings: Settings,
    *,
    metrics_runtime: MetricsRuntime | None = None,
    tracing_runtime: TracingRuntime | None = None,
) -> None:
    """Run the pinned Uvicorn server with its native lifecycle ownership."""
    configuration = uvicorn.Config(
        "taskforge.api.application:create_production_app",
        factory=True,
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
        log_config=uvicorn_log_config(settings.log_level),
        access_log=False,
        timeout_graceful_shutdown=settings.api_graceful_shutdown_timeout_seconds,
    )
    InstrumentedServer(
        configuration,
        metrics_runtime=metrics_runtime,
        tracing_runtime=tracing_runtime,
        cleanup_timeout_seconds=settings.resource_shutdown_timeout_seconds,
    ).run()
