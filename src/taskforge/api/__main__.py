"""Minimal executable entry point for the Taskforge API process."""

import uvicorn

from taskforge.logging import configure_logging, uvicorn_log_config
from taskforge.metrics import configure_metrics
from taskforge.settings import Settings
from taskforge.tracing import configure_tracing


def main() -> int:
    """Run the API using only typed runtime configuration."""
    settings = Settings()
    configure_logging(
        service_name=settings.application_name,
        environment=settings.environment,
        process_role="api",
        level=settings.log_level,
    )
    tracing = configure_tracing(
        enabled=settings.tracing_enabled,
        exporter=settings.tracing_exporter,
        endpoint=settings.tracing_otlp_endpoint,
        sample_ratio=settings.tracing_sample_ratio,
        export_timeout_seconds=settings.tracing_export_timeout_seconds,
        shutdown_timeout_seconds=settings.tracing_shutdown_timeout_seconds,
        service_name=settings.application_name,
        environment=settings.environment,
        process_role="api",
    )
    metric_runtime = configure_metrics(
        enabled=settings.metrics_enabled,
        exporter=settings.metrics_exporter,
        endpoint=settings.metrics_otlp_endpoint,
        export_interval_seconds=settings.metrics_export_interval_seconds,
        export_timeout_seconds=settings.metrics_export_timeout_seconds,
        shutdown_timeout_seconds=settings.metrics_shutdown_timeout_seconds,
        outbox_staleness_seconds=settings.metrics_outbox_staleness_seconds,
        service_name=settings.application_name,
        environment=settings.environment,
        process_role="api",
    )
    try:
        uvicorn.run(
            "taskforge.api.application:create_app",
            factory=True,
            host=settings.api_host,
            port=settings.api_port,
            log_level=settings.log_level.lower(),
            log_config=uvicorn_log_config(settings.log_level),
            access_log=False,
        )
    finally:
        metric_runtime.shutdown()
        tracing.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
