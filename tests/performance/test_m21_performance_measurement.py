"""Opt-in Milestone 21 Task 2 measurements of the canonical workload."""

from __future__ import annotations

import asyncio
import os
import signal
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from time import perf_counter
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from taskforge.metrics import configure_metrics
from tests.integration.postgresql import migration_database_url, temporary_database
from tests.performance.m21_measurement import (
    M21MeasurementConfiguration,
    M21MeasurementObserverImpl,
    aggregate_repetitions,
    environment_metadata,
    write_bounded_artifact,
)
from tests.performance.m21_runner import run_m21_workload

pytestmark = [
    pytest.mark.integration,
    pytest.mark.workload,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_M21_MEASUREMENT") != "1",
        reason="set TASKFORGE_RUN_M21_MEASUREMENT=1 explicitly",
    ),
]


class MeasurementDeadlineExceeded(TimeoutError):
    pass


_FAILURE_PHASES = {
    "initialize",
    "warmup_execution",
    "measured_repetition_execution",
    "aggregate_metrics",
    "collect_metadata",
    "serialize_artifact",
}


def _safe_error_type(error: BaseException) -> str:
    name = type(error).__name__[:64]
    return name if name.replace("_", "").isalnum() else "Exception"


def _write_minimal_failure_evidence(
    output: Path,
    evidence: dict[str, Any],
    primary_error: BaseException,
) -> None:
    phase = evidence.get("phase")
    if phase not in _FAILURE_PHASES:
        phase = "unknown"
    repetition = evidence.get("active_repetition")
    safe_repetition = None
    if (
        isinstance(repetition, dict)
        and repetition.get("kind") in {"warmup", "measured"}
        and isinstance(repetition.get("index"), int)
        and 0 <= repetition["index"] <= 5
    ):
        safe_repetition = {
            "kind": repetition["kind"],
            "index": repetition["index"],
        }
    repetitions = evidence.get("repetitions")
    completed = len(repetitions) if isinstance(repetitions, list) else 0
    minimal = {
        "schema_version": 1,
        "status": "failed",
        "phase": phase,
        "failure": {
            "error_type": _safe_error_type(primary_error),
            "repetition": safe_repetition,
            "diagnostics": {
                "completed_measured_repetitions": min(completed, 5),
                "artifact_mode": "minimal_failure",
            },
        },
    }
    try:
        write_bounded_artifact(output, minimal)
    except BaseException as evidence_error:
        primary_error.__dict__["m21_failure_evidence_write_error"] = {
            "error_type": _safe_error_type(evidence_error),
        }


@contextmanager
def _alarm_deadline(seconds: float) -> Iterator[None]:
    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_delay, previous_interval = signal.getitimer(signal.ITIMER_REAL)
    started = perf_counter()

    def expire(_signum: int, _frame: Any) -> None:
        raise MeasurementDeadlineExceeded

    signal.signal(signal.SIGALRM, expire)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_delay > 0:
            remaining = max(0.001, previous_delay - (perf_counter() - started))
            signal.setitimer(signal.ITIMER_REAL, remaining, previous_interval)


def _metrics_configuration(reader: InMemoryMetricReader | None) -> Any:
    return configure_metrics(
        enabled=reader is not None,
        exporter="none",
        endpoint=None,
        export_interval_seconds=60,
        export_timeout_seconds=5,
        shutdown_timeout_seconds=5,
        outbox_staleness_seconds=120,
        service_name="taskforge-m21-measurement",
        environment="test",
        process_role="measurement",
        reader=reader,
    )


def _metric_snapshot(reader: InMemoryMetricReader) -> dict[str, Any]:
    collected = reader.get_metrics_data()
    if collected is None:
        return {}
    result: dict[str, list[dict[str, Any]]] = {}
    for resource_metrics in collected.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                points = []
                for point in metric.data.data_points:
                    item: dict[str, Any] = {
                        "attributes": dict(point.attributes or {}),
                    }
                    for name in ("value", "count", "sum", "min", "max"):
                        value = getattr(point, name, None)
                        if value is not None:
                            item[name] = value
                    bucket_counts = getattr(point, "bucket_counts", None)
                    if bucket_counts is not None:
                        item["bucket_counts"] = list(bucket_counts)
                    points.append(item)
                result[metric.name] = points
    return result


def _metric_total(snapshot: dict[str, Any], name: str) -> int | float:
    total: int | float = 0
    for point in snapshot.get(name, []):
        value = point.get("value", point.get("sum", point.get("count", 0)))
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            total += value
    return total


def _run_repetition(
    *,
    ordinal: int,
    kind: str,
    amqp_url: str,
    tmp_path: Path,
    configuration: M21MeasurementConfiguration,
) -> tuple[Any, dict[str, Any], dict[str, Any]]:
    observer = M21MeasurementObserverImpl(configuration.sample_interval_seconds)
    reader = InMemoryMetricReader()
    metrics_runtime = _metrics_configuration(reader)
    primary_error: BaseException | None = None
    cleanup_errors: list[dict[str, str]] = []
    task_evidence: Any = None
    snapshot: dict[str, Any] = {}
    try:
        with _alarm_deadline(configuration.repetition_timeout_seconds):
            with temporary_database(
                "TASKFORGE_M21_DATABASE_URL", "taskforge_m21_measurement"
            ) as url:
                with migration_database_url(url.render_as_string(hide_password=False)):
                    command.upgrade(Config("alembic.ini"), "head")
                task_evidence = asyncio.run(
                    run_m21_workload(
                        url,
                        amqp_url,
                        tmp_path / f"m21-{kind}-{ordinal}.json",
                        observer=observer,
                        emit_evidence=False,
                    )
                )
            snapshot = _metric_snapshot(reader)
    except BaseException as error:
        primary_error = error
    finally:
        try:
            metrics_runtime.shutdown()
        except BaseException as error:
            cleanup_errors.append(
                {"operation": "metrics_provider", "error_type": type(error).__name__}
            )
        try:
            _metrics_configuration(None)
        except BaseException as error:
            cleanup_errors.append(
                {"operation": "metrics_noop_reset", "error_type": type(error).__name__}
            )
    if primary_error is not None:
        if cleanup_errors:
            primary_error.__dict__["m21_cleanup_errors"] = cleanup_errors
        raise primary_error.with_traceback(primary_error.__traceback__)
    if cleanup_errors:
        raise RuntimeError("M21 measurement repetition cleanup failed")
    assert task_evidence is not None
    return (
        observer.result,
        snapshot,
        {
            "status": task_evidence.status,
            "checkpoints": task_evidence.checkpoints,
        },
    )


def test_m21_performance_measurement(tmp_path: Path) -> None:
    amqp_url = os.getenv("TASKFORGE_M21_AMQP_URL")
    output_value = os.getenv("TASKFORGE_M21_MEASUREMENT_OUTPUT")
    if not amqp_url:
        pytest.fail("TASKFORGE_M21_AMQP_URL is required")
    if not output_value:
        pytest.fail("TASKFORGE_M21_MEASUREMENT_OUTPUT is required")
    output = Path(output_value)
    configuration = M21MeasurementConfiguration()
    evidence: dict[str, Any] = {
        "schema_version": 1,
        "status": "running",
        "phase": "initialize",
        "configuration": asdict(configuration),
        "metric_definitions": {
            "api_run_detail_latency_ms": (
                "Client monotonic request start through complete authenticated "
                "GET response body read and validation; milliseconds."
            ),
            "initial_root_creation_to_dispatch_latency_ms": (
                "Canonical initially-runnable root task creation to persisted "
                "runnable-to-dispatched event; not a general runnable timestamp."
            ),
            "initial_root_creation_to_completion_latency_ms": (
                "Canonical initially-runnable root task creation to authoritative "
                "result; not valid for tasks promoted to runnable later."
            ),
            "dispatch_publication_latency_ms": (
                "task_dispatch_outbox.created_at to published_at; milliseconds."
            ),
            "claim_latency_ms": (
                "Persisted runnable-to-dispatched event to "
                "task_attempt_claims.acquired_at; milliseconds."
            ),
            "claim_to_running_latency_ms": (
                "task_attempt_claims.acquired_at to persisted running event; "
                "milliseconds."
            ),
            "completion_latency_ms": (
                "Persisted running event to task_attempt_results.completed_at; "
                "milliseconds."
            ),
            "worker_occupancy": (
                "Persisted result claim_generation joined to task_attempt_claims."
            ),
            "api_p99_interpretation": (
                "Descriptive for this fixed workload; not a production SLO."
            ),
        },
        "repetitions": [],
        "failure": None,
    }
    started = perf_counter()
    primary_error: BaseException | None = None
    try:
        with _alarm_deadline(configuration.overall_timeout_seconds):
            evidence["phase"] = "warmup_execution"
            warmup, warmup_metrics, warmup_task = _run_repetition(
                ordinal=0,
                kind="warmup",
                amqp_url=amqp_url,
                tmp_path=tmp_path,
                configuration=configuration,
            )
            evidence["warmup"] = {
                "discarded": True,
                "authoritative_throughput_boundary": (
                    warmup.authoritative_throughput_boundary
                ),
                "sample_counts": {
                    name: len(values)
                    for name, values in warmup.latency_samples().items()
                },
                "production_metrics": warmup_metrics,
                "task1": warmup_task,
            }
            measured = []
            measured_metric_snapshots = []
            for ordinal in range(configuration.measured_repetitions):
                elapsed = perf_counter() - started
                if (
                    elapsed + configuration.repetition_timeout_seconds
                    > configuration.controlled_deadline_seconds
                ):
                    raise MeasurementDeadlineExceeded(
                        "controlled Task 2 deadline reached"
                    )
                evidence["phase"] = "measured_repetition_execution"
                evidence["active_repetition"] = {"kind": "measured", "index": ordinal}
                result, metrics, task = _run_repetition(
                    ordinal=ordinal,
                    kind="measured",
                    amqp_url=amqp_url,
                    tmp_path=tmp_path,
                    configuration=configuration,
                )
                measured.append(result)
                measured_metric_snapshots.append(metrics)
                evidence["repetitions"].append(
                    {
                        "index": ordinal,
                        "status": "passed",
                        "measurements": asdict(result),
                        "summaries": {
                            name: summarize
                            for name, summarize in (
                                (sample_name, _summary(sample_values))
                                for sample_name, sample_values in result.latency_samples().items()
                            )
                        },
                        "production_metrics": metrics,
                        "task1": task,
                    }
                )
            evidence["phase"] = "aggregate_metrics"
            evidence["aggregate"] = aggregate_repetitions(measured)
            evidence["production_metric_cross_checks"] = {
                name: sum(
                    _metric_total(snapshot, name)
                    for snapshot in measured_metric_snapshots
                )
                for name in (
                    "taskforge.api.requests",
                    "taskforge.dispatch.created",
                    "taskforge.handler.executions",
                    "taskforge.task.result_submissions",
                )
            }
            evidence["phase"] = "collect_metadata"
            evidence["environment"] = environment_metadata()
            evidence["status"] = "passed"
            evidence["phase"] = "serialize_artifact"
            evidence.pop("active_repetition", None)
            write_bounded_artifact(output, evidence)
    except BaseException as error:
        primary_error = error
        _write_minimal_failure_evidence(output, evidence, error)
    if primary_error is not None:
        raise primary_error.with_traceback(primary_error.__traceback__)


def _summary(samples: list[float]) -> dict[str, float | int]:
    from tests.performance.m21_measurement import summarize_samples

    return summarize_samples(samples)
