"""Opt-in M21 Task 4 profiling of database, broker, and WebSocket paths."""

from __future__ import annotations

import asyncio
import os
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
    environment_metadata,
    summarize_samples,
    write_bounded_artifact,
)
from tests.performance.m21_profiling import (
    M21ProfilingObserver,
    run_websocket_fanout_profile,
)
from tests.performance.m21_runner import run_m21_workload

pytestmark = [
    pytest.mark.integration,
    pytest.mark.workload,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_M21_PROFILING") != "1",
        reason="set TASKFORGE_RUN_M21_PROFILING=1 explicitly",
    ),
]


def _configure_metrics(reader: InMemoryMetricReader | None) -> Any:
    return configure_metrics(
        enabled=reader is not None,
        exporter="none",
        endpoint=None,
        export_interval_seconds=60,
        export_timeout_seconds=5,
        shutdown_timeout_seconds=5,
        outbox_staleness_seconds=120,
        service_name="taskforge-m21-profiling",
        environment="test",
        process_role="profiling",
        reader=reader,
    )


def _measurement_summary(observer: M21ProfilingObserver) -> dict[str, Any]:
    result = observer.result
    return {
        "latencies": {
            name: summarize_samples(values)
            for name, values in result.latency_samples().items()
        },
        "throughput": result.throughput,
        "authoritative_throughput_boundary": (result.authoritative_throughput_boundary),
        "worker_occupancy": result.worker_occupancy,
        "postgres_waits": result.postgres_waits,
        "resources": result.resources,
        "infrastructure": result.infrastructure,
        "application_measurement_interval_seconds": result.interval_seconds,
    }


def _run_canonical_repetition(
    *,
    ordinal: int,
    kind: str,
    amqp_address: str,
    temporary_path: Path,
    configuration: M21MeasurementConfiguration,
) -> tuple[dict[str, Any], dict[str, Any]]:
    reader = InMemoryMetricReader()
    metrics_runtime = _configure_metrics(reader)
    observer = M21ProfilingObserver(
        reader, sample_interval_seconds=configuration.sample_interval_seconds
    )
    task_evidence: Any = None
    primary_error: BaseException | None = None
    try:
        with temporary_database(
            "TASKFORGE_M21_DATABASE_URL", "taskforge_m21_profiling"
        ) as database_address:
            with migration_database_url(
                database_address.render_as_string(hide_password=False)
            ):
                command.upgrade(Config("alembic.ini"), "head")
            task_evidence = asyncio.run(
                run_m21_workload(
                    database_address,
                    amqp_address,
                    temporary_path / f"m21-profile-{kind}-{ordinal}.json",
                    observer=observer,
                    emit_evidence=False,
                )
            )
    except BaseException as error:
        primary_error = error
    finally:
        try:
            metrics_runtime.shutdown()
        finally:
            _configure_metrics(None)
    if primary_error is not None:
        raise primary_error.with_traceback(primary_error.__traceback__)
    assert task_evidence is not None
    return (
        {
            "measurement": _measurement_summary(observer),
            "profile": observer.profile_summary(),
        },
        {
            "status": task_evidence.status,
            "checkpoints": task_evidence.checkpoints,
        },
    )


def _run_fanout() -> list[dict[str, Any]]:
    with temporary_database(
        "TASKFORGE_M21_DATABASE_URL", "taskforge_m21_profiling"
    ) as database_address:
        with migration_database_url(
            database_address.render_as_string(hide_password=False)
        ):
            command.upgrade(Config("alembic.ini"), "head")
        return asyncio.run(run_websocket_fanout_profile(database_address))


def test_m21_bottleneck_profiling(tmp_path: Path) -> None:
    amqp_address = os.getenv("TASKFORGE_M21_AMQP_URL")
    output_value = os.getenv("TASKFORGE_M21_PROFILE_OUTPUT")
    if not amqp_address:
        pytest.fail("TASKFORGE_M21_AMQP_URL is required")
    if not output_value:
        pytest.fail("TASKFORGE_M21_PROFILE_OUTPUT is required")
    assert amqp_address is not None and output_value is not None
    output = Path(output_value)
    configuration = M21MeasurementConfiguration()
    evidence: dict[str, Any] = {
        "schema_version": 1,
        "status": "running",
        "phase": "initialize",
        "configuration": asdict(configuration),
        "artifact_bounds": {
            "maximum_bytes": 2 * 1024 * 1024,
            "maximum_query_aggregates_per_repetition": 512,
            "top_query_fingerprints_per_ranking": 25,
            "maximum_targeted_explain_plans": 3,
            "maximum_targeted_explain_plan_bytes": 32 * 1024,
        },
        "repetitions": [],
        "targeted_explain_plans": [],
        "failure": None,
    }
    started = perf_counter()
    try:
        evidence["phase"] = "warmup"
        warmup, warmup_task = _run_canonical_repetition(
            ordinal=0,
            kind="warmup",
            amqp_address=amqp_address,
            temporary_path=tmp_path,
            configuration=configuration,
        )
        evidence["warmup"] = {
            "discarded_from_measured_aggregate": True,
            "query_aggregate_count": warmup["profile"]["query_aggregate_count"],
            "pool": warmup["profile"]["pool"],
            "task1": warmup_task,
        }
        for ordinal in range(configuration.measured_repetitions):
            if (
                perf_counter() - started + configuration.repetition_timeout_seconds
                > configuration.controlled_deadline_seconds
            ):
                raise TimeoutError("controlled Task 4 profiling deadline reached")
            evidence["phase"] = "measured_repetition"
            evidence["active_repetition"] = ordinal
            repetition, task = _run_canonical_repetition(
                ordinal=ordinal,
                kind="measured",
                amqp_address=amqp_address,
                temporary_path=tmp_path,
                configuration=configuration,
            )
            evidence["repetitions"].append(
                {
                    "index": ordinal,
                    "status": "passed",
                    **repetition,
                    "task1": task,
                }
            )
        evidence["phase"] = "websocket_fanout"
        evidence["websocket_fanout"] = _run_fanout()
        evidence["phase"] = "collect_metadata"
        evidence["environment"] = environment_metadata()
        evidence["classification"] = {
            "postgresql": "pending_evidence_review",
            "broker": "pending_evidence_review",
            "websocket": "pending_evidence_review",
        }
        evidence["status"] = "passed"
        evidence["phase"] = "serialize_artifact"
        evidence.pop("active_repetition", None)
        write_bounded_artifact(output, evidence)
        print(
            f"M21 Task 4 profile passed: repetitions=5 fanout=1,5,10 artifact={output}"
        )
    except BaseException as error:
        failure = {
            "schema_version": 1,
            "status": "failed",
            "phase": str(evidence.get("phase", "unknown"))[:64],
            "failure": {"error_type": type(error).__name__[:64]},
        }
        try:
            write_bounded_artifact(output, failure)
        except BaseException as artifact_error:
            error.__dict__["m21_profile_artifact_error"] = type(artifact_error).__name__
        raise
