"""Deterministic calculation tests for M21 Task 2 measurement evidence."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import tests.performance.test_m21_performance_measurement as measurement_workload
from tests.performance.m21_measurement import (
    MAX_ARTIFACT_BYTES,
    M21MeasurementConfiguration,
    RepetitionMeasurements,
    aggregate_repetitions,
    nearest_rank,
    summarize_samples,
    summarize_wait_samples,
    union_duration_seconds,
    write_bounded_artifact,
)


def test_nearest_rank_uses_raw_samples_without_interpolation() -> None:
    samples = [9.0, 1.0, 5.0, 3.0]

    assert nearest_rank(samples, 50) == 3.0
    assert nearest_rank(samples, 95) == 9.0
    assert nearest_rank(samples, 99) == 9.0
    assert summarize_samples(samples) == {
        "count": 4,
        "min": 1.0,
        "mean": 4.5,
        "max": 9.0,
        "p50": 3.0,
        "p95": 9.0,
        "p99": 9.0,
    }


@pytest.mark.parametrize("samples", [[], [-1.0], [float("inf")], [float("nan")]])
def test_nearest_rank_rejects_invalid_samples(samples: list[float]) -> None:
    with pytest.raises(ValueError):
        nearest_rank(samples, 95)


def test_worker_interval_union_does_not_double_count_overlap() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = start + timedelta(seconds=10)

    assert (
        union_duration_seconds(
            [
                (start - timedelta(seconds=2), start + timedelta(seconds=3)),
                (start + timedelta(seconds=2), start + timedelta(seconds=7)),
                (start + timedelta(seconds=9), end + timedelta(seconds=2)),
            ],
            start,
            end,
        )
        == 8.0
    )


def test_wait_exposure_uses_elapsed_sample_spacing() -> None:
    summary = summarize_wait_samples(
        [
            {"observed_at": 1.0, "groups": {"Lock:transactionid": 2}},
            {"observed_at": 1.25, "groups": {"running": 1}},
            {"observed_at": 1.5, "groups": {}},
        ]
    )

    assert summary["sample_count"] == 3
    assert summary["max_waiting_sessions"] == 2
    assert summary["wait_session_seconds"] == {"Lock:transactionid": 0.5}


def test_aggregate_requires_exactly_five_complete_repetitions() -> None:
    with pytest.raises(ValueError):
        aggregate_repetitions([RepetitionMeasurements()] * 4)


def test_aggregate_rejects_repetition_without_authoritative_boundaries() -> None:
    repetitions = [RepetitionMeasurements()] * 5

    with pytest.raises(ValueError, match="incomplete authoritative throughput"):
        aggregate_repetitions(repetitions)


def test_authoritative_throughput_boundary_uses_exact_persisted_timestamps(
    tmp_path: Path,
) -> None:
    first_dispatch = datetime(2026, 1, 1, 12, 0, 0, 123456, tzinfo=UTC)
    last_result = first_dispatch + timedelta(seconds=4, microseconds=250000)
    measurement = RepetitionMeasurements(
        throughput={"authoritative_task_throughput_per_second": 100 / 4.25},
        authoritative_throughput_boundary={
            "first_dispatch": first_dispatch.isoformat(),
            "first_dispatch_semantics": "persisted dispatch event",
            "last_result": last_result.isoformat(),
            "last_result_semantics": "persisted authoritative result",
            "denominator_seconds": 4.25,
            "authoritative_task_throughput_per_second": 100 / 4.25,
        },
    )
    repetitions = [measurement] * 5

    with pytest.raises(ValueError, match="unexpected sample count"):
        aggregate_repetitions(repetitions)
    boundary = measurement.authoritative_throughput_boundary
    assert datetime.fromisoformat(boundary["last_result"]) - datetime.fromisoformat(
        boundary["first_dispatch"]
    ) == timedelta(seconds=boundary["denominator_seconds"])
    assert boundary["authoritative_task_throughput_per_second"] == pytest.approx(
        100 / boundary["denominator_seconds"]
    )
    output = tmp_path / "measurement.json"
    write_bounded_artifact(output, {"repetition": asdict(measurement)})
    persisted = json.loads(output.read_text(encoding="utf-8"))["repetition"][
        "authoritative_throughput_boundary"
    ]
    assert persisted["first_dispatch"] == first_dispatch.isoformat()
    assert persisted["last_result"] == last_result.isoformat()
    assert persisted["denominator_seconds"] == 4.25


def test_canonical_deadline_and_repetition_policy_is_fixed() -> None:
    configuration = M21MeasurementConfiguration()

    assert configuration.warmup_repetitions == 1
    assert configuration.measured_repetitions == 5
    assert configuration.repetition_timeout_seconds == 120
    assert configuration.controlled_deadline_seconds == 840
    assert configuration.overall_timeout_seconds == 900

    with pytest.raises(ValueError):
        M21MeasurementConfiguration(measured_repetitions=4)


def test_artifact_writer_rejects_connection_and_authority_material(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError):
        write_bounded_artifact(
            tmp_path / "unsafe.json", {"status": "failed", "password": "value"}
        )
    with pytest.raises(ValueError):
        write_bounded_artifact(
            tmp_path / "unsafe.json",
            {"status": "failed", "diagnostic": "amqp://example.invalid"},
        )


def test_oversized_success_artifact_preserves_primary_size_error(
    tmp_path: Path,
) -> None:
    output = tmp_path / "measurement.json"
    evidence = {
        "phase": "serialize_artifact",
        "repetitions": [],
        "oversized": "x" * (MAX_ARTIFACT_BYTES + 1),
    }

    with pytest.raises(ValueError, match="exceeds the 2 MiB bound") as caught:
        try:
            write_bounded_artifact(output, evidence)
        except BaseException as primary_error:
            measurement_workload._write_minimal_failure_evidence(
                output, evidence, primary_error
            )
            raise primary_error.with_traceback(primary_error.__traceback__) from None

    assert type(caught.value) is ValueError
    assert json.loads(output.read_text(encoding="utf-8"))["status"] == "failed"


def test_failure_evidence_write_failure_does_not_replace_primary(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    primary_error = RuntimeError("primary detail must not be recorded")

    def fail_write(_output: Path, _evidence: dict[str, object]) -> None:
        raise OSError("secondary detail must not be recorded")

    monkeypatch.setattr(measurement_workload, "write_bounded_artifact", fail_write)
    measurement_workload._write_minimal_failure_evidence(
        tmp_path / "measurement.json",
        {"phase": "initialize", "repetitions": []},
        primary_error,
    )

    assert primary_error.__dict__["m21_failure_evidence_write_error"] == {
        "error_type": "OSError"
    }


def test_minimal_failure_evidence_is_bounded_and_sanitized(tmp_path: Path) -> None:
    output = tmp_path / "measurement.json"
    primary_error = RuntimeError("Bearer authority-value")
    evidence = {
        "phase": "not-an-allowlisted-phase-with-sensitive-context",
        "active_repetition": {"kind": "measured", "index": 3},
        "repetitions": [{"credential": "sensitive"}] * 20,
        "connection": "amqp://sensitive.invalid",
        "payload": "x" * (MAX_ARTIFACT_BYTES + 1),
    }

    measurement_workload._write_minimal_failure_evidence(
        output, evidence, primary_error
    )

    raw = output.read_bytes()
    persisted = json.loads(raw)
    assert len(raw) < 1024
    assert persisted == {
        "failure": {
            "diagnostics": {
                "artifact_mode": "minimal_failure",
                "completed_measured_repetitions": 5,
            },
            "error_type": "RuntimeError",
            "repetition": {"index": 3, "kind": "measured"},
        },
        "phase": "unknown",
        "schema_version": 1,
        "status": "failed",
    }
    assert b"authority-value" not in raw
    assert b"sensitive" not in raw
