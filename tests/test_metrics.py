"""Deterministic bounded-cardinality metrics tests."""

from __future__ import annotations

from collections.abc import Iterator
from time import monotonic
from typing import Any

import pytest
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

import taskforge.metrics as task_metrics
from taskforge.metrics import (
    AGE_BUCKETS,
    API_DURATION_BUCKETS,
    FAST_DURATION_BUCKETS,
    HANDLER_DURATION_BUCKETS,
    WEBSOCKET_DURATION_BUCKETS,
    add,
    configure_metrics,
    normalize_http_method,
    normalize_http_route,
    record,
    register_http_routes,
    status_class,
    update_outbox_observation,
)


@pytest.fixture
def metric_reader() -> Iterator[InMemoryMetricReader]:
    reader = InMemoryMetricReader()
    runtime = configure_metrics(
        enabled=True,
        exporter="none",
        endpoint=None,
        export_interval_seconds=60,
        export_timeout_seconds=1,
        shutdown_timeout_seconds=1,
        outbox_staleness_seconds=120,
        service_name="taskforge",
        environment="test",
        process_role="test",
        reader=reader,
    )
    try:
        yield reader
    finally:
        runtime.shutdown()
        configure_metrics(
            enabled=False,
            exporter="none",
            endpoint=None,
            export_interval_seconds=60,
            export_timeout_seconds=1,
            shutdown_timeout_seconds=1,
            outbox_staleness_seconds=120,
            service_name="taskforge",
            environment="test",
            process_role="test",
        )


def _metrics(reader: InMemoryMetricReader) -> dict[str, Any]:
    data = reader.get_metrics_data()
    assert data is not None
    return {
        metric.name: metric
        for resource in data.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
    }


def _points(reader: InMemoryMetricReader, name: str) -> tuple[Any, ...]:
    return tuple(_metrics(reader)[name].data.data_points)


def test_instrument_names_types_and_units_are_exact() -> None:
    expected = {
        "taskforge.api.requests": ("Counter", "{request}"),
        "taskforge.api.request.duration": ("Histogram", "s"),
        "taskforge.dispatch.created": ("Counter", "{dispatch}"),
        "taskforge.dispatch.publications": ("Counter", "{publication}"),
        "taskforge.dispatch.publish.duration": ("Histogram", "s"),
        "taskforge.dispatch.publication_records": ("Counter", "{record}"),
        "taskforge.dispatch.outbox.invalid": ("Counter", "{dispatch}"),
        "taskforge.dispatch.outbox.duration": ("Histogram", "s"),
        "taskforge.dispatch.outbox.pending": ("ObservableGauge", "{dispatch}"),
        "taskforge.dispatch.outbox.oldest.age": ("ObservableGauge", "s"),
        "taskforge.retry.transitions": ("Counter", "{transition}"),
        "taskforge.retry.delay": ("Histogram", "s"),
        "taskforge.retry.dispatches": ("Counter", "{dispatch}"),
        "taskforge.retry.due.age": ("Histogram", "s"),
        "taskforge.handler.executions": ("Counter", "{execution}"),
        "taskforge.handler.duration": ("Histogram", "s"),
        "taskforge.worker.claims": ("Counter", "{claim}"),
        "taskforge.task.result_submissions": ("Counter", "{submission}"),
        "taskforge.worker.running.deliveries": ("UpDownCounter", "{delivery}"),
        "taskforge.worker.heartbeats": ("Counter", "{heartbeat}"),
        "taskforge.recovery.scan.candidates": ("Counter", "{candidate}"),
        "taskforge.recovery.scan.operations": ("Counter", "{scan}"),
        "taskforge.recovery.operations": ("Counter", "{recovery}"),
        "taskforge.recovery.duration": ("Histogram", "s"),
        "taskforge.dead_letters.created": ("Counter", "{dead_letter}"),
        "taskforge.dead_letters.operations": ("Counter", "{operation}"),
        "taskforge.websocket.connection.attempts": ("Counter", "{connection}"),
        "taskforge.websocket.connections.active": ("UpDownCounter", "{connection}"),
        "taskforge.websocket.connection.duration": ("Histogram", "s"),
        "taskforge.websocket.disconnections": ("Counter", "{connection}"),
        "taskforge.websocket.backpressure": ("Counter", "{event}"),
        "taskforge.websocket.resume.outcomes": ("Counter", "{connection}"),
    }
    assert set(task_metrics._instruments) == set(expected)
    for name, (kind, unit) in expected.items():
        instrument = task_metrics._instruments[name]
        assert type(instrument).__name__.endswith(kind)
        assert instrument._unit == unit


@pytest.mark.parametrize(
    ("name", "expected"),
    (
        ("taskforge.api.request.duration", API_DURATION_BUCKETS),
        ("taskforge.dispatch.publish.duration", FAST_DURATION_BUCKETS),
        ("taskforge.dispatch.outbox.duration", AGE_BUCKETS),
        ("taskforge.retry.delay", AGE_BUCKETS),
        ("taskforge.retry.due.age", AGE_BUCKETS),
        ("taskforge.handler.duration", HANDLER_DURATION_BUCKETS),
        ("taskforge.recovery.duration", FAST_DURATION_BUCKETS),
        ("taskforge.websocket.connection.duration", WEBSOCKET_DURATION_BUCKETS),
    ),
)
def test_histograms_use_explicit_buckets(
    metric_reader: InMemoryMetricReader,
    name: str,
    expected: tuple[float, ...],
) -> None:
    record(name, 0.1)
    assert _points(metric_reader, name)[0].explicit_bounds == expected


def test_route_method_and_status_normalization_is_bounded() -> None:
    register_http_routes({"/api/v1/runs/{run_id}", "/health", "/ready"})

    assert normalize_http_route("/api/v1/runs/{run_id}") == ("/api/v1/runs/{run_id}")
    assert normalize_http_route("/api/v1/runs/secret-id") == "unmatched"
    assert normalize_http_route("sentinel-secret") == "unmatched"
    assert normalize_http_method("get") == "GET"
    assert normalize_http_method("SENTINEL") == "other"
    assert status_class(404) == "4xx"
    assert status_class("secret") == "unknown"


def test_forbidden_attributes_and_secrets_are_dropped(
    metric_reader: InMemoryMetricReader,
) -> None:
    secret = "sentinel-secret-identifier"
    add(
        "taskforge.worker.claims",
        attributes={
            "taskforge.outcome": "rejected",
            "taskforge.rejection.reason": "stale_attempt",
            "task.run.id": secret,
            "broker.route": secret,
            "error.type": secret,
            "taskforge.reason": secret,
        },
    )

    point = _points(metric_reader, "taskforge.worker.claims")[0]
    assert dict(point.attributes) == {
        "taskforge.outcome": "rejected",
        "taskforge.rejection.reason": "stale_attempt",
    }
    assert secret not in repr(_metrics(metric_reader))


def test_globally_valid_attribute_is_dropped_for_the_wrong_instrument(
    metric_reader: InMemoryMetricReader,
) -> None:
    add(
        "taskforge.dispatch.created",
        attributes={
            "taskforge.outcome": "accepted",
            "taskforge.scan.kind": "expired_claim",
            "taskforge.reason": "retry_exhausted",
        },
    )

    point = _points(metric_reader, "taskforge.dispatch.created")[0]
    assert dict(point.attributes) == {}


def test_pending_outbox_schema_allows_only_boolean_saturation() -> None:
    assert task_metrics._INSTRUMENT_ATTRIBUTE_KEYS[
        "taskforge.dispatch.outbox.pending"
    ] == frozenset({"taskforge.saturated"})
    assert task_metrics._safe_attributes(
        "taskforge.dispatch.outbox.pending",
        {"taskforge.saturated": True},
    ) == {"taskforge.saturated": True}
    assert (
        task_metrics._safe_attributes(
            "taskforge.dispatch.outbox.pending",
            {
                "taskforge.saturated": "true",
                "taskforge.outcome": "accepted",
            },
        )
        is None
    )
    assert (
        task_metrics._safe_attributes(
            "taskforge.dispatch.created",
            {"taskforge.saturated": True},
        )
        is None
    )


def test_capped_outbox_observation_and_stale_omission(
    metric_reader: InMemoryMetricReader,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_at = monotonic()
    monkeypatch.setattr(task_metrics, "monotonic", lambda: observed_at)
    update_outbox_observation(
        pending=10_000,
        saturated=True,
        oldest_age_seconds=42,
    )

    pending = _points(metric_reader, "taskforge.dispatch.outbox.pending")[0]
    oldest = _points(metric_reader, "taskforge.dispatch.outbox.oldest.age")[0]
    assert pending.value == 10_000
    assert dict(pending.attributes) == {"taskforge.saturated": True}
    assert oldest.value == 42

    monkeypatch.setattr(task_metrics, "monotonic", lambda: observed_at + 121)
    assert tuple(task_metrics._observe_outbox_pending(None)) == ()  # type: ignore[arg-type]
    assert tuple(task_metrics._observe_outbox_oldest_age(None)) == ()  # type: ignore[arg-type]


def test_process_local_up_down_counter_balances(
    metric_reader: InMemoryMetricReader,
) -> None:
    add("taskforge.worker.running.deliveries", 1)
    add("taskforge.worker.running.deliveries", 1)
    add("taskforge.worker.running.deliveries", -1)
    add("taskforge.worker.running.deliveries", -1)

    assert _points(metric_reader, "taskforge.worker.running.deliveries")[0].value == 0


def test_disabled_metrics_are_a_true_noop() -> None:
    runtime = configure_metrics(
        enabled=False,
        exporter="none",
        endpoint=None,
        export_interval_seconds=60,
        export_timeout_seconds=1,
        shutdown_timeout_seconds=1,
        outbox_staleness_seconds=120,
        service_name="taskforge",
        environment="test",
        process_role="test",
    )
    add("taskforge.dispatch.created")
    runtime.shutdown()
    assert not runtime.enabled


def test_instrument_failure_does_not_affect_authoritative_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailingCounter:
        def add(self, value: int, attributes: object) -> None:
            del value, attributes
            raise RuntimeError("telemetry-secret")

    monkeypatch.setitem(
        task_metrics._instruments, "taskforge.dispatch.created", FailingCounter()
    )
    authoritative_body_ran = False
    add("taskforge.dispatch.created")
    authoritative_body_ran = True
    assert authoritative_body_ran
