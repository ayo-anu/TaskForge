"""Failure-isolated, bounded-cardinality OpenTelemetry metrics."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from threading import Thread
from time import monotonic
from typing import Final, Protocol, cast

from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.metrics import CallbackOptions, Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import MetricReader, PeriodicExportingMetricReader
from opentelemetry.sdk.metrics.view import ExplicitBucketHistogramAggregation, View
from opentelemetry.sdk.resources import Resource

_INSTRUMENTATION_NAME: Final = "taskforge"
OUTBOX_OBSERVATION_LIMIT: Final = 10_000

API_DURATION_BUCKETS: Final = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
)
FAST_DURATION_BUCKETS: Final = (
    0.001,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
)
HANDLER_DURATION_BUCKETS: Final = (
    0.01,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
    300.0,
    900.0,
    3600.0,
)
AGE_BUCKETS: Final = (
    0.1,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
    300.0,
    900.0,
    3600.0,
    21600.0,
    86400.0,
)
WEBSOCKET_DURATION_BUCKETS: Final = (
    1.0,
    5.0,
    15.0,
    30.0,
    60.0,
    300.0,
    900.0,
    1800.0,
    3600.0,
    21600.0,
    86400.0,
)


@dataclass(frozen=True)
class OutboxObservation:
    """One capped durable-backlog observation held entirely in memory."""

    pending: int
    saturated: bool
    oldest_age_seconds: float | None
    observed_at_monotonic: float


@dataclass
class MetricsRuntime:
    provider: MeterProvider | None
    shutdown_timeout_seconds: float

    @property
    def enabled(self) -> bool:
        return self.provider is not None

    def shutdown(self) -> None:
        if self.provider is None:
            return
        provider, self.provider = self.provider, None

        def close() -> None:
            try:
                provider.shutdown()
            except Exception:
                pass

        thread = Thread(target=close, name="taskforge-metrics-shutdown", daemon=True)
        try:
            thread.start()
            thread.join(self.shutdown_timeout_seconds)
        except Exception:
            pass


_provider: MeterProvider | None = None
_meter = metrics.get_meter(_INSTRUMENTATION_NAME)
_outbox_observation: OutboxObservation | None = None
_outbox_staleness_seconds = 120.0
_known_http_routes: frozenset[str] = frozenset()


class _AddInstrument(Protocol):
    def add(
        self, value: int, attributes: Mapping[str, str | bool] | None = None
    ) -> None: ...


class _RecordInstrument(Protocol):
    def record(
        self, value: float, attributes: Mapping[str, str | bool] | None = None
    ) -> None: ...


_ALLOWED_ATTRIBUTE_VALUES: Final[dict[str, frozenset[str]]] = {
    "http.request.method": frozenset(
        {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "other"}
    ),
    "http.response.status_class": frozenset(
        {"1xx", "2xx", "3xx", "4xx", "5xx", "unknown"}
    ),
    "taskforge.outcome": frozenset(
        {
            "completed",
            "unhandled_exception",
            "accepted",
            "timeout",
            "rejected",
            "unavailable",
            "recorded",
            "already_recorded",
            "invariant_failure",
            "persistence_failure",
            "scheduled",
            "failed_no_policy",
            "failed_exhausted",
            "already_scheduled",
            "not_eligible",
            "dispatched",
            "skipped",
            "acquired_active",
            "replayed_active",
            "replayed_expired",
            "stale",
            "conflict",
            "replayed_identical",
            "success",
            "retryable_failure",
            "permanent_failure",
            "cancellation",
            "service_unavailable",
            "capacity_rejected",
            "policy_rejected",
            "resume_rejected",
            "not_requested",
            "resumed",
            "invalid_cursor",
            "cursor_ahead",
            "snapshot_required",
            "session_ended",
            "candidate_refreshed",
            "session_already_ended",
            "cancelled",
            "retry_scheduled",
            "candidate_no_longer_expired",
            "claim_already_terminated",
            "attempt_no_longer_latest",
            "task_not_eligible",
            "workflow_not_eligible",
            "result_already_accepted",
            "already_recovered",
            "infrastructure_failure",
            "not_found",
            "limit_exceeded",
            "limited",
            "degraded_allowed",
            "degraded_limited",
            "rate_limited",
        }
    ),
    "taskforge.result.kind": frozenset(
        {"success", "retryable_failure", "permanent_failure", "cancellation"}
    ),
    "taskforge.failure.kind": frozenset(
        {"handler_reported", "handler_exception", "execution_timeout", "claim_expired"}
    ),
    "taskforge.rejection.reason": frozenset(
        {
            "invalid_dispatch",
            "stale_attempt",
            "obsolete_task",
            "worker_authority_rejected",
            "worker_session_unavailable",
            "worker_session_inactive",
            "worker_unavailable",
            "capability_mismatch",
            "already_authoritative",
            "stale_heartbeat",
            "heartbeat_sequence_gap",
            "heartbeat_replay_conflict",
        }
    ),
    "taskforge.scan.kind": frozenset({"expired_claim", "stale_worker_session"}),
    "taskforge.recovery.kind": frozenset({"expired_claim", "stale_worker_session"}),
    "taskforge.reason": frozenset({"permanent_failure", "retry_exhausted"}),
    "taskforge.operation": frozenset({"acknowledge", "resolve", "redrive"}),
    "taskforge.rate_limit.policy": frozenset(
        {
            "api_auth_network",
            "api_auth_credential",
            "worker_auth_network",
            "worker_auth_credential",
            "run_create",
            "run_replay",
            "dead_letter_redrive",
            "worker_register",
            "worker_result",
            "websocket_network",
            "websocket_principal",
        }
    ),
    "taskforge.disconnect.kind": frozenset(
        {
            "client",
            "slow_consumer",
            "session_expired",
            "service_failure",
            "service_restart",
            "handshake_abort",
        }
    ),
}

_INSTRUMENT_ATTRIBUTE_KEYS: Final[dict[str, frozenset[str]]] = {
    "taskforge.api.requests": frozenset(
        {
            "http.request.method",
            "http.route",
            "http.response.status_class",
            "taskforge.outcome",
        }
    ),
    "taskforge.api.request.duration": frozenset(
        {
            "http.request.method",
            "http.route",
            "http.response.status_class",
            "taskforge.outcome",
        }
    ),
    "taskforge.dispatch.publications": frozenset({"taskforge.outcome"}),
    "taskforge.dispatch.publish.duration": frozenset({"taskforge.outcome"}),
    "taskforge.dispatch.publication_records": frozenset({"taskforge.outcome"}),
    "taskforge.dispatch.outbox.pending": frozenset({"taskforge.saturated"}),
    "taskforge.retry.transitions": frozenset({"taskforge.outcome"}),
    "taskforge.retry.dispatches": frozenset({"taskforge.outcome"}),
    "taskforge.handler.executions": frozenset(
        {"taskforge.result.kind", "taskforge.failure.kind"}
    ),
    "taskforge.handler.duration": frozenset(
        {"taskforge.result.kind", "taskforge.failure.kind"}
    ),
    "taskforge.worker.claims": frozenset(
        {"taskforge.outcome", "taskforge.rejection.reason"}
    ),
    "taskforge.task.result_submissions": frozenset(
        {"taskforge.outcome", "taskforge.result.kind", "taskforge.failure.kind"}
    ),
    "taskforge.worker.heartbeats": frozenset(
        {"taskforge.outcome", "taskforge.rejection.reason"}
    ),
    "taskforge.recovery.scan.candidates": frozenset({"taskforge.scan.kind"}),
    "taskforge.recovery.scan.operations": frozenset(
        {"taskforge.scan.kind", "taskforge.outcome"}
    ),
    "taskforge.recovery.operations": frozenset(
        {"taskforge.recovery.kind", "taskforge.outcome"}
    ),
    "taskforge.recovery.duration": frozenset(
        {"taskforge.recovery.kind", "taskforge.outcome"}
    ),
    "taskforge.dead_letters.created": frozenset({"taskforge.reason"}),
    "taskforge.dead_letters.operations": frozenset(
        {"taskforge.operation", "taskforge.outcome"}
    ),
    "taskforge.websocket.connection.attempts": frozenset({"taskforge.outcome"}),
    "taskforge.websocket.connection.duration": frozenset({"taskforge.disconnect.kind"}),
    "taskforge.websocket.disconnections": frozenset({"taskforge.disconnect.kind"}),
    "taskforge.websocket.resume.outcomes": frozenset({"taskforge.outcome"}),
    "taskforge.rate_limit.decisions": frozenset(
        {"taskforge.rate_limit.policy", "taskforge.outcome"}
    ),
    "taskforge.rate_limit.cleanup_failures": frozenset(),
    "taskforge.dependency.state.transitions": frozenset(
        {"taskforge.dependency", "taskforge.dependency.state"}
    ),
    "taskforge.process.readiness.transitions": frozenset(
        {"taskforge.readiness.status"}
    ),
}

_ATTRIBUTE_VALUES_BY_INSTRUMENT_KEY: Final[dict[tuple[str, str], frozenset[str]]] = {}


def _allow(name: str, key: str, *values: str) -> None:
    _ATTRIBUTE_VALUES_BY_INSTRUMENT_KEY[(name, key)] = frozenset(values)


for _api_name in ("taskforge.api.requests", "taskforge.api.request.duration"):
    _ATTRIBUTE_VALUES_BY_INSTRUMENT_KEY[(_api_name, "http.request.method")] = (
        _ALLOWED_ATTRIBUTE_VALUES["http.request.method"]
    )
    _ATTRIBUTE_VALUES_BY_INSTRUMENT_KEY[(_api_name, "http.response.status_class")] = (
        _ALLOWED_ATTRIBUTE_VALUES["http.response.status_class"]
    )
    _allow(_api_name, "taskforge.outcome", "completed", "unhandled_exception")

for _publish_name in (
    "taskforge.dispatch.publications",
    "taskforge.dispatch.publish.duration",
):
    _allow(
        _publish_name,
        "taskforge.outcome",
        "accepted",
        "timeout",
        "rejected",
        "unavailable",
    )
_allow(
    "taskforge.dispatch.publication_records",
    "taskforge.outcome",
    "recorded",
    "already_recorded",
    "invariant_failure",
    "persistence_failure",
)
_allow(
    "taskforge.retry.transitions",
    "taskforge.outcome",
    "scheduled",
    "failed_no_policy",
    "failed_exhausted",
    "already_scheduled",
    "not_eligible",
    "invariant_failure",
    "persistence_failure",
)
_allow(
    "taskforge.retry.dispatches",
    "taskforge.outcome",
    "dispatched",
    "skipped",
    "invariant_failure",
    "persistence_failure",
)
for _handler_name in (
    "taskforge.handler.executions",
    "taskforge.handler.duration",
    "taskforge.task.result_submissions",
):
    _ATTRIBUTE_VALUES_BY_INSTRUMENT_KEY[(_handler_name, "taskforge.result.kind")] = (
        _ALLOWED_ATTRIBUTE_VALUES["taskforge.result.kind"]
    )
    _ATTRIBUTE_VALUES_BY_INSTRUMENT_KEY[(_handler_name, "taskforge.failure.kind")] = (
        _ALLOWED_ATTRIBUTE_VALUES["taskforge.failure.kind"]
    )
_allow(
    "taskforge.worker.claims",
    "taskforge.outcome",
    "acquired_active",
    "replayed_active",
    "replayed_expired",
    "rejected",
    "infrastructure_failure",
)
_allow(
    "taskforge.worker.claims",
    "taskforge.rejection.reason",
    "invalid_dispatch",
    "stale_attempt",
    "obsolete_task",
    "worker_authority_rejected",
    "worker_session_unavailable",
    "worker_session_inactive",
    "worker_unavailable",
    "capability_mismatch",
    "already_authoritative",
)
_allow(
    "taskforge.task.result_submissions",
    "taskforge.outcome",
    "accepted",
    "replayed_identical",
    "stale",
    "conflict",
    "rejected",
    "invariant_failure",
    "persistence_failure",
)
_allow(
    "taskforge.worker.heartbeats",
    "taskforge.outcome",
    "accepted",
    "rejected",
    "persistence_failure",
)
_allow(
    "taskforge.worker.heartbeats",
    "taskforge.rejection.reason",
    "worker_authority_rejected",
    "worker_session_unavailable",
    "worker_session_inactive",
    "stale_heartbeat",
    "heartbeat_sequence_gap",
    "heartbeat_replay_conflict",
)
for _scan_name in (
    "taskforge.recovery.scan.candidates",
    "taskforge.recovery.scan.operations",
):
    _allow(
        _scan_name,
        "taskforge.scan.kind",
        "expired_claim",
        "stale_worker_session",
    )
_allow(
    "taskforge.recovery.scan.operations",
    "taskforge.outcome",
    "completed",
    "invariant_failure",
    "persistence_failure",
)
for _recovery_name in (
    "taskforge.recovery.operations",
    "taskforge.recovery.duration",
):
    _allow(
        _recovery_name,
        "taskforge.recovery.kind",
        "expired_claim",
        "stale_worker_session",
    )
    _allow(
        _recovery_name,
        "taskforge.outcome",
        "cancelled",
        "retry_scheduled",
        "failed_no_policy",
        "failed_exhausted",
        "candidate_no_longer_expired",
        "claim_already_terminated",
        "attempt_no_longer_latest",
        "task_not_eligible",
        "workflow_not_eligible",
        "result_already_accepted",
        "already_recovered",
        "session_ended",
        "candidate_refreshed",
        "session_already_ended",
        "invariant_failure",
        "persistence_failure",
    )
_allow(
    "taskforge.dead_letters.created",
    "taskforge.reason",
    "permanent_failure",
    "retry_exhausted",
)
_allow(
    "taskforge.dead_letters.operations",
    "taskforge.operation",
    "acknowledge",
    "resolve",
    "redrive",
)
_allow(
    "taskforge.dead_letters.operations",
    "taskforge.outcome",
    "completed",
    "not_found",
    "not_eligible",
    "limit_exceeded",
    "conflict",
    "persistence_failure",
)
_allow(
    "taskforge.websocket.connection.attempts",
    "taskforge.outcome",
    "accepted",
    "policy_rejected",
    "service_unavailable",
    "capacity_rejected",
    "resume_rejected",
    "rate_limited",
)
_ATTRIBUTE_VALUES_BY_INSTRUMENT_KEY[
    ("taskforge.rate_limit.decisions", "taskforge.rate_limit.policy")
] = _ALLOWED_ATTRIBUTE_VALUES["taskforge.rate_limit.policy"]
_allow(
    "taskforge.rate_limit.decisions",
    "taskforge.outcome",
    "allowed",
    "limited",
    "degraded_allowed",
    "degraded_limited",
)
for _disconnect_name in (
    "taskforge.websocket.connection.duration",
    "taskforge.websocket.disconnections",
):
    _ATTRIBUTE_VALUES_BY_INSTRUMENT_KEY[
        (_disconnect_name, "taskforge.disconnect.kind")
    ] = _ALLOWED_ATTRIBUTE_VALUES["taskforge.disconnect.kind"]
_allow(
    "taskforge.websocket.resume.outcomes",
    "taskforge.outcome",
    "not_requested",
    "resumed",
    "invalid_cursor",
    "cursor_ahead",
    "snapshot_required",
)
_allow(
    "taskforge.dependency.state.transitions",
    "taskforge.dependency",
    "postgresql",
    "execution_stream",
)
_allow(
    "taskforge.dependency.state.transitions",
    "taskforge.dependency.state",
    "available",
    "unavailable",
)
_allow(
    "taskforge.process.readiness.transitions",
    "taskforge.readiness.status",
    "ready",
    "degraded",
    "not_ready",
)


def _counter(name: str, unit: str) -> metrics.Counter:
    return _meter.create_counter(name, unit=unit)


def _histogram(name: str, unit: str) -> metrics.Histogram:
    return _meter.create_histogram(name, unit=unit)


def _up_down_counter(name: str, unit: str) -> metrics.UpDownCounter:
    return _meter.create_up_down_counter(name, unit=unit)


def _build_instruments() -> dict[str, object]:
    instruments: dict[str, object] = {}
    counters = {
        "taskforge.api.requests": "{request}",
        "taskforge.dispatch.created": "{dispatch}",
        "taskforge.dispatch.publications": "{publication}",
        "taskforge.dispatch.publication_records": "{record}",
        "taskforge.dispatch.outbox.invalid": "{dispatch}",
        "taskforge.retry.transitions": "{transition}",
        "taskforge.retry.dispatches": "{dispatch}",
        "taskforge.handler.executions": "{execution}",
        "taskforge.worker.claims": "{claim}",
        "taskforge.task.result_submissions": "{submission}",
        "taskforge.worker.heartbeats": "{heartbeat}",
        "taskforge.recovery.scan.candidates": "{candidate}",
        "taskforge.recovery.scan.operations": "{scan}",
        "taskforge.recovery.operations": "{recovery}",
        "taskforge.dead_letters.created": "{dead_letter}",
        "taskforge.dead_letters.operations": "{operation}",
        "taskforge.websocket.connection.attempts": "{connection}",
        "taskforge.websocket.disconnections": "{connection}",
        "taskforge.websocket.backpressure": "{event}",
        "taskforge.websocket.resume.outcomes": "{connection}",
        "taskforge.dependency.state.transitions": "{transition}",
        "taskforge.process.readiness.transitions": "{transition}",
    }
    histograms = {
        "taskforge.api.request.duration": "s",
        "taskforge.dispatch.publish.duration": "s",
        "taskforge.dispatch.outbox.duration": "s",
        "taskforge.retry.delay": "s",
        "taskforge.retry.due.age": "s",
        "taskforge.handler.duration": "s",
        "taskforge.recovery.duration": "s",
        "taskforge.websocket.connection.duration": "s",
    }
    up_down = {
        "taskforge.worker.running.deliveries": "{delivery}",
        "taskforge.websocket.connections.active": "{connection}",
    }
    for name, unit in counters.items():
        instruments[name] = _counter(name, unit)
    for name, unit in histograms.items():
        instruments[name] = _histogram(name, unit)
    for name, unit in up_down.items():
        instruments[name] = _up_down_counter(name, unit)
    instruments["taskforge.dispatch.outbox.pending"] = _meter.create_observable_gauge(
        "taskforge.dispatch.outbox.pending",
        callbacks=(_observe_outbox_pending,),
        unit="{dispatch}",
    )
    instruments["taskforge.dispatch.outbox.oldest.age"] = (
        _meter.create_observable_gauge(
            "taskforge.dispatch.outbox.oldest.age",
            callbacks=(_observe_outbox_oldest_age,),
            unit="s",
        )
    )
    return instruments


_instruments: dict[str, object] = {}


def _views() -> tuple[View, ...]:
    boundaries = {
        "taskforge.api.request.duration": API_DURATION_BUCKETS,
        "taskforge.dispatch.publish.duration": FAST_DURATION_BUCKETS,
        "taskforge.dispatch.outbox.duration": AGE_BUCKETS,
        "taskforge.retry.delay": AGE_BUCKETS,
        "taskforge.retry.due.age": AGE_BUCKETS,
        "taskforge.handler.duration": HANDLER_DURATION_BUCKETS,
        "taskforge.recovery.duration": FAST_DURATION_BUCKETS,
        "taskforge.websocket.connection.duration": WEBSOCKET_DURATION_BUCKETS,
    }
    return tuple(
        View(
            instrument_name=name,
            aggregation=ExplicitBucketHistogramAggregation(values),
        )
        for name, values in boundaries.items()
    )


def configure_metrics(
    *,
    enabled: bool,
    exporter: str,
    endpoint: str | None,
    export_interval_seconds: float,
    export_timeout_seconds: float,
    shutdown_timeout_seconds: float,
    outbox_staleness_seconds: float,
    service_name: str,
    environment: str,
    process_role: str,
    reader: MetricReader | None = None,
) -> MetricsRuntime:
    """Configure an independent metrics provider; disabled is a true no-op."""
    global _provider, _meter, _instruments, _outbox_staleness_seconds
    _outbox_staleness_seconds = outbox_staleness_seconds
    if not enabled:
        _provider = None
        _meter = metrics.NoOpMeterProvider().get_meter(_INSTRUMENTATION_NAME)
        _instruments = _build_instruments()
        return MetricsRuntime(None, shutdown_timeout_seconds)
    readers: list[MetricReader] = []
    if reader is not None:
        readers.append(reader)
    if exporter == "otlp_http":
        assert endpoint is not None
        readers.append(
            PeriodicExportingMetricReader(
                OTLPMetricExporter(endpoint=endpoint, timeout=export_timeout_seconds),
                export_interval_millis=export_interval_seconds * 1000,
                export_timeout_millis=export_timeout_seconds * 1000,
            )
        )
    provider = MeterProvider(
        metric_readers=readers,
        resource=Resource.create(
            {
                "service.name": service_name,
                "deployment.environment.name": environment,
                "taskforge.process.role": process_role,
            }
        ),
        views=_views(),
    )
    _provider = provider
    _meter = provider.get_meter(_INSTRUMENTATION_NAME)
    _instruments = _build_instruments()
    return MetricsRuntime(provider, shutdown_timeout_seconds)


def register_http_routes(routes: Iterable[str]) -> None:
    """Install the immutable set of trusted application route templates."""
    global _known_http_routes
    try:
        _known_http_routes = frozenset(
            route
            for route in routes
            if isinstance(route, str) and route not in {"/health", "/ready"}
        )
    except Exception:
        _known_http_routes = frozenset()


def normalize_http_route(candidate: object) -> str:
    return (
        candidate
        if isinstance(candidate, str) and candidate in _known_http_routes
        else "unmatched"
    )


def normalize_http_method(candidate: object) -> str:
    if not isinstance(candidate, str):
        return "other"
    normalized = candidate.upper()
    return (
        normalized
        if normalized in {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}
        else "other"
    )


def status_class(status_code: object) -> str:
    if (
        isinstance(status_code, int)
        and not isinstance(status_code, bool)
        and 100 <= status_code <= 599
    ):
        return f"{status_code // 100}xx"
    return "unknown"


def add(
    name: str, value: int = 1, attributes: Mapping[str, str | bool] | None = None
) -> None:
    """Best-effort internal counter/up-down update."""
    try:
        instrument = cast(_AddInstrument, _instruments[name])
        instrument.add(value, _safe_attributes(name, attributes))
    except Exception:
        pass


def record(
    name: str, value: float, attributes: Mapping[str, str | bool] | None = None
) -> None:
    """Best-effort internal histogram recording."""
    try:
        instrument = cast(_RecordInstrument, _instruments[name])
        instrument.record(max(0.0, value), _safe_attributes(name, attributes))
    except Exception:
        pass


def update_outbox_observation(
    *, pending: int, saturated: bool, oldest_age_seconds: float | None
) -> None:
    global _outbox_observation
    try:
        _outbox_observation = OutboxObservation(
            max(0, pending),
            bool(saturated),
            None if oldest_age_seconds is None else max(0.0, oldest_age_seconds),
            monotonic(),
        )
    except Exception:
        pass


def _safe_attributes(
    name: str,
    attributes: Mapping[str, str | bool] | None,
) -> dict[str, str | bool] | None:
    if not attributes:
        return None
    safe: dict[str, str | bool] = {}
    allowed_keys = _INSTRUMENT_ATTRIBUTE_KEYS.get(name, frozenset())
    for key, value in attributes.items():
        if key not in allowed_keys:
            continue
        if key == "taskforge.saturated" and isinstance(value, bool):
            safe[key] = value
        elif key == "http.route" and isinstance(value, str):
            safe[key] = normalize_http_route(value)
        elif isinstance(
            value, str
        ) and value in _ATTRIBUTE_VALUES_BY_INSTRUMENT_KEY.get(
            (name, key), frozenset()
        ):
            safe[key] = value
    return safe or None


def _fresh_outbox_observation() -> tuple[OutboxObservation, float] | None:
    observation = _outbox_observation
    if observation is None:
        return None
    elapsed = monotonic() - observation.observed_at_monotonic
    if elapsed < 0 or elapsed > _outbox_staleness_seconds:
        return None
    return observation, elapsed


def _observe_outbox_pending(
    _options: CallbackOptions,
) -> Iterable[Observation]:
    current = _fresh_outbox_observation()
    if current is None:
        return ()
    observation, _ = current
    return (
        Observation(
            observation.pending,
            {"taskforge.saturated": observation.saturated},
        ),
    )


def _observe_outbox_oldest_age(
    _options: CallbackOptions,
) -> Iterable[Observation]:
    current = _fresh_outbox_observation()
    if current is None:
        return ()
    observation, elapsed = current
    if observation.oldest_age_seconds is None:
        return ()
    return (Observation(observation.oldest_age_seconds + elapsed),)


def current_provider() -> MeterProvider | None:
    return _provider


_instruments = _build_instruments()
