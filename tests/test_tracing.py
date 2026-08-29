"""Deterministic tracing, isolation, propagation, and leakage tests."""

from __future__ import annotations

import asyncio
import io
import json
import logging
from collections.abc import Iterator, Sequence
from types import SimpleNamespace
from typing import Any

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import (
    SimpleSpanProcessor,
    SpanExporter,
    SpanExportResult,
)
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.sdk.trace.sampling import ALWAYS_ON
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import Receive, Scope, Send

from taskforge.api.errors import RequestIDMiddleware
from taskforge.logging import TaskforgeJSONFormatter, log_event
from taskforge.metrics import register_http_routes
from taskforge.tracing import (
    DeferredSpan,
    configure_tracing,
    inject_trace_context,
    set_error,
    set_tracer_for_testing,
    span,
)


@pytest.fixture
def spans() -> Iterator[tuple[TracerProvider, InMemorySpanExporter]]:
    provider = TracerProvider(sampler=ALWAYS_ON)
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    previous = set_tracer_for_testing(provider.get_tracer("taskforge-test"))
    try:
        yield provider, exporter
    finally:
        set_tracer_for_testing(previous)
        provider.shutdown()


def test_disabled_tracing_is_a_true_noop() -> None:
    runtime = configure_tracing(
        enabled=False,
        exporter="none",
        endpoint=None,
        sample_ratio=1,
        export_timeout_seconds=1,
        shutdown_timeout_seconds=1,
        service_name="taskforge",
        environment="test",
        process_role="test",
    )

    with span("disabled") as active:
        assert active is not None
        assert not active.is_recording()
        assert inject_trace_context() is None

    assert not runtime.enabled


def test_application_exception_identity_is_preserved_when_traced(spans: object) -> None:
    del spans
    expected = RuntimeError("sentinel-secret")

    with pytest.raises(RuntimeError) as raised:
        with span("taskforge.test.operation"):
            raise expected

    assert raised.value is expected


def test_telemetry_start_failure_does_not_suppress_application_exception() -> None:
    class FailingTracer:
        def start_span(self, *args: object, **kwargs: object) -> Any:
            del args, kwargs
            raise RuntimeError("telemetry-secret")

    previous = set_tracer_for_testing(FailingTracer())  # type: ignore[arg-type]
    expected = LookupError("application-secret")
    try:
        with pytest.raises(LookupError) as raised:
            with span("taskforge.test.failure"):
                raise expected
        assert raised.value is expected
    finally:
        set_tracer_for_testing(previous)


def test_deferred_span_ends_created_span_when_context_attachment_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class CreatedSpan:
        def __init__(self) -> None:
            self.end_calls = 0

        def end(self) -> None:
            self.end_calls += 1

    class CreatingTracer:
        def __init__(self, created: CreatedSpan) -> None:
            self.created = created

        def start_span(self, *args: object, **kwargs: object) -> CreatedSpan:
            del args, kwargs
            return self.created

    def fail_attach(context: object) -> object:
        del context
        raise RuntimeError("telemetry-activation-failure")

    created = CreatedSpan()
    previous = set_tracer_for_testing(CreatingTracer(created))  # type: ignore[arg-type]
    prior_context = trace.get_current_span()
    monkeypatch.setattr("taskforge.tracing.attach", fail_attach)
    deferred = DeferredSpan()
    authoritative_body_ran = False
    try:
        assert deferred.start("taskforge.test.deferred") is None
        authoritative_body_ran = True
        assert created.end_calls == 1
        assert trace.get_current_span() is prior_context

        deferred.end()
        assert created.end_calls == 1
        assert trace.get_current_span() is prior_context
    finally:
        set_tracer_for_testing(previous)

    assert authoritative_body_ran


def test_safe_errors_export_no_exception_message_stack_or_event(
    spans: tuple[TracerProvider, InMemorySpanExporter],
) -> None:
    _, exporter = spans
    secret = "credential-payload-sentinel"
    error = RuntimeError(secret)

    with span("taskforge.test.safe_error") as active:
        local_secret = secret
        assert local_secret == secret
        set_error(active, error, "safe_category")

    exported = exporter.get_finished_spans()[0]
    rendered = repr(exported.to_json())
    assert secret not in rendered
    assert exported.events == ()
    assert exported.status.status_code is trace.StatusCode.ERROR
    assert exported.status.description is None
    assert exported.attributes is not None
    assert exported.attributes["error.type"] == "RuntimeError"


def test_authentication_material_cannot_escape_through_error_spans(
    spans: tuple[TracerProvider, InMemorySpanExporter],
) -> None:
    _, exporter = spans
    bearer = "tf_worker_v1.00000000-0000-0000-0000-000000000000.raw-secret"
    raw_secret = "raw-secret"
    verifier = "v1$sha256$verifier-sentinel"

    with span("taskforge.test.authentication_failure") as active:
        set_error(
            active,
            RuntimeError(f"{bearer} {verifier}"),
            "authentication_failure",
        )

    rendered = exporter.get_finished_spans()[0].to_json()
    assert bearer not in rendered
    assert raw_secret not in rendered
    assert verifier not in rendered


def test_owned_log_captures_current_ids_and_third_party_log_does_not(
    spans: tuple[TracerProvider, InMemorySpanExporter],
) -> None:
    del spans
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(TaskforgeJSONFormatter())
    logger = logging.getLogger("test.trace-log")
    logger.handlers = [handler]
    logger.propagate = False
    logger.setLevel(logging.INFO)

    with span("taskforge.test.log") as active:
        assert active is not None
        expected = active.get_span_context()
        log_event(logger, logging.INFO, "test.owned")
    logger.info("third-party-sentinel")

    owned, external = (json.loads(line) for line in stream.getvalue().splitlines())
    assert owned["trace_id"] == f"{expected.trace_id:032x}"
    assert owned["span_id"] == f"{expected.span_id:016x}"
    assert owned["trace_sampled"] is True
    assert "trace_id" not in external
    assert "span_id" not in external


def test_concurrent_async_contexts_do_not_cross_contaminate(
    spans: tuple[TracerProvider, InMemorySpanExporter],
) -> None:
    del spans

    async def observe(name: str) -> tuple[int, int]:
        with span(name) as active:
            assert active is not None
            before = active.get_span_context().span_id
            await asyncio.sleep(0)
            after = trace.get_current_span().get_span_context().span_id
            return before, after

    async def run() -> tuple[tuple[int, int], tuple[int, int]]:
        return await asyncio.gather(observe("left"), observe("right"))

    left, right = asyncio.run(run())
    assert left[0] == left[1]
    assert right[0] == right[1]
    assert left[0] != right[0]


def test_api_server_span_uses_w3c_parent_without_duplicate_asgi_spans(
    spans: tuple[TracerProvider, InMemorySpanExporter],
) -> None:
    _, exporter = spans
    register_http_routes({"/items/{item_id}"})
    with span("upstream") as upstream:
        assert upstream is not None
        upstream_context = upstream.get_span_context()
        propagated = inject_trace_context()
    assert propagated is not None
    scope: dict[str, Any] = {
        "type": "http",
        "method": "GET",
        "path": "/items/123",
        "headers": [(b"traceparent", propagated.traceparent.encode())],
        "route": SimpleNamespace(path="/items/{item_id}"),
    }
    request = Request(scope)

    async def call_next(_request: Request) -> Response:
        return Response(status_code=200)

    async def unused_app(scope: Scope, receive: Receive, send: Send) -> None:
        del scope, receive, send

    middleware = RequestIDMiddleware(unused_app)
    response = asyncio.run(middleware.dispatch(request, call_next))
    assert response.status_code == 200

    server = next(
        item
        for item in exporter.get_finished_spans()
        if item.name == "GET /items/{item_id}"
    )
    assert server.parent is not None
    assert server.parent.trace_id == upstream_context.trace_id
    assert [item.name for item in exporter.get_finished_spans()].count(
        "GET /items/{item_id}"
    ) == 1


def test_api_unhandled_exception_is_safe_error_and_preserves_identity(
    spans: tuple[TracerProvider, InMemorySpanExporter],
) -> None:
    _, exporter = spans
    secret = "api-exception-stack-payload-sentinel"
    expected = RuntimeError(secret)
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/failure",
            "headers": [],
            "route": SimpleNamespace(path="/failure"),
        }
    )

    async def fail(_request: Request) -> Response:
        traceback_local_secret = secret
        assert traceback_local_secret == secret
        raise expected

    async def unused_app(scope: Scope, receive: Receive, send: Send) -> None:
        del scope, receive, send

    middleware = RequestIDMiddleware(unused_app)
    with pytest.raises(RuntimeError) as raised:
        asyncio.run(middleware.dispatch(request, fail))

    assert raised.value is expected
    server = next(
        item
        for item in exporter.get_finished_spans()
        if item.name == "taskforge.api.request"
    )
    assert server.status.status_code is trace.StatusCode.ERROR
    assert server.status.description is None
    assert server.events == ()
    assert server.attributes is not None
    assert server.attributes["error.type"] == "RuntimeError"
    assert server.attributes["taskforge.error.category"] == ("api_unhandled_exception")
    assert secret not in server.to_json()


class FailingExporter(SpanExporter):
    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        del spans
        raise RuntimeError("exporter-secret")


def test_exporter_failure_cannot_change_authoritative_result() -> None:
    provider = TracerProvider(sampler=ALWAYS_ON)
    provider.add_span_processor(SimpleSpanProcessor(FailingExporter()))
    previous = set_tracer_for_testing(provider.get_tracer("failure-test"))
    try:
        with span("taskforge.test.authoritative"):
            result = "committed-and-acknowledged"
        assert result == "committed-and-acknowledged"
    finally:
        set_tracer_for_testing(previous)
        provider.shutdown()
