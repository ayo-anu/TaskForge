"""Failure-isolated OpenTelemetry tracing and W3C propagation."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from threading import Thread
from typing import TYPE_CHECKING, Final

from opentelemetry import trace
from opentelemetry.context import Context, attach, detach
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
from opentelemetry.trace import Link, Span, SpanKind, Status, StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.util.types import AttributeValue

if TYPE_CHECKING:
    from taskforge.dispatch.envelope import TraceContext

_INSTRUMENTATION_NAME: Final = "taskforge"
_propagator = TraceContextTextMapPropagator()
_tracer: trace.Tracer = trace.get_tracer(_INSTRUMENTATION_NAME)
_provider: TracerProvider | None = None


@dataclass
class DeferredSpan:
    """Activate after an outer transaction enters and end after it exits."""

    active: Span | None = None
    _token: object | None = None

    def start(
        self,
        name: str,
        *,
        attributes: Mapping[str, AttributeValue] | None = None,
        root: bool = False,
    ) -> Span | None:
        if self.active is not None:
            return self.active
        try:
            started = _tracer.start_span(
                name,
                context=Context() if root else None,
                attributes=attributes,
            )
        except Exception:
            return None
        try:
            token = attach(trace.set_span_in_context(started))
        except Exception:
            try:
                started.end()
            except Exception:
                pass
            return None
        self.active = started
        self._token = token
        return started

    def end(self) -> None:
        active, self.active = self.active, None
        token, self._token = self._token, None
        if token is not None:
            try:
                detach(token)  # type: ignore[arg-type]
            except Exception:
                pass
        if active is not None:
            try:
                active.end()
            except Exception:
                pass


@dataclass
class TracingRuntime:
    """One process-owned provider with bounded best-effort lifecycle."""

    provider: TracerProvider | None
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

        thread = Thread(target=close, name="taskforge-tracing-shutdown", daemon=True)
        try:
            thread.start()
            thread.join(self.shutdown_timeout_seconds)
        except Exception:
            pass


def configure_tracing(
    *,
    enabled: bool,
    exporter: str,
    endpoint: str | None,
    sample_ratio: float,
    export_timeout_seconds: float,
    shutdown_timeout_seconds: float,
    service_name: str,
    environment: str,
    process_role: str,
) -> TracingRuntime:
    """Build an isolated provider; disabled tracing remains a true no-op."""
    global _provider, _tracer
    if not enabled:
        _provider = None
        _tracer = trace.NoOpTracerProvider().get_tracer(_INSTRUMENTATION_NAME)
        return TracingRuntime(None, shutdown_timeout_seconds)
    provider = TracerProvider(
        sampler=ParentBased(TraceIdRatioBased(sample_ratio)),
        resource=Resource.create(
            {
                "service.name": service_name,
                "deployment.environment.name": environment,
                "taskforge.process.role": process_role,
            }
        ),
    )
    if exporter == "otlp_http":
        assert endpoint is not None
        otlp = OTLPSpanExporter(
            endpoint=endpoint,
            timeout=export_timeout_seconds,
        )
        provider.add_span_processor(
            BatchSpanProcessor(
                otlp, export_timeout_millis=export_timeout_seconds * 1000
            )
        )
    _tracer = provider.get_tracer(_INSTRUMENTATION_NAME)
    _provider = provider
    return TracingRuntime(provider, shutdown_timeout_seconds)


def set_tracer_for_testing(tracer: trace.Tracer) -> trace.Tracer:
    """Replace the module tracer and return its predecessor for test restoration."""
    global _tracer
    previous, _tracer = _tracer, tracer
    return previous


@contextmanager
def span(
    name: str,
    *,
    kind: SpanKind = SpanKind.INTERNAL,
    attributes: Mapping[str, AttributeValue] | None = None,
    enabled: bool = True,
    root: bool = False,
    parent: Context | None = None,
    links: tuple[Link, ...] = (),
) -> Iterator[Span | None]:
    """Activate a span without ever suppressing exceptions from the body."""
    if not enabled:
        yield None
        return
    try:
        started = _tracer.start_span(
            name,
            context=Context() if root else parent,
            kind=kind,
            attributes=attributes,
            links=links,
        )
        token = attach(trace.set_span_in_context(started))
    except Exception:
        yield None
        return
    try:
        yield started
    finally:
        try:
            detach(token)
        except Exception:
            pass
        try:
            started.end()
        except Exception:
            pass


def set_attributes(active: Span | None, fields: Mapping[str, AttributeValue]) -> None:
    if active is None:
        return
    try:
        active.set_attributes(fields)
    except Exception:
        pass


def add_link(active: Span | None, link: Link | None) -> None:
    if active is None or link is None:
        return
    try:
        active.add_link(link.context, attributes=link.attributes)
    except Exception:
        pass


def current_provider() -> TracerProvider | None:
    return _provider


def set_error(active: Span | None, error: BaseException, category: str) -> None:
    """Record only bounded safe classifications, never exception contents."""
    if active is None:
        return
    try:
        active.set_attribute("error.type", type(error).__name__[:128])
        active.set_attribute("taskforge.error.category", category[:128])
        active.set_status(Status(StatusCode.ERROR))
    except Exception:
        pass


def set_error_type(active: Span | None, error_type: str, category: str) -> None:
    if active is None:
        return
    try:
        active.set_attribute("error.type", error_type[:128])
        active.set_attribute("taskforge.error.category", category[:128])
        active.set_status(Status(StatusCode.ERROR))
    except Exception:
        pass


def inject_trace_context() -> TraceContext | None:
    carrier: dict[str, str] = {}
    try:
        _propagator.inject(carrier)
        traceparent = carrier.get("traceparent")
        if traceparent is None:
            return None
        from taskforge.dispatch.envelope import TraceContext

        return TraceContext(traceparent, carrier.get("tracestate"))
    except Exception:
        return None


def extract_trace_context(value: TraceContext | None) -> Context | None:
    if value is None:
        return None
    carrier = {"traceparent": value.traceparent}
    if value.tracestate is not None:
        carrier["tracestate"] = value.tracestate
    return extract_carrier(carrier)


def extract_carrier(carrier: Mapping[str, str]) -> Context | None:
    """Extract W3C Trace Context only; baggage is deliberately unsupported."""
    try:
        extracted = _propagator.extract(carrier)
        context = trace.get_current_span(extracted).get_span_context()
        return extracted if context.is_valid else None
    except Exception:
        return None


def link_from_trace_context(value: TraceContext | None) -> Link | None:
    extracted = extract_trace_context(value)
    if extracted is None:
        return None
    try:
        context = trace.get_current_span(extracted).get_span_context()
        return Link(context) if context.is_valid else None
    except Exception:
        return None


def current_log_trace_fields() -> dict[str, object]:
    """Capture trusted log-correlation fields in the originating span context."""
    try:
        context = trace.get_current_span().get_span_context()
        if not context.is_valid:
            return {}
        return {
            "trace_id": f"{context.trace_id:032x}",
            "span_id": f"{context.span_id:016x}",
            "trace_sampled": context.trace_flags.sampled,
        }
    except Exception:
        return {}


def update_name(active: Span | None, name: str) -> None:
    if active is None:
        return
    try:
        active.update_name(name)
    except Exception:
        pass
