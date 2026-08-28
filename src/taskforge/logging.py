"""Bounded structured logging and async-local diagnostic context."""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from enum import Enum
from types import MappingProxyType
from typing import Final
from uuid import UUID

MAX_LOG_STRING_LENGTH: Final = 512
MAX_LOG_COLLECTION_ITEMS: Final = 32
MAX_LOG_RECORD_BYTES: Final = 16 * 1024

CANONICAL_IDENTIFIER_FIELDS: Final = frozenset(
    {
        "request.id",
        "correlation.id",
        "operation.id",
        "principal.id",
        "workflow.id",
        "workflow.version.id",
        "workflow.run.id",
        "task.run.id",
        "task.attempt.id",
        "dispatch.id",
        "worker.id",
        "worker.session.id",
        "dead_letter.id",
        "execution_event.id",
    }
)
_APPROVED_FIELDS: Final = CANONICAL_IDENTIFIER_FIELDS | frozenset(
    {
        "task.attempt.number",
        "claim.generation",
        "http.method",
        "http.route",
        "http.status_code",
        "duration_ms",
        "task.type",
        "worker.capability",
        "broker.exchange",
        "broker.route",
        "broker.redelivered",
        "outcome",
        "reason.code",
        "error.type",
        "error.category",
        "error.retryable",
        "examined",
        "published",
        "acknowledged",
        "already_acknowledged",
        "durable_invalid",
        "dispatched",
        "skipped",
        "reached_end",
        "pass_limit_reached",
        "dependency.name",
        "dependency.state",
        "readiness.status",
    }
)
_SENSITIVE_FRAGMENTS: Final = (
    "authorization",
    "cookie",
    "password",
    "passwd",
    "secret",
    "token",
    "credential",
    "verifier",
    "authority",
    "api_key",
    "access_key",
    "private_key",
    "idempotency_key",
    "traceparent",
    "tracestate",
    "connection_string",
    "dsn",
    "payload",
    "body",
    "parameters",
    "references",
    "input",
    "output",
    "result",
    "metadata",
    "provenance",
)
_context: ContextVar[Mapping[str, object]] = ContextVar(
    "taskforge_log_context", default=MappingProxyType({})
)
_process_fields: dict[str, object] = {
    "service.name": "taskforge",
    "service.environment": "development",
    "process.role": "unknown",
}
_diagnostic_logger = logging.getLogger("taskforge.logging")
_FINAL_FALLBACK = '{"event.name":"logging.final_fallback","severity":"ERROR"}'


def configure_logging(
    *, service_name: str, environment: str, process_role: str, level: str
) -> None:
    """Configure one process-wide NDJSON stream without Uvicorn access duplication."""
    _process_fields.update(
        {
            "service.name": _bounded_string(service_name),
            "service.environment": _bounded_string(environment),
            "process.role": _bounded_string(process_role),
        }
    )
    handler = logging.StreamHandler()
    handler.setFormatter(TaskforgeJSONFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)


def uvicorn_log_config(level: str) -> dict[str, object]:
    """Return Uvicorn configuration using the safe third-party formatter policy."""
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"taskforge": {"()": "taskforge.logging.TaskforgeJSONFormatter"}},
        "handlers": {
            "default": {
                "class": "logging.StreamHandler",
                "formatter": "taskforge",
                "stream": "ext://sys.stderr",
            }
        },
        "loggers": {
            "uvicorn": {"handlers": ["default"], "level": level, "propagate": False},
            "uvicorn.error": {"level": level},
            "uvicorn.access": {"handlers": [], "propagate": False},
        },
    }


@contextmanager
def bind_log_context(**fields: object) -> Iterator[None]:
    """Temporarily add validated canonical fields to this async execution context."""
    try:
        validated = _validate_fields(fields)
    except Exception:
        _emit_owned(
            _diagnostic_logger,
            logging.ERROR,
            "logging.context_rejected",
            {},
        )
        yield
        return
    current = dict(_context.get())
    conflicts = _identifier_conflicts(current, validated)
    for name in conflicts:
        validated.pop(name)
    if conflicts:
        _emit_owned(
            _diagnostic_logger,
            logging.ERROR,
            "logging.context_conflict",
            current,
        )
    current.update(validated)
    token = _context.set(current)
    try:
        yield
    finally:
        _context.reset(token)


def current_log_context() -> Mapping[str, object]:
    return dict(_context.get())


def log_event(
    logger: logging.Logger,
    level: int,
    event_name: str,
    fields: Mapping[str, object] | None = None,
    *,
    error: Exception | None = None,
) -> None:
    """Emit one Taskforge-owned event through the approved structured path."""
    try:
        event_fields = _validate_fields(fields or {})
        context = dict(_context.get())
        conflicts = _identifier_conflicts(context, event_fields)
        if conflicts:
            _emit_owned(
                logger,
                logging.ERROR,
                "logging.context_conflict",
                context,
            )
            return
        context.update(event_fields)
        _emit_owned(
            logger,
            level,
            _bounded_string(event_name),
            context,
            error_type=type(error).__name__ if error is not None else None,
        )
    except Exception:
        _emit_owned(logger, logging.ERROR, "logging.event_rejected", {})


def _emit_owned(
    logger: logging.Logger,
    level: int,
    event_name: str,
    fields: Mapping[str, object],
    *,
    error_type: str | None = None,
) -> None:
    try:
        from taskforge.tracing import current_log_trace_fields

        logger.log(
            level,
            event_name,
            extra={
                "_taskforge_event": True,
                "_event_name": event_name,
                "_event_fields": fields,
                "_safe_error_type": error_type,
                "_trace_fields": current_log_trace_fields(),
            },
        )
    except Exception:
        try:
            logger.log(
                logging.ERROR,
                "logging.emission_failed",
                extra={
                    "_taskforge_event": True,
                    "_event_name": "logging.emission_failed",
                    "_event_fields": {},
                    "_safe_error_type": None,
                },
            )
        except Exception:
            pass


class TaskforgeJSONFormatter(logging.Formatter):
    """Serialize owned events and conservatively represent external records."""

    def format(self, record: logging.LogRecord) -> str:
        try:
            if getattr(record, "_taskforge_event", False):
                data = self._owned_record(record)
            else:
                data = self._third_party_record(record)
            encoded = _encode(data)
            if len(encoded.encode("utf-8")) > MAX_LOG_RECORD_BYTES:
                return _encode(self._fallback(record, "logging.record_oversized"))
            return encoded
        except Exception:
            try:
                return _encode(self._fallback(record, "logging.serialization_failed"))
            except Exception:
                return _FINAL_FALLBACK

    def _base(self, record: logging.LogRecord, event_name: str) -> dict[str, object]:
        return {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "severity": record.levelname,
            **_process_fields,
            "process.pid": os.getpid(),
            "event.name": event_name,
            "logger.name": _bounded_string(record.name),
        }

    def _owned_record(self, record: logging.LogRecord) -> dict[str, object]:
        data = self._base(record, str(record.__dict__["_event_name"]))
        data.update(_defensive_sanitize(getattr(record, "_event_fields", {})))
        data.update(_trusted_trace_fields(getattr(record, "_trace_fields", {})))
        error_type = getattr(record, "_safe_error_type", None)
        if error_type is not None:
            data["error.type"] = _bounded_string(error_type)
        return data

    def _third_party_record(self, record: logging.LogRecord) -> dict[str, object]:
        data = self._base(record, "third_party.log")
        # Messages, arguments, and normal exception text are uncontrolled. They are
        # deliberately omitted rather than passed through Taskforge's field policy.
        if record.exc_info is not None and record.exc_info[0] is not None:
            data["error.type"] = _bounded_string(record.exc_info[0].__name__)
        return data

    def _fallback(
        self, record: logging.LogRecord, event_name: str
    ) -> dict[str, object]:
        return self._base(record, event_name)


def _validate_fields(fields: Mapping[str, object]) -> dict[str, object]:
    validated: dict[str, object] = {}
    for name, value in fields.items():
        if name not in _APPROVED_FIELDS or _is_sensitive_name(name):
            raise ValueError("unapproved structured log field")
        validated[name] = _approved_value(value)
    return validated


def _approved_value(value: object) -> object:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Enum):
        return _approved_value(value.value)
    if isinstance(value, str):
        return _bounded_string(value)
    if isinstance(value, (tuple, list)):
        if len(value) > MAX_LOG_COLLECTION_ITEMS:
            raise ValueError("structured log collection is too large")
        return [_approved_value(item) for item in value]
    raise TypeError("unsupported structured log value")


def _identifier_conflicts(
    bound: Mapping[str, object], event: Mapping[str, object]
) -> set[str]:
    return {
        name
        for name in CANONICAL_IDENTIFIER_FIELDS & bound.keys() & event.keys()
        if bound[name] != event[name]
    }


def _bounded_string(value: str) -> str:
    if len(value) <= MAX_LOG_STRING_LENGTH:
        return value
    return value[: MAX_LOG_STRING_LENGTH - 11] + "<truncated>"


def _is_sensitive_name(name: str) -> bool:
    lowered = name.lower()
    return any(fragment in lowered for fragment in _SENSITIVE_FRAGMENTS)


def _defensive_sanitize(value: object, *, depth: int = 0) -> dict[str, object]:
    if depth > 2 or not isinstance(value, Mapping):
        return {}
    result: dict[str, object] = {}
    for index, (raw_name, raw_value) in enumerate(value.items()):
        if index >= MAX_LOG_COLLECTION_ITEMS or not isinstance(raw_name, str):
            break
        if _is_sensitive_name(raw_name):
            result[raw_name] = "<redacted>"
            continue
        try:
            result[raw_name] = _approved_value(raw_value)
        except (TypeError, ValueError):
            result[raw_name] = "<redacted>"
    return result


def _encode(data: Mapping[str, object]) -> str:
    return json.dumps(data, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def _trusted_trace_fields(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        return {}
    trace_id, span_id, sampled = (
        value.get("trace_id"),
        value.get("span_id"),
        value.get("trace_sampled"),
    )
    if not (
        isinstance(trace_id, str)
        and len(trace_id) == 32
        and all(character in "0123456789abcdef" for character in trace_id)
        and isinstance(span_id, str)
        and len(span_id) == 16
        and all(character in "0123456789abcdef" for character in span_id)
        and isinstance(sampled, bool)
    ):
        return {}
    return {"trace_id": trace_id, "span_id": span_id, "trace_sampled": sampled}
