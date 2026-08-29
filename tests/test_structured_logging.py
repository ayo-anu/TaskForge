"""Security and concurrency tests for Taskforge structured logging."""

from __future__ import annotations

import asyncio
import inspect
import io
import json
import logging
from uuid import uuid4

import pytest

import taskforge.logging as structured_logging
from taskforge.logging import (
    TaskforgeJSONFormatter,
    bind_log_context,
    current_log_context,
    log_event,
)


def formatter() -> TaskforgeJSONFormatter:
    return TaskforgeJSONFormatter()


def record(
    message: object,
    *,
    args: tuple[object, ...] = (),
    exc_info: tuple[type[BaseException], BaseException, object] | None = None,
) -> logging.LogRecord:
    return logging.LogRecord(
        "external.library",
        logging.ERROR,
        __file__,
        1,
        message,
        args,
        exc_info,  # type: ignore[arg-type]
    )


def test_direct_logging_arguments_cannot_escape() -> None:
    secret = "presented-credential-secret"
    output = formatter().format(record("authentication failed for %s", args=(secret,)))

    decoded = json.loads(output)
    assert secret not in output
    assert "message" not in decoded
    assert decoded["event.name"] == "third_party.log"


def test_direct_literal_message_is_not_treated_as_safe_structured_data() -> None:
    secret = "literal-legacy-secret"
    output = formatter().format(record(secret))

    assert secret not in output
    assert "message" not in json.loads(output)


def test_log_event_has_no_free_form_message_parameter() -> None:
    assert "message" not in inspect.signature(log_event).parameters


def test_third_party_exception_text_and_arguments_cannot_escape() -> None:
    secret = "database-password-secret"
    error = RuntimeError(secret)
    output = formatter().format(
        record(
            "driver failed: %s", args=(secret,), exc_info=(RuntimeError, error, None)
        )
    )

    decoded = json.loads(output)
    assert secret not in output
    assert decoded["error.type"] == "RuntimeError"
    assert "message" not in decoded


def test_authentication_material_cannot_escape_through_log_records() -> None:
    bearer = "tf_api_v1.00000000-0000-0000-0000-000000000000.raw-secret"
    raw_secret = "raw-secret"
    verifier = "v1$sha256$verifier-sentinel"
    error = RuntimeError(f"authentication failed: {bearer} {verifier}")

    output = formatter().format(
        record(
            "credential %s failed against %s",
            args=(bearer, verifier),
            exc_info=(RuntimeError, error, None),
        )
    )

    assert bearer not in output
    assert raw_secret not in output
    assert verifier not in output
    assert json.loads(output)["error.type"] == "RuntimeError"


def test_oversized_record_uses_valid_minimal_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(structured_logging, "MAX_LOG_RECORD_BYTES", 32)
    output = formatter().format(record("fixed developer message"))

    decoded = json.loads(output)
    assert decoded["event.name"] == "logging.record_oversized"
    assert "fixed developer message" not in output


def test_serialization_failure_uses_valid_minimal_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = structured_logging._encode
    calls = 0

    def fail_once(value: object) -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TypeError("unsafe-secret")
        return original(value)  # type: ignore[arg-type]

    monkeypatch.setattr(structured_logging, "_encode", fail_once)
    output = formatter().format(record("safe literal"))

    assert json.loads(output)["event.name"] == "logging.serialization_failed"
    assert "unsafe-secret" not in output


def test_conflicting_identifier_preserves_bound_value_and_emits_diagnostic() -> None:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(formatter())
    logger = logging.getLogger("test.context-conflict")
    logger.handlers = [handler]
    logger.propagate = False
    logger.setLevel(logging.INFO)
    authoritative = uuid4()

    with bind_log_context(**{"workflow.run.id": authoritative}):
        log_event(
            logger,
            logging.INFO,
            "worker.handler.started",
            {"workflow.run.id": uuid4()},
        )

    decoded = json.loads(stream.getvalue())
    assert decoded["event.name"] == "logging.context_conflict"
    assert decoded["workflow.run.id"] == str(authoritative)


def test_nested_bind_conflict_is_observable_and_preserves_authority(
    caplog: pytest.LogCaptureFixture,
) -> None:
    authoritative = uuid4()
    caplog.set_level(logging.ERROR, logger="taskforge.logging")

    with bind_log_context(**{"workflow.run.id": authoritative}):
        with bind_log_context(**{"workflow.run.id": uuid4()}):
            assert current_log_context()["workflow.run.id"] == str(authoritative)

    event = next(
        record
        for record in caplog.records
        if getattr(record, "_event_name", None) == "logging.context_conflict"
    )
    assert event.__dict__["_event_fields"]["workflow.run.id"] == str(authoritative)


def test_logger_failure_and_fallback_failure_never_escape() -> None:
    class FailingLogger(logging.Logger):
        def log(self, level: int, msg: object, *args: object, **kwargs: object) -> None:
            del level, msg, args, kwargs
            raise RuntimeError("handler-secret")

    log_event(FailingLogger("failing"), logging.INFO, "test.event")


def test_invalid_context_is_rejected_without_preventing_body_execution(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class Unsafe:
        def __repr__(self) -> str:
            return "context-value-secret"

    executed = False
    caplog.set_level(logging.ERROR, logger="taskforge.logging")

    with bind_log_context(**{"correlation.id": Unsafe()}):
        executed = True
        assert current_log_context() == {}

    assert executed
    event = next(
        record
        for record in caplog.records
        if getattr(record, "_event_name", None) == "logging.context_rejected"
    )
    assert event.__dict__["_event_fields"] == {}
    assert "context-value-secret" not in repr(event.__dict__)


def test_async_contexts_are_isolated_and_reset() -> None:
    first, second = uuid4(), uuid4()

    async def observe(value: object) -> MappingSnapshot:
        with bind_log_context(**{"correlation.id": value}):
            await asyncio.sleep(0)
            return dict(current_log_context())

    async def run() -> tuple[MappingSnapshot, MappingSnapshot]:
        left, right = await asyncio.gather(observe(first), observe(second))
        return left, right

    left, right = asyncio.run(run())
    assert left["correlation.id"] == str(first)
    assert right["correlation.id"] == str(second)
    assert current_log_context() == {}


type MappingSnapshot = dict[str, object]


def test_log_event_rejects_arbitrary_objects_without_rendering_repr() -> None:
    class Unsafe:
        def __repr__(self) -> str:
            return "object-repr-secret"

    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(formatter())
    logger = logging.getLogger("test.rejected")
    logger.handlers = [handler]
    logger.propagate = False
    logger.setLevel(logging.INFO)

    log_event(logger, logging.INFO, "test.event", {"outcome": Unsafe()})

    output = stream.getvalue()
    assert json.loads(output)["event.name"] == "logging.event_rejected"
    assert "object-repr-secret" not in output
