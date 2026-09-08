"""Behavioral tests for the repository-owned bounded task handlers."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import cast
from uuid import uuid4

import pytest

from taskforge.dispatch.envelope import FrozenJSONMapping, create_dispatch_envelope
from taskforge.tasks import handlers
from taskforge.tasks.catalog import (
    INGEST_TASK_TYPE,
    INGESTION_CAPABILITY,
    NOTIFICATION_CAPABILITY,
    NOTIFY_TASK_TYPE,
    PROCESSING_CAPABILITY,
    TRANSFORM_TASK_TYPE,
    VALIDATE_TASK_TYPE,
)
from taskforge.worker.cancellation import TaskCancellationToken
from taskforge.worker.handlers import (
    TaskContext,
    TaskHandler,
    TaskHandlerResult,
    create_task_context,
)
from taskforge.worker.result_submission import MAX_TASK_RESULT_OUTPUT_BYTES
from taskforge.worker.results import TaskCancellation, TaskPermanentFailure

_CANCELLED_AT = datetime(2030, 1, 1, tzinfo=UTC)


def context(
    task_type: str,
    capability: str,
    parameters: dict[str, object],
    *,
    token: TaskCancellationToken | None = None,
) -> TaskContext:
    envelope = create_dispatch_envelope(
        dispatch_id=uuid4(),
        task_attempt_id=uuid4(),
        task_run_id=uuid4(),
        workflow_run_id=uuid4(),
        attempt_number=1,
        task_type=task_type,
        required_capability=capability,
        task_payload=parameters,
        references={},
    )
    return create_task_context(
        dispatch_id=envelope.dispatch_id,
        workflow_run_id=envelope.workflow_run_id,
        task_run_id=envelope.task_run_id,
        task_attempt_id=envelope.task_attempt_id,
        attempt_number=envelope.attempt_number,
        task_type=envelope.task_type,
        parameters=envelope.task_payload,
        references=envelope.references,
        correlation_id=None,
        trace_context=None,
        cancellation_requested_at_start=False,
        cancellation_token=token or TaskCancellationToken(),
        deadline=None,
    )


def result_size(result: object) -> int:
    return len(
        json.dumps(
            result,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    )


async def invoke(handler: TaskHandler, invocation: TaskContext) -> TaskHandlerResult:
    return await handler(invocation)


def test_ingest_known_answer_is_deterministic_and_bounded() -> None:
    invocation = context(
        INGEST_TASK_TYPE,
        INGESTION_CAPABILITY,
        {"document_id": "document-1", "content": "Café\r\nLine\rTail\n"},
    )

    first = asyncio.run(handlers.ingest(invocation))
    second = asyncio.run(handlers.ingest(invocation))

    assert (
        first
        == second
        == {
            "document_id": "document-1",
            "content": "Café\nLine\nTail\n",
            "content_sha256": (
                "2f401031c20cbb24d4b3f98fa0962f73d62e5bf6843282405a1294f94d312dd2"
            ),
            "content_bytes": 16,
        }
    )
    assert result_size(first) < MAX_TASK_RESULT_OUTPUT_BYTES


def test_validate_known_answers_remove_float_and_preserve_unicode_forms() -> None:
    nfc = context(
        VALIDATE_TASK_TYPE,
        PROCESSING_CAPABILITY,
        {
            "document_id": "document-1",
            "document": {"title": "Café", "count": 7, "ready": True},
            "required_fields": ["title", "count"],
        },
    )
    nfd = context(
        VALIDATE_TASK_TYPE,
        PROCESSING_CAPABILITY,
        {
            "document_id": "document-1",
            "document": {"value": "e\u0301"},
            "required_fields": ["value"],
        },
    )
    nfc_single = context(
        VALIDATE_TASK_TYPE,
        PROCESSING_CAPABILITY,
        {
            "document_id": "document-1",
            "document": {"value": "é"},
            "required_fields": ["value"],
        },
    )

    result = asyncio.run(handlers.validate(nfc))
    assert result == {
        "document_id": "document-1",
        "valid": True,
        "field_count": 3,
        "document_sha256": (
            "4fde1a3e20e597df960deb3e5e336f4735ef98e5fa5a6e15d5dc08c4f9e66b7a"
        ),
    }
    nfc_result = asyncio.run(handlers.validate(nfc_single))
    nfd_result = asyncio.run(handlers.validate(nfd))
    assert isinstance(nfc_result, dict) and isinstance(nfd_result, dict)
    assert nfc_result["document_sha256"] == (
        "69e46f3f0688000ab7eeb9e40e6a516a254268cd644a24f4f69cf7ad063cf479"
    )
    assert nfd_result["document_sha256"] == (
        "1b986c631da83257beca64c93ff3e5174908d558af7bb9dfb36b25a6198f7c38"
    )
    assert nfc_result["document_sha256"] != nfd_result["document_sha256"]
    assert result_size(result) < MAX_TASK_RESULT_OUTPUT_BYTES


def test_validate_missing_or_null_required_field_is_permanent() -> None:
    for required in ("missing", "empty"):
        invocation = context(
            VALIDATE_TASK_TYPE,
            PROCESSING_CAPABILITY,
            {
                "document_id": "document-1",
                "document": {"present": "yes", "empty": None},
                "required_fields": [required],
            },
        )
        assert isinstance(
            asyncio.run(handlers.validate(invocation)), TaskPermanentFailure
        )


def test_transform_known_answer_uses_only_ascii_whitespace_and_case() -> None:
    invocation = context(
        TRANSFORM_TASK_TYPE,
        PROCESSING_CAPABILITY,
        {
            "document_id": "document-1",
            "content": " \tAlpha \n  BETA\u00a0X \r",
            "operations": ["strip", "collapse_whitespace", "lowercase_ascii"],
        },
    )

    first = asyncio.run(handlers.transform(invocation))
    second = asyncio.run(handlers.transform(invocation))

    assert (
        first
        == second
        == {
            "document_id": "document-1",
            "content": "alpha beta\u00a0x",
            "content_sha256": (
                "1888d67c6c16d896ca54d1218c66fd5d3b6383c115ca08441c4027a044f2c0e3"
            ),
            "content_bytes": 13,
            "operations": ["strip", "collapse_whitespace", "lowercase_ascii"],
        }
    )
    assert result_size(first) < MAX_TASK_RESULT_OUTPUT_BYTES


def test_transform_does_not_collapse_other_unicode_whitespace() -> None:
    invocation = context(
        TRANSFORM_TASK_TYPE,
        PROCESSING_CAPABILITY,
        {
            "document_id": "document-1",
            "content": "A\u2003B\u00a0C\tD",
            "operations": ["collapse_whitespace", "lowercase_ascii"],
        },
    )
    result = asyncio.run(handlers.transform(invocation))
    assert isinstance(result, dict)
    assert result["content"] == "a\u2003b\u00a0c d"


def test_notify_known_answer_is_context_independent_and_bounded() -> None:
    parameters: dict[str, object] = {
        "notification_key": "notice-001",
        "topic": "pipeline.complete",
        "message": "Café ready",
    }
    first = asyncio.run(
        handlers.notify(context(NOTIFY_TASK_TYPE, NOTIFICATION_CAPABILITY, parameters))
    )
    second = asyncio.run(
        handlers.notify(context(NOTIFY_TASK_TYPE, NOTIFICATION_CAPABILITY, parameters))
    )

    assert (
        first
        == second
        == {
            "notification_key": "notice-001",
            "topic": "pipeline.complete",
            "receipt_id": (
                "e467cad6d470b611e925dd2d40d6e094db82924d3dabc22d6b8155b8363786ae"
            ),
            "status": "recorded",
        }
    )
    assert result_size(first) < MAX_TASK_RESULT_OUTPUT_BYTES


@pytest.mark.parametrize(
    ("handler", "task_type", "capability", "parameters"),
    (
        (
            handlers.ingest,
            INGEST_TASK_TYPE,
            INGESTION_CAPABILITY,
            {"document_id": "document-1", "content": "value"},
        ),
        (
            handlers.validate,
            VALIDATE_TASK_TYPE,
            PROCESSING_CAPABILITY,
            {
                "document_id": "document-1",
                "document": {"value": 1},
                "required_fields": ["value"],
            },
        ),
        (
            handlers.transform,
            TRANSFORM_TASK_TYPE,
            PROCESSING_CAPABILITY,
            {
                "document_id": "document-1",
                "content": "value",
                "operations": ["strip"],
            },
        ),
        (
            handlers.notify,
            NOTIFY_TASK_TYPE,
            NOTIFICATION_CAPABILITY,
            {
                "notification_key": "notice-1",
                "topic": "pipeline.complete",
                "message": "value",
            },
        ),
    ),
)
def test_all_handlers_honor_pre_requested_cancellation(
    handler: TaskHandler,
    task_type: str,
    capability: str,
    parameters: dict[str, object],
) -> None:
    token = TaskCancellationToken()
    token._request(_CANCELLED_AT)
    invocation = context(task_type, capability, parameters, token=token)

    assert isinstance(asyncio.run(invoke(handler, invocation)), TaskCancellation)


def test_transform_observes_cancellation_at_real_operation_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> object:
        reached = asyncio.Event()
        release = asyncio.Event()

        async def controlled_sleep(delay: float) -> None:
            assert delay == 0
            reached.set()
            await release.wait()

        monkeypatch.setattr("taskforge.tasks.handlers.asyncio.sleep", controlled_sleep)
        token = TaskCancellationToken()
        invocation = context(
            TRANSFORM_TASK_TYPE,
            PROCESSING_CAPABILITY,
            {
                "document_id": "document-1",
                "content": " Alpha ",
                "operations": ["strip", "lowercase_ascii"],
            },
            token=token,
        )
        running = asyncio.create_task(handlers.transform(invocation))
        await reached.wait()
        token._request(_CANCELLED_AT)
        release.set()
        return await running

    assert isinstance(asyncio.run(exercise()), TaskCancellation)


@pytest.mark.parametrize(
    ("handler", "task_type", "capability"),
    (
        (handlers.ingest, INGEST_TASK_TYPE, INGESTION_CAPABILITY),
        (handlers.validate, VALIDATE_TASK_TYPE, PROCESSING_CAPABILITY),
        (handlers.transform, TRANSFORM_TASK_TYPE, PROCESSING_CAPABILITY),
        (handlers.notify, NOTIFY_TASK_TYPE, NOTIFICATION_CAPABILITY),
    ),
)
def test_defensively_malformed_handler_payload_fails_permanently(
    handler: TaskHandler, task_type: str, capability: str
) -> None:
    invocation = context(task_type, capability, {"module": "os", "callable": "system"})
    assert isinstance(asyncio.run(invoke(handler, invocation)), TaskPermanentFailure)


@pytest.mark.parametrize(
    ("handler", "task_type", "capability", "parameters"),
    (
        (
            handlers.ingest,
            INGEST_TASK_TYPE,
            INGESTION_CAPABILITY,
            {"document_id": "document-1", "content": "\ud800"},
        ),
        (
            handlers.validate,
            VALIDATE_TASK_TYPE,
            PROCESSING_CAPABILITY,
            {
                "document_id": "document-1",
                "document": {"value": "\ud800"},
                "required_fields": ["value"],
            },
        ),
        (
            handlers.transform,
            TRANSFORM_TASK_TYPE,
            PROCESSING_CAPABILITY,
            {
                "document_id": "document-1",
                "content": "\ud800",
                "operations": ["strip"],
            },
        ),
        (
            handlers.notify,
            NOTIFY_TASK_TYPE,
            NOTIFICATION_CAPABILITY,
            {
                "notification_key": "notice-1",
                "topic": "pipeline.complete",
                "message": "\ud800",
            },
        ),
    ),
)
def test_non_utf8_handler_text_fails_permanently(
    handler: TaskHandler,
    task_type: str,
    capability: str,
    parameters: dict[str, object],
) -> None:
    del capability
    invocation = create_task_context(
        dispatch_id=uuid4(),
        workflow_run_id=uuid4(),
        task_run_id=uuid4(),
        task_attempt_id=uuid4(),
        attempt_number=1,
        task_type=task_type,
        parameters=cast(FrozenJSONMapping, parameters),
        references={},
        correlation_id=None,
        trace_context=None,
        cancellation_requested_at_start=False,
        cancellation_token=TaskCancellationToken(),
        deadline=None,
    )
    assert isinstance(asyncio.run(invoke(handler, invocation)), TaskPermanentFailure)
