"""Trusted bounded handlers for TaskForge's representative pipeline."""

from __future__ import annotations

import asyncio
import hashlib
import re

from taskforge.tasks.catalog import (
    COLLAPSE_WHITESPACE_OPERATION,
    LOWERCASE_ASCII_OPERATION,
    STRIP_OPERATION,
    UPPERCASE_ASCII_OPERATION,
    canonical_json_bytes,
    parse_ingest_parameters,
    parse_notify_parameters,
    parse_transform_parameters,
    parse_validate_parameters,
)
from taskforge.worker.handlers import TaskContext, TaskHandlerResult
from taskforge.worker.results import TaskCancellation, TaskPermanentFailure

_ASCII_WHITESPACE = "\t\n\v\f\r "
_ASCII_WHITESPACE_RUN = re.compile(r"[\t\n\v\f\r ]+")
_LOWERCASE_ASCII = str.maketrans(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"
)
_UPPERCASE_ASCII = str.maketrans(
    "abcdefghijklmnopqrstuvwxyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
)


def _cancellation_requested(context: TaskContext) -> bool:
    return context.cancellation_token.is_cancellation_requested


async def ingest(context: TaskContext) -> TaskHandlerResult:
    """Normalize and identify one bounded inline document."""
    if _cancellation_requested(context):
        return TaskCancellation()
    parameters, issues = parse_ingest_parameters(context.parameters)
    if issues or parameters is None:
        return TaskPermanentFailure()
    normalized = parameters.content.replace("\r\n", "\n").replace("\r", "\n")
    content = normalized.encode("utf-8")
    return {
        "document_id": parameters.document_id,
        "content": normalized,
        "content_sha256": hashlib.sha256(content).hexdigest(),
        "content_bytes": len(content),
    }


async def validate(context: TaskContext) -> TaskHandlerResult:
    """Validate required non-null fields in one bounded document."""
    if _cancellation_requested(context):
        return TaskCancellation()
    parameters, issues = parse_validate_parameters(context.parameters)
    if issues or parameters is None:
        return TaskPermanentFailure()
    if any(
        field not in parameters.document or parameters.document[field] is None
        for field in parameters.required_fields
    ):
        return TaskPermanentFailure()
    document = canonical_json_bytes(parameters.document)
    return {
        "document_id": parameters.document_id,
        "valid": True,
        "field_count": len(parameters.document),
        "document_sha256": hashlib.sha256(document).hexdigest(),
    }


async def transform(context: TaskContext) -> TaskHandlerResult:
    """Apply an ordered, bounded list of allowlisted text transformations."""
    if _cancellation_requested(context):
        return TaskCancellation()
    parameters, issues = parse_transform_parameters(context.parameters)
    if issues or parameters is None:
        return TaskPermanentFailure()

    transformed = parameters.content
    for index, operation in enumerate(parameters.operations):
        if index:
            await asyncio.sleep(0)
        if _cancellation_requested(context):
            return TaskCancellation()
        if operation == STRIP_OPERATION:
            transformed = transformed.strip(_ASCII_WHITESPACE)
        elif operation == COLLAPSE_WHITESPACE_OPERATION:
            transformed = _ASCII_WHITESPACE_RUN.sub(" ", transformed)
        elif operation == LOWERCASE_ASCII_OPERATION:
            transformed = transformed.translate(_LOWERCASE_ASCII)
        elif operation == UPPERCASE_ASCII_OPERATION:
            transformed = transformed.translate(_UPPERCASE_ASCII)

    content = transformed.encode("utf-8")
    return {
        "document_id": parameters.document_id,
        "content": transformed,
        "content_sha256": hashlib.sha256(content).hexdigest(),
        "content_bytes": len(content),
        "operations": list(parameters.operations),
    }


async def notify(context: TaskContext) -> TaskHandlerResult:
    """Record a deterministic local notification-sink receipt."""
    if _cancellation_requested(context):
        return TaskCancellation()
    parameters, issues = parse_notify_parameters(context.parameters)
    if issues or parameters is None:
        return TaskPermanentFailure()
    receipt = canonical_json_bytes(
        {
            "notification_key": parameters.notification_key,
            "topic": parameters.topic,
            "message": parameters.message,
        }
    )
    return {
        "notification_key": parameters.notification_key,
        "topic": parameters.topic,
        "receipt_id": hashlib.sha256(receipt).hexdigest(),
        "status": "recorded",
    }
