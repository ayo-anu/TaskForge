"""Metadata-only catalog for TaskForge's representative pipeline tasks."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

from taskforge.workflows.task_types import (
    JSONMapping,
    TaskTypeDefinition,
    WorkflowValidationIssue,
)

INGEST_TASK_TYPE: Final = "pipeline.ingest"
VALIDATE_TASK_TYPE: Final = "pipeline.validate"
TRANSFORM_TASK_TYPE: Final = "pipeline.transform"
NOTIFY_TASK_TYPE: Final = "pipeline.notify"

INGESTION_CAPABILITY: Final = "pipeline.ingestion"
PROCESSING_CAPABILITY: Final = "pipeline.processing"
NOTIFICATION_CAPABILITY: Final = "pipeline.notification"

MAX_IDENTIFIER_LENGTH: Final = 64
MAX_INLINE_TEXT_BYTES: Final = 4_096
MAX_DOCUMENT_FIELDS: Final = 32
MAX_REQUIRED_FIELDS: Final = 16
MAX_DOCUMENT_STRING_BYTES: Final = 512
MAX_TRANSFORM_OPERATIONS: Final = 4
MIN_SIGNED_INT64: Final = -(2**63)
MAX_SIGNED_INT64: Final = 2**63 - 1

STRIP_OPERATION: Final = "strip"
COLLAPSE_WHITESPACE_OPERATION: Final = "collapse_whitespace"
LOWERCASE_ASCII_OPERATION: Final = "lowercase_ascii"
UPPERCASE_ASCII_OPERATION: Final = "uppercase_ascii"
TRANSFORM_OPERATIONS: Final = frozenset(
    {
        STRIP_OPERATION,
        COLLAPSE_WHITESPACE_OPERATION,
        LOWERCASE_ASCII_OPERATION,
        UPPERCASE_ASCII_OPERATION,
    }
)

_IDENTIFIER = re.compile(r"\A[a-z0-9][a-z0-9._-]{0,63}\Z")
_FIELD_NAME = re.compile(r"\A[a-z][a-z0-9._-]{0,63}\Z")


@dataclass(frozen=True)
class IngestParameters:
    document_id: str
    content: str


@dataclass(frozen=True)
class ValidateParameters:
    document_id: str
    document: dict[str, bool | int | str | None]
    required_fields: tuple[str, ...]


@dataclass(frozen=True)
class TransformParameters:
    document_id: str
    content: str
    operations: tuple[str, ...]


@dataclass(frozen=True)
class NotifyParameters:
    notification_key: str
    topic: str
    message: str


class _IngestValidator:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        return parse_ingest_parameters(parameters)[1]


class _ValidateValidator:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        return parse_validate_parameters(parameters)[1]


class _TransformValidator:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        return parse_transform_parameters(parameters)[1]


class _NotifyValidator:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        return parse_notify_parameters(parameters)[1]


def provide_task_catalog() -> tuple[TaskTypeDefinition, ...]:
    """Return the one repository-owned metadata-only task catalog."""
    return (
        TaskTypeDefinition(INGEST_TASK_TYPE, INGESTION_CAPABILITY, _IngestValidator()),
        TaskTypeDefinition(
            VALIDATE_TASK_TYPE, PROCESSING_CAPABILITY, _ValidateValidator()
        ),
        TaskTypeDefinition(
            TRANSFORM_TASK_TYPE, PROCESSING_CAPABILITY, _TransformValidator()
        ),
        TaskTypeDefinition(
            NOTIFY_TASK_TYPE, NOTIFICATION_CAPABILITY, _NotifyValidator()
        ),
    )


def canonical_json_bytes(value: object) -> bytes:
    """Serialize a validated value under the durable Task 3 hash contract."""
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def parse_ingest_parameters(
    parameters: Mapping[str, object],
) -> tuple[IngestParameters | None, tuple[WorkflowValidationIssue, ...]]:
    issues = _object_shape_issues(parameters, frozenset({"document_id", "content"}))
    document_id = parameters.get("document_id")
    content = parameters.get("content")
    issues.extend(_identifier_issues(document_id, ("document_id",)))
    issues.extend(
        _text_issues(
            content,
            ("content",),
            maximum_bytes=MAX_INLINE_TEXT_BYTES,
            allow_empty=False,
        )
    )
    if issues:
        return None, tuple(issues)
    assert isinstance(document_id, str)
    assert isinstance(content, str)
    return IngestParameters(document_id, content), ()


def parse_validate_parameters(
    parameters: Mapping[str, object],
) -> tuple[ValidateParameters | None, tuple[WorkflowValidationIssue, ...]]:
    issues = _object_shape_issues(
        parameters, frozenset({"document_id", "document", "required_fields"})
    )
    document_id = parameters.get("document_id")
    document = parameters.get("document")
    required_fields = parameters.get("required_fields")
    issues.extend(_identifier_issues(document_id, ("document_id",)))

    validated_document: dict[str, bool | int | str | None] = {}
    if not isinstance(document, Mapping):
        issues.append(_invalid(("document",), "Document must be an object."))
    else:
        if not 1 <= len(document) <= MAX_DOCUMENT_FIELDS:
            issues.append(
                _invalid(
                    ("document",), "Document must contain between 1 and 32 fields."
                )
            )
        for key, value in document.items():
            if not isinstance(key, str) or _FIELD_NAME.fullmatch(key) is None:
                issues.append(
                    _invalid(("document",), "Document field name is invalid.")
                )
                continue
            path = ("document", key)
            if value is None or isinstance(value, bool):
                validated_document[key] = value
            elif isinstance(value, int):
                if not MIN_SIGNED_INT64 <= value <= MAX_SIGNED_INT64:
                    issues.append(
                        _invalid(path, "Document integer must fit signed 64-bit range.")
                    )
                else:
                    validated_document[key] = value
            elif isinstance(value, str):
                value_issues = _text_issues(
                    value,
                    path,
                    maximum_bytes=MAX_DOCUMENT_STRING_BYTES,
                    allow_empty=True,
                )
                issues.extend(value_issues)
                if not value_issues:
                    validated_document[key] = value
            else:
                issues.append(
                    _invalid(
                        path,
                        "Document value must be null, boolean, signed integer, or string.",
                    )
                )

    validated_required: list[str] = []
    if not isinstance(required_fields, (list, tuple)):
        issues.append(
            _invalid(("required_fields",), "Required fields must be an array.")
        )
    else:
        if not 1 <= len(required_fields) <= MAX_REQUIRED_FIELDS:
            issues.append(
                _invalid(
                    ("required_fields",),
                    "Required fields must contain between 1 and 16 items.",
                )
            )
        seen: set[str] = set()
        for index, value in enumerate(required_fields):
            required_path = ("required_fields", index)
            if not isinstance(value, str) or _FIELD_NAME.fullmatch(value) is None:
                issues.append(
                    _invalid(required_path, "Required field name is invalid.")
                )
                continue
            if value in seen:
                issues.append(
                    _invalid(required_path, "Required field name is duplicated.")
                )
                continue
            seen.add(value)
            validated_required.append(value)

    if issues:
        return None, tuple(issues)
    assert isinstance(document_id, str)
    return (
        ValidateParameters(document_id, validated_document, tuple(validated_required)),
        (),
    )


def parse_transform_parameters(
    parameters: Mapping[str, object],
) -> tuple[TransformParameters | None, tuple[WorkflowValidationIssue, ...]]:
    issues = _object_shape_issues(
        parameters, frozenset({"document_id", "content", "operations"})
    )
    document_id = parameters.get("document_id")
    content = parameters.get("content")
    operations = parameters.get("operations")
    issues.extend(_identifier_issues(document_id, ("document_id",)))
    issues.extend(
        _text_issues(
            content,
            ("content",),
            maximum_bytes=MAX_INLINE_TEXT_BYTES,
            allow_empty=True,
        )
    )

    validated_operations: list[str] = []
    if not isinstance(operations, (list, tuple)):
        issues.append(_invalid(("operations",), "Operations must be an array."))
    else:
        if not 1 <= len(operations) <= MAX_TRANSFORM_OPERATIONS:
            issues.append(
                _invalid(
                    ("operations",),
                    "Operations must contain between 1 and 4 items.",
                )
            )
        seen: set[str] = set()
        for index, operation in enumerate(operations):
            path = ("operations", index)
            if not isinstance(operation, str) or operation not in TRANSFORM_OPERATIONS:
                issues.append(_invalid(path, "Transform operation is not supported."))
                continue
            if operation in seen:
                issues.append(_invalid(path, "Transform operation is duplicated."))
                continue
            seen.add(operation)
            validated_operations.append(operation)
        if {
            LOWERCASE_ASCII_OPERATION,
            UPPERCASE_ASCII_OPERATION,
        }.issubset(seen):
            issues.append(
                _invalid(
                    ("operations",),
                    "ASCII lowercase and uppercase operations conflict.",
                )
            )

    if issues:
        return None, tuple(issues)
    assert isinstance(document_id, str)
    assert isinstance(content, str)
    return (
        TransformParameters(document_id, content, tuple(validated_operations)),
        (),
    )


def parse_notify_parameters(
    parameters: Mapping[str, object],
) -> tuple[NotifyParameters | None, tuple[WorkflowValidationIssue, ...]]:
    issues = _object_shape_issues(
        parameters, frozenset({"notification_key", "topic", "message"})
    )
    notification_key = parameters.get("notification_key")
    topic = parameters.get("topic")
    message = parameters.get("message")
    issues.extend(_identifier_issues(notification_key, ("notification_key",)))
    if not isinstance(topic, str) or _FIELD_NAME.fullmatch(topic) is None:
        issues.append(_invalid(("topic",), "Notification topic is invalid."))
    issues.extend(
        _text_issues(
            message,
            ("message",),
            maximum_bytes=MAX_INLINE_TEXT_BYTES,
            allow_empty=False,
        )
    )
    if issues:
        return None, tuple(issues)
    assert isinstance(notification_key, str)
    assert isinstance(topic, str)
    assert isinstance(message, str)
    return NotifyParameters(notification_key, topic, message), ()


def _object_shape_issues(
    parameters: Mapping[str, object], expected: frozenset[str]
) -> list[WorkflowValidationIssue]:
    issues: list[WorkflowValidationIssue] = []
    actual = set(parameters)
    for field in sorted(expected - actual):
        issues.append(_invalid((field,), "Required parameter is missing."))
    for field in sorted(actual - expected):
        issues.append(_invalid((field,), "Parameter is not supported."))
    return issues


def _identifier_issues(
    value: object, path: tuple[str | int, ...]
) -> tuple[WorkflowValidationIssue, ...]:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        return (_invalid(path, "Identifier is invalid."),)
    return ()


def _text_issues(
    value: object,
    path: tuple[str | int, ...],
    *,
    maximum_bytes: int,
    allow_empty: bool,
) -> tuple[WorkflowValidationIssue, ...]:
    if not isinstance(value, str):
        return (_invalid(path, "Value must be a string."),)
    if not value and not allow_empty:
        return (_invalid(path, "Value must not be empty."),)
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError:
        return (_invalid(path, "Value must contain valid Unicode."),)
    if len(encoded) > maximum_bytes:
        return (_invalid(path, "String exceeds its UTF-8 byte limit."),)
    return ()


def _invalid(path: tuple[str | int, ...], message: str) -> WorkflowValidationIssue:
    return WorkflowValidationIssue("invalid_task_parameters", path, message)
