"""Contract tests for the repository-owned production task catalog."""

from __future__ import annotations

import pytest

from taskforge.tasks.catalog import (
    INGEST_TASK_TYPE,
    INGESTION_CAPABILITY,
    MAX_DOCUMENT_FIELDS,
    MAX_DOCUMENT_STRING_BYTES,
    MAX_INLINE_TEXT_BYTES,
    MAX_REQUIRED_FIELDS,
    MAX_SIGNED_INT64,
    NOTIFICATION_CAPABILITY,
    NOTIFY_TASK_TYPE,
    PROCESSING_CAPABILITY,
    TRANSFORM_TASK_TYPE,
    VALIDATE_TASK_TYPE,
    provide_task_catalog,
)
from taskforge.workflows.task_types import TaskTypeRegistry


def registry() -> TaskTypeRegistry:
    return TaskTypeRegistry(provide_task_catalog())


def codes(task_type: str, parameters: object) -> set[str]:
    _validated, issues = registry().validate(task_type, parameters)
    return {issue.code for issue in issues}


def test_catalog_contains_only_approved_metadata() -> None:
    catalog = registry()

    assert catalog.names == {
        INGEST_TASK_TYPE,
        VALIDATE_TASK_TYPE,
        TRANSFORM_TASK_TYPE,
        NOTIFY_TASK_TYPE,
    }
    assert catalog.required_capabilities == {
        INGESTION_CAPABILITY,
        PROCESSING_CAPABILITY,
        NOTIFICATION_CAPABILITY,
    }
    expected = {
        INGEST_TASK_TYPE: INGESTION_CAPABILITY,
        VALIDATE_TASK_TYPE: PROCESSING_CAPABILITY,
        TRANSFORM_TASK_TYPE: PROCESSING_CAPABILITY,
        NOTIFY_TASK_TYPE: NOTIFICATION_CAPABILITY,
    }
    for task_type, capability in expected.items():
        definition = catalog.definition(task_type)
        assert definition is not None
        assert definition.required_capability == capability


@pytest.mark.parametrize(
    ("task_type", "parameters"),
    (
        (
            INGEST_TASK_TYPE,
            {"document_id": "document-1", "content": "hello"},
        ),
        (
            VALIDATE_TASK_TYPE,
            {
                "document_id": "document-1",
                "document": {"title": "hello", "count": 1, "ready": True},
                "required_fields": ["title", "count"],
            },
        ),
        (
            TRANSFORM_TASK_TYPE,
            {
                "document_id": "document-1",
                "content": " hello ",
                "operations": ["strip", "uppercase_ascii"],
            },
        ),
        (
            NOTIFY_TASK_TYPE,
            {
                "notification_key": "notice-1",
                "topic": "pipeline.complete",
                "message": "ready",
            },
        ),
    ),
)
def test_valid_task_parameters_are_accepted(
    task_type: str, parameters: dict[str, object]
) -> None:
    validated, issues = registry().validate(task_type, parameters)

    assert issues == ()
    assert validated == parameters


@pytest.mark.parametrize(
    ("task_type", "parameters", "path"),
    (
        (INGEST_TASK_TYPE, {"document_id": "document-1"}, ("content",)),
        (
            INGEST_TASK_TYPE,
            {"document_id": "document-1", "content": "x", "module": "evil"},
            ("module",),
        ),
        (
            NOTIFY_TASK_TYPE,
            {"notification_key": "notice", "topic": "pipeline.complete"},
            ("message",),
        ),
    ),
)
def test_catalog_rejects_missing_and_unexpected_fields_without_echoing_values(
    task_type: str, parameters: dict[str, object], path: tuple[str, ...]
) -> None:
    validated, issues = registry().validate(task_type, parameters)

    assert validated is None
    assert any(issue.path == path for issue in issues)
    assert {issue.code for issue in issues} == {"invalid_task_parameters"}
    assert "evil" not in repr(issues)


@pytest.mark.parametrize(
    "identifier",
    ("", "A", "../value", "module:value", "a" * 65),
)
def test_opaque_identifiers_are_strict_and_bounded(identifier: str) -> None:
    assert "invalid_task_parameters" in codes(
        INGEST_TASK_TYPE, {"document_id": identifier, "content": "x"}
    )


def test_identifier_and_utf8_text_boundaries_are_exact() -> None:
    catalog = registry()
    at_identifier = "a" * 64
    at_bytes = "é" * (MAX_INLINE_TEXT_BYTES // 2)
    accepted, issues = catalog.validate(
        INGEST_TASK_TYPE,
        {"document_id": at_identifier, "content": at_bytes},
    )
    rejected, rejected_issues = catalog.validate(
        INGEST_TASK_TYPE,
        {"document_id": at_identifier, "content": f"{at_bytes}x"},
    )

    assert accepted is not None and issues == ()
    assert rejected is None
    assert {issue.code for issue in rejected_issues} == {"invalid_task_parameters"}


def test_validate_accepts_only_bounded_non_float_scalars() -> None:
    catalog = registry()
    valid_document = {
        "none": None,
        "false": False,
        "true": True,
        "minimum": -(2**63),
        "maximum": MAX_SIGNED_INT64,
        "text": "é" * (MAX_DOCUMENT_STRING_BYTES // 2),
    }
    valid, issues = catalog.validate(
        VALIDATE_TASK_TYPE,
        {
            "document_id": "document-1",
            "document": valid_document,
            "required_fields": ["false", "minimum"],
        },
    )
    assert valid is not None and issues == ()

    invalid_values: tuple[object, ...] = (
        1.0,
        -(2**63) - 1,
        2**63,
        [],
        {},
        "é" * (MAX_DOCUMENT_STRING_BYTES // 2) + "x",
    )
    for value in invalid_values:
        assert "invalid_task_parameters" in codes(
            VALIDATE_TASK_TYPE,
            {
                "document_id": "document-1",
                "document": {"value": value},
                "required_fields": ["value"],
            },
        )


def test_validate_collection_bounds_duplicates_and_business_missing_are_exact() -> None:
    catalog = registry()
    maximum_document = {f"field-{index}": index for index in range(MAX_DOCUMENT_FIELDS)}
    maximum_required = [f"field-{index}" for index in range(MAX_REQUIRED_FIELDS)]
    valid, issues = catalog.validate(
        VALIDATE_TASK_TYPE,
        {
            "document_id": "document-1",
            "document": maximum_document,
            "required_fields": maximum_required,
        },
    )
    missing_business_field, missing_issues = catalog.validate(
        VALIDATE_TASK_TYPE,
        {
            "document_id": "document-1",
            "document": {"present": "yes", "null": None},
            "required_fields": ["absent", "null"],
        },
    )

    assert valid is not None and issues == ()
    assert missing_business_field is not None and missing_issues == ()
    assert "invalid_task_parameters" in codes(
        VALIDATE_TASK_TYPE,
        {
            "document_id": "document-1",
            "document": {**maximum_document, "overflow": 1},
            "required_fields": ["field-0"],
        },
    )
    assert "invalid_task_parameters" in codes(
        VALIDATE_TASK_TYPE,
        {
            "document_id": "document-1",
            "document": {"field": 1},
            "required_fields": ["field"] * 2,
        },
    )
    assert "invalid_task_parameters" in codes(
        VALIDATE_TASK_TYPE,
        {
            "document_id": "document-1",
            "document": {"field": 1},
            "required_fields": [
                *(f"field-{index}" for index in range(MAX_REQUIRED_FIELDS)),
                "overflow",
            ],
        },
    )


@pytest.mark.parametrize(
    "operations",
    (
        [],
        ["strip"] * 2,
        ["unknown"],
        ["lowercase_ascii", "uppercase_ascii"],
        ["strip", "collapse_whitespace", "lowercase_ascii", "strip", "strip"],
    ),
)
def test_transform_operation_allowlist_and_bounds(operations: list[str]) -> None:
    assert "invalid_task_parameters" in codes(
        TRANSFORM_TASK_TYPE,
        {"document_id": "document-1", "content": "value", "operations": operations},
    )


def test_notification_topic_and_message_are_bounded() -> None:
    catalog = registry()
    at_limit, issues = catalog.validate(
        NOTIFY_TASK_TYPE,
        {
            "notification_key": "notice-1",
            "topic": "pipeline.complete",
            "message": "x" * MAX_INLINE_TEXT_BYTES,
        },
    )
    assert at_limit is not None and issues == ()
    assert "invalid_task_parameters" in codes(
        NOTIFY_TASK_TYPE,
        {
            "notification_key": "notice-1",
            "topic": "https://example.invalid",
            "message": "x",
        },
    )
