"""Registered task types and bounded JSON parameter validation tests."""

from __future__ import annotations

import math
from dataclasses import dataclass

import pytest

from taskforge.workflows.task_types import (
    MAX_COLLECTION_ITEMS,
    MAX_PARAMETER_DEPTH,
    MAX_PARAMETER_KEY_LENGTH,
    MAX_PARAMETER_NODES,
    MAX_PARAMETER_STRING_LENGTH,
    JSONMapping,
    TaskTypeDefinition,
    TaskTypeRegistry,
    WorkflowValidationIssue,
    validate_parameters,
)


@dataclass(frozen=True)
class AcceptParameters:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        del parameters
        return ()


@dataclass(frozen=True)
class RequireValue:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        if "value" in parameters:
            return ()
        return (
            WorkflowValidationIssue(
                "missing_value", ("value",), "A value is required."
            ),
        )


def registry(*names: str) -> TaskTypeRegistry:
    return TaskTypeRegistry(
        tuple(
            TaskTypeDefinition(name, "test-workers", AcceptParameters())
            for name in names
        )
    )


def test_registry_accepts_only_explicitly_registered_task_types() -> None:
    task_types = registry("document.extract", "notify-email")

    validated, issues = task_types.validate("document.extract", {"value": 1})
    unknown, unknown_issues = task_types.validate("document.unknown", {})

    assert validated == {"value": 1}
    assert issues == ()
    assert unknown is None
    assert [issue.code for issue in unknown_issues] == ["unsupported_task_type"]
    assert task_types.names == frozenset({"document.extract", "notify-email"})
    assert task_types.definition("document.extract") == TaskTypeDefinition(
        "document.extract", "test-workers", AcceptParameters()
    )
    assert task_types.definition("document.unknown") is None


@pytest.mark.parametrize("capability", ("", "Uppercase", "has space", ".leading"))
def test_registry_rejects_invalid_required_capabilities(capability: str) -> None:
    with pytest.raises(ValueError, match="invalid registered task capability"):
        TaskTypeRegistry(
            (TaskTypeDefinition("test.task", capability, AcceptParameters()),)
        )


@pytest.mark.parametrize("name", ("", "Uppercase", "has space", ".leading"))
def test_registry_rejects_invalid_task_type_names(name: str) -> None:
    with pytest.raises(ValueError, match="invalid registered task type"):
        registry(name)


def test_registry_rejects_duplicate_definitions() -> None:
    with pytest.raises(ValueError, match="duplicate registered task type"):
        registry("test.task", "test.task")


def test_task_specific_validation_returns_safe_issues() -> None:
    task_types = TaskTypeRegistry(
        (TaskTypeDefinition("test.required", "test-workers", RequireValue()),)
    )

    validated, issues = task_types.validate(
        "test.required", {}, path=("steps", 2, "parameters")
    )

    assert validated is None
    assert [(issue.code, issue.path) for issue in issues] == [
        ("missing_value", ("steps", 2, "parameters", "value"))
    ]


def test_unexpected_task_validator_errors_propagate() -> None:
    class BrokenValidator:
        def validate(
            self, parameters: JSONMapping
        ) -> tuple[WorkflowValidationIssue, ...]:
            del parameters
            raise RuntimeError("validator programming error")

    task_types = TaskTypeRegistry(
        (TaskTypeDefinition("test.broken", "test-workers", BrokenValidator()),)
    )

    with pytest.raises(RuntimeError, match="validator programming error"):
        task_types.validate("test.broken", {})


@pytest.mark.parametrize(
    ("parameters", "expected_code"),
    (
        ([], "invalid_parameters"),
        ({1: "value"}, "invalid_parameters"),
        ({"value": object()}, "invalid_parameters"),
        ({"value": math.nan}, "invalid_parameters"),
        ({"value": math.inf}, "invalid_parameters"),
        (
            {"value": "x" * (MAX_PARAMETER_STRING_LENGTH + 1)},
            "parameter_string_too_large",
        ),
        (
            {"x" * (MAX_PARAMETER_KEY_LENGTH + 1): "value"},
            "parameter_key_too_large",
        ),
        (
            {"values": list(range(MAX_COLLECTION_ITEMS + 1))},
            "parameters_too_complex",
        ),
        (
            {str(index): index for index in range(MAX_PARAMETER_NODES + 1)},
            "parameters_too_complex",
        ),
    ),
)
def test_generic_parameter_validation_rejects_invalid_or_unbounded_values(
    parameters: object,
    expected_code: str,
) -> None:
    issues, validated = validate_parameters(parameters)

    assert validated is None
    assert expected_code in {issue.code for issue in issues}


def test_parameter_validation_rejects_excessive_depth() -> None:
    parameters: dict[str, object] = {}
    cursor = parameters
    for _ in range(MAX_PARAMETER_DEPTH + 1):
        nested: dict[str, object] = {}
        cursor["nested"] = nested
        cursor = nested

    issues, validated = validate_parameters(parameters)

    assert validated is None
    assert "parameters_too_deep" in {issue.code for issue in issues}


def test_parameter_validation_rejects_recursive_containers() -> None:
    parameters: dict[str, object] = {}
    parameters["recursive"] = parameters

    issues, validated = validate_parameters(parameters)

    assert validated is None
    assert "invalid_parameters" in {issue.code for issue in issues}


def test_parameter_size_is_based_on_canonical_utf8_json() -> None:
    small = {f"field_{index}": "x" * 3000 for index in range(5)}
    large = {f"field_{index}": "x" * 4000 for index in range(5)}

    small_issues, small_validated = validate_parameters(small)
    large_issues, large_validated = validate_parameters(large)

    assert small_issues == ()
    assert small_validated == small
    assert large_validated is None
    assert [issue.code for issue in large_issues] == ["parameters_too_large"]


def test_generic_validation_does_not_assign_semantics_to_field_names() -> None:
    parameters = {
        "password": "synthetic",
        "secret": "synthetic",
        "api_key": "synthetic",
        "credential": "synthetic",
    }

    issues, validated = validate_parameters(parameters)

    assert issues == ()
    assert validated == parameters
