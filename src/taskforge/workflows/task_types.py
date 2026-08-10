"""Registered task-type and bounded JSON parameter validation."""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from taskforge.capabilities import is_valid_capability_name

if TYPE_CHECKING:
    from taskforge.workflows.dag_validation import DAGValidationResult

MAX_PARAMETER_BYTES = 16 * 1024
MAX_PARAMETER_DEPTH = 8
MAX_PARAMETER_NODES = 512
MAX_COLLECTION_ITEMS = 128
MAX_PARAMETER_STRING_LENGTH = 4096
MAX_PARAMETER_KEY_LENGTH = 128

type JSONScalar = bool | int | float | str | None
type JSONValue = JSONScalar | list[JSONValue] | dict[str, JSONValue]
type JSONMapping = dict[str, JSONValue]
type ValidationPath = tuple[str | int, ...]

_TASK_TYPE_NAME = re.compile(r"\A[a-z][a-z0-9_.-]{0,127}\Z")


@dataclass(frozen=True)
class WorkflowValidationIssue:
    code: str
    path: ValidationPath
    message: str


class WorkflowValidationError(ValueError):
    """One or more safe, deterministic workflow validation issues."""

    def __init__(
        self,
        issues: tuple[WorkflowValidationIssue, ...] = (),
        *,
        graph_result: DAGValidationResult | None = None,
    ) -> None:
        if bool(issues) == (graph_result is not None):
            raise ValueError("exactly one validation issue form is required")
        self.issues = issues
        self.graph_result = graph_result
        super().__init__("workflow validation failed")

    @classmethod
    def from_graph(cls, result: DAGValidationResult) -> WorkflowValidationError:
        """Represent an invalid graph through the existing validation family."""
        if result.is_valid:
            raise ValueError("graph validation result must be invalid")
        return cls(graph_result=result)


class TaskParameterValidator(Protocol):
    def validate(
        self,
        parameters: JSONMapping,
    ) -> tuple[WorkflowValidationIssue, ...]:
        """Return safe issues for an otherwise structurally valid JSON object."""


@dataclass(frozen=True)
class TaskTypeDefinition:
    name: str
    required_capability: str
    parameter_validator: TaskParameterValidator


class TaskTypeRegistry:
    """Immutable task-type catalog assembled explicitly at composition time."""

    def __init__(self, definitions: tuple[TaskTypeDefinition, ...]) -> None:
        registered: dict[str, TaskTypeDefinition] = {}
        for definition in definitions:
            if _TASK_TYPE_NAME.fullmatch(definition.name) is None:
                raise ValueError("invalid registered task type")
            if not is_valid_capability_name(definition.required_capability):
                raise ValueError("invalid registered task capability")
            if definition.name in registered:
                raise ValueError("duplicate registered task type")
            registered[definition.name] = definition
        self._registered = registered

    @property
    def names(self) -> frozenset[str]:
        return frozenset(self._registered)

    @property
    def required_capabilities(self) -> frozenset[str]:
        """Return the unique stable capabilities required by registered handlers."""
        return frozenset(
            definition.required_capability for definition in self._registered.values()
        )

    def definition(self, task_type: str) -> TaskTypeDefinition | None:
        """Return the stable registered handler contract for a task type."""
        return self._registered.get(task_type)

    def validate(
        self,
        task_type: str,
        parameters: object,
        *,
        path: ValidationPath = (),
    ) -> tuple[JSONMapping | None, tuple[WorkflowValidationIssue, ...]]:
        structural_issues, validated = validate_parameters(parameters, path=path)
        if structural_issues:
            return None, structural_issues
        definition = self._registered.get(task_type)
        if definition is None:
            return None, (
                WorkflowValidationIssue(
                    "unsupported_task_type",
                    ("task_type",),
                    "Task type is not registered.",
                ),
            )
        assert validated is not None
        task_issues = definition.parameter_validator.validate(validated)
        if task_issues:
            return None, tuple(
                WorkflowValidationIssue(
                    issue.code,
                    (*path, *issue.path),
                    issue.message,
                )
                for issue in task_issues
            )
        return validated, ()


def validate_parameters(
    parameters: object,
    *,
    path: ValidationPath = (),
) -> tuple[tuple[WorkflowValidationIssue, ...], JSONMapping | None]:
    """Validate a bounded JSON object without task-specific semantics."""
    if not isinstance(parameters, dict):
        return (
            WorkflowValidationIssue(
                "invalid_parameters", path, "Parameters must be a JSON object."
            ),
        ), None

    issues: list[WorkflowValidationIssue] = []
    active_containers: set[int] = set()
    node_count = 0

    def visit(value: object, current_path: ValidationPath, depth: int) -> None:
        nonlocal node_count
        node_count += 1
        if node_count > MAX_PARAMETER_NODES:
            if not any(issue.code == "parameters_too_complex" for issue in issues):
                issues.append(
                    WorkflowValidationIssue(
                        "parameters_too_complex",
                        path,
                        "Parameters contain too many values.",
                    )
                )
            return
        if depth > MAX_PARAMETER_DEPTH:
            issues.append(
                WorkflowValidationIssue(
                    "parameters_too_deep",
                    current_path,
                    "Parameters are nested too deeply.",
                )
            )
            return
        if value is None or isinstance(value, (bool, int)):
            return
        if isinstance(value, float):
            if not math.isfinite(value):
                issues.append(
                    WorkflowValidationIssue(
                        "invalid_parameters",
                        current_path,
                        "Parameters contain a non-finite number.",
                    )
                )
            return
        if isinstance(value, str):
            if len(value) > MAX_PARAMETER_STRING_LENGTH:
                issues.append(
                    WorkflowValidationIssue(
                        "parameter_string_too_large",
                        current_path,
                        "Parameter string is too large.",
                    )
                )
            return
        if not isinstance(value, (dict, list)):
            issues.append(
                WorkflowValidationIssue(
                    "invalid_parameters",
                    current_path,
                    "Parameters contain a non-JSON value.",
                )
            )
            return
        identity = id(value)
        if identity in active_containers:
            issues.append(
                WorkflowValidationIssue(
                    "invalid_parameters",
                    current_path,
                    "Parameters contain a recursive value.",
                )
            )
            return
        if len(value) > MAX_COLLECTION_ITEMS:
            issues.append(
                WorkflowValidationIssue(
                    "parameters_too_complex",
                    current_path,
                    "Parameter collection contains too many items.",
                )
            )
        active_containers.add(identity)
        try:
            if isinstance(value, dict):
                for key, item in value.items():
                    if not isinstance(key, str):
                        issues.append(
                            WorkflowValidationIssue(
                                "invalid_parameters",
                                current_path,
                                "Parameter object keys must be strings.",
                            )
                        )
                        continue
                    key_path = (*current_path, key)
                    if len(key) > MAX_PARAMETER_KEY_LENGTH:
                        issues.append(
                            WorkflowValidationIssue(
                                "parameter_key_too_large",
                                key_path,
                                "Parameter key is too large.",
                            )
                        )
                    visit(item, key_path, depth + 1)
            else:
                for index, item in enumerate(value):
                    visit(item, (*current_path, index), depth + 1)
        finally:
            active_containers.remove(identity)

    visit(parameters, path, 0)
    if issues:
        return tuple(issues), None
    validated = parameters
    try:
        encoded = json.dumps(
            validated,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError):
        return (
            WorkflowValidationIssue(
                "invalid_parameters", path, "Parameters are not valid JSON."
            ),
        ), None
    if len(encoded) > MAX_PARAMETER_BYTES:
        return (
            WorkflowValidationIssue(
                "parameters_too_large", path, "Parameters are too large."
            ),
        ), None
    return (), validated
