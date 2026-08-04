"""Focused tests for deterministic, transport-neutral acyclicity detection."""

from __future__ import annotations

import ast
from collections.abc import Sequence
from pathlib import Path
from typing import cast

import pytest

from taskforge.workflows.dag_validation import (
    MAX_DAG_DEPENDENCIES,
    MAX_DAG_STEPS,
    DAGEdge,
    DAGViolationCode,
    validate_dag,
)


@pytest.mark.parametrize(
    ("steps", "edges"),
    (
        ((), ()),
        (("only",), ()),
        (
            ("first", "second", "third"),
            (DAGEdge("first", "second"), DAGEdge("second", "third")),
        ),
        (
            ("root", "left", "right"),
            (DAGEdge("root", "left"), DAGEdge("root", "right")),
        ),
        (
            ("left", "right", "join"),
            (DAGEdge("left", "join"), DAGEdge("right", "join")),
        ),
        (("a", "b", "c", "d"), (DAGEdge("a", "b"), DAGEdge("c", "d"))),
    ),
)
def test_structurally_admissible_acyclic_graphs_are_accepted(
    steps: tuple[str, ...],
    edges: tuple[DAGEdge, ...],
) -> None:
    assert validate_dag(steps, edges).is_acyclic is True


def test_multi_node_cycle_is_detected() -> None:
    result = validate_dag(
        ("first", "second", "third"),
        (
            DAGEdge("first", "second"),
            DAGEdge("second", "third"),
            DAGEdge("third", "first"),
        ),
    )

    assert result.is_acyclic is False


def test_cycle_in_one_disconnected_component_invalidates_the_graph() -> None:
    result = validate_dag(
        ("root", "leaf", "cycle_a", "cycle_b"),
        (
            DAGEdge("root", "leaf"),
            DAGEdge("cycle_a", "cycle_b"),
            DAGEdge("cycle_b", "cycle_a"),
        ),
    )

    assert result.is_acyclic is False


def test_result_is_independent_of_input_iteration_order() -> None:
    steps = ("root", "middle_a", "middle_b", "join")
    edges = (
        DAGEdge("root", "middle_a"),
        DAGEdge("root", "middle_b"),
        DAGEdge("middle_a", "join"),
        DAGEdge("middle_b", "join"),
    )

    expected = validate_dag(steps, edges)

    assert validate_dag(tuple(reversed(steps)), tuple(reversed(edges))) == expected
    assert validate_dag(steps, edges) == expected


def test_validation_does_not_mutate_caller_owned_sequences() -> None:
    steps = ["root", "leaf"]
    edges = [DAGEdge("root", "leaf")]
    original_steps, original_edges = steps.copy(), edges.copy()

    validate_dag(steps, edges)

    assert steps == original_steps
    assert edges == original_edges


def test_long_graph_uses_iterative_traversal() -> None:
    steps = tuple(f"step_{index:04d}" for index in range(MAX_DAG_STEPS))
    edges = tuple(
        DAGEdge(steps[index], steps[index + 1]) for index in range(len(steps) - 1)
    )

    assert validate_dag(steps, edges).is_acyclic is True


def test_module_has_no_transport_or_persistence_imports() -> None:
    module_path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "taskforge"
        / "workflows"
        / "dag_validation.py"
    )
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    imported_modules = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
    }

    assert not any(
        module.startswith(
            (
                "fastapi",
                "pydantic",
                "sqlalchemy",
                "taskforge.api",
                "taskforge.persistence",
                "taskforge.settings",
            )
        )
        for module in imported_modules
    )


def test_empty_graph_is_mathematically_acyclic_but_invalid() -> None:
    result = validate_dag((), ())

    assert result.is_acyclic is True
    assert result.is_valid is False
    assert result.violations == (DAGViolationCode.EMPTY_GRAPH,)


def test_step_limit_is_inclusive_and_oversize_skips_traversal() -> None:
    at_limit = tuple(f"step_{index}" for index in range(MAX_DAG_STEPS))
    over_limit = (*at_limit, "one_too_many")

    assert DAGViolationCode.TOO_MANY_STEPS not in validate_dag(at_limit, ()).violations
    result = validate_dag(over_limit, ())
    assert result.is_acyclic is False
    assert result.is_valid is False
    assert result.violations == (DAGViolationCode.TOO_MANY_STEPS,)


def test_oversized_graph_is_not_iterated_after_length_check() -> None:
    class LenOnlyOversizedInput:
        def __len__(self) -> int:
            return MAX_DAG_STEPS + 1

        def __iter__(self) -> None:
            raise AssertionError("oversized input must not be iterated")

    result = validate_dag(cast(Sequence[str], LenOnlyOversizedInput()), ())

    assert result.is_acyclic is False
    assert result.violations == (DAGViolationCode.TOO_MANY_STEPS,)


def _unique_edges(count: int) -> tuple[DAGEdge, ...]:
    identifiers = tuple(f"step_{index}" for index in range(MAX_DAG_STEPS))
    edges: list[DAGEdge] = []
    for predecessor in identifiers:
        for successor in identifiers:
            if predecessor != successor:
                edges.append(DAGEdge(predecessor, successor))
                if len(edges) == count:
                    return tuple(edges)
    raise AssertionError("test requested too many unique edges")


def test_dependency_limit_is_inclusive_and_oversize_skips_traversal() -> None:
    identifiers = tuple(f"step_{index}" for index in range(MAX_DAG_STEPS))
    at_limit = _unique_edges(MAX_DAG_DEPENDENCIES)
    over_limit = _unique_edges(MAX_DAG_DEPENDENCIES + 1)

    assert (
        DAGViolationCode.TOO_MANY_DEPENDENCIES
        not in validate_dag(identifiers, at_limit).violations
    )
    result = validate_dag(identifiers, over_limit)
    assert result.is_acyclic is False
    assert result.violations == (DAGViolationCode.TOO_MANY_DEPENDENCIES,)


def test_both_size_violations_are_reported_in_policy_order() -> None:
    result = validate_dag(
        tuple(f"step_{index}" for index in range(MAX_DAG_STEPS + 1)),
        _unique_edges(MAX_DAG_DEPENDENCIES + 1),
    )

    assert result.violations == (
        DAGViolationCode.TOO_MANY_STEPS,
        DAGViolationCode.TOO_MANY_DEPENDENCIES,
    )


@pytest.mark.parametrize(
    ("steps", "edges", "code"),
    (
        (
            ("duplicate", "duplicate"),
            (),
            DAGViolationCode.DUPLICATE_STEP_IDENTIFIER,
        ),
        (
            ("present",),
            (DAGEdge("missing", "present"),),
            DAGViolationCode.MISSING_DEPENDENCY_REFERENCE,
        ),
        (
            ("present",),
            (DAGEdge("present", "missing"),),
            DAGViolationCode.MISSING_DEPENDENCY_REFERENCE,
        ),
        (
            ("step",),
            (DAGEdge("step", "step"),),
            DAGViolationCode.SELF_DEPENDENCY,
        ),
        (
            ("first", "second"),
            (DAGEdge("first", "second"), DAGEdge("first", "second")),
            DAGViolationCode.DUPLICATE_DEPENDENCY,
        ),
    ),
)
def test_structural_violation_categories_are_detected(
    steps: tuple[str, ...],
    edges: tuple[DAGEdge, ...],
    code: DAGViolationCode,
) -> None:
    result = validate_dag(steps, edges)

    assert code in result.violations
    assert result.is_valid is False


def test_reversed_edges_are_distinct_and_form_a_cycle() -> None:
    result = validate_dag(
        ("first", "second"),
        (DAGEdge("first", "second"), DAGEdge("second", "first")),
    )

    assert result.violations == (DAGViolationCode.CYCLE,)
    assert result.is_acyclic is False


def test_self_edge_has_specific_violation_without_redundant_cycle() -> None:
    result = validate_dag(("step",), (DAGEdge("step", "step"),))

    assert result.violations == (DAGViolationCode.SELF_DEPENDENCY,)
    assert result.is_acyclic is True


def test_unrelated_cycle_is_detected_alongside_invalid_edges() -> None:
    result = validate_dag(
        ("cycle_a", "cycle_b", "other"),
        (
            DAGEdge("cycle_a", "cycle_b"),
            DAGEdge("cycle_b", "cycle_a"),
            DAGEdge("missing", "other"),
            DAGEdge("other", "other"),
        ),
    )

    assert result.violations == (
        DAGViolationCode.MISSING_DEPENDENCY_REFERENCE,
        DAGViolationCode.SELF_DEPENDENCY,
        DAGViolationCode.CYCLE,
    )


def test_violation_order_and_result_are_independent_of_input_order() -> None:
    steps = ("duplicate", "duplicate", "cycle_a", "cycle_b", "other")
    edges = (
        DAGEdge("cycle_a", "cycle_b"),
        DAGEdge("cycle_b", "cycle_a"),
        DAGEdge("missing", "other"),
        DAGEdge("other", "other"),
        DAGEdge("cycle_a", "cycle_b"),
    )

    expected = validate_dag(steps, edges)
    reordered = validate_dag(tuple(reversed(steps)), tuple(reversed(edges)))

    assert reordered == expected
    assert expected.violations == (
        DAGViolationCode.DUPLICATE_STEP_IDENTIFIER,
        DAGViolationCode.MISSING_DEPENDENCY_REFERENCE,
        DAGViolationCode.SELF_DEPENDENCY,
        DAGViolationCode.DUPLICATE_DEPENDENCY,
        DAGViolationCode.CYCLE,
    )


def test_violation_result_does_not_expose_identifiers() -> None:
    sensitive_identifier = "customer-secret-step"
    result = validate_dag(
        ("present",),
        (DAGEdge(sensitive_identifier, "present"),),
    )

    assert sensitive_identifier not in repr(result)
