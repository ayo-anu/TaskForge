"""Focused tests for deterministic, transport-neutral acyclicity detection."""

from __future__ import annotations

import ast
from collections.abc import Sequence
from dataclasses import FrozenInstanceError, fields
from pathlib import Path
from typing import cast

import pytest

from taskforge.workflows.dag_validation import (
    MAX_DAG_DEPENDENCIES,
    MAX_DAG_STEPS,
    DAGEdge,
    DAGValidationIssue,
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
    result = validate_dag(steps, edges)

    assert result.is_acyclic is True


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

    result = validate_dag(steps, edges)

    assert result.is_acyclic is True
    assert result.topological_order == steps


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

    assert reordered.is_valid == expected.is_valid
    assert reordered.is_acyclic == expected.is_acyclic
    assert reordered.violations == expected.violations
    assert reordered.topological_order == expected.topological_order
    assert reordered.cycle == expected.cycle
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


@pytest.mark.parametrize(
    ("steps", "edges", "expected_order"),
    (
        (("only",), (), ("only",)),
        (
            ("third", "first", "second"),
            (DAGEdge("first", "second"), DAGEdge("second", "third")),
            ("first", "second", "third"),
        ),
        (
            ("right", "root", "left"),
            (DAGEdge("root", "right"), DAGEdge("root", "left")),
            ("root", "left", "right"),
        ),
        (
            ("join", "right", "left"),
            (DAGEdge("right", "join"), DAGEdge("left", "join")),
            ("left", "right", "join"),
        ),
        (
            ("delta", "charlie", "bravo", "alpha"),
            (DAGEdge("alpha", "bravo"), DAGEdge("charlie", "delta")),
            ("alpha", "bravo", "charlie", "delta"),
        ),
    ),
)
def test_valid_graphs_return_deterministic_complete_topological_order(
    steps: tuple[str, ...],
    edges: tuple[DAGEdge, ...],
    expected_order: tuple[str, ...],
) -> None:
    result = validate_dag(steps, edges)

    assert result.is_valid is True
    assert result.issues == ()
    assert result.topological_order == expected_order
    assert result.cycle is None
    positions = {identifier: index for index, identifier in enumerate(expected_order)}
    assert all(
        positions[edge.predecessor] < positions[edge.successor] for edge in edges
    )


def test_field_issues_have_exact_semantic_paths_and_no_message_field() -> None:
    result = validate_dag(
        ("duplicate", "duplicate", "present"),
        (
            DAGEdge("missing", "also_missing"),
            DAGEdge("present", "present"),
            DAGEdge("duplicate", "present"),
            DAGEdge("duplicate", "present"),
        ),
    )

    assert tuple((issue.code, issue.path) for issue in result.issues) == (
        (
            DAGViolationCode.DUPLICATE_STEP_IDENTIFIER,
            ("steps", 1, "identifier"),
        ),
        (
            DAGViolationCode.MISSING_DEPENDENCY_REFERENCE,
            ("dependencies", 0, "predecessor"),
        ),
        (
            DAGViolationCode.MISSING_DEPENDENCY_REFERENCE,
            ("dependencies", 0, "successor"),
        ),
        (DAGViolationCode.SELF_DEPENDENCY, ("dependencies", 1, "successor")),
        (DAGViolationCode.DUPLICATE_DEPENDENCY, ("dependencies", 3)),
    )
    assert {field.name for field in fields(DAGValidationIssue)} == {"code", "path"}
    assert result.topological_order is None


def test_multiple_occurrences_have_deduplicated_violation_codes() -> None:
    result = validate_dag(
        ("present",),
        (
            DAGEdge("missing_one", "present"),
            DAGEdge("missing_two", "present"),
        ),
    )

    assert len(result.issues) == 2
    assert result.violations == (DAGViolationCode.MISSING_DEPENDENCY_REFERENCE,)


def test_cycle_is_closed_and_canonicalized_to_smallest_identifier() -> None:
    steps = ("gamma", "alpha", "beta")
    edges = (
        DAGEdge("gamma", "alpha"),
        DAGEdge("beta", "gamma"),
        DAGEdge("alpha", "beta"),
    )

    result = validate_dag(steps, edges)
    reordered = validate_dag(tuple(reversed(steps)), tuple(reversed(edges)))

    assert result.is_acyclic is False
    assert result.topological_order is None
    assert result.cycle == ("alpha", "beta", "gamma", "alpha")
    assert reordered.cycle == result.cycle
    assert result.issues[-1] == DAGValidationIssue(
        DAGViolationCode.CYCLE,
        ("dependencies",),
    )


def test_cycle_excludes_downstream_nodes() -> None:
    result = validate_dag(
        ("cycle_a", "cycle_b", "downstream"),
        (
            DAGEdge("cycle_a", "cycle_b"),
            DAGEdge("cycle_b", "cycle_a"),
            DAGEdge("cycle_b", "downstream"),
        ),
    )

    assert result.cycle == ("cycle_a", "cycle_b", "cycle_a")
    assert "downstream" not in result.cycle


def test_multiple_cycles_choose_deterministic_lexicographic_traversal_result() -> None:
    result = validate_dag(
        ("delta", "charlie", "bravo", "alpha"),
        (
            DAGEdge("charlie", "delta"),
            DAGEdge("delta", "charlie"),
            DAGEdge("alpha", "bravo"),
            DAGEdge("bravo", "alpha"),
        ),
    )

    assert result.cycle == ("alpha", "bravo", "alpha")


def test_size_issue_paths_suppress_order_and_cycle_details() -> None:
    result = validate_dag(
        tuple(f"step_{index}" for index in range(MAX_DAG_STEPS + 1)),
        _unique_edges(MAX_DAG_DEPENDENCIES + 1),
    )

    assert result.issues == (
        DAGValidationIssue(DAGViolationCode.TOO_MANY_STEPS, ("steps",)),
        DAGValidationIssue(
            DAGViolationCode.TOO_MANY_DEPENDENCIES,
            ("dependencies",),
        ),
    )
    assert result.is_acyclic is False
    assert result.topological_order is None
    assert result.cycle is None


def test_result_and_issue_types_are_immutable() -> None:
    result = validate_dag((), ())

    with pytest.raises(FrozenInstanceError):
        result.is_acyclic = False  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        result.issues[0].path = ()  # type: ignore[misc]
