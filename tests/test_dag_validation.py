"""Focused tests for deterministic, transport-neutral acyclicity detection."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from taskforge.workflows.dag_validation import DAGEdge, validate_dag


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
    steps = tuple(f"step_{index:04d}" for index in range(2_000))
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


def test_task_one_documents_structural_checks_as_task_two_preconditions() -> None:
    module_path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "taskforge"
        / "workflows"
        / "dag_validation.py"
    )
    module = ast.parse(module_path.read_text(encoding="utf-8"))
    documentation = ast.get_docstring(module)

    assert documentation is not None
    assert "unique step identifiers" in documentation  # no duplicate identifiers
    assert "unique, non-self dependencies" in documentation  # no duplicate/self edges
    assert "endpoints are present" in documentation  # no missing endpoints
    assert "next roadmap task" in documentation
