"""Deterministic, bounded structural validation for directed workflow graphs."""

from __future__ import annotations

import heapq
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum

MAX_DAG_STEPS = 256
MAX_DAG_DEPENDENCIES = 2_048


@dataclass(frozen=True)
class DAGEdge:
    predecessor: str
    successor: str


class DAGViolationCode(StrEnum):
    EMPTY_GRAPH = "empty_graph"
    TOO_MANY_STEPS = "too_many_steps"
    TOO_MANY_DEPENDENCIES = "too_many_dependencies"
    DUPLICATE_STEP_IDENTIFIER = "duplicate_step_identifier"
    MISSING_DEPENDENCY_REFERENCE = "missing_dependency_reference"
    SELF_DEPENDENCY = "self_dependency"
    DUPLICATE_DEPENDENCY = "duplicate_dependency"
    CYCLE = "cycle"


@dataclass(frozen=True)
class DAGValidationResult:
    is_acyclic: bool
    violations: tuple[DAGViolationCode, ...] = ()

    @property
    def is_valid(self) -> bool:
        return self.is_acyclic and not self.violations


def validate_dag(
    step_identifiers: Sequence[str],
    dependencies: Sequence[DAGEdge],
) -> DAGValidationResult:
    """Detect bounded structural and cycle violations without mutating input.

    Lexicographic identifier ordering is used only to make traversal choices
    deterministic. It carries no chronological, priority, or execution meaning.
    """
    step_count = len(step_identifiers)
    dependency_count = len(dependencies)
    violations: list[DAGViolationCode] = []
    if step_count == 0:
        violations.append(DAGViolationCode.EMPTY_GRAPH)
    if step_count > MAX_DAG_STEPS:
        violations.append(DAGViolationCode.TOO_MANY_STEPS)
    if dependency_count > MAX_DAG_DEPENDENCIES:
        violations.append(DAGViolationCode.TOO_MANY_DEPENDENCIES)
    if any(
        violation
        in {
            DAGViolationCode.TOO_MANY_STEPS,
            DAGViolationCode.TOO_MANY_DEPENDENCIES,
        }
        for violation in violations
    ):
        return DAGValidationResult(is_acyclic=False, violations=tuple(violations))

    identifiers = tuple(step_identifiers)
    unique_identifiers = set(identifiers)
    if len(unique_identifiers) != len(identifiers):
        violations.append(DAGViolationCode.DUPLICATE_STEP_IDENTIFIER)

    missing_reference = False
    self_dependency = False
    duplicate_dependency = False
    seen_edges: set[tuple[str, str]] = set()
    admissible_edges: list[DAGEdge] = []
    for dependency in dependencies:
        if (
            dependency.predecessor not in unique_identifiers
            or dependency.successor not in unique_identifiers
        ):
            missing_reference = True
            continue
        if dependency.predecessor == dependency.successor:
            self_dependency = True
            continue
        edge = (dependency.predecessor, dependency.successor)
        if edge in seen_edges:
            duplicate_dependency = True
            continue
        seen_edges.add(edge)
        admissible_edges.append(dependency)

    if missing_reference:
        violations.append(DAGViolationCode.MISSING_DEPENDENCY_REFERENCE)
    if self_dependency:
        violations.append(DAGViolationCode.SELF_DEPENDENCY)
    if duplicate_dependency:
        violations.append(DAGViolationCode.DUPLICATE_DEPENDENCY)

    is_acyclic = _is_acyclic(tuple(unique_identifiers), tuple(admissible_edges))
    if not is_acyclic:
        violations.append(DAGViolationCode.CYCLE)
    return DAGValidationResult(is_acyclic=is_acyclic, violations=tuple(violations))


def _is_acyclic(
    step_identifiers: tuple[str, ...],
    dependencies: tuple[DAGEdge, ...],
) -> bool:
    """Run deterministic iterative Kahn traversal on admissible input."""
    indegree = dict.fromkeys(step_identifiers, 0)
    outgoing: dict[str, list[str]] = {identifier: [] for identifier in step_identifiers}
    for dependency in dependencies:
        outgoing[dependency.predecessor].append(dependency.successor)
        indegree[dependency.successor] += 1

    for successors in outgoing.values():
        successors.sort()
    available = [identifier for identifier, degree in indegree.items() if degree == 0]
    heapq.heapify(available)

    processed = 0
    while available:
        identifier = heapq.heappop(available)
        processed += 1
        for successor in outgoing[identifier]:
            indegree[successor] -= 1
            if indegree[successor] == 0:
                heapq.heappush(available, successor)

    return processed == len(indegree)
