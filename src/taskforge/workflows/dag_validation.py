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


type DAGValidationPath = tuple[str | int, ...]


@dataclass(frozen=True)
class DAGValidationIssue:
    code: DAGViolationCode
    path: DAGValidationPath


@dataclass(frozen=True)
class DAGValidationResult:
    is_acyclic: bool
    issues: tuple[DAGValidationIssue, ...] = ()
    topological_order: tuple[str, ...] | None = None
    cycle: tuple[str, ...] | None = None

    @property
    def violations(self) -> tuple[DAGViolationCode, ...]:
        return tuple(dict.fromkeys(issue.code for issue in self.issues))

    @property
    def is_valid(self) -> bool:
        return self.is_acyclic and not self.issues


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
    issues: list[DAGValidationIssue] = []
    if step_count == 0:
        issues.append(DAGValidationIssue(DAGViolationCode.EMPTY_GRAPH, ("steps",)))
    if step_count > MAX_DAG_STEPS:
        issues.append(DAGValidationIssue(DAGViolationCode.TOO_MANY_STEPS, ("steps",)))
    if dependency_count > MAX_DAG_DEPENDENCIES:
        issues.append(
            DAGValidationIssue(
                DAGViolationCode.TOO_MANY_DEPENDENCIES,
                ("dependencies",),
            )
        )
    if any(
        issue.code
        in {
            DAGViolationCode.TOO_MANY_STEPS,
            DAGViolationCode.TOO_MANY_DEPENDENCIES,
        }
        for issue in issues
    ):
        return DAGValidationResult(is_acyclic=False, issues=tuple(issues))

    identifiers = tuple(step_identifiers)
    unique_identifiers = set(identifiers)
    seen_identifiers: set[str] = set()
    duplicate_step_issues: list[DAGValidationIssue] = []
    for index, identifier in enumerate(identifiers):
        if identifier in seen_identifiers:
            duplicate_step_issues.append(
                DAGValidationIssue(
                    DAGViolationCode.DUPLICATE_STEP_IDENTIFIER,
                    ("steps", index, "identifier"),
                )
            )
        else:
            seen_identifiers.add(identifier)

    missing_reference_issues: list[DAGValidationIssue] = []
    self_dependency_issues: list[DAGValidationIssue] = []
    duplicate_dependency_issues: list[DAGValidationIssue] = []
    seen_edges: set[tuple[str, str]] = set()
    admissible_edges: list[DAGEdge] = []
    for index, dependency in enumerate(dependencies):
        predecessor_missing = dependency.predecessor not in unique_identifiers
        successor_missing = dependency.successor not in unique_identifiers
        if predecessor_missing:
            missing_reference_issues.append(
                DAGValidationIssue(
                    DAGViolationCode.MISSING_DEPENDENCY_REFERENCE,
                    ("dependencies", index, "predecessor"),
                )
            )
        if successor_missing:
            missing_reference_issues.append(
                DAGValidationIssue(
                    DAGViolationCode.MISSING_DEPENDENCY_REFERENCE,
                    ("dependencies", index, "successor"),
                )
            )
        if predecessor_missing or successor_missing:
            continue
        if dependency.predecessor == dependency.successor:
            self_dependency_issues.append(
                DAGValidationIssue(
                    DAGViolationCode.SELF_DEPENDENCY,
                    ("dependencies", index, "successor"),
                )
            )
            continue
        edge = (dependency.predecessor, dependency.successor)
        if edge in seen_edges:
            duplicate_dependency_issues.append(
                DAGValidationIssue(
                    DAGViolationCode.DUPLICATE_DEPENDENCY,
                    ("dependencies", index),
                )
            )
            continue
        seen_edges.add(edge)
        admissible_edges.append(dependency)

    issues.extend(duplicate_step_issues)
    issues.extend(missing_reference_issues)
    issues.extend(self_dependency_issues)
    issues.extend(duplicate_dependency_issues)

    normalized_identifiers = tuple(sorted(unique_identifiers))
    normalized_edges = tuple(admissible_edges)
    order = _topological_order(normalized_identifiers, normalized_edges)
    is_acyclic = order is not None
    cycle = None
    if not is_acyclic:
        issues.append(DAGValidationIssue(DAGViolationCode.CYCLE, ("dependencies",)))
        cycle = _find_cycle(normalized_identifiers, normalized_edges)
    return DAGValidationResult(
        is_acyclic=is_acyclic,
        issues=tuple(issues),
        topological_order=order if not issues else None,
        cycle=cycle,
    )


def _topological_order(
    step_identifiers: tuple[str, ...],
    dependencies: tuple[DAGEdge, ...],
) -> tuple[str, ...] | None:
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

    processed: list[str] = []
    while available:
        identifier = heapq.heappop(available)
        processed.append(identifier)
        for successor in outgoing[identifier]:
            indegree[successor] -= 1
            if indegree[successor] == 0:
                heapq.heappush(available, successor)

    if len(processed) != len(indegree):
        return None
    return tuple(processed)


def _find_cycle(
    step_identifiers: tuple[str, ...],
    dependencies: tuple[DAGEdge, ...],
) -> tuple[str, ...]:
    """Return one deterministic closed cycle using iterative depth-first search."""
    outgoing: dict[str, list[str]] = {identifier: [] for identifier in step_identifiers}
    for dependency in dependencies:
        outgoing[dependency.predecessor].append(dependency.successor)
    for successors in outgoing.values():
        successors.sort()

    state = dict.fromkeys(step_identifiers, 0)
    for root in step_identifiers:
        if state[root] != 0:
            continue
        path = [root]
        active_positions = {root: 0}
        state[root] = 1
        stack: list[tuple[str, int]] = [(root, 0)]
        while stack:
            node, successor_index = stack[-1]
            if successor_index == len(outgoing[node]):
                stack.pop()
                state[node] = 2
                active_positions.pop(node)
                path.pop()
                continue
            successor = outgoing[node][successor_index]
            stack[-1] = (node, successor_index + 1)
            if state[successor] == 0:
                active_positions[successor] = len(path)
                path.append(successor)
                state[successor] = 1
                stack.append((successor, 0))
            elif state[successor] == 1:
                cycle = (*path[active_positions[successor] :], successor)
                return _canonical_cycle(cycle)
    raise RuntimeError("cyclic graph did not contain a discoverable cycle")


def _canonical_cycle(cycle: tuple[str, ...]) -> tuple[str, ...]:
    members = cycle[:-1]
    start = min(range(len(members)), key=members.__getitem__)
    canonical = (*members[start:], *members[:start])
    return (*canonical, canonical[0])
