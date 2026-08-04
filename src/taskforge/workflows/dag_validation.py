"""Deterministic acyclicity detection for structurally admissible graphs.

Callers must provide unique step identifiers and unique, non-self dependencies
whose endpoints are present in ``step_identifiers``. Comprehensive validation
of those structural preconditions belongs to the next roadmap task.
"""

from __future__ import annotations

import heapq
from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class DAGEdge:
    predecessor: str
    successor: str


@dataclass(frozen=True)
class DAGValidationResult:
    is_acyclic: bool


def validate_dag(
    step_identifiers: Sequence[str],
    dependencies: Sequence[DAGEdge],
) -> DAGValidationResult:
    """Determine acyclicity without mutating structurally admissible input.

    Lexicographic identifier ordering is used only to make traversal choices
    deterministic. It carries no chronological, priority, or execution meaning.
    """
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

    return DAGValidationResult(is_acyclic=processed == len(indegree))
