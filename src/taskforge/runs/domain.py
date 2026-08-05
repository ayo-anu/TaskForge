"""Transport- and persistence-neutral workflow run target selection."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from taskforge.workflows.domain import WorkflowDefinitionStatus


class InvalidWorkflowVersionSelection(ValueError):
    """A workflow version selector is malformed."""


class WorkflowRunTargetUnavailable(Exception):
    """A workflow definition does not currently permit new runs."""

    def __init__(self, status: WorkflowDefinitionStatus) -> None:
        self.status = status
        super().__init__("workflow definition is unavailable for new runs")


@dataclass(frozen=True)
class ExplicitWorkflowVersion:
    version_number: int

    def __post_init__(self) -> None:
        if (
            isinstance(self.version_number, bool)
            or not isinstance(self.version_number, int)
            or self.version_number <= 0
        ):
            raise InvalidWorkflowVersionSelection(
                "workflow version number must be a positive integer"
            )


@dataclass(frozen=True)
class LatestWorkflowVersion:
    """Select the greatest committed version number visible to the lookup."""


WorkflowVersionSelection = ExplicitWorkflowVersion | LatestWorkflowVersion


@dataclass(frozen=True)
class ResolvedWorkflowVersion:
    workflow_definition_id: UUID
    workflow_version_id: UUID
    version_number: int

    def __post_init__(self) -> None:
        if (
            isinstance(self.version_number, bool)
            or not isinstance(self.version_number, int)
            or self.version_number <= 0
        ):
            raise ValueError("resolved workflow version number must be positive")


def require_run_available(status: WorkflowDefinitionStatus) -> None:
    """Reject every definition state except enabled."""
    if status is not WorkflowDefinitionStatus.ENABLED:
        raise WorkflowRunTargetUnavailable(status)
