"""Workflow run target selection domain tests."""

from uuid import uuid4

import pytest

from taskforge.runs.domain import (
    ExplicitWorkflowVersion,
    InvalidWorkflowVersionSelection,
    LatestWorkflowVersion,
    ResolvedWorkflowVersion,
    WorkflowRunTargetUnavailable,
    require_run_available,
)
from taskforge.workflows.domain import WorkflowDefinitionStatus


@pytest.mark.parametrize("value", (True, False, 0, -1, "1", None))
def test_explicit_version_requires_a_positive_non_boolean_integer(
    value: object,
) -> None:
    with pytest.raises(InvalidWorkflowVersionSelection):
        ExplicitWorkflowVersion(value)  # type: ignore[arg-type]


def test_version_selectors_and_resolved_identity_are_immutable() -> None:
    workflow_id, version_id = uuid4(), uuid4()

    assert ExplicitWorkflowVersion(2).version_number == 2
    assert LatestWorkflowVersion() == LatestWorkflowVersion()
    assert ResolvedWorkflowVersion(workflow_id, version_id, 2) == (
        ResolvedWorkflowVersion(workflow_id, version_id, 2)
    )


@pytest.mark.parametrize(
    "status",
    (
        WorkflowDefinitionStatus.DRAFT,
        WorkflowDefinitionStatus.DISABLED,
        WorkflowDefinitionStatus.ARCHIVED,
    ),
)
def test_only_enabled_definitions_are_available_for_new_runs(
    status: WorkflowDefinitionStatus,
) -> None:
    with pytest.raises(WorkflowRunTargetUnavailable) as caught:
        require_run_available(status)

    assert caught.value.status is status
    assert status.value not in str(caught.value)


def test_enabled_definition_is_available_for_resolution() -> None:
    require_run_available(WorkflowDefinitionStatus.ENABLED)
