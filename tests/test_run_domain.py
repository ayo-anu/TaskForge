"""Workflow run target selection domain tests."""

from uuid import uuid4

import pytest

from taskforge.runs.domain import (
    ExplicitWorkflowVersion,
    InvalidWorkflowRunInput,
    InvalidWorkflowVersionSelection,
    LatestWorkflowVersion,
    ResolvedWorkflowVersion,
    TaskRunStatus,
    WorkflowRunTargetUnavailable,
    WorkflowRunVersionDependency,
    WorkflowRunVersionSnapshot,
    WorkflowVersionSnapshotInvalid,
    create_workflow_run_input,
    materialize_initial_tasks,
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


def test_run_input_reuses_bounded_validation_and_defensively_copies() -> None:
    payload = {"nested": [1, True, None]}
    references = {"artifact": {"kind": "object"}}

    accepted = create_workflow_run_input(payload, references)
    payload["nested"].append(2)
    references["artifact"]["kind"] = "changed"

    assert accepted.payload == {"nested": [1, True, None]}
    assert accepted.input_references == {"artifact": {"kind": "object"}}
    assert "nested" not in repr(accepted)
    assert "artifact" not in repr(accepted)


@pytest.mark.parametrize(
    ("payload", "references"),
    (([], {}), ({}, []), ({"value": float("inf")}, {})),
)
def test_invalid_run_input_retains_safe_field_paths(
    payload: object, references: object
) -> None:
    with pytest.raises(InvalidWorkflowRunInput) as caught:
        create_workflow_run_input(payload, references)

    assert caught.value.issues
    assert caught.value.issues[0].path[0] in {"payload", "input_references"}


def snapshot(
    steps: tuple[str, ...],
    dependencies: tuple[tuple[str, str], ...],
) -> WorkflowRunVersionSnapshot:
    return WorkflowRunVersionSnapshot(
        workflow_definition_id=uuid4(),
        workflow_version_id=uuid4(),
        version_number=1,
        step_identifiers=steps,
        dependencies=tuple(
            WorkflowRunVersionDependency(predecessor, successor)
            for predecessor, successor in dependencies
        ),
    )


def test_materialization_is_deterministic_with_runnable_roots_and_blocked_dependents() -> (
    None
):
    tasks = materialize_initial_tasks(
        snapshot(
            ("join", "right", "root", "left", "independent"),
            (
                ("root", "right"),
                ("root", "left"),
                ("right", "join"),
                ("left", "join"),
            ),
        )
    )

    assert tuple(task.step_identifier for task in tasks) == (
        "independent",
        "join",
        "left",
        "right",
        "root",
    )
    assert {
        task.step_identifier for task in tasks if task.status is TaskRunStatus.RUNNABLE
    } == {
        "independent",
        "root",
    }
    assert {
        task.step_identifier for task in tasks if task.status is TaskRunStatus.BLOCKED
    } == {
        "join",
        "left",
        "right",
    }


@pytest.mark.parametrize(
    "invalid_snapshot",
    (
        snapshot((), ()),
        snapshot(("one", "one"), ()),
        snapshot(("one",), (("one", "one"),)),
        snapshot(("one", "two"), (("one", "missing"),)),
        snapshot(("one", "two"), (("one", "two"), ("one", "two"))),
    ),
)
def test_invalid_version_snapshots_fail_closed(
    invalid_snapshot: WorkflowRunVersionSnapshot,
) -> None:
    with pytest.raises(WorkflowVersionSnapshotInvalid):
        materialize_initial_tasks(invalid_snapshot)
