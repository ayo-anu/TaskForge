"""Workflow run target selection domain tests."""

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from taskforge.runs.domain import (
    MAX_WORKFLOW_RECONCILIATION_ITERATIONS,
    CreatedWorkflowRun,
    DependencyFailurePropagationResult,
    ExplicitWorkflowVersion,
    InspectedTaskRun,
    InspectedWorkflowRun,
    InvalidWorkflowRunIdempotencyKey,
    InvalidWorkflowRunInput,
    InvalidWorkflowVersionSelection,
    LatestWorkflowVersion,
    ResolvedWorkflowVersion,
    RunnableTransitionResult,
    TaskRunStatus,
    WorkflowRunEvaluationResult,
    WorkflowRunIdempotency,
    WorkflowRunReconciliationResult,
    WorkflowRunStatus,
    WorkflowRunTargetUnavailable,
    WorkflowRunVersionDependency,
    WorkflowRunVersionSnapshot,
    WorkflowVersionSnapshotInvalid,
    create_workflow_run_idempotency,
    create_workflow_run_input,
    materialize_initial_tasks,
    require_run_available,
)
from taskforge.runs.schema import TASK_RUN_STATUSES, WORKFLOW_RUN_STATUSES
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


def test_resolved_version_and_created_run_reject_invalid_metadata() -> None:
    with pytest.raises(ValueError):
        ResolvedWorkflowVersion(uuid4(), uuid4(), 0)

    with pytest.raises(ValueError):
        CreatedWorkflowRun(
            id=uuid4(),
            workflow_definition_id=uuid4(),
            workflow_version_id=uuid4(),
            version_number=1,
            requested_by_principal_id=uuid4(),
            status=WorkflowRunStatus.PENDING,
            created_at=datetime.now(),
            task_count=0,
            runnable_task_count=0,
            blocked_task_count=0,
        )


def test_inspection_records_require_aware_timestamps() -> None:
    now = datetime.now(UTC)
    with pytest.raises(ValueError):
        InspectedWorkflowRun(
            uuid4(),
            uuid4(),
            uuid4(),
            1,
            uuid4(),
            WorkflowRunStatus.PENDING,
            datetime.now(),
            now,
        )
    with pytest.raises(ValueError):
        InspectedTaskRun(
            uuid4(),
            uuid4(),
            uuid4(),
            "root",
            TaskRunStatus.RUNNABLE,
            now,
            datetime.now(),
        )


def test_runnable_transition_result_is_immutable_and_counts_transitions() -> None:
    run_id, first_id, second_id = uuid4(), uuid4(), uuid4()
    result = RunnableTransitionResult(
        run_id, (first_id, second_id), ("first", "second")
    )

    assert result.transitioned_count == 2
    assert result.made_progress
    with pytest.raises(AttributeError):
        result.workflow_run_id = uuid4()  # type: ignore[misc]


def test_empty_runnable_transition_result_is_a_successful_no_op() -> None:
    result = RunnableTransitionResult(uuid4(), (), ())
    assert result.transitioned_count == 0
    assert not result.made_progress


def test_runnable_transition_result_rejects_unpaired_or_duplicate_identities() -> None:
    task_id = uuid4()
    with pytest.raises(ValueError):
        RunnableTransitionResult(uuid4(), (task_id,), ())
    with pytest.raises(ValueError):
        RunnableTransitionResult(uuid4(), (task_id, task_id), ("one", "two"))
    with pytest.raises(ValueError):
        RunnableTransitionResult(uuid4(), (uuid4(), uuid4()), ("one", "one"))


def test_dependency_failure_result_is_immutable_and_counts_skipped_tasks() -> None:
    run_id, first_id, second_id = uuid4(), uuid4(), uuid4()
    result = DependencyFailurePropagationResult(
        run_id, (first_id, second_id), ("first", "second")
    )

    assert result.skipped_count == 2
    assert result.made_progress
    with pytest.raises(AttributeError):
        result.workflow_run_id = uuid4()  # type: ignore[misc]


def test_empty_dependency_failure_result_is_a_successful_no_op() -> None:
    result = DependencyFailurePropagationResult(uuid4(), (), ())
    assert result.skipped_count == 0
    assert not result.made_progress


def test_dependency_failure_result_rejects_unpaired_or_duplicate_identities() -> None:
    task_id = uuid4()
    with pytest.raises(ValueError):
        DependencyFailurePropagationResult(uuid4(), (task_id,), ())
    with pytest.raises(ValueError):
        DependencyFailurePropagationResult(uuid4(), (task_id, task_id), ("one", "two"))
    with pytest.raises(ValueError):
        DependencyFailurePropagationResult(uuid4(), (uuid4(), uuid4()), ("one", "one"))


def test_workflow_run_evaluation_result_distinguishes_transition_and_no_op() -> None:
    run_id = uuid4()
    transitioned = WorkflowRunEvaluationResult(
        run_id,
        True,
        WorkflowRunStatus.PENDING,
        WorkflowRunStatus.RUNNING,
    )
    no_op = WorkflowRunEvaluationResult(
        run_id,
        True,
        WorkflowRunStatus.RUNNING,
        WorkflowRunStatus.RUNNING,
    )
    missing = WorkflowRunEvaluationResult(run_id, False, None, None)

    assert transitioned.transitioned
    assert transitioned.made_progress
    assert not no_op.transitioned
    assert not no_op.made_progress
    assert not missing.transitioned
    assert not missing.made_progress
    with pytest.raises(AttributeError):
        transitioned.found = False  # type: ignore[misc]


@pytest.mark.parametrize(
    ("found", "previous", "resulting"),
    (
        (False, WorkflowRunStatus.PENDING, WorkflowRunStatus.PENDING),
        (True, None, None),
        (True, WorkflowRunStatus.PENDING, None),
        (False, None, WorkflowRunStatus.RUNNING),
    ),
)
def test_workflow_run_evaluation_result_enforces_presence_invariants(
    found: bool,
    previous: WorkflowRunStatus | None,
    resulting: WorkflowRunStatus | None,
) -> None:
    with pytest.raises(ValueError):
        WorkflowRunEvaluationResult(uuid4(), found, previous, resulting)


def reconciliation_result(
    **overrides: object,
) -> WorkflowRunReconciliationResult:
    values: dict[str, object] = {
        "workflow_run_id": uuid4(),
        "found": True,
        "iterations": 1,
        "runnable_transition_count": 0,
        "skipped_transition_count": 0,
        "workflow_transition_count": 0,
        "final_status": WorkflowRunStatus.RUNNING,
        "quiescent": True,
        "bound_reached": False,
    }
    values.update(overrides)
    return WorkflowRunReconciliationResult(**values)  # type: ignore[arg-type]


def test_reconciliation_result_supports_quiescence_bound_and_missing_outcomes() -> None:
    quiescent = reconciliation_result()
    bounded = reconciliation_result(
        iterations=MAX_WORKFLOW_RECONCILIATION_ITERATIONS,
        quiescent=False,
        bound_reached=True,
    )
    missing = reconciliation_result(
        found=False,
        final_status=None,
        quiescent=False,
    )

    assert quiescent.quiescent
    assert bounded.bound_reached
    assert not missing.found
    with pytest.raises(AttributeError):
        quiescent.quiescent = False  # type: ignore[misc]


@pytest.mark.parametrize(
    "overrides",
    (
        {"iterations": 0},
        {"iterations": MAX_WORKFLOW_RECONCILIATION_ITERATIONS + 1},
        {"runnable_transition_count": -1},
        {"skipped_transition_count": -1},
        {"workflow_transition_count": 2},
        {"found": False},
        {"final_status": None},
        {"quiescent": True, "bound_reached": True},
        {"bound_reached": True, "quiescent": False},
        {"found": False, "final_status": None, "quiescent": True},
        {"final_status": WorkflowRunStatus.FAILED, "quiescent": False},
    ),
)
def test_reconciliation_result_rejects_inconsistent_outcomes(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        reconciliation_result(**overrides)


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


def test_domain_run_statuses_cover_the_persisted_task_one_vocabulary() -> None:
    assert {status.value for status in WorkflowRunStatus} == set(WORKFLOW_RUN_STATUSES)
    assert {status.value for status in TaskRunStatus} == set(TASK_RUN_STATUSES)


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


def test_snapshot_rejects_non_positive_version_number() -> None:
    invalid = snapshot(("root",), ())
    object.__setattr__(invalid, "version_number", 0)

    with pytest.raises(WorkflowVersionSnapshotInvalid):
        materialize_initial_tasks(invalid)


def idempotency_for(
    key: object = "abcdefghijklmnop",
    *,
    selection: object | None = None,
    payload: object | None = None,
    workflow_id: object | None = None,
    principal_id: object | None = None,
) -> WorkflowRunIdempotency:
    return create_workflow_run_idempotency(
        key,
        workflow_definition_id=(
            workflow_id if isinstance(workflow_id, UUID) else UUID(int=1)
        ),
        requested_by_principal_id=(
            principal_id if isinstance(principal_id, UUID) else UUID(int=2)
        ),
        selection=(
            selection
            if isinstance(selection, (ExplicitWorkflowVersion, LatestWorkflowVersion))
            else LatestWorkflowVersion()
        ),
        input_snapshot=create_workflow_run_input(
            payload if payload is not None else {}, {}
        ),
    )


@pytest.mark.parametrize(
    "key",
    (
        "!" * 16,
        "~" * 128,
        "punctuation!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~",
    ),
)
def test_idempotency_keys_accept_bounded_non_whitespace_printable_ascii(
    key: str,
) -> None:
    value = idempotency_for(key)

    assert value.key_digest.startswith("sha256:v1:")
    assert key not in repr(value)


@pytest.mark.parametrize(
    "key",
    (
        None,
        "short",
        "a" * 129,
        "contains space---",
        "contains\ttab----",
        "contains\nline---",
        "non-ascii-é------",
        "control-\x7f------",
    ),
)
def test_invalid_idempotency_keys_are_rejected_safely(key: object) -> None:
    with pytest.raises(InvalidWorkflowRunIdempotencyKey) as caught:
        idempotency_for(key)

    assert str(key) not in str(caught.value)


def test_request_fingerprint_is_canonical_and_covers_request_semantics() -> None:
    first = idempotency_for(payload={"b": 2, "a": 1})
    reordered = idempotency_for(payload={"a": 1, "b": 2})
    explicit = idempotency_for(selection=ExplicitWorkflowVersion(1))
    different_payload = idempotency_for(payload={"a": 2, "b": 2})
    different_workflow = idempotency_for(workflow_id=UUID(int=3))
    different_principal = idempotency_for(principal_id=UUID(int=4))

    assert first.request_fingerprint == reordered.request_fingerprint
    assert (
        len(
            {
                first.request_fingerprint,
                explicit.request_fingerprint,
                different_payload.request_fingerprint,
                different_workflow.request_fingerprint,
                different_principal.request_fingerprint,
            }
        )
        == 5
    )
    assert first.key_digest == explicit.key_digest
