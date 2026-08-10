"""SQL statement invariants for atomic task dispatch persistence."""

from uuid import uuid4

from sqlalchemy.dialects import postgresql

from taskforge.persistence.dispatch import (
    _next_attempt_number_statement,
    _runnable_task_dispatch_snapshot_statement,
    _runnable_to_dispatched_statement,
    _workflow_run_dispatch_lock_statement,
)


def sql(statement) -> str:
    return str(
        statement.compile(
            dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
        )
    )


def test_outer_lock_targets_only_owning_workflow_run() -> None:
    statement = sql(_workflow_run_dispatch_lock_statement(uuid4(), uuid4()))

    assert "JOIN task_runs" in statement
    assert "task_runs.workflow_run_id = workflow_runs.id" in statement
    assert (
        "task_runs.workflow_version_id = workflow_runs.workflow_version_id" in statement
    )
    assert "FOR UPDATE OF workflow_runs" in statement


def test_snapshot_requires_owned_runnable_version_step() -> None:
    statement = sql(
        _runnable_task_dispatch_snapshot_statement(uuid4(), uuid4(), uuid4())
    )

    assert "workflow_version_steps" in statement
    assert "task_runs.workflow_run_id" in statement
    assert "task_runs.workflow_version_id" in statement
    assert "task_runs.status = 'runnable'" in statement


def test_attempt_allocation_is_scoped_to_task_run() -> None:
    statement = sql(_next_attempt_number_statement(uuid4()))

    assert "max(task_attempts.attempt_number)" in statement
    assert "task_attempts.task_run_id" in statement
    assert "+ 1" in statement


def test_final_transition_is_guarded_by_identity_ownership_and_state() -> None:
    statement = sql(_runnable_to_dispatched_statement(uuid4(), uuid4()))

    assert "task_runs.id" in statement
    assert "task_runs.workflow_run_id" in statement
    assert "task_runs.status = 'runnable'" in statement
    assert "status='dispatched'" in statement
    assert "RETURNING task_runs.id" in statement
