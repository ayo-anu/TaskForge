"""Structural tests for the atomic workflow cancellation adapter."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy.dialects import postgresql

from taskforge.identity.authorization import OwnerFilter
from taskforge.persistence.runs import (
    _workflow_run_cancellation_lock_statement,
    _workflow_run_cancellation_request_statement,
    _workflow_run_to_cancelling_statement,
)
from taskforge.runs.domain import WorkflowRunStatus


def sql(statement: object) -> str:
    return " ".join(
        str(
            statement.compile(  # type: ignore[attr-defined]
                dialect=postgresql.dialect()  # type: ignore[no-untyped-call]
            )
        ).split()
    )


def test_owner_lock_is_run_first_exclusive_and_owner_scoped() -> None:
    owner = sql(
        _workflow_run_cancellation_lock_statement(uuid4(), OwnerFilter.only(uuid4()))
    )
    administrator = sql(
        _workflow_run_cancellation_lock_statement(uuid4(), OwnerFilter.all_owners())
    )
    assert "FOR UPDATE OF workflow_runs" in owner
    assert "workflow_definitions.owner_principal_id =" in owner
    assert "workflow_definitions.owner_principal_id =" not in administrator
    for statement in (owner, administrator):
        assert "task_runs" not in statement
        assert "task_attempts" not in statement
        assert "task_attempt_claims" not in statement


def test_canonical_request_read_is_primary_key_scoped_and_lock_free() -> None:
    statement = sql(_workflow_run_cancellation_request_statement(uuid4()))
    assert "workflow_run_cancellation_requests.workflow_run_id =" in statement
    assert "FOR UPDATE" not in statement


def test_guarded_transition_uses_observed_active_status_and_database_time() -> None:
    now = datetime.now(UTC)
    for status in (WorkflowRunStatus.PENDING, WorkflowRunStatus.RUNNING):
        statement = _workflow_run_to_cancelling_statement(uuid4(), status, now)
        rendered = sql(statement)
        assert "UPDATE workflow_runs SET status=" in rendered
        assert "workflow_runs.id =" in rendered
        assert "workflow_runs.status =" in rendered
        assert statement._values["status"].value == "cancelling"
        assert statement._values["updated_at"].value == now
        assert "RETURNING workflow_runs.status" in rendered
