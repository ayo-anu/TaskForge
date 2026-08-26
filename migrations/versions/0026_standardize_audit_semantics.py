"""Standardize prospective audit and execution-history semantics.

Revision ID: 0026_standardize_audit_semantics
Revises: 0025_complete_audit_history
Create Date: 2026-08-26
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0026_standardize_audit_semantics"
down_revision: str | None = "0025_complete_audit_history"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_RESULT_COMPONENT_CONSTRAINT = "ck_task_result_events_actor_component_valid"
_LEGACY_RESULT_COMPONENT_CONSTRAINT = (
    "ck_task_result_events_ck_task_result_events_actor_compo_214b"
)
_CORRELATION_TABLES = (
    "task_claim_events",
    "task_result_events",
    "task_retry_events",
    "worker_heartbeats",
)


def upgrade() -> None:
    op.execute(
        "ALTER TABLE task_result_events RENAME CONSTRAINT "
        f"{_LEGACY_RESULT_COMPONENT_CONSTRAINT} TO {_RESULT_COMPONENT_CONSTRAINT}"
    )
    op.execute(
        "ALTER TABLE audit_records ADD CONSTRAINT "
        "ck_audit_records_action_namespaced CHECK "
        "(action ~ '^[a-z][a-z0-9_-]*(\\.[a-z][a-z0-9_-]*)+$') NOT VALID"
    )
    op.execute(
        "ALTER TABLE workflow_run_execution_events ADD CONSTRAINT "
        "ck_workflow_run_execution_events_event_type_namespaced CHECK "
        "(event_type ~ '^[a-z][a-z0-9_-]*(\\.[a-z][a-z0-9_-]*)+$') NOT VALID"
    )
    for table in _CORRELATION_TABLES:
        op.execute(
            f"ALTER TABLE {table} ADD CONSTRAINT ck_{table}_correlation_valid "
            "CHECK (correlation_id IS NULL OR "
            "(length(correlation_id) BETWEEN 1 AND 128 AND "
            "correlation_id !~ '[^ -~]')) NOT VALID"
        )


def downgrade() -> None:
    for table in reversed(_CORRELATION_TABLES):
        op.execute(f"ALTER TABLE {table} DROP CONSTRAINT ck_{table}_correlation_valid")
    op.execute(
        "ALTER TABLE workflow_run_execution_events DROP CONSTRAINT "
        "ck_workflow_run_execution_events_event_type_namespaced"
    )
    op.execute(
        "ALTER TABLE audit_records DROP CONSTRAINT ck_audit_records_action_namespaced"
    )
    op.execute(
        "ALTER TABLE task_result_events RENAME CONSTRAINT "
        f"{_RESULT_COMPONENT_CONSTRAINT} TO {_LEGACY_RESULT_COMPONENT_CONSTRAINT}"
    )
