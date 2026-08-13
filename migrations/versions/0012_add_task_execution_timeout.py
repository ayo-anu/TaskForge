"""Add durable task execution-timeout policy.

Revision ID: 0012_task_execution_timeout
Revises: 0011_task_context_deadlines
Create Date: 2026-08-13
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0012_task_execution_timeout"
down_revision: str | None = "0011_task_context_deadlines"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_EXECUTION_TIMEOUT_CHECK = (
    "execution_policy IS NULL "
    "OR NOT (execution_policy ? 'execution_timeout_seconds') "
    "OR (jsonb_typeof(execution_policy -> 'execution_timeout_seconds') = 'number' "
    "AND (execution_policy ->> 'execution_timeout_seconds') ~ '^[0-9]+$' "
    "AND (execution_policy ->> 'execution_timeout_seconds')::numeric "
    "BETWEEN 1 AND 31536000)"
)


def upgrade() -> None:
    op.add_column(
        "task_runs",
        sa.Column("execution_timeout_seconds", sa.Integer(), nullable=True),
    )
    op.create_check_constraint(
        op.f("ck_task_runs_execution_timeout_seconds_valid"),
        "task_runs",
        "execution_timeout_seconds IS NULL "
        "OR execution_timeout_seconds BETWEEN 1 AND 31536000",
    )
    for table in ("workflow_definitions", "workflow_draft_steps"):
        op.create_check_constraint(
            op.f(f"ck_{table}_execution_timeout_seconds_valid"),
            table,
            _EXECUTION_TIMEOUT_CHECK,
        )
    op.execute(
        "ALTER TABLE workflow_versions ADD CONSTRAINT "
        "ck_workflow_versions_execution_timeout_seconds_valid CHECK ("
        + _EXECUTION_TIMEOUT_CHECK
        + ") NOT VALID"
    )
    op.execute(
        "ALTER TABLE workflow_version_steps ADD CONSTRAINT "
        "ck_workflow_version_steps_execution_timeout_seconds_valid CHECK ("
        + _EXECUTION_TIMEOUT_CHECK
        + ") NOT VALID"
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE workflow_version_steps DROP CONSTRAINT "
        "ck_workflow_version_steps_execution_timeout_seconds_valid"
    )
    op.execute(
        "ALTER TABLE workflow_versions DROP CONSTRAINT "
        "ck_workflow_versions_execution_timeout_seconds_valid"
    )
    for table in ("workflow_draft_steps", "workflow_definitions"):
        op.drop_constraint(
            op.f(f"ck_{table}_execution_timeout_seconds_valid"),
            table,
            type_="check",
        )
    op.drop_constraint(
        op.f("ck_task_runs_execution_timeout_seconds_valid"),
        "task_runs",
        type_="check",
    )
    op.drop_column("task_runs", "execution_timeout_seconds")
