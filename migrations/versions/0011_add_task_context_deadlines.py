"""Add execution deadline policy and resolved task-run deadlines.

Revision ID: 0011_task_context_deadlines
Revises: 0010_task_claim_events
Create Date: 2026-08-13
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0011_task_context_deadlines"
down_revision: str | None = "0010_task_claim_events"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_DEADLINE_CHECK = (
    "execution_policy IS NULL OR NOT (execution_policy ? 'deadline_seconds') "
    "OR (jsonb_typeof(execution_policy -> 'deadline_seconds') = 'number' "
    "AND (execution_policy ->> 'deadline_seconds') ~ '^[0-9]+$' "
    "AND (execution_policy ->> 'deadline_seconds')::numeric "
    "BETWEEN 1 AND 31536000)"
)


def upgrade() -> None:
    op.add_column(
        "workflow_definitions",
        sa.Column(
            "execution_policy",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    op.add_column(
        "workflow_draft_steps",
        sa.Column(
            "execution_policy",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    op.add_column(
        "task_runs",
        sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=True),
    )
    for table in ("workflow_definitions", "workflow_draft_steps"):
        op.create_check_constraint(
            op.f(f"ck_{table}_execution_policy_object"),
            table,
            "execution_policy IS NULL OR jsonb_typeof(execution_policy) = 'object'",
        )
        op.create_check_constraint(
            op.f(f"ck_{table}_deadline_seconds_valid"), table, _DEADLINE_CHECK
        )
    op.execute(
        "ALTER TABLE workflow_versions ADD CONSTRAINT "
        "ck_workflow_versions_deadline_seconds_valid CHECK ("
        + _DEADLINE_CHECK
        + ") NOT VALID"
    )
    op.execute(
        "ALTER TABLE workflow_version_steps ADD CONSTRAINT "
        "ck_workflow_version_steps_deadline_seconds_valid CHECK ("
        + _DEADLINE_CHECK
        + ") NOT VALID"
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE workflow_version_steps DROP CONSTRAINT "
        "ck_workflow_version_steps_deadline_seconds_valid"
    )
    op.execute(
        "ALTER TABLE workflow_versions DROP CONSTRAINT "
        "ck_workflow_versions_deadline_seconds_valid"
    )
    for table in ("workflow_draft_steps", "workflow_definitions"):
        op.drop_constraint(
            op.f(f"ck_{table}_deadline_seconds_valid"), table, type_="check"
        )
        op.drop_constraint(
            op.f(f"ck_{table}_execution_policy_object"), table, type_="check"
        )
    op.drop_column("task_runs", "deadline_at")
    op.drop_column("workflow_draft_steps", "execution_policy")
    op.drop_column("workflow_definitions", "execution_policy")
