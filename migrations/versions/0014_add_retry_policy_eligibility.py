"""Add retry-policy validation and durable attempt eligibility time.

Revision ID: 0014_retry_eligibility
Revises: 0013_task_attempt_results
Create Date: 2026-08-14
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0014_retry_eligibility"
down_revision: str | None = "0013_task_attempt_results"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_RETRY_POLICY_CHECK = (
    "execution_policy IS NULL OR NOT (execution_policy ? 'retry_policy') OR ("
    "jsonb_typeof(execution_policy -> 'retry_policy') = 'object' "
    "AND (NOT (execution_policy -> 'retry_policy' ? 'maximum_attempts') OR ("
    "jsonb_typeof(execution_policy -> 'retry_policy' -> 'maximum_attempts') "
    "= 'number' AND (execution_policy -> 'retry_policy' ->> "
    "'maximum_attempts') ~ '^[0-9]+$' AND (execution_policy -> "
    "'retry_policy' ->> 'maximum_attempts')::numeric >= 1)) "
    "AND (NOT (execution_policy -> 'retry_policy' ? 'initial_delay_seconds') OR ("
    "jsonb_typeof(execution_policy -> 'retry_policy' -> "
    "'initial_delay_seconds') = 'number' AND (execution_policy -> "
    "'retry_policy' ->> 'initial_delay_seconds') ~ '^[0-9]+$')) "
    "AND (NOT (execution_policy -> 'retry_policy' ? 'multiplier') OR ("
    "jsonb_typeof(execution_policy -> 'retry_policy' -> 'multiplier') = "
    "'number' AND (execution_policy -> 'retry_policy' ->> "
    "'multiplier')::numeric >= 1)) "
    "AND (NOT (execution_policy -> 'retry_policy' ? 'maximum_delay_seconds') OR ("
    "jsonb_typeof(execution_policy -> 'retry_policy' -> "
    "'maximum_delay_seconds') = 'number' AND (execution_policy -> "
    "'retry_policy' ->> 'maximum_delay_seconds') ~ '^[0-9]+$')))"
)


def upgrade() -> None:
    op.add_column(
        "task_attempts",
        sa.Column("next_eligible_at", sa.DateTime(timezone=True), nullable=True),
    )
    for table in ("workflow_definitions", "workflow_draft_steps"):
        op.create_check_constraint(
            op.f(f"ck_{table}_retry_policy_valid"), table, _RETRY_POLICY_CHECK
        )
    for table in ("workflow_versions", "workflow_version_steps"):
        op.execute(
            f"ALTER TABLE {table} ADD CONSTRAINT ck_{table}_retry_policy_valid "
            f"CHECK ({_RETRY_POLICY_CHECK}) NOT VALID"
        )
    op.create_index(
        "ix_task_attempts_scheduled_next_eligible_at_id",
        "task_attempts",
        ["next_eligible_at", "id"],
        unique=False,
        postgresql_where=sa.text("next_eligible_at IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index(
        "ix_task_attempts_scheduled_next_eligible_at_id",
        table_name="task_attempts",
    )
    for table in ("workflow_version_steps", "workflow_versions"):
        op.execute(f"ALTER TABLE {table} DROP CONSTRAINT ck_{table}_retry_policy_valid")
    for table in ("workflow_draft_steps", "workflow_definitions"):
        op.drop_constraint(op.f(f"ck_{table}_retry_policy_valid"), table, type_="check")
    op.drop_column("task_attempts", "next_eligible_at")
