"""Add durable dead-letter redrive target lineage.

Revision ID: 0019_dead_letter_redrive
Revises: 0018_dead_letter_persistence
Create Date: 2026-08-20
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0019_dead_letter_redrive"
down_revision: str | None = "0018_dead_letter_persistence"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Require every accepted redrive request to name one materialized run."""
    op.add_column(
        "dead_letter_redrive_requests",
        sa.Column(
            "target_workflow_run_id", postgresql.UUID(as_uuid=True), nullable=False
        ),
    )
    op.add_column(
        "dead_letter_redrive_requests",
        sa.Column("reason", sa.Text(), nullable=True),
    )
    op.create_check_constraint(
        op.f("ck_dead_letter_redrive_requests_reason_valid"),
        "dead_letter_redrive_requests",
        "reason IS NULL OR length(btrim(reason)) BETWEEN 1 AND 2000",
    )
    op.create_foreign_key(
        "fk_dead_letter_redrive_requests_target_run",
        "dead_letter_redrive_requests",
        "workflow_runs",
        ["target_workflow_run_id"],
        ["id"],
        onupdate="RESTRICT",
        ondelete="RESTRICT",
    )
    op.create_unique_constraint(
        "uq_dead_letter_redrive_requests_item",
        "dead_letter_redrive_requests",
        ["dead_letter_item_id"],
    )
    op.create_unique_constraint(
        "uq_dead_letter_redrive_requests_target_run",
        "dead_letter_redrive_requests",
        ["target_workflow_run_id"],
    )


def downgrade() -> None:
    """Remove target lineage while retaining the Task 1 request foundation."""
    op.drop_constraint(
        "uq_dead_letter_redrive_requests_target_run",
        "dead_letter_redrive_requests",
        type_="unique",
    )
    op.drop_constraint(
        "uq_dead_letter_redrive_requests_item",
        "dead_letter_redrive_requests",
        type_="unique",
    )
    op.drop_constraint(
        "fk_dead_letter_redrive_requests_target_run",
        "dead_letter_redrive_requests",
        type_="foreignkey",
    )
    op.drop_constraint(
        op.f("ck_dead_letter_redrive_requests_reason_valid"),
        "dead_letter_redrive_requests",
        type_="check",
    )
    op.drop_column("dead_letter_redrive_requests", "reason")
    op.drop_column("dead_letter_redrive_requests", "target_workflow_run_id")
