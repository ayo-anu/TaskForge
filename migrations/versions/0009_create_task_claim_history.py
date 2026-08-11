"""Create task-attempt claim lifecycle and history storage.

Revision ID: 0009_task_claim_history
Revises: 0008_worker_sessions_health
Create Date: 2026-08-11
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0009_task_claim_history"
down_revision: str | None = "0008_worker_sessions_health"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create constrained claim ownership, lease, and generation lifecycles."""
    op.create_table(
        "task_attempt_claims",
        sa.Column("task_attempt_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("worker_session_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "acquired_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("statement_timestamp()"),
            nullable=False,
        ),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("terminated_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "generation > 0",
            name=op.f("ck_task_attempt_claims_generation_positive"),
        ),
        sa.CheckConstraint(
            "lease_expires_at > acquired_at",
            name=op.f("ck_task_attempt_claims_lease_expires_after_acquisition"),
        ),
        sa.CheckConstraint(
            "terminated_at IS NULL OR terminated_at >= acquired_at",
            name=op.f("ck_task_attempt_claims_terminated_not_before_acquisition"),
        ),
        sa.ForeignKeyConstraint(
            ["task_attempt_id"],
            ["task_attempts.id"],
            name="fk_task_attempt_claims_task_attempt_id_task_attempts",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["worker_session_id"],
            ["worker_sessions.id"],
            name="fk_task_attempt_claims_worker_session_id_worker_sessions",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "task_attempt_id",
            "generation",
            name="pk_task_attempt_claims",
        ),
    )
    op.create_index(
        "uq_task_attempt_claims_current_task_attempt_id",
        "task_attempt_claims",
        ["task_attempt_id"],
        unique=True,
        postgresql_where=sa.text("terminated_at IS NULL"),
    )
    op.create_index(
        "ix_task_attempt_claims_current_lease_expires_at",
        "task_attempt_claims",
        ["lease_expires_at"],
        unique=False,
        postgresql_where=sa.text("terminated_at IS NULL"),
    )
    op.create_index(
        "ix_task_attempt_claims_current_worker_session_id",
        "task_attempt_claims",
        ["worker_session_id"],
        unique=False,
        postgresql_where=sa.text("terminated_at IS NULL"),
    )


def downgrade() -> None:
    """Remove task-attempt claim lifecycle and history storage."""
    op.drop_index(
        "ix_task_attempt_claims_current_worker_session_id",
        table_name="task_attempt_claims",
    )
    op.drop_index(
        "ix_task_attempt_claims_current_lease_expires_at",
        table_name="task_attempt_claims",
    )
    op.drop_index(
        "uq_task_attempt_claims_current_task_attempt_id",
        table_name="task_attempt_claims",
    )
    op.drop_table("task_attempt_claims")
