"""Create task-attempt and durable dispatch-outbox storage.

Revision ID: 0007_attempt_dispatch_outbox
Revises: 0006_run_foundation
Create Date: 2026-08-10
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0007_attempt_dispatch_outbox"
down_revision: str | None = "0006_run_foundation"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create constrained attempt history and durable dispatch intent."""
    op.create_table(
        "task_attempts",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("task_run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "attempt_number > 0",
            name=op.f("ck_task_attempts_attempt_number_positive"),
        ),
        sa.ForeignKeyConstraint(
            ["task_run_id"],
            ["task_runs.id"],
            name="fk_task_attempts_task_run_id_task_runs",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_task_attempts"),
        sa.UniqueConstraint(
            "task_run_id",
            "attempt_number",
            name="uq_task_attempts_task_run_id_attempt_number",
        ),
    )
    op.create_table(
        "task_dispatch_outbox",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("task_attempt_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("route", sa.String(length=255), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "length(btrim(route)) > 0",
            name=op.f("ck_task_dispatch_outbox_route_not_blank"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object'",
            name=op.f("ck_task_dispatch_outbox_payload_object"),
        ),
        sa.ForeignKeyConstraint(
            ["task_attempt_id"],
            ["task_attempts.id"],
            name="fk_task_dispatch_outbox_task_attempt_id_task_attempts",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_task_dispatch_outbox"),
        sa.UniqueConstraint(
            "task_attempt_id",
            name="uq_task_dispatch_outbox_task_attempt_id",
        ),
    )
    op.create_index(
        "ix_task_dispatch_outbox_unpublished_created_at_id",
        "task_dispatch_outbox",
        ["created_at", "id"],
        unique=False,
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    """Remove task-attempt and durable dispatch-outbox storage."""
    op.drop_index(
        "ix_task_dispatch_outbox_unpublished_created_at_id",
        table_name="task_dispatch_outbox",
    )
    op.drop_table("task_dispatch_outbox")
    op.drop_table("task_attempts")
