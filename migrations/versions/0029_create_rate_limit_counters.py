"""Create shared fixed-window rate-limit counters.

Revision ID: 0029_create_rate_limit_counters
Revises: 0028_authorized_history_queries
Create Date: 2026-08-28
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0029_create_rate_limit_counters"
down_revision: str | None = "0028_authorized_history_queries"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "rate_limit_counters",
        sa.Column("policy", sa.String(length=64), nullable=False),
        sa.Column("key_digest", sa.LargeBinary(length=32), nullable=False),
        sa.Column("window_started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("count", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("count > 0", name="ck_rate_limit_counters_count_positive"),
        sa.PrimaryKeyConstraint("policy", "key_digest", name="pk_rate_limit_counters"),
    )
    op.create_index(
        "ix_rate_limit_counters_updated_at",
        "rate_limit_counters",
        ["updated_at"],
    )
    op.execute(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE rate_limit_counters "
        "TO taskforge_runtime"
    )


def downgrade() -> None:
    op.drop_index("ix_rate_limit_counters_updated_at", table_name="rate_limit_counters")
    op.drop_table("rate_limit_counters")
