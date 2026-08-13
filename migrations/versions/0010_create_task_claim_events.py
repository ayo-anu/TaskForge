"""Create immutable task claim lifecycle events.

Revision ID: 0010_task_claim_events
Revises: 0009_task_claim_history
Create Date: 2026-08-11
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0010_task_claim_events"
down_revision: str | None = "0009_task_claim_history"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

IMMUTABILITY_FUNCTION = "reject_task_claim_event_mutation"
IMMUTABILITY_SQLSTATE = "TF003"
IMMUTABILITY_MESSAGE = "task claim events are immutable"
ROW_TRIGGER = "trg_task_claim_events_reject_mutation"
TRUNCATE_TRIGGER = "trg_task_claim_events_reject_truncate"


def upgrade() -> None:
    """Create constrained, append-only claim lifecycle events."""
    op.create_table(
        "task_claim_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("task_attempt_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "previous_lease_expires_at", sa.DateTime(timezone=True), nullable=True
        ),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "event_type IN ('claim_acquired', 'lease_renewed')",
            name=op.f("ck_task_claim_events_event_type_valid"),
        ),
        sa.CheckConstraint(
            "(event_type = 'claim_acquired' "
            "AND previous_lease_expires_at IS NULL "
            "AND lease_expires_at > occurred_at) OR "
            "(event_type = 'lease_renewed' "
            "AND previous_lease_expires_at IS NOT NULL "
            "AND lease_expires_at > previous_lease_expires_at)",
            name=op.f("ck_task_claim_events_event_shape_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["task_attempt_id", "generation"],
            [
                "task_attempt_claims.task_attempt_id",
                "task_attempt_claims.generation",
            ],
            name="fk_task_claim_events_claim_generation",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_task_claim_events"),
    )
    op.create_index(
        "uq_task_claim_events_acquired_generation",
        "task_claim_events",
        ["task_attempt_id", "generation"],
        unique=True,
        postgresql_where=sa.text("event_type = 'claim_acquired'"),
    )
    op.create_index(
        "uq_task_claim_events_renewal_transition",
        "task_claim_events",
        [
            "task_attempt_id",
            "generation",
            "previous_lease_expires_at",
            "lease_expires_at",
        ],
        unique=True,
        postgresql_where=sa.text("event_type = 'lease_renewed'"),
    )
    op.execute(
        f"""
        CREATE FUNCTION {IMMUTABILITY_FUNCTION}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        BEGIN
            RAISE EXCEPTION USING
                ERRCODE = '{IMMUTABILITY_SQLSTATE}',
                MESSAGE = '{IMMUTABILITY_MESSAGE}';
        END;
        $function$
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {ROW_TRIGGER}
        BEFORE UPDATE OR DELETE ON task_claim_events
        FOR EACH ROW
        EXECUTE FUNCTION {IMMUTABILITY_FUNCTION}()
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {TRUNCATE_TRIGGER}
        BEFORE TRUNCATE ON task_claim_events
        FOR EACH STATEMENT
        EXECUTE FUNCTION {IMMUTABILITY_FUNCTION}()
        """
    )


def downgrade() -> None:
    """Remove immutable claim lifecycle events."""
    op.execute(f"DROP TRIGGER {TRUNCATE_TRIGGER} ON task_claim_events")
    op.execute(f"DROP TRIGGER {ROW_TRIGGER} ON task_claim_events")
    op.execute(f"DROP FUNCTION {IMMUTABILITY_FUNCTION}()")
    op.drop_index(
        "uq_task_claim_events_renewal_transition", table_name="task_claim_events"
    )
    op.drop_index(
        "uq_task_claim_events_acquired_generation", table_name="task_claim_events"
    )
    op.drop_table("task_claim_events")
