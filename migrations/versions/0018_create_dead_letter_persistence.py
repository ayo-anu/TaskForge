"""Create immutable dead-letter facts, audit history, and redrive requests.

Revision ID: 0018_dead_letter_persistence
Revises: 0017_recovery_result_events
Create Date: 2026-08-20
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0018_dead_letter_persistence"
down_revision: str | None = "0017_recovery_result_events"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

IMMUTABILITY_FUNCTION = "reject_dead_letter_history_mutation"
IMMUTABILITY_SQLSTATE = "TF006"
IMMUTABILITY_MESSAGE = "dead-letter history is immutable"
IMMUTABLE_TABLES = (
    "dead_letter_items",
    "dead_letter_operator_actions",
    "dead_letter_redrive_requests",
)


def upgrade() -> None:
    """Create constrained dead-letter facts and operational persistence."""
    op.create_unique_constraint(
        "uq_task_attempts_task_run_id_id",
        "task_attempts",
        ["task_run_id", "id"],
    )
    op.create_table(
        "dead_letter_items",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("task_run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "source_task_attempt_id", postgresql.UUID(as_uuid=True), nullable=False
        ),
        sa.Column("reason", sa.String(length=32), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("statement_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "reason IN ('permanent_failure', 'retry_exhausted')",
            name=op.f("ck_dead_letter_items_reason_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["task_run_id", "source_task_attempt_id"],
            ["task_attempts.task_run_id", "task_attempts.id"],
            name="fk_dead_letter_items_source_attempt",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_task_attempt_id"],
            ["task_attempt_results.task_attempt_id"],
            name="fk_dead_letter_items_source_result",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_dead_letter_items"),
        sa.UniqueConstraint(
            "source_task_attempt_id",
            name="uq_dead_letter_items_source_task_attempt_id",
        ),
    )
    op.create_index(
        "ix_dead_letter_items_task_run_id_created_at_id",
        "dead_letter_items",
        ["task_run_id", sa.text("created_at DESC"), sa.text("id DESC")],
        unique=False,
    )
    op.create_index(
        "ix_dead_letter_items_created_at_id",
        "dead_letter_items",
        [sa.text("created_at DESC"), sa.text("id DESC")],
        unique=False,
    )

    op.create_table(
        "dead_letter_status",
        sa.Column("dead_letter_item_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "status", sa.String(length=32), server_default="open", nullable=False
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("statement_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "status IN ('open', 'acknowledged', 'resolved')",
            name=op.f("ck_dead_letter_status_status_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["dead_letter_item_id"],
            ["dead_letter_items.id"],
            name="fk_dead_letter_status_dead_letter_item_id_dead_letter_items",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("dead_letter_item_id", name="pk_dead_letter_status"),
    )
    op.create_index(
        "ix_dead_letter_status_status_updated_at_item_id",
        "dead_letter_status",
        ["status", sa.text("updated_at DESC"), sa.text("dead_letter_item_id DESC")],
        unique=False,
    )

    op.create_table(
        "dead_letter_operator_actions",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("dead_letter_item_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "operator_principal_id", postgresql.UUID(as_uuid=True), nullable=False
        ),
        sa.Column("action_type", sa.String(length=32), nullable=False),
        sa.Column("previous_status", sa.String(length=32), nullable=False),
        sa.Column("new_status", sa.String(length=32), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("correlation_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "occurred_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("statement_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "action_type IN ('acknowledged', 'resolved')",
            name=op.f("ck_dead_letter_operator_actions_action_type_valid"),
        ),
        sa.CheckConstraint(
            "(action_type = 'acknowledged' AND previous_status = 'open' "
            "AND new_status = 'acknowledged') OR "
            "(action_type = 'resolved' "
            "AND previous_status IN ('open', 'acknowledged') "
            "AND new_status = 'resolved' AND reason IS NOT NULL)",
            name=op.f("ck_dead_letter_operator_actions_action_shape_valid"),
        ),
        sa.CheckConstraint(
            "reason IS NULL OR length(btrim(reason)) BETWEEN 1 AND 2000",
            name=op.f("ck_dead_letter_operator_actions_reason_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["dead_letter_item_id"],
            ["dead_letter_items.id"],
            name="fk_dead_letter_operator_actions_item",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["operator_principal_id"],
            ["api_principals.id"],
            name="fk_dead_letter_operator_actions_operator",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_dead_letter_operator_actions"),
    )
    op.create_index(
        "ix_dead_letter_operator_actions_item_occurred_at_id",
        "dead_letter_operator_actions",
        ["dead_letter_item_id", "occurred_at", "id"],
        unique=False,
    )
    op.create_index(
        "ix_dead_letter_operator_actions_operator_occurred_at_id",
        "dead_letter_operator_actions",
        [
            "operator_principal_id",
            sa.text("occurred_at DESC"),
            sa.text("id DESC"),
        ],
        unique=False,
    )

    op.create_table(
        "dead_letter_redrive_requests",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("dead_letter_item_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "requested_by_principal_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("idempotency_key_digest", sa.String(length=64), nullable=False),
        sa.Column("request_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("correlation_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "requested_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("statement_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "idempotency_key_digest ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_dead_letter_redrive_requests_key_digest_valid"),
        ),
        sa.CheckConstraint(
            "request_fingerprint ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_dead_letter_redrive_requests_fingerprint_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["dead_letter_item_id"],
            ["dead_letter_items.id"],
            name="fk_dead_letter_redrive_requests_item",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["requested_by_principal_id"],
            ["api_principals.id"],
            name="fk_dead_letter_redrive_requests_requester",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_dead_letter_redrive_requests"),
        sa.UniqueConstraint(
            "dead_letter_item_id",
            "requested_by_principal_id",
            "idempotency_key_digest",
            name="uq_dead_letter_redrive_requests_item_requester_key",
        ),
    )
    op.create_index(
        "ix_dead_letter_redrive_requests_item_requested_at_id",
        "dead_letter_redrive_requests",
        ["dead_letter_item_id", sa.text("requested_at DESC"), sa.text("id DESC")],
        unique=False,
    )

    op.execute(
        f"""
        CREATE FUNCTION {IMMUTABILITY_FUNCTION}()
        RETURNS trigger LANGUAGE plpgsql AS $function$
        BEGIN
            RAISE EXCEPTION USING ERRCODE = '{IMMUTABILITY_SQLSTATE}',
                MESSAGE = '{IMMUTABILITY_MESSAGE}';
        END;
        $function$
        """
    )
    for table in IMMUTABLE_TABLES:
        op.execute(
            f"CREATE TRIGGER trg_{table}_reject_mutation "
            f"BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW "
            f"EXECUTE FUNCTION {IMMUTABILITY_FUNCTION}()"
        )
        op.execute(
            f"CREATE TRIGGER trg_{table}_reject_truncate "
            f"BEFORE TRUNCATE ON {table} FOR EACH STATEMENT "
            f"EXECUTE FUNCTION {IMMUTABILITY_FUNCTION}()"
        )


def downgrade() -> None:
    """Remove dead-letter persistence in reverse dependency order."""
    for table in reversed(IMMUTABLE_TABLES):
        op.execute(f"DROP TRIGGER trg_{table}_reject_truncate ON {table}")
        op.execute(f"DROP TRIGGER trg_{table}_reject_mutation ON {table}")
    op.execute(f"DROP FUNCTION {IMMUTABILITY_FUNCTION}()")
    op.drop_index(
        "ix_dead_letter_redrive_requests_item_requested_at_id",
        table_name="dead_letter_redrive_requests",
    )
    op.drop_table("dead_letter_redrive_requests")
    op.drop_index(
        "ix_dead_letter_operator_actions_operator_occurred_at_id",
        table_name="dead_letter_operator_actions",
    )
    op.drop_index(
        "ix_dead_letter_operator_actions_item_occurred_at_id",
        table_name="dead_letter_operator_actions",
    )
    op.drop_table("dead_letter_operator_actions")
    op.drop_index(
        "ix_dead_letter_status_status_updated_at_item_id",
        table_name="dead_letter_status",
    )
    op.drop_table("dead_letter_status")
    op.drop_index("ix_dead_letter_items_created_at_id", table_name="dead_letter_items")
    op.drop_index(
        "ix_dead_letter_items_task_run_id_created_at_id",
        table_name="dead_letter_items",
    )
    op.drop_table("dead_letter_items")
    op.drop_constraint(
        "uq_task_attempts_task_run_id_id", "task_attempts", type_="unique"
    )
