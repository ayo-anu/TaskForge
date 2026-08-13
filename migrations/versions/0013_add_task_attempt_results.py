"""Add authoritative task-attempt results and submission audit events.

Revision ID: 0013_task_attempt_results
Revises: 0012_task_execution_timeout
Create Date: 2026-08-14
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0013_task_attempt_results"
down_revision: str | None = "0012_task_execution_timeout"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

IMMUTABILITY_FUNCTION = "reject_task_result_history_mutation"
IMMUTABILITY_SQLSTATE = "TF004"
IMMUTABILITY_MESSAGE = "task result history is immutable"

_RESULT_SHAPE = (
    "(result_kind = 'success' AND failure_kind IS NULL) OR "
    "(result_kind = 'retryable_failure' AND failure_kind IN "
    "('handler_reported', 'handler_exception', 'execution_timeout')) OR "
    "(result_kind = 'permanent_failure' AND failure_kind = "
    "'handler_reported') OR "
    "(result_kind = 'cancellation' AND failure_kind IS NULL)"
)


def upgrade() -> None:
    op.execute("ALTER TYPE task_run_status ADD VALUE 'retry_pending'")
    op.create_table(
        "task_attempt_results",
        sa.Column("task_attempt_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("claim_generation", sa.BigInteger(), nullable=False),
        sa.Column("dispatch_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("result_kind", sa.String(length=32), nullable=False),
        sa.Column("failure_kind", sa.String(length=32), nullable=True),
        sa.Column("output", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("result_fingerprint", sa.String(length=64), nullable=False),
        sa.Column(
            "completed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("statement_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "claim_generation > 0",
            name=op.f("ck_task_attempt_results_claim_generation_positive"),
        ),
        sa.CheckConstraint(
            "result_fingerprint ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_task_attempt_results_result_fingerprint_valid"),
        ),
        sa.CheckConstraint(
            _RESULT_SHAPE,
            name=op.f("ck_task_attempt_results_result_shape_valid"),
        ),
        sa.CheckConstraint(
            "(result_kind = 'success' AND output IS NOT NULL) OR "
            "(result_kind <> 'success' AND output IS NULL)",
            name=op.f("ck_task_attempt_results_output_presence_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["task_attempt_id"],
            ["task_attempts.id"],
            name="fk_task_attempt_results_task_attempt_id_task_attempts",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["task_attempt_id", "claim_generation"],
            [
                "task_attempt_claims.task_attempt_id",
                "task_attempt_claims.generation",
            ],
            name="fk_task_attempt_results_claim_generation",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["dispatch_id"],
            ["task_dispatch_outbox.id"],
            name="fk_task_attempt_results_dispatch_id_task_dispatch_outbox",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("task_attempt_id", name="pk_task_attempt_results"),
        sa.UniqueConstraint("dispatch_id", name="uq_task_attempt_results_dispatch_id"),
    )
    op.create_table(
        "task_result_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("task_attempt_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("claim_generation", sa.BigInteger(), nullable=False),
        sa.Column("worker_session_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("dispatch_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column("result_kind", sa.String(length=32), nullable=False),
        sa.Column("failure_kind", sa.String(length=32), nullable=True),
        sa.Column("result_fingerprint", sa.String(length=64), nullable=False),
        sa.Column(
            "occurred_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("statement_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "claim_generation > 0",
            name=op.f("ck_task_result_events_claim_generation_positive"),
        ),
        sa.CheckConstraint(
            "event_type IN ('result_accepted', 'result_replayed', "
            "'result_conflict_rejected', 'result_stale_rejected')",
            name=op.f("ck_task_result_events_event_type_valid"),
        ),
        sa.CheckConstraint(
            "result_fingerprint ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_task_result_events_result_fingerprint_valid"),
        ),
        sa.CheckConstraint(
            _RESULT_SHAPE,
            name=op.f("ck_task_result_events_result_shape_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["task_attempt_id", "claim_generation"],
            [
                "task_attempt_claims.task_attempt_id",
                "task_attempt_claims.generation",
            ],
            name="fk_task_result_events_claim_generation",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["worker_session_id"],
            ["worker_sessions.id"],
            name="fk_task_result_events_worker_session_id_worker_sessions",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["dispatch_id"],
            ["task_dispatch_outbox.id"],
            name="fk_task_result_events_dispatch_id_task_dispatch_outbox",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_task_result_events"),
    )
    op.create_index(
        "ix_task_result_events_task_attempt_id_occurred_at_id",
        "task_result_events",
        ["task_attempt_id", "occurred_at", "id"],
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
    for table in ("task_attempt_results", "task_result_events"):
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
    for table in ("task_result_events", "task_attempt_results"):
        op.execute(f"DROP TRIGGER trg_{table}_reject_truncate ON {table}")
        op.execute(f"DROP TRIGGER trg_{table}_reject_mutation ON {table}")
    op.execute(f"DROP FUNCTION {IMMUTABILITY_FUNCTION}()")
    op.drop_index(
        "ix_task_result_events_task_attempt_id_occurred_at_id",
        table_name="task_result_events",
    )
    op.drop_table("task_result_events")
    op.drop_table("task_attempt_results")
    op.execute(
        "ALTER TYPE task_run_status RENAME TO task_run_status_with_retry_pending"
    )
    op.execute(
        "CREATE TYPE task_run_status AS ENUM "
        "('blocked', 'runnable', 'dispatched', 'claimed', 'running', "
        "'retry_scheduled', 'succeeded', 'failed', 'skipped', 'cancelled')"
    )
    op.execute(
        "ALTER TABLE task_runs ALTER COLUMN status TYPE task_run_status "
        "USING status::text::task_run_status"
    )
    op.execute("DROP TYPE task_run_status_with_retry_pending")
