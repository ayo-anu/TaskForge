"""Add immutable task retry lifecycle events.

Revision ID: 0015_task_retry_events
Revises: 0014_retry_eligibility
Create Date: 2026-08-14
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0015_task_retry_events"
down_revision: str | None = "0014_retry_eligibility"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

IMMUTABILITY_FUNCTION = "reject_task_retry_event_mutation"
IMMUTABILITY_SQLSTATE = "TF005"
IMMUTABILITY_MESSAGE = "task retry events are immutable"

_EVENT_SHAPE = (
    "(event_type = 'retry_scheduled' AND failed_attempt_number IS NOT NULL "
    "AND retry_attempt_number = failed_attempt_number + 1 "
    "AND next_eligible_at IS NOT NULL AND decision_reason IS NULL) OR "
    "(event_type = 'retry_dispatched' AND failed_attempt_number IS NULL "
    "AND retry_attempt_number > 1 AND next_eligible_at IS NULL "
    "AND decision_reason IS NULL) OR "
    "(event_type = 'retry_not_scheduled' AND failed_attempt_number IS NOT NULL "
    "AND retry_attempt_number IS NULL AND next_eligible_at IS NULL "
    "AND decision_reason IN ('no_policy', 'exhausted'))"
)


def upgrade() -> None:
    op.create_table(
        "task_retry_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("task_run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column("failed_attempt_number", sa.Integer(), nullable=True),
        sa.Column("retry_attempt_number", sa.Integer(), nullable=True),
        sa.Column("next_eligible_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("decision_reason", sa.String(length=32), nullable=True),
        sa.Column(
            "occurred_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("statement_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "event_type IN ('retry_scheduled', 'retry_dispatched', "
            "'retry_not_scheduled')",
            name=op.f("ck_task_retry_events_event_type_valid"),
        ),
        sa.CheckConstraint(
            _EVENT_SHAPE,
            name=op.f("ck_task_retry_events_event_shape_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["task_run_id"],
            ["task_runs.id"],
            name="fk_task_retry_events_task_run_id_task_runs",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["task_run_id", "failed_attempt_number"],
            ["task_attempts.task_run_id", "task_attempts.attempt_number"],
            name="fk_task_retry_events_failed_attempt",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["task_run_id", "retry_attempt_number"],
            ["task_attempts.task_run_id", "task_attempts.attempt_number"],
            name="fk_task_retry_events_retry_attempt",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_task_retry_events"),
    )
    op.create_index(
        "uq_task_retry_events_scheduled_attempt",
        "task_retry_events",
        ["task_run_id", "retry_attempt_number"],
        unique=True,
        postgresql_where=sa.text("event_type = 'retry_scheduled'"),
    )
    op.create_index(
        "uq_task_retry_events_dispatched_attempt",
        "task_retry_events",
        ["task_run_id", "retry_attempt_number"],
        unique=True,
        postgresql_where=sa.text("event_type = 'retry_dispatched'"),
    )
    op.create_index(
        "uq_task_retry_events_not_scheduled_attempt",
        "task_retry_events",
        ["task_run_id", "failed_attempt_number"],
        unique=True,
        postgresql_where=sa.text("event_type = 'retry_not_scheduled'"),
    )
    op.create_index(
        "ix_task_retry_events_task_run_id_occurred_at_id",
        "task_retry_events",
        ["task_run_id", "occurred_at", "id"],
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
    for operation in ("UPDATE OR DELETE", "TRUNCATE"):
        suffix = "mutation" if operation != "TRUNCATE" else "truncate"
        level = "ROW" if operation != "TRUNCATE" else "STATEMENT"
        op.execute(
            f"CREATE TRIGGER trg_task_retry_events_reject_{suffix} "
            f"BEFORE {operation} ON task_retry_events FOR EACH {level} "
            f"EXECUTE FUNCTION {IMMUTABILITY_FUNCTION}()"
        )


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER trg_task_retry_events_reject_truncate ON task_retry_events"
    )
    op.execute(
        "DROP TRIGGER trg_task_retry_events_reject_mutation ON task_retry_events"
    )
    op.execute(f"DROP FUNCTION {IMMUTABILITY_FUNCTION}()")
    op.drop_index(
        "ix_task_retry_events_task_run_id_occurred_at_id",
        table_name="task_retry_events",
    )
    op.drop_index(
        "uq_task_retry_events_not_scheduled_attempt",
        table_name="task_retry_events",
    )
    op.drop_index(
        "uq_task_retry_events_dispatched_attempt",
        table_name="task_retry_events",
    )
    op.drop_index(
        "uq_task_retry_events_scheduled_attempt",
        table_name="task_retry_events",
    )
    op.drop_table("task_retry_events")
