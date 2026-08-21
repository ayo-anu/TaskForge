"""Create ordered, append-only workflow-run execution events.

Revision ID: 0022_run_execution_events
Revises: 0021_recovered_cancellation
Create Date: 2026-08-21
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0022_run_execution_events"
down_revision: str | None = "0021_recovered_cancellation"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

ALLOCATION_FUNCTION = "allocate_workflow_run_execution_event_cursor"
IMMUTABILITY_FUNCTION = "reject_workflow_run_execution_event_mutation"
IMMUTABILITY_SQLSTATE = "TF006"
IMMUTABILITY_MESSAGE = "workflow run execution events are immutable"


def upgrade() -> None:
    op.add_column(
        "workflow_runs",
        sa.Column(
            "last_execution_event_cursor",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
    )
    op.create_check_constraint(
        op.f("ck_workflow_runs_last_execution_event_cursor_nonnegative"),
        "workflow_runs",
        "last_execution_event_cursor >= 0",
    )
    op.create_unique_constraint(
        "uq_task_runs_workflow_run_id_id",
        "task_runs",
        ["workflow_run_id", "id"],
    )
    op.create_table(
        "workflow_run_execution_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("workflow_run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("cursor", sa.BigInteger(), nullable=False),
        sa.Column("task_run_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("event_type", sa.String(length=128), nullable=False),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "occurred_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("statement_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "cursor > 0",
            name=op.f("ck_workflow_run_execution_events_cursor_positive"),
        ),
        sa.CheckConstraint(
            "length(btrim(event_type)) BETWEEN 1 AND 128",
            name=op.f("ck_workflow_run_execution_events_event_type_valid"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object'",
            name=op.f("ck_workflow_run_execution_events_payload_object"),
        ),
        sa.ForeignKeyConstraint(
            ["workflow_run_id"],
            ["workflow_runs.id"],
            name="fk_workflow_run_execution_events_run",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["workflow_run_id", "task_run_id"],
            ["task_runs.workflow_run_id", "task_runs.id"],
            name="fk_workflow_run_execution_events_task_ownership",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_workflow_run_execution_events"),
        sa.UniqueConstraint(
            "workflow_run_id",
            "cursor",
            name="uq_workflow_run_execution_events_run_cursor",
        ),
    )
    op.execute(
        f"""
        CREATE FUNCTION {ALLOCATION_FUNCTION}()
        RETURNS trigger LANGUAGE plpgsql AS $function$
        BEGIN
            IF NEW.cursor IS NOT NULL THEN
                RAISE EXCEPTION USING ERRCODE = '22023',
                    MESSAGE = 'execution event cursor is database assigned';
            END IF;

            UPDATE workflow_runs
            SET last_execution_event_cursor = last_execution_event_cursor + 1
            WHERE id = NEW.workflow_run_id
            RETURNING last_execution_event_cursor INTO NEW.cursor;

            IF NOT FOUND THEN
                RAISE EXCEPTION USING ERRCODE = '23503',
                    MESSAGE = 'execution event workflow run does not exist';
            END IF;
            RETURN NEW;
        END;
        $function$
        """
    )
    op.execute(
        f"CREATE TRIGGER trg_workflow_run_execution_events_allocate_cursor "
        f"BEFORE INSERT ON workflow_run_execution_events FOR EACH ROW "
        f"EXECUTE FUNCTION {ALLOCATION_FUNCTION}()"
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
    op.execute(
        f"CREATE TRIGGER trg_workflow_run_execution_events_reject_mutation "
        f"BEFORE UPDATE OR DELETE ON workflow_run_execution_events FOR EACH ROW "
        f"EXECUTE FUNCTION {IMMUTABILITY_FUNCTION}()"
    )
    op.execute(
        f"CREATE TRIGGER trg_workflow_run_execution_events_reject_truncate "
        f"BEFORE TRUNCATE ON workflow_run_execution_events FOR EACH STATEMENT "
        f"EXECUTE FUNCTION {IMMUTABILITY_FUNCTION}()"
    )


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER trg_workflow_run_execution_events_reject_truncate "
        "ON workflow_run_execution_events"
    )
    op.execute(
        "DROP TRIGGER trg_workflow_run_execution_events_reject_mutation "
        "ON workflow_run_execution_events"
    )
    op.execute(f"DROP FUNCTION {IMMUTABILITY_FUNCTION}()")
    op.execute(
        "DROP TRIGGER trg_workflow_run_execution_events_allocate_cursor "
        "ON workflow_run_execution_events"
    )
    op.execute(f"DROP FUNCTION {ALLOCATION_FUNCTION}()")
    op.drop_table("workflow_run_execution_events")
    op.drop_constraint("uq_task_runs_workflow_run_id_id", "task_runs", type_="unique")
    op.drop_constraint(
        op.f("ck_workflow_runs_last_execution_event_cursor_nonnegative"),
        "workflow_runs",
        type_="check",
    )
    op.drop_column("workflow_runs", "last_execution_event_cursor")
