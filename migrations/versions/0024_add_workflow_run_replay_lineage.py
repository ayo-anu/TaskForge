"""Add immutable workflow-run replay lineage.

Revision ID: 0024_run_replay_lineage
Revises: 0023_execution_event_wakeups
Create Date: 2026-08-22
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0024_run_replay_lineage"
down_revision: str | None = "0023_execution_event_wakeups"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

workflow_replay_mode = postgresql.ENUM(
    "full",
    "failed_subgraph",
    name="workflow_replay_mode",
    create_type=False,
)

IMMUTABILITY_FUNCTION = "reject_workflow_run_replay_mutation"
IMMUTABILITY_SQLSTATE = "TF008"
IMMUTABILITY_MESSAGE = "workflow run replay lineage is immutable"
ROW_TRIGGER = "trg_workflow_run_replays_reject_mutation"
TRUNCATE_TRIGGER = "trg_workflow_run_replays_reject_truncate"


def upgrade() -> None:
    """Create constrained, immutable immediate-source replay lineage."""
    workflow_replay_mode.create(op.get_bind(), checkfirst=False)
    op.create_table(
        "workflow_run_replays",
        sa.Column("workflow_run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "source_workflow_run_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("mode", workflow_replay_mode, nullable=False),
        sa.Column(
            "requested_scope",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("statement_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "workflow_run_id <> source_workflow_run_id",
            name=op.f("ck_workflow_run_replays_source_not_self"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(requested_scope) = 'object'",
            name=op.f("ck_workflow_run_replays_requested_scope_object"),
        ),
        sa.ForeignKeyConstraint(
            ["workflow_run_id"],
            ["workflow_runs.id"],
            name="fk_workflow_run_replays_run",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_workflow_run_id"],
            ["workflow_runs.id"],
            name="fk_workflow_run_replays_source_run",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("workflow_run_id", name="pk_workflow_run_replays"),
    )
    op.create_index(
        "ix_workflow_run_replays_source_workflow_run_id",
        "workflow_run_replays",
        ["source_workflow_run_id"],
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
    op.execute(
        f"CREATE TRIGGER {ROW_TRIGGER} BEFORE UPDATE OR DELETE ON "
        "workflow_run_replays FOR EACH ROW "
        f"EXECUTE FUNCTION {IMMUTABILITY_FUNCTION}()"
    )
    op.execute(
        f"CREATE TRIGGER {TRUNCATE_TRIGGER} BEFORE TRUNCATE ON "
        "workflow_run_replays FOR EACH STATEMENT "
        f"EXECUTE FUNCTION {IMMUTABILITY_FUNCTION}()"
    )


def downgrade() -> None:
    """Remove workflow-run replay lineage support."""
    op.execute(f"DROP TRIGGER {TRUNCATE_TRIGGER} ON workflow_run_replays")
    op.execute(f"DROP TRIGGER {ROW_TRIGGER} ON workflow_run_replays")
    op.execute(f"DROP FUNCTION {IMMUTABILITY_FUNCTION}()")
    op.drop_index(
        "ix_workflow_run_replays_source_workflow_run_id",
        table_name="workflow_run_replays",
    )
    op.drop_table("workflow_run_replays")
    workflow_replay_mode.drop(op.get_bind(), checkfirst=False)
