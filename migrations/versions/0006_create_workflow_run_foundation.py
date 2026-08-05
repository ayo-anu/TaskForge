"""Create the workflow run persistence foundation.

Revision ID: 0006_run_foundation
Revises: 0005_version_immutability
Create Date: 2026-08-05
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0006_run_foundation"
down_revision: str | None = "0005_version_immutability"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

workflow_run_status = postgresql.ENUM(
    "pending",
    "running",
    "cancelling",
    "succeeded",
    "failed",
    "cancelled",
    name="workflow_run_status",
    create_type=False,
)
task_run_status = postgresql.ENUM(
    "blocked",
    "runnable",
    "dispatched",
    "claimed",
    "running",
    "retry_scheduled",
    "succeeded",
    "failed",
    "skipped",
    "cancelled",
    name="task_run_status",
    create_type=False,
)

IMMUTABILITY_FUNCTION = "reject_workflow_run_creation_snapshot_mutation"
IMMUTABILITY_SQLSTATE = "TF002"
IMMUTABILITY_MESSAGE = "workflow run creation snapshots are immutable"
IMMUTABLE_TABLE_TRIGGERS = (
    (
        "workflow_run_inputs",
        "trg_workflow_run_inputs_reject_mutation",
        "trg_workflow_run_inputs_reject_truncate",
    ),
    (
        "workflow_run_idempotency",
        "trg_workflow_run_idempotency_reject_mutation",
        "trg_workflow_run_idempotency_reject_truncate",
    ),
)


def upgrade() -> None:
    """Create constrained run, task, input, and idempotency storage."""
    workflow_run_status.create(op.get_bind(), checkfirst=False)
    task_run_status.create(op.get_bind(), checkfirst=False)

    op.create_unique_constraint(
        "uq_workflow_versions_workflow_definition_id_id",
        "workflow_versions",
        ["workflow_definition_id", "id"],
    )
    op.create_table(
        "workflow_runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "workflow_definition_id", postgresql.UUID(as_uuid=True), nullable=False
        ),
        sa.Column("workflow_version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "requested_by_principal_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("status", workflow_run_status, nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["requested_by_principal_id"],
            ["api_principals.id"],
            name=("fk_workflow_runs_requested_by_principal_id_api_principals"),
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["workflow_definition_id", "workflow_version_id"],
            ["workflow_versions.workflow_definition_id", "workflow_versions.id"],
            name="fk_workflow_runs_definition_version",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_workflow_runs"),
        sa.UniqueConstraint(
            "id",
            "workflow_version_id",
            name="uq_workflow_runs_id_workflow_version_id",
        ),
        sa.UniqueConstraint(
            "id",
            "requested_by_principal_id",
            "workflow_definition_id",
            name="uq_workflow_runs_id_requester_definition",
        ),
    )
    op.create_index(
        "ix_workflow_runs_workflow_definition_id_created_at_id",
        "workflow_runs",
        [
            "workflow_definition_id",
            sa.text("created_at DESC"),
            sa.text("id DESC"),
        ],
        unique=False,
    )
    op.create_index(
        "ix_workflow_runs_workflow_definition_id_workflow_version_id",
        "workflow_runs",
        ["workflow_definition_id", "workflow_version_id"],
        unique=False,
    )
    op.create_table(
        "workflow_run_inputs",
        sa.Column("workflow_run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "input_references",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object'",
            name="ck_workflow_run_inputs_payload_object",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(input_references) = 'object'",
            name="ck_workflow_run_inputs_input_references_object",
        ),
        sa.ForeignKeyConstraint(
            ["workflow_run_id"],
            ["workflow_runs.id"],
            name="fk_workflow_run_inputs_workflow_run_id_workflow_runs",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "workflow_run_id",
            name="pk_workflow_run_inputs",
        ),
    )
    op.create_table(
        "task_runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("workflow_run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("workflow_version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("step_identifier", sa.String(length=128), nullable=False),
        sa.Column("status", task_run_status, nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "length(btrim(step_identifier)) > 0",
            name="ck_task_runs_step_identifier_not_blank",
        ),
        sa.ForeignKeyConstraint(
            ["workflow_run_id", "workflow_version_id"],
            ["workflow_runs.id", "workflow_runs.workflow_version_id"],
            name="fk_task_runs_run_version",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["workflow_version_id", "step_identifier"],
            [
                "workflow_version_steps.workflow_version_id",
                "workflow_version_steps.step_identifier",
            ],
            name="fk_task_runs_version_step",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_task_runs"),
        sa.UniqueConstraint(
            "workflow_run_id",
            "step_identifier",
            name="uq_task_runs_workflow_run_id_step_identifier",
        ),
    )
    op.create_index(
        "ix_task_runs_workflow_version_id_step_identifier",
        "task_runs",
        ["workflow_version_id", "step_identifier"],
        unique=False,
    )
    op.create_table(
        "workflow_run_idempotency",
        sa.Column("principal_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "workflow_definition_id", postgresql.UUID(as_uuid=True), nullable=False
        ),
        sa.Column("idempotency_key_digest", sa.String(length=256), nullable=False),
        sa.Column("request_fingerprint", sa.String(length=256), nullable=False),
        sa.Column("workflow_run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "length(btrim(idempotency_key_digest)) > 0",
            name="ck_workflow_run_idempotency_digest_not_blank",
        ),
        sa.CheckConstraint(
            "length(btrim(request_fingerprint)) > 0",
            name="ck_workflow_run_idempotency_fingerprint_not_blank",
        ),
        sa.ForeignKeyConstraint(
            ["workflow_run_id", "principal_id", "workflow_definition_id"],
            [
                "workflow_runs.id",
                "workflow_runs.requested_by_principal_id",
                "workflow_runs.workflow_definition_id",
            ],
            name="fk_workflow_run_idempotency_run_scope",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "principal_id",
            "workflow_definition_id",
            "idempotency_key_digest",
            name="pk_workflow_run_idempotency",
        ),
        sa.UniqueConstraint(
            "workflow_run_id",
            name="uq_workflow_run_idempotency_workflow_run_id",
        ),
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
    for table_name, row_trigger, truncate_trigger in IMMUTABLE_TABLE_TRIGGERS:
        op.execute(
            f"""
            CREATE TRIGGER {row_trigger}
            BEFORE UPDATE OR DELETE ON {table_name}
            FOR EACH ROW
            EXECUTE FUNCTION {IMMUTABILITY_FUNCTION}()
            """
        )
        op.execute(
            f"""
            CREATE TRIGGER {truncate_trigger}
            BEFORE TRUNCATE ON {table_name}
            FOR EACH STATEMENT
            EXECUTE FUNCTION {IMMUTABILITY_FUNCTION}()
            """
        )


def downgrade() -> None:
    """Remove the workflow run persistence foundation."""
    for table_name, row_trigger, truncate_trigger in reversed(IMMUTABLE_TABLE_TRIGGERS):
        op.execute(f"DROP TRIGGER {truncate_trigger} ON {table_name}")
        op.execute(f"DROP TRIGGER {row_trigger} ON {table_name}")
    op.execute(f"DROP FUNCTION {IMMUTABILITY_FUNCTION}()")

    op.drop_table("workflow_run_idempotency")
    op.drop_index(
        "ix_task_runs_workflow_version_id_step_identifier",
        table_name="task_runs",
    )
    op.drop_table("task_runs")
    op.drop_table("workflow_run_inputs")
    op.drop_index(
        "ix_workflow_runs_workflow_definition_id_workflow_version_id",
        table_name="workflow_runs",
    )
    op.drop_index(
        "ix_workflow_runs_workflow_definition_id_created_at_id",
        table_name="workflow_runs",
    )
    op.drop_table("workflow_runs")
    op.drop_constraint(
        "uq_workflow_versions_workflow_definition_id_id",
        "workflow_versions",
        type_="unique",
    )

    task_run_status.drop(op.get_bind(), checkfirst=False)
    workflow_run_status.drop(op.get_bind(), checkfirst=False)
