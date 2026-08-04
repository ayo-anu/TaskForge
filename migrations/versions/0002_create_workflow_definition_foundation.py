"""Create the mutable workflow definition and draft graph foundation.

Revision ID: 0002_workflows
Revises: 0001_identity
Create Date: 2026-08-04
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002_workflows"
down_revision: str | None = "0001_identity"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

workflow_definition_status = postgresql.ENUM(
    "draft",
    "enabled",
    "disabled",
    "archived",
    name="workflow_definition_status",
    create_type=False,
)


def upgrade() -> None:
    """Create workflow definitions, draft steps, and dependency edges."""
    workflow_definition_status.create(op.get_bind(), checkfirst=False)
    op.create_table(
        "workflow_definitions",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("owner_principal_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "status",
            workflow_definition_status,
            server_default="draft",
            nullable=False,
        ),
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
            "length(name) > 0",
            name=op.f("ck_workflow_definitions_name_not_empty"),
        ),
        sa.ForeignKeyConstraint(
            ["owner_principal_id"],
            ["api_principals.id"],
            name=op.f("fk_workflow_definitions_owner_principal_id_api_principals"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_workflow_definitions")),
    )
    op.create_index(
        op.f("ix_workflow_definitions_owner_principal_id"),
        "workflow_definitions",
        ["owner_principal_id"],
        unique=False,
    )
    op.create_table(
        "workflow_draft_steps",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "workflow_definition_id", postgresql.UUID(as_uuid=True), nullable=False
        ),
        sa.Column("step_identifier", sa.String(length=128), nullable=False),
        sa.Column("task_type", sa.String(length=128), nullable=False),
        sa.Column(
            "parameters", postgresql.JSONB(astext_type=sa.Text()), nullable=False
        ),
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
            "length(step_identifier) > 0",
            name=op.f("ck_workflow_draft_steps_identifier_not_empty"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(parameters) = 'object'",
            name=op.f("ck_workflow_draft_steps_parameters_object"),
        ),
        sa.CheckConstraint(
            "length(task_type) > 0",
            name=op.f("ck_workflow_draft_steps_task_type_not_empty"),
        ),
        sa.ForeignKeyConstraint(
            ["workflow_definition_id"],
            ["workflow_definitions.id"],
            name=op.f(
                "fk_workflow_draft_steps_workflow_definition_id_workflow_definitions"
            ),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_workflow_draft_steps")),
        sa.UniqueConstraint(
            "workflow_definition_id",
            "id",
            name=op.f("uq_workflow_draft_steps_workflow_definition_id_id"),
        ),
        sa.UniqueConstraint(
            "workflow_definition_id",
            "step_identifier",
            name=op.f("uq_workflow_draft_steps_workflow_definition_id_step_identifier"),
        ),
    )
    op.create_index(
        op.f("ix_workflow_draft_steps_workflow_definition_id"),
        "workflow_draft_steps",
        ["workflow_definition_id"],
        unique=False,
    )
    op.create_table(
        "workflow_draft_dependencies",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "workflow_definition_id", postgresql.UUID(as_uuid=True), nullable=False
        ),
        sa.Column("predecessor_step_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("successor_step_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "predecessor_step_id <> successor_step_id",
            name=op.f("ck_workflow_draft_dependencies_steps_distinct"),
        ),
        sa.ForeignKeyConstraint(
            ["workflow_definition_id"],
            ["workflow_definitions.id"],
            name=op.f(
                "fk_workflow_draft_dependencies_workflow_definition_id_workflow_definitions"
            ),
        ),
        sa.ForeignKeyConstraint(
            ["workflow_definition_id", "predecessor_step_id"],
            ["workflow_draft_steps.workflow_definition_id", "workflow_draft_steps.id"],
            name=op.f(
                "fk_workflow_draft_dependencies_workflow_definition_id_"
                "predecessor_step_id_workflow_draft_steps"
            ),
        ),
        sa.ForeignKeyConstraint(
            ["workflow_definition_id", "successor_step_id"],
            ["workflow_draft_steps.workflow_definition_id", "workflow_draft_steps.id"],
            name=op.f(
                "fk_workflow_draft_dependencies_workflow_definition_id_"
                "successor_step_id_workflow_draft_steps"
            ),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_workflow_draft_dependencies")),
        sa.UniqueConstraint(
            "workflow_definition_id",
            "predecessor_step_id",
            "successor_step_id",
            name=op.f(
                "uq_workflow_draft_dependencies_workflow_definition_id_"
                "predecessor_step_id_successor_step_id"
            ),
        ),
    )
    op.create_index(
        op.f(
            "ix_workflow_draft_dependencies_workflow_definition_id_predecessor_step_id"
        ),
        "workflow_draft_dependencies",
        ["workflow_definition_id", "predecessor_step_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_workflow_draft_dependencies_workflow_definition_id_successor_step_id"),
        "workflow_draft_dependencies",
        ["workflow_definition_id", "successor_step_id"],
        unique=False,
    )


def downgrade() -> None:
    """Remove workflow definition tables and their enum type."""
    op.drop_index(
        op.f("ix_workflow_draft_dependencies_workflow_definition_id_successor_step_id"),
        table_name="workflow_draft_dependencies",
    )
    op.drop_index(
        op.f(
            "ix_workflow_draft_dependencies_workflow_definition_id_predecessor_step_id"
        ),
        table_name="workflow_draft_dependencies",
    )
    op.drop_table("workflow_draft_dependencies")
    op.drop_index(
        op.f("ix_workflow_draft_steps_workflow_definition_id"),
        table_name="workflow_draft_steps",
    )
    op.drop_table("workflow_draft_steps")
    op.drop_index(
        op.f("ix_workflow_definitions_owner_principal_id"),
        table_name="workflow_definitions",
    )
    op.drop_table("workflow_definitions")
    workflow_definition_status.drop(op.get_bind(), checkfirst=False)
