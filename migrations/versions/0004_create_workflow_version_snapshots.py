"""Create immutable workflow version snapshot storage.

Revision ID: 0004_versions
Revises: 0003_workflow_list
Create Date: 2026-08-05
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0004_versions"
down_revision: str | None = "0003_workflow_list"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create version metadata and complete graph snapshot tables."""
    op.create_table(
        "workflow_versions",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "workflow_definition_id", postgresql.UUID(as_uuid=True), nullable=False
        ),
        sa.Column("version_number", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "execution_policy",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "published_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "execution_policy IS NULL OR jsonb_typeof(execution_policy) = 'object'",
            name=op.f("ck_workflow_versions_execution_policy_object"),
        ),
        sa.CheckConstraint(
            "length(name) > 0",
            name=op.f("ck_workflow_versions_name_not_empty"),
        ),
        sa.CheckConstraint(
            "version_number > 0",
            name=op.f("ck_workflow_versions_version_number_positive"),
        ),
        sa.ForeignKeyConstraint(
            ["workflow_definition_id"],
            ["workflow_definitions.id"],
            name=op.f(
                "fk_workflow_versions_workflow_definition_id_workflow_definitions"
            ),
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_workflow_versions")),
        sa.UniqueConstraint(
            "workflow_definition_id",
            "version_number",
            name=op.f("uq_workflow_versions_workflow_definition_id_version_number"),
        ),
    )
    op.create_table(
        "workflow_version_steps",
        sa.Column("workflow_version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("step_identifier", sa.String(length=128), nullable=False),
        sa.Column("task_type", sa.String(length=128), nullable=False),
        sa.Column(
            "parameters", postgresql.JSONB(astext_type=sa.Text()), nullable=False
        ),
        sa.Column(
            "execution_policy",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.CheckConstraint(
            "execution_policy IS NULL OR jsonb_typeof(execution_policy) = 'object'",
            name=op.f("ck_workflow_version_steps_execution_policy_object"),
        ),
        sa.CheckConstraint(
            "length(step_identifier) > 0",
            name=op.f("ck_workflow_version_steps_identifier_not_empty"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(parameters) = 'object'",
            name=op.f("ck_workflow_version_steps_parameters_object"),
        ),
        sa.CheckConstraint(
            "length(task_type) > 0",
            name=op.f("ck_workflow_version_steps_task_type_not_empty"),
        ),
        sa.ForeignKeyConstraint(
            ["workflow_version_id"],
            ["workflow_versions.id"],
            name=op.f(
                "fk_workflow_version_steps_workflow_version_id_workflow_versions"
            ),
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "workflow_version_id",
            "step_identifier",
            name=op.f("pk_workflow_version_steps"),
        ),
    )
    op.create_table(
        "workflow_version_dependencies",
        sa.Column("workflow_version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("predecessor_step_identifier", sa.String(length=128), nullable=False),
        sa.Column("successor_step_identifier", sa.String(length=128), nullable=False),
        sa.CheckConstraint(
            "predecessor_step_identifier <> successor_step_identifier",
            name=op.f("ck_workflow_version_dependencies_steps_distinct"),
        ),
        sa.ForeignKeyConstraint(
            ["workflow_version_id"],
            ["workflow_versions.id"],
            name=op.f(
                "fk_workflow_version_dependencies_workflow_version_id_workflow_versions"
            ),
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["workflow_version_id", "predecessor_step_identifier"],
            [
                "workflow_version_steps.workflow_version_id",
                "workflow_version_steps.step_identifier",
            ],
            name=op.f(
                "fk_workflow_version_dependencies_workflow_version_id_"
                "predecessor_step_identifier_workflow_version_steps"
            ),
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["workflow_version_id", "successor_step_identifier"],
            [
                "workflow_version_steps.workflow_version_id",
                "workflow_version_steps.step_identifier",
            ],
            name=op.f(
                "fk_workflow_version_dependencies_workflow_version_id_"
                "successor_step_identifier_workflow_version_steps"
            ),
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "workflow_version_id",
            "predecessor_step_identifier",
            "successor_step_identifier",
            name=op.f("pk_workflow_version_dependencies"),
        ),
    )
    op.create_index(
        op.f(
            "ix_workflow_version_dependencies_workflow_version_id_"
            "successor_step_identifier"
        ),
        "workflow_version_dependencies",
        ["workflow_version_id", "successor_step_identifier"],
        unique=False,
    )


def downgrade() -> None:
    """Remove workflow version snapshot storage."""
    op.drop_index(
        op.f(
            "ix_workflow_version_dependencies_workflow_version_id_"
            "successor_step_identifier"
        ),
        table_name="workflow_version_dependencies",
    )
    op.drop_table("workflow_version_dependencies")
    op.drop_table("workflow_version_steps")
    op.drop_table("workflow_versions")
