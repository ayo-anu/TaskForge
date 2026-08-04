"""Add the stable owner-scoped workflow list index.

Revision ID: 0003_workflow_list
Revises: 0002_workflows
Create Date: 2026-08-04
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003_workflow_list"
down_revision: str | None = "0002_workflows"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Replace the owner-only index with the stable keyset-order index."""
    # owner_principal_id remains the leading B-tree column, so this index
    # supports both the current owner predicate and its stable list ordering.
    op.drop_index(
        op.f("ix_workflow_definitions_owner_principal_id"),
        table_name="workflow_definitions",
    )
    op.create_index(
        "ix_workflow_definitions_owner_created_id",
        "workflow_definitions",
        [
            "owner_principal_id",
            sa.text("created_at DESC"),
            sa.text("id DESC"),
        ],
        unique=False,
    )


def downgrade() -> None:
    """Restore the pre-pagination owner-only index."""
    op.drop_index(
        "ix_workflow_definitions_owner_created_id",
        table_name="workflow_definitions",
    )
    op.create_index(
        op.f("ix_workflow_definitions_owner_principal_id"),
        "workflow_definitions",
        ["owner_principal_id"],
        unique=False,
    )
