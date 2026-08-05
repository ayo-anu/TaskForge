"""Protect immutable workflow version snapshots from mutation.

Revision ID: 0005_version_immutability
Revises: 0004_versions
Create Date: 2026-08-05
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0005_version_immutability"
down_revision: str | None = "0004_versions"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

FUNCTION_NAME = "reject_workflow_version_snapshot_mutation"
IMMUTABLE_SQLSTATE = "TF001"
IMMUTABLE_MESSAGE = "workflow version snapshots are immutable"

TRIGGERS = (
    ("workflow_versions", "trg_workflow_versions_reject_mutation"),
    ("workflow_version_steps", "trg_workflow_version_steps_reject_mutation"),
    (
        "workflow_version_dependencies",
        "trg_workflow_version_dependencies_reject_mutation",
    ),
)


def upgrade() -> None:
    """Reject updates and deletes against committed version snapshots."""
    op.execute(
        f"""
        CREATE FUNCTION {FUNCTION_NAME}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        BEGIN
            RAISE EXCEPTION USING
                ERRCODE = '{IMMUTABLE_SQLSTATE}',
                MESSAGE = '{IMMUTABLE_MESSAGE}';
        END;
        $function$
        """
    )
    for table_name, trigger_name in TRIGGERS:
        op.execute(
            f"""
            CREATE TRIGGER {trigger_name}
            BEFORE UPDATE OR DELETE ON {table_name}
            FOR EACH ROW
            EXECUTE FUNCTION {FUNCTION_NAME}()
            """
        )


def downgrade() -> None:
    """Restore the prior mutable snapshot-table behavior."""
    for table_name, trigger_name in reversed(TRIGGERS):
        op.execute(f"DROP TRIGGER {trigger_name} ON {table_name}")
    op.execute(f"DROP FUNCTION {FUNCTION_NAME}()")
