"""Create immutable workflow-run cancellation requests.

Revision ID: 0020_run_cancellation
Revises: 0019_dead_letter_redrive
Create Date: 2026-08-20
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0020_run_cancellation"
down_revision: str | None = "0019_dead_letter_redrive"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

IMMUTABILITY_FUNCTION = "reject_workflow_run_cancellation_request_mutation"
IMMUTABILITY_SQLSTATE = "TF007"
IMMUTABILITY_MESSAGE = "workflow run cancellation requests are immutable"
ROW_TRIGGER = "trg_workflow_run_cancellation_requests_reject_mutation"
TRUNCATE_TRIGGER = "trg_workflow_run_cancellation_requests_reject_truncate"


def upgrade() -> None:
    """Create one immutable cancellation intention per workflow run."""
    op.create_table(
        "workflow_run_cancellation_requests",
        sa.Column("workflow_run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "requested_by_principal_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("idempotency_key_digest", sa.String(length=64), nullable=False),
        sa.Column("request_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column(
            "requested_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("statement_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "idempotency_key_digest ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_workflow_run_cancellation_requests_key_digest_valid"),
        ),
        sa.CheckConstraint(
            "request_fingerprint ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_workflow_run_cancellation_requests_fingerprint_valid"),
        ),
        sa.CheckConstraint(
            "reason IS NULL OR length(btrim(reason)) BETWEEN 1 AND 2000",
            name=op.f("ck_workflow_run_cancellation_requests_reason_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["workflow_run_id"],
            ["workflow_runs.id"],
            name="fk_workflow_run_cancellation_requests_run",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["requested_by_principal_id"],
            ["api_principals.id"],
            name="fk_workflow_run_cancellation_requests_requester",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "workflow_run_id",
            name="pk_workflow_run_cancellation_requests",
        ),
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
        "workflow_run_cancellation_requests FOR EACH ROW "
        f"EXECUTE FUNCTION {IMMUTABILITY_FUNCTION}()"
    )
    op.execute(
        f"CREATE TRIGGER {TRUNCATE_TRIGGER} BEFORE TRUNCATE ON "
        "workflow_run_cancellation_requests FOR EACH STATEMENT "
        f"EXECUTE FUNCTION {IMMUTABILITY_FUNCTION}()"
    )


def downgrade() -> None:
    """Remove workflow-run cancellation request persistence."""
    op.execute(f"DROP TRIGGER {TRUNCATE_TRIGGER} ON workflow_run_cancellation_requests")
    op.execute(f"DROP TRIGGER {ROW_TRIGGER} ON workflow_run_cancellation_requests")
    op.execute(f"DROP FUNCTION {IMMUTABILITY_FUNCTION}()")
    op.drop_table("workflow_run_cancellation_requests")
