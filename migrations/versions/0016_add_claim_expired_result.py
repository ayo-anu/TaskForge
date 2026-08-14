"""Allow recovered claims to close an attempt with an explicit failure kind.

Revision ID: 0016_claim_expired_result
Revises: 0015_task_retry_events
Create Date: 2026-08-14
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0016_claim_expired_result"
down_revision: str | None = "0015_task_retry_events"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

CONSTRAINT = "ck_task_attempt_results_result_shape_valid"
OLD_SHAPE = (
    "(result_kind = 'success' AND failure_kind IS NULL) OR "
    "(result_kind = 'retryable_failure' AND failure_kind IN "
    "('handler_reported', 'handler_exception', 'execution_timeout')) OR "
    "(result_kind = 'permanent_failure' AND failure_kind = 'handler_reported') OR "
    "(result_kind = 'cancellation' AND failure_kind IS NULL)"
)
NEW_SHAPE = OLD_SHAPE.replace(
    "'execution_timeout')", "'execution_timeout', 'claim_expired')"
)


def upgrade() -> None:
    op.drop_constraint(op.f(CONSTRAINT), "task_attempt_results", type_="check")
    op.create_check_constraint(op.f(CONSTRAINT), "task_attempt_results", NEW_SHAPE)


def downgrade() -> None:
    op.drop_constraint(op.f(CONSTRAINT), "task_attempt_results", type_="check")
    op.create_check_constraint(op.f(CONSTRAINT), "task_attempt_results", OLD_SHAPE)
