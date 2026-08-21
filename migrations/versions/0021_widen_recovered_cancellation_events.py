"""Allow recovered cancellation outcomes in task-result history.

Revision ID: 0021_recovered_cancellation
Revises: 0020_run_cancellation
Create Date: 2026-08-21
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0021_recovered_cancellation"
down_revision: str | None = "0020_run_cancellation"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

RESULT_SHAPE_CONSTRAINT = "ck_task_result_events_result_shape_valid"
IMMUTABILITY_FUNCTION = "reject_task_result_history_mutation"
MUTATION_TRIGGER = "trg_task_result_events_reject_mutation"

OLD_RESULT_SHAPE = (
    "(event_type = 'result_recovered' AND result_kind = 'retryable_failure' "
    "AND failure_kind = 'claim_expired') OR "
    "(event_type IN ('result_accepted', 'result_replayed', "
    "'result_conflict_rejected', 'result_stale_rejected') AND "
    "((result_kind = 'success' AND failure_kind IS NULL) OR "
    "(result_kind = 'retryable_failure' AND failure_kind IN "
    "('handler_reported', 'handler_exception', 'execution_timeout')) OR "
    "(result_kind = 'permanent_failure' AND failure_kind = "
    "'handler_reported') OR "
    "(result_kind = 'cancellation' AND failure_kind IS NULL)))"
)
NEW_RESULT_SHAPE = (
    "(event_type = 'result_recovered' AND "
    "((result_kind = 'retryable_failure' AND failure_kind = 'claim_expired') OR "
    "(result_kind = 'cancellation' AND failure_kind IS NULL))) OR "
    "(event_type IN ('result_accepted', 'result_replayed', "
    "'result_conflict_rejected', 'result_stale_rejected') AND "
    "((result_kind = 'success' AND failure_kind IS NULL) OR "
    "(result_kind = 'retryable_failure' AND failure_kind IN "
    "('handler_reported', 'handler_exception', 'execution_timeout')) OR "
    "(result_kind = 'permanent_failure' AND failure_kind = "
    "'handler_reported') OR "
    "(result_kind = 'cancellation' AND failure_kind IS NULL)))"
)


def upgrade() -> None:
    op.drop_constraint(
        op.f(RESULT_SHAPE_CONSTRAINT), "task_result_events", type_="check"
    )
    op.create_check_constraint(
        op.f(RESULT_SHAPE_CONSTRAINT), "task_result_events", NEW_RESULT_SHAPE
    )


def downgrade() -> None:
    op.execute(f"DROP TRIGGER {MUTATION_TRIGGER} ON task_result_events")
    op.execute(
        "DELETE FROM task_result_events WHERE event_type = 'result_recovered' "
        "AND result_kind = 'cancellation'"
    )
    op.drop_constraint(
        op.f(RESULT_SHAPE_CONSTRAINT), "task_result_events", type_="check"
    )
    op.create_check_constraint(
        op.f(RESULT_SHAPE_CONSTRAINT), "task_result_events", OLD_RESULT_SHAPE
    )
    op.execute(
        f"CREATE TRIGGER {MUTATION_TRIGGER} BEFORE UPDATE OR DELETE ON "
        "task_result_events FOR EACH ROW "
        f"EXECUTE FUNCTION {IMMUTABILITY_FUNCTION}()"
    )
