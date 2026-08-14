"""Audit claim-expiry recovery in immutable task-result history.

Revision ID: 0017_recovery_result_events
Revises: 0016_claim_expired_result
Create Date: 2026-08-14
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0017_recovery_result_events"
down_revision: str | None = "0016_claim_expired_result"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

EVENT_TYPE_CONSTRAINT = "ck_task_result_events_event_type_valid"
RESULT_SHAPE_CONSTRAINT = "ck_task_result_events_result_shape_valid"
RECOVERY_UNIQUENESS = "uq_task_result_events_recovered_generation"
IMMUTABILITY_FUNCTION = "reject_task_result_history_mutation"
MUTATION_TRIGGER = "trg_task_result_events_reject_mutation"

OLD_EVENT_TYPES = (
    "event_type IN ('result_accepted', 'result_replayed', "
    "'result_conflict_rejected', 'result_stale_rejected')"
)
NEW_EVENT_TYPES = OLD_EVENT_TYPES[:-1] + ", 'result_recovered')"
OLD_RESULT_SHAPE = (
    "(result_kind = 'success' AND failure_kind IS NULL) OR "
    "(result_kind = 'retryable_failure' AND failure_kind IN "
    "('handler_reported', 'handler_exception', 'execution_timeout')) OR "
    "(result_kind = 'permanent_failure' AND failure_kind = 'handler_reported') OR "
    "(result_kind = 'cancellation' AND failure_kind IS NULL)"
)
NEW_RESULT_SHAPE = (
    "(event_type = 'result_recovered' AND result_kind = 'retryable_failure' "
    "AND failure_kind = 'claim_expired') OR "
    "(event_type IN ('result_accepted', 'result_replayed', "
    "'result_conflict_rejected', 'result_stale_rejected') AND ("
    f"{OLD_RESULT_SHAPE}))"
)


def upgrade() -> None:
    op.drop_constraint(op.f(EVENT_TYPE_CONSTRAINT), "task_result_events", type_="check")
    op.drop_constraint(
        op.f(RESULT_SHAPE_CONSTRAINT), "task_result_events", type_="check"
    )
    op.create_check_constraint(
        op.f(EVENT_TYPE_CONSTRAINT), "task_result_events", NEW_EVENT_TYPES
    )
    op.create_check_constraint(
        op.f(RESULT_SHAPE_CONSTRAINT), "task_result_events", NEW_RESULT_SHAPE
    )
    op.execute(
        """
        DO $block$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM task_attempt_results AS result
                JOIN task_attempt_claims AS claim
                  ON claim.task_attempt_id = result.task_attempt_id
                 AND claim.generation = result.claim_generation
                WHERE result.result_kind = 'retryable_failure'
                  AND result.failure_kind = 'claim_expired'
                  AND claim.terminated_at IS NULL
            ) THEN
                RAISE EXCEPTION 'claim_expired result has an active claim';
            END IF;
        END
        $block$
        """
    )
    op.execute(
        """
        INSERT INTO task_result_events (
            id,
            task_attempt_id,
            claim_generation,
            worker_session_id,
            dispatch_id,
            event_type,
            result_kind,
            failure_kind,
            result_fingerprint,
            occurred_at
        )
        SELECT
            md5(
                'taskforge:result_recovered:' || result.task_attempt_id::text ||
                ':' || result.claim_generation::text
            )::uuid,
            result.task_attempt_id,
            result.claim_generation,
            claim.worker_session_id,
            result.dispatch_id,
            'result_recovered',
            result.result_kind,
            result.failure_kind,
            result.result_fingerprint,
            result.completed_at
        FROM task_attempt_results AS result
        JOIN task_attempt_claims AS claim
          ON claim.task_attempt_id = result.task_attempt_id
         AND claim.generation = result.claim_generation
        WHERE result.result_kind = 'retryable_failure'
          AND result.failure_kind = 'claim_expired'
          AND claim.terminated_at IS NOT NULL
        """
    )
    op.create_index(
        RECOVERY_UNIQUENESS,
        "task_result_events",
        ["task_attempt_id", "claim_generation"],
        unique=True,
        postgresql_where=sa.text("event_type = 'result_recovered'"),
    )


def downgrade() -> None:
    op.execute(f"DROP TRIGGER {MUTATION_TRIGGER} ON task_result_events")
    op.execute("DELETE FROM task_result_events WHERE event_type = 'result_recovered'")
    op.drop_index(RECOVERY_UNIQUENESS, table_name="task_result_events")
    op.drop_constraint(op.f(EVENT_TYPE_CONSTRAINT), "task_result_events", type_="check")
    op.drop_constraint(
        op.f(RESULT_SHAPE_CONSTRAINT), "task_result_events", type_="check"
    )
    op.create_check_constraint(
        op.f(EVENT_TYPE_CONSTRAINT), "task_result_events", OLD_EVENT_TYPES
    )
    op.create_check_constraint(
        op.f(RESULT_SHAPE_CONSTRAINT), "task_result_events", OLD_RESULT_SHAPE
    )
    op.execute(
        f"CREATE TRIGGER {MUTATION_TRIGGER} BEFORE UPDATE OR DELETE ON "
        "task_result_events FOR EACH ROW "
        f"EXECUTE FUNCTION {IMMUTABILITY_FUNCTION}()"
    )
