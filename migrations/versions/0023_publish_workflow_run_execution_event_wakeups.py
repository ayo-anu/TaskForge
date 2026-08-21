"""Publish ephemeral workflow-run execution-event wake-ups.

Revision ID: 0023_execution_event_wakeups
Revises: 0022_run_execution_events
Create Date: 2026-08-21
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0023_execution_event_wakeups"
down_revision: str | None = "0022_run_execution_events"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

WAKEUP_CHANNEL = "taskforge_workflow_run_execution_events"
WAKEUP_FUNCTION = "publish_workflow_run_execution_event_wakeup"
WAKEUP_TRIGGER = "trg_workflow_run_execution_events_publish_wakeup"


def upgrade() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {WAKEUP_FUNCTION}()
        RETURNS trigger LANGUAGE plpgsql AS $function$
        BEGIN
            PERFORM pg_notify(
                '{WAKEUP_CHANNEL}',
                json_build_object(
                    'workflow_run_id', NEW.workflow_run_id::text
                )::text
            );
            RETURN NEW;
        END;
        $function$
        """
    )
    op.execute(
        f"CREATE TRIGGER {WAKEUP_TRIGGER} "
        "AFTER INSERT ON workflow_run_execution_events FOR EACH ROW "
        f"EXECUTE FUNCTION {WAKEUP_FUNCTION}()"
    )


def downgrade() -> None:
    op.execute(f"DROP TRIGGER {WAKEUP_TRIGGER} ON workflow_run_execution_events")
    op.execute(f"DROP FUNCTION {WAKEUP_FUNCTION}()")
