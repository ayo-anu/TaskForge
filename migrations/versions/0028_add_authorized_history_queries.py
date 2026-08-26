"""Add authorized history query privileges and indexes.

Revision ID: 0028_authorized_history_queries
Revises: 0027_enforce_history_privileges
Create Date: 2026-08-26
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0028_authorized_history_queries"
down_revision: str | None = "0027_enforce_history_privileges"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INDEXES = {
    "ix_audit_records_occurred_at_id": "ON audit_records (occurred_at DESC, id DESC)",
    "ix_audit_records_resource_occurred_at_id": "ON audit_records (resource_type, resource_id, occurred_at DESC, id DESC)",
    "ix_audit_records_action_occurred_at_id": "ON audit_records (action, occurred_at DESC, id DESC)",
    "ix_audit_records_correlation_occurred_at_id": "ON audit_records (correlation_id, occurred_at DESC, id DESC) WHERE correlation_id IS NOT NULL",
    "ix_audit_records_actor_occurred_at_id": "ON audit_records (actor_kind, api_principal_id, worker_identity_id, system_component, occurred_at DESC, id DESC)",
    "ix_audit_records_rejected_reason_occurred_at_id": "ON audit_records (reason_code, occurred_at DESC, id DESC) WHERE outcome = 'rejected'",
    "ix_workflow_run_execution_events_run_occurred_at_id": "ON workflow_run_execution_events (workflow_run_id, occurred_at DESC, id DESC)",
    "ix_task_claim_events_attempt_occurred_at_id": "ON task_claim_events (task_attempt_id, occurred_at DESC, id DESC)",
    "ix_worker_heartbeats_identity_received_session_sequence": "ON worker_heartbeats (worker_identity_id, received_at DESC, worker_session_id DESC, sequence DESC) WHERE worker_identity_id IS NOT NULL",
    "ix_workflow_run_replays_source_created_at_run": "ON workflow_run_replays (source_workflow_run_id, created_at DESC, workflow_run_id DESC)",
}
_LEGACY_REPLAY = "ix_workflow_run_replays_source_workflow_run_id"


def _normalized(value: str) -> str:
    return " ".join(
        value.replace("public.", "")
        .replace(" USING btree", "")
        .replace("::text", "")
        .replace("(", " ")
        .replace(")", " ")
        .split()
    ).lower()


def _ensure_index(name: str, definition: str) -> None:
    bind = op.get_bind()
    row = (
        bind.execute(
            sa.text(
                "SELECT i.indisvalid, pg_get_indexdef(i.indexrelid) definition FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE c.relname=:name AND c.relnamespace='public'::regnamespace"
            ),
            {"name": name},
        )
        .mappings()
        .one_or_none()
    )
    create = f"CREATE INDEX {name} {definition}"
    if row is not None and row["indisvalid"]:
        if _normalized(row["definition"]) != _normalized(create):
            raise RuntimeError(f"valid index {name} has an unexpected definition")
        return
    if row is not None:
        op.execute(f"DROP INDEX CONCURRENTLY {name}")
    op.execute(f"CREATE INDEX CONCURRENTLY {name} {definition}")


def upgrade() -> None:
    context = op.get_context()
    with context.autocommit_block():
        for name, definition in _INDEXES.items():
            _ensure_index(name, definition)
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_LEGACY_REPLAY}")
    op.execute("GRANT SELECT ON TABLE audit_records TO taskforge_runtime")


def downgrade() -> None:
    op.execute("REVOKE SELECT ON TABLE audit_records FROM taskforge_runtime")
    context = op.get_context()
    with context.autocommit_block():
        for name in reversed(_INDEXES):
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")
        op.execute(
            "CREATE INDEX CONCURRENTLY "
            + _LEGACY_REPLAY
            + " ON workflow_run_replays (source_workflow_run_id)"
        )
