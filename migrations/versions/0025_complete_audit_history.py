"""Complete prospective append-only audit and actor provenance.

Revision ID: 0025_complete_audit_history
Revises: 0024_run_replay_lineage
Create Date: 2026-08-25
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0025_complete_audit_history"
down_revision: str | None = "0024_run_replay_lineage"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

AUDIT_FUNCTION = "reject_audit_record_mutation"
HEARTBEAT_FUNCTION = "reject_worker_heartbeat_mutation"


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_worker_sessions_id_worker_identity_id",
        "worker_sessions",
        ["id", "worker_identity_id"],
    )
    op.create_table(
        "audit_records",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("actor_kind", sa.String(24), nullable=False),
        sa.Column("api_principal_id", postgresql.UUID(as_uuid=True)),
        sa.Column("worker_identity_id", postgresql.UUID(as_uuid=True)),
        sa.Column("worker_session_id", postgresql.UUID(as_uuid=True)),
        sa.Column("system_component", sa.String(32)),
        sa.Column("action", sa.String(128), nullable=False),
        sa.Column("outcome", sa.String(16), nullable=False),
        sa.Column("reason_code", sa.String(128)),
        sa.Column("resource_type", sa.String(64), nullable=False),
        sa.Column("resource_id", postgresql.UUID(as_uuid=True)),
        sa.Column("correlation_id", sa.String(128)),
        sa.Column(
            "diagnostic_provenance",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "occurred_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("statement_timestamp()"),
        ),
        sa.CheckConstraint(
            "actor_kind IN ('api_principal','worker','system')",
            name="ck_audit_records_actor_kind_valid",
        ),
        sa.CheckConstraint(
            "outcome IN ('accepted','rejected')", name="ck_audit_records_outcome_valid"
        ),
        sa.CheckConstraint(
            "action ~ '^[a-z][a-z0-9_.-]{0,127}$' AND "
            "resource_type ~ '^[a-z][a-z0-9_.-]{0,63}$' AND "
            "(reason_code IS NULL OR reason_code ~ '^[a-z][a-z0-9_.-]{0,127}$') AND "
            "(system_component IS NULL OR system_component ~ '^[a-z][a-z0-9_.-]{0,31}$')",
            name="ck_audit_records_names_valid",
        ),
        sa.CheckConstraint(
            "(outcome='accepted' AND reason_code IS NULL) OR (outcome='rejected' AND reason_code IS NOT NULL)",
            name="ck_audit_records_outcome_reason_valid",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(diagnostic_provenance)='object' AND octet_length(convert_to(diagnostic_provenance::text, 'UTF8')) <= 2048",
            name="ck_audit_records_provenance_valid",
        ),
        sa.CheckConstraint(
            "correlation_id IS NULL OR (length(correlation_id) BETWEEN 1 AND 128 AND correlation_id !~ '[^ -~]')",
            name="ck_audit_records_correlation_valid",
        ),
        sa.CheckConstraint(
            "(actor_kind='api_principal' AND api_principal_id IS NOT NULL AND worker_identity_id IS NULL AND worker_session_id IS NULL AND system_component IS NULL) OR (actor_kind='worker' AND api_principal_id IS NULL AND worker_identity_id IS NOT NULL AND system_component IS NULL) OR (actor_kind='system' AND api_principal_id IS NULL AND worker_identity_id IS NULL AND worker_session_id IS NULL AND system_component IS NOT NULL)",
            name="ck_audit_records_actor_shape_valid",
        ),
        sa.ForeignKeyConstraint(
            ["api_principal_id"],
            ["api_principals.id"],
            name="fk_audit_records_api_principal",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["worker_identity_id"],
            ["worker_identities.id"],
            name="fk_audit_records_worker_identity",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["worker_session_id", "worker_identity_id"],
            ["worker_sessions.id", "worker_sessions.worker_identity_id"],
            name="fk_audit_records_worker_session_identity",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_audit_records"),
    )
    op.add_column(
        "task_claim_events",
        sa.Column("worker_session_id", postgresql.UUID(as_uuid=True)),
    )
    for table in ("task_claim_events", "task_result_events"):
        op.add_column(
            table, sa.Column("worker_identity_id", postgresql.UUID(as_uuid=True))
        )
        op.add_column(table, sa.Column("correlation_id", sa.String(128)))
        op.create_foreign_key(
            f"fk_{table}_worker_session_identity",
            table,
            "worker_sessions",
            ["worker_session_id", "worker_identity_id"],
            ["id", "worker_identity_id"],
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        )
    op.add_column("task_result_events", sa.Column("actor_component", sa.String(32)))
    op.execute(
        "ALTER TABLE task_result_events ADD CONSTRAINT "
        "ck_task_result_events_actor_contract CHECK ("
        "(event_type IN ('result_accepted','result_replayed','result_conflict_rejected','result_stale_rejected') "
        "AND worker_identity_id IS NOT NULL AND actor_component IS NULL) OR "
        "(event_type='result_recovered' AND worker_identity_id IS NULL AND "
        "((result_kind='retryable_failure' AND failure_kind='claim_expired' AND actor_component='expired_claim_recovery') OR "
        "(result_kind='cancellation' AND failure_kind IS NULL AND actor_component='cancellation_recovery')))) NOT VALID"
    )
    op.create_check_constraint(
        "ck_task_result_events_actor_component_valid",
        "task_result_events",
        "actor_component IS NULL OR actor_component IN ('expired_claim_recovery','cancellation_recovery')",
    )
    op.add_column("task_retry_events", sa.Column("actor_component", sa.String(32)))
    op.add_column("task_retry_events", sa.Column("correlation_id", sa.String(128)))
    op.execute(
        "ALTER TABLE task_retry_events ADD CONSTRAINT "
        "ck_task_retry_events_actor_component_valid CHECK "
        "(actor_component IS NOT NULL AND actor_component IN "
        "('retry_transition','retry_dispatch','expired_claim_recovery')) NOT VALID"
    )
    op.add_column(
        "workflow_run_cancellation_requests",
        sa.Column("correlation_id", postgresql.UUID(as_uuid=True)),
    )
    op.add_column(
        "worker_heartbeats",
        sa.Column("worker_identity_id", postgresql.UUID(as_uuid=True)),
    )
    op.add_column("worker_heartbeats", sa.Column("correlation_id", sa.String(128)))
    op.execute(
        "ALTER TABLE task_claim_events ADD CONSTRAINT "
        "ck_task_claim_events_actor_attribution CHECK "
        "(worker_identity_id IS NOT NULL AND worker_session_id IS NOT NULL) NOT VALID"
    )
    op.execute(
        "ALTER TABLE worker_heartbeats ADD CONSTRAINT "
        "ck_worker_heartbeats_actor_attribution CHECK "
        "(worker_identity_id IS NOT NULL) NOT VALID"
    )
    op.create_foreign_key(
        "fk_worker_heartbeats_worker_session_identity",
        "worker_heartbeats",
        "worker_sessions",
        ["worker_session_id", "worker_identity_id"],
        ["id", "worker_identity_id"],
        onupdate="RESTRICT",
        ondelete="RESTRICT",
    )
    _immutable(AUDIT_FUNCTION, "audit_records", "TF009", "audit records are immutable")
    _immutable(
        HEARTBEAT_FUNCTION,
        "worker_heartbeats",
        "TF010",
        "worker heartbeat history is immutable",
    )


def _immutable(function: str, table: str, state: str, message: str) -> None:
    op.execute(
        f"CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING ERRCODE='{state}', MESSAGE='{message}'; END; $$"
    )
    op.execute(
        f"CREATE TRIGGER trg_{table}_reject_mutation BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION {function}()"
    )
    op.execute(
        f"CREATE TRIGGER trg_{table}_reject_truncate BEFORE TRUNCATE ON {table} FOR EACH STATEMENT EXECUTE FUNCTION {function}()"
    )


def downgrade() -> None:
    for function, table in (
        (HEARTBEAT_FUNCTION, "worker_heartbeats"),
        (AUDIT_FUNCTION, "audit_records"),
    ):
        op.execute(f"DROP TRIGGER trg_{table}_reject_truncate ON {table}")
        op.execute(f"DROP TRIGGER trg_{table}_reject_mutation ON {table}")
        op.execute(f"DROP FUNCTION {function}()")
    op.execute(
        "ALTER TABLE worker_heartbeats DROP CONSTRAINT ck_worker_heartbeats_actor_attribution"
    )
    op.execute(
        "ALTER TABLE task_claim_events DROP CONSTRAINT ck_task_claim_events_actor_attribution"
    )
    op.drop_constraint(
        "fk_worker_heartbeats_worker_session_identity",
        "worker_heartbeats",
        type_="foreignkey",
    )
    op.drop_column("worker_heartbeats", "correlation_id")
    op.drop_column("worker_heartbeats", "worker_identity_id")
    op.drop_column("workflow_run_cancellation_requests", "correlation_id")
    op.execute(
        "ALTER TABLE task_retry_events DROP CONSTRAINT ck_task_retry_events_actor_component_valid"
    )
    op.drop_column("task_retry_events", "correlation_id")
    op.drop_column("task_retry_events", "actor_component")
    op.drop_constraint(
        "ck_task_result_events_actor_component_valid",
        "task_result_events",
        type_="check",
    )
    op.execute(
        "ALTER TABLE task_result_events DROP CONSTRAINT ck_task_result_events_actor_contract"
    )
    op.drop_column("task_result_events", "actor_component")
    for table in ("task_result_events", "task_claim_events"):
        op.drop_constraint(
            f"fk_{table}_worker_session_identity", table, type_="foreignkey"
        )
        op.drop_column(table, "correlation_id")
        op.drop_column(table, "worker_identity_id")
    op.drop_column("task_claim_events", "worker_session_id")
    op.drop_table("audit_records")
    op.drop_constraint(
        "uq_worker_sessions_id_worker_identity_id", "worker_sessions", type_="unique"
    )
