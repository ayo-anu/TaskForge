"""Enforce runtime least privilege and complete snapshot immutability.

Revision ID: 0027_enforce_history_privileges
Revises: 0026_standardize_audit_semantics
Create Date: 2026-08-26
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0027_enforce_history_privileges"
down_revision: str | None = "0026_standardize_audit_semantics"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

RUNTIME_ROLE = "taskforge_runtime"

SELECT_TABLES = (
    "api_principals",
    "api_principal_roles",
    "api_credentials",
    "worker_identities",
    "worker_credentials",
    "workflow_definitions",
    "workflow_draft_steps",
    "workflow_draft_dependencies",
    "workflow_versions",
    "workflow_version_steps",
    "workflow_version_dependencies",
    "workflow_runs",
    "workflow_run_replays",
    "workflow_run_cancellation_requests",
    "workflow_run_inputs",
    "task_runs",
    "workflow_run_execution_events",
    "task_attempts",
    "task_attempt_results",
    "task_attempt_claims",
    "task_claim_events",
    "task_result_events",
    "task_retry_events",
    "task_dispatch_outbox",
    "workflow_run_idempotency",
    "worker_sessions",
    "worker_session_capabilities",
    "worker_session_health",
    "worker_heartbeats",
    "dead_letter_items",
    "dead_letter_status",
    "dead_letter_operator_actions",
    "dead_letter_redrive_requests",
)
INSERT_TABLES = (
    "audit_records",
    "workflow_versions",
    "workflow_version_steps",
    "workflow_version_dependencies",
    "workflow_run_replays",
    "workflow_run_cancellation_requests",
    "workflow_run_inputs",
    "workflow_run_execution_events",
    "task_attempt_results",
    "task_claim_events",
    "task_result_events",
    "task_retry_events",
    "workflow_run_idempotency",
    "dead_letter_items",
    "dead_letter_operator_actions",
    "dead_letter_redrive_requests",
    "worker_heartbeats",
    "workflow_definitions",
    "workflow_draft_steps",
    "workflow_draft_dependencies",
    "workflow_runs",
    "task_runs",
    "task_attempts",
    "task_attempt_claims",
    "task_dispatch_outbox",
    "worker_sessions",
    "worker_session_capabilities",
    "worker_session_health",
    "dead_letter_status",
)
UPDATE_TABLES = (
    "workflow_definitions",
    "workflow_runs",
    "task_runs",
    "task_attempt_claims",
    "task_dispatch_outbox",
    "worker_sessions",
    "worker_session_health",
    "dead_letter_status",
)
DELETE_TABLES = ("worker_session_capabilities",)
ALL_TABLES = tuple(sorted(set(SELECT_TABLES) | set(INSERT_TABLES)))

TASKFORGE_FUNCTIONS = (
    "reject_workflow_version_snapshot_mutation",
    "reject_workflow_run_creation_snapshot_mutation",
    "reject_task_claim_event_mutation",
    "reject_task_result_history_mutation",
    "reject_task_retry_event_mutation",
    "reject_dead_letter_history_mutation",
    "reject_workflow_run_cancellation_request_mutation",
    "allocate_workflow_run_execution_event_cursor",
    "reject_workflow_run_execution_event_mutation",
    "publish_workflow_run_execution_event_wakeup",
    "reject_workflow_run_replay_mutation",
    "reject_audit_record_mutation",
    "reject_worker_heartbeat_mutation",
)
SNAPSHOT_TABLES = (
    "workflow_versions",
    "workflow_version_steps",
    "workflow_version_dependencies",
)


def _tables(names: tuple[str, ...]) -> str:
    return ", ".join(names)


def upgrade() -> None:
    op.execute(
        f"""
        DO $block$
        DECLARE runtime_oid oid;
        BEGIN
            SELECT oid INTO runtime_oid FROM pg_roles WHERE rolname = '{RUNTIME_ROLE}';
            IF runtime_oid IS NULL THEN
                RAISE EXCEPTION '{RUNTIME_ROLE} must be provisioned before migration';
            END IF;
            IF EXISTS (
                SELECT FROM pg_roles WHERE oid = runtime_oid AND
                (rolsuper OR rolcreatedb OR rolcreaterole OR rolinherit OR
                 rolreplication OR rolbypassrls)
            ) THEN
                RAISE EXCEPTION '{RUNTIME_ROLE} has forbidden role attributes';
            END IF;
            IF EXISTS (
                WITH RECURSIVE memberships(roleid) AS (
                    SELECT roleid FROM pg_auth_members WHERE member = runtime_oid
                    UNION
                    SELECT m.roleid FROM pg_auth_members m
                    JOIN memberships p ON m.member = p.roleid
                ) SELECT FROM memberships
            ) THEN
                RAISE EXCEPTION '{RUNTIME_ROLE} must have no role memberships';
            END IF;
            IF EXISTS (
                SELECT FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public' AND c.relname IN
                ({", ".join(repr(name) for name in ALL_TABLES)})
                AND pg_get_userbyid(c.relowner) <> current_user
            ) THEN
                RAISE EXCEPTION 'Alembic role must own every TaskForge table';
            END IF;
            IF EXISTS (SELECT FROM pg_class WHERE relowner = runtime_oid)
               OR EXISTS (SELECT FROM pg_namespace WHERE nspowner = runtime_oid)
               OR EXISTS (SELECT FROM pg_proc WHERE proowner = runtime_oid) THEN
                RAISE EXCEPTION '{RUNTIME_ROLE} must not own schema objects';
            END IF;
        END
        $block$
        """
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE {_tables(ALL_TABLES)} FROM {RUNTIME_ROLE}"
    )
    op.execute(f"GRANT SELECT ON TABLE {_tables(SELECT_TABLES)} TO {RUNTIME_ROLE}")
    op.execute(f"GRANT INSERT ON TABLE {_tables(INSERT_TABLES)} TO {RUNTIME_ROLE}")
    op.execute(f"GRANT UPDATE ON TABLE {_tables(UPDATE_TABLES)} TO {RUNTIME_ROLE}")
    op.execute(f"GRANT DELETE ON TABLE {_tables(DELETE_TABLES)} TO {RUNTIME_ROLE}")
    for function in TASKFORGE_FUNCTIONS:
        op.execute(
            f"REVOKE EXECUTE ON FUNCTION {function}() FROM PUBLIC, {RUNTIME_ROLE}"
        )
    for table in SNAPSHOT_TABLES:
        op.execute(
            f"CREATE TRIGGER trg_{table}_reject_truncate BEFORE TRUNCATE ON {table} "
            "FOR EACH STATEMENT EXECUTE FUNCTION "
            "reject_workflow_version_snapshot_mutation()"
        )
    op.execute("ALTER DEFAULT PRIVILEGES REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC")
    op.execute(
        f"ALTER DEFAULT PRIVILEGES REVOKE EXECUTE ON FUNCTIONS FROM {RUNTIME_ROLE}"
    )
    op.execute("ALTER DEFAULT PRIVILEGES REVOKE ALL ON TABLES FROM PUBLIC")
    op.execute(f"ALTER DEFAULT PRIVILEGES REVOKE ALL ON TABLES FROM {RUNTIME_ROLE}")
    op.execute("ALTER DEFAULT PRIVILEGES REVOKE ALL ON SEQUENCES FROM PUBLIC")
    op.execute(f"ALTER DEFAULT PRIVILEGES REVOKE ALL ON SEQUENCES FROM {RUNTIME_ROLE}")


def downgrade() -> None:
    op.execute("ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO PUBLIC")
    for table in reversed(SNAPSHOT_TABLES):
        op.execute(f"DROP TRIGGER trg_{table}_reject_truncate ON {table}")
    for function in TASKFORGE_FUNCTIONS:
        op.execute(f"GRANT EXECUTE ON FUNCTION {function}() TO PUBLIC")
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE {_tables(ALL_TABLES)} FROM {RUNTIME_ROLE}"
    )
