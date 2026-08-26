#!/bin/sh
set -eu

: "${POSTGRES_DB:?POSTGRES_DB must be set}"
: "${POSTGRES_USER:?POSTGRES_USER must be set}"
: "${TASKFORGE_RUNTIME_USER:?TASKFORGE_RUNTIME_USER must be set}"
: "${TASKFORGE_RUNTIME_PASSWORD:?TASKFORGE_RUNTIME_PASSWORD must be set}"

psql --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set runtime_user="$TASKFORGE_RUNTIME_USER" \
  --set runtime_password="$TASKFORGE_RUNTIME_PASSWORD" <<'SQL'
DO $block$
DECLARE
    database_owner oid;
    schema_owner oid;
BEGIN
    SELECT datdba INTO database_owner FROM pg_database WHERE datname = current_database();
    SELECT nspowner INTO schema_owner FROM pg_namespace WHERE nspname = 'public';
    IF NOT pg_has_role(current_user, database_owner, 'USAGE') THEN
        RAISE EXCEPTION 'TaskForge privilege bootstrap must run as the database owner';
    END IF;
    IF schema_owner IS NULL OR NOT pg_has_role(current_user, schema_owner, 'USAGE') THEN
        RAISE EXCEPTION 'TaskForge privilege bootstrap cannot administer schema public';
    END IF;
    IF NOT EXISTS (
        SELECT FROM pg_roles WHERE rolname = current_user
        AND (rolsuper OR rolcreaterole)
    ) THEN
        RAISE EXCEPTION 'TaskForge privilege bootstrap requires CREATEROLE administration';
    END IF;
    IF EXISTS (
        SELECT FROM pg_class object
        JOIN pg_namespace namespace ON namespace.oid = object.relnamespace
        WHERE namespace.nspname = 'public'
        AND object.relname IN (
            'api_credentials', 'api_principal_roles', 'api_principals',
            'audit_records', 'dead_letter_items', 'dead_letter_operator_actions',
            'dead_letter_redrive_requests', 'dead_letter_status',
            'task_attempt_claims', 'task_attempt_results', 'task_attempts',
            'task_claim_events', 'task_dispatch_outbox', 'task_result_events',
            'task_retry_events', 'task_runs', 'worker_credentials',
            'worker_heartbeats', 'worker_identities',
            'worker_session_capabilities', 'worker_session_health',
            'worker_sessions', 'workflow_definitions',
            'workflow_draft_dependencies', 'workflow_draft_steps',
            'workflow_run_cancellation_requests',
            'workflow_run_execution_events', 'workflow_run_idempotency',
            'workflow_run_inputs', 'workflow_run_replays', 'workflow_runs',
            'workflow_version_dependencies', 'workflow_version_steps',
            'workflow_versions'
        )
        AND pg_get_userbyid(object.relowner) <> current_user
    ) THEN
        RAISE EXCEPTION 'TaskForge privilege bootstrap administrator does not own every existing TaskForge table';
    END IF;
    IF EXISTS (
        SELECT FROM pg_proc object
        JOIN pg_namespace namespace ON namespace.oid = object.pronamespace
        WHERE namespace.nspname = 'public'
        AND object.proname IN (
            'allocate_workflow_run_execution_event_cursor',
            'publish_workflow_run_execution_event_wakeup',
            'reject_audit_record_mutation',
            'reject_dead_letter_history_mutation',
            'reject_task_claim_event_mutation',
            'reject_task_result_history_mutation',
            'reject_task_retry_event_mutation',
            'reject_worker_heartbeat_mutation',
            'reject_workflow_run_cancellation_request_mutation',
            'reject_workflow_run_creation_snapshot_mutation',
            'reject_workflow_run_execution_event_mutation',
            'reject_workflow_run_replay_mutation',
            'reject_workflow_version_snapshot_mutation'
        )
        AND pg_get_userbyid(object.proowner) <> current_user
    ) THEN
        RAISE EXCEPTION 'TaskForge privilege bootstrap administrator does not own every existing TaskForge function';
    END IF;
END
$block$;

SELECT format(
    'CREATE ROLE %I LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS',
    :'runtime_user',
    :'runtime_password'
) WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'runtime_user') \gexec

SELECT format(
    'ALTER ROLE %I NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS',
    :'runtime_user'
) \gexec

SELECT format($statement$
DO $block$
BEGIN
    IF EXISTS (
        WITH RECURSIVE memberships(roleid) AS (
            SELECT roleid FROM pg_auth_members
            WHERE member = (SELECT oid FROM pg_roles WHERE rolname = %L)
            UNION
            SELECT member.roleid FROM pg_auth_members member
            JOIN memberships parent ON member.member = parent.roleid
        )
        SELECT FROM memberships
    ) THEN
        RAISE EXCEPTION 'TaskForge runtime role must have no role memberships';
    END IF;
END
$block$
$statement$, :'runtime_user') \gexec

SELECT format('REVOKE CONNECT, TEMPORARY ON DATABASE %I FROM PUBLIC', current_database()) \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'runtime_user') \gexec
REVOKE ALL ON SCHEMA public FROM PUBLIC;
SELECT format('GRANT USAGE ON SCHEMA public TO %I', :'runtime_user') \gexec
SQL
