#!/bin/sh
set -eu

: "${POSTGRES_DB:?POSTGRES_DB must be set}"
: "${POSTGRES_USER:?POSTGRES_USER must be set}"
: "${TASKFORGE_RUNTIME_USER:?TASKFORGE_RUNTIME_USER must be set}"
: "${TASKFORGE_RUNTIME_PASSWORD:?TASKFORGE_RUNTIME_PASSWORD must be set}"
: "${TASKFORGE_MIGRATION_LOCK_TIMEOUT_SECONDS:=300}"

case "$TASKFORGE_MIGRATION_LOCK_TIMEOUT_SECONDS" in
  *[!0-9]*|'')
    echo "TASKFORGE_MIGRATION_LOCK_TIMEOUT_SECONDS must be a whole number from 1 to 3600" >&2
    exit 2
    ;;
esac
if [ "$TASKFORGE_MIGRATION_LOCK_TIMEOUT_SECONDS" -lt 1 ] || \
   [ "$TASKFORGE_MIGRATION_LOCK_TIMEOUT_SECONDS" -gt 3600 ]; then
  echo "TASKFORGE_MIGRATION_LOCK_TIMEOUT_SECONDS must be from 1 to 3600" >&2
  exit 2
fi

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set runtime_user="$TASKFORGE_RUNTIME_USER" \
  --set runtime_password="$TASKFORGE_RUNTIME_PASSWORD" \
  --set lock_timeout_seconds="$TASKFORGE_MIGRATION_LOCK_TIMEOUT_SECONDS" <<'SQL'
SELECT pg_catalog.set_config(
    'taskforge.migration_lock_timeout_seconds',
    :'lock_timeout_seconds',
    false
);
SELECT pg_catalog.set_config(
    'taskforge.bootstrap_backend_pid',
    pg_catalog.pg_backend_pid()::pg_catalog.text,
    false
);

DO $block$
DECLARE
    lock_key bigint;
    lock_deadline timestamp with time zone;
BEGIN
    IF pg_catalog.current_setting('taskforge.bootstrap_backend_pid')::integer
       <> pg_catalog.pg_backend_pid() THEN
        RAISE EXCEPTION 'TaskForge bootstrap changed PostgreSQL sessions';
    END IF;
    SELECT
        (CAST(1413893447 AS bigint) << 32) | database.oid::bigint
    INTO lock_key
    FROM pg_catalog.pg_database AS database
    WHERE database.datname = pg_catalog.current_database();
    lock_deadline := pg_catalog.clock_timestamp() + pg_catalog.make_interval(
        secs => pg_catalog.current_setting(
            'taskforge.migration_lock_timeout_seconds'
        )::double precision
    );
    LOOP
        EXIT WHEN pg_catalog.pg_try_advisory_lock(lock_key);
        IF pg_catalog.clock_timestamp() >= lock_deadline THEN
            RAISE EXCEPTION 'TaskForge migration lock acquisition timed out';
        END IF;
        PERFORM pg_catalog.pg_sleep(
            LEAST(
                0.1,
                GREATEST(
                    0.0,
                    EXTRACT(
                        EPOCH FROM lock_deadline - pg_catalog.clock_timestamp()
                    )
                )
            )
        );
    END LOOP;
END
$block$;

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
            'lock_valid_worker_authority',
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

SELECT format($statement$
DO $block$
DECLARE
    runtime_oid oid;
BEGIN
    SELECT oid INTO runtime_oid
    FROM pg_catalog.pg_roles
    WHERE rolname = %L;
    IF runtime_oid IS NULL THEN
        RAISE EXCEPTION 'TaskForge runtime role was not created';
    END IF;
    IF EXISTS (
        SELECT FROM pg_catalog.pg_roles
        WHERE oid = runtime_oid
          AND (rolsuper OR rolcreatedb OR rolcreaterole OR rolinherit
               OR rolreplication OR rolbypassrls)
    ) THEN
        RAISE EXCEPTION 'TaskForge runtime role has forbidden attributes';
    END IF;
    IF EXISTS (
        WITH RECURSIVE memberships(roleid) AS (
            SELECT roleid FROM pg_catalog.pg_auth_members
            WHERE member = runtime_oid
            UNION
            SELECT membership.roleid
            FROM pg_catalog.pg_auth_members AS membership
            JOIN memberships AS parent ON membership.member = parent.roleid
        )
        SELECT FROM memberships
    ) THEN
        RAISE EXCEPTION 'TaskForge runtime role has memberships';
    END IF;
    IF EXISTS (
        SELECT FROM pg_catalog.pg_class WHERE relowner = runtime_oid
    ) OR EXISTS (
        SELECT FROM pg_catalog.pg_namespace WHERE nspowner = runtime_oid
    ) OR EXISTS (
        SELECT FROM pg_catalog.pg_proc WHERE proowner = runtime_oid
    ) THEN
        RAISE EXCEPTION 'TaskForge runtime role owns schema objects';
    END IF;
    IF NOT pg_catalog.has_database_privilege(
        %L, pg_catalog.current_database(), 'CONNECT'
    ) THEN
        RAISE EXCEPTION 'TaskForge runtime role lacks database CONNECT';
    END IF;
    IF NOT pg_catalog.has_schema_privilege(%L, 'public', 'USAGE')
       OR pg_catalog.has_schema_privilege(%L, 'public', 'CREATE') THEN
        RAISE EXCEPTION 'TaskForge runtime schema privileges are unsafe';
    END IF;
END
$block$
$statement$, :'runtime_user', :'runtime_user', :'runtime_user', :'runtime_user') \gexec

DO $block$
DECLARE
    lock_key bigint;
BEGIN
    IF pg_catalog.current_setting('taskforge.bootstrap_backend_pid')::integer
       <> pg_catalog.pg_backend_pid() THEN
        RAISE EXCEPTION 'TaskForge bootstrap changed PostgreSQL sessions';
    END IF;
    SELECT
        (CAST(1413893447 AS bigint) << 32) | database.oid::bigint
    INTO lock_key
    FROM pg_catalog.pg_database AS database
    WHERE database.datname = pg_catalog.current_database();
    IF NOT pg_catalog.pg_advisory_unlock(lock_key) THEN
        RAISE EXCEPTION 'TaskForge migration lock was not held by bootstrap session';
    END IF;
END
$block$;
SQL
