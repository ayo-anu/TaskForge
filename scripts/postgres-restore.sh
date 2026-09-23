#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: scripts/postgres-restore.sh ABSOLUTE_ARCHIVE_PATH" >&2
    exit 2
fi

archive=$1
if [[ $archive != /* ]]; then
    echo "restore archive path must be absolute" >&2
    exit 2
fi
if [[ ! -f $archive || -L $archive || ! -s $archive ]]; then
    echo "restore archive must be a nonempty regular file" >&2
    exit 2
fi
if [[ ! -f $archive.sha256 || -L $archive.sha256 ]]; then
    echo "restore checksum is missing or unsafe" >&2
    exit 2
fi

expected_database=${TASKFORGE_RESTORE_CONFIRM_DATABASE:-}
if [[ -z $expected_database ]]; then
    echo "TASKFORGE_RESTORE_CONFIRM_DATABASE is required" >&2
    exit 2
fi

archive_directory=$(dirname -- "$archive")
archive_name=$(basename -- "$archive")
if ! (cd -- "$archive_directory" && sha256sum --check --status "$archive_name.sha256"); then
    echo "TaskForge PostgreSQL backup checksum validation failed" >&2
    exit 1
fi
if ! docker compose exec -T postgres pg_restore --list <"$archive" >/dev/null; then
    echo "TaskForge PostgreSQL backup archive is unreadable" >&2
    exit 1
fi

target_database=$(docker compose exec -T postgres \
    sh -eu -c 'printf "%s" "$POSTGRES_DB"')
if [[ $expected_database != "$target_database" ]]; then
    echo "restore confirmation does not match the target database" >&2
    exit 2
fi

role_contract=$(docker compose exec -T postgres \
    sh -eu -c 'exec psql --no-psqlrc --quiet --tuples-only --no-align \
        --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
        --set runtime_user="$TASKFORGE_RUNTIME_USER"' <<'SQL'
SELECT CASE WHEN (
    pg_catalog.pg_get_userbyid(database.datdba) = current_user
    AND EXISTS (
        SELECT FROM pg_catalog.pg_roles
        WHERE rolname = :'runtime_user'
          AND NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole
          AND NOT rolinherit AND NOT rolreplication AND NOT rolbypassrls
    )
) THEN 'ready' ELSE 'invalid' END
FROM pg_catalog.pg_database AS database
WHERE database.datname = pg_catalog.current_database();
SQL
)
if [[ $role_contract != ready ]]; then
    echo "TaskForge owner/runtime roles must be provisioned before restore" >&2
    exit 1
fi

clean_target=$(docker compose exec -T postgres \
    sh -eu -c 'exec psql --no-psqlrc --quiet --tuples-only --no-align \
        --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"' <<'SQL'
WITH nonempty(category) AS (
    SELECT 'schema'
    FROM pg_catalog.pg_namespace
    WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'public')
      AND nspname !~ '^pg_(?:temp|toast_temp)_[0-9]+$'
    UNION ALL
    SELECT 'relation'
    FROM pg_catalog.pg_class AS object
    JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = object.relnamespace
    WHERE namespace.nspname = 'public'
    UNION ALL
    SELECT 'routine'
    FROM pg_catalog.pg_proc AS object
    JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = object.pronamespace
    WHERE namespace.nspname = 'public'
    UNION ALL
    SELECT 'type'
    FROM pg_catalog.pg_type AS object
    JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = object.typnamespace
    WHERE namespace.nspname = 'public'
    UNION ALL
    SELECT 'extension'
    FROM pg_catalog.pg_extension
    WHERE extname <> 'plpgsql'
    UNION ALL
    SELECT 'large_object'
    FROM pg_catalog.pg_largeobject_metadata
    UNION ALL
    SELECT 'event_trigger'
    FROM pg_catalog.pg_event_trigger
    UNION ALL
    SELECT 'publication'
    FROM pg_catalog.pg_publication
    UNION ALL
    SELECT 'subscription'
    FROM pg_catalog.pg_subscription
    WHERE subdbid = (SELECT oid FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database())
)
SELECT CASE WHEN NOT EXISTS (SELECT FROM nonempty)
    THEN 'clean' ELSE 'nonempty' END;
SQL
)
if [[ $clean_target != clean ]]; then
    echo "TaskForge restore refused because the target database is not clean" >&2
    exit 1
fi

if ! docker compose exec -T -e PGAPPNAME=taskforge-restore postgres \
    sh -eu -c 'exec pg_restore --no-owner --exit-on-error --single-transaction \
        --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"' <"$archive"; then
    echo "TaskForge PostgreSQL restore failed" >&2
    exit 1
fi

echo "TaskForge PostgreSQL restore completed database=$target_database"
