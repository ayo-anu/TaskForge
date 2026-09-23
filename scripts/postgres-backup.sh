#!/usr/bin/env bash
set -euo pipefail

readonly LOCK_NAMESPACE=1413893447
readonly LOCK_POLL_SECONDS=0.1
readonly LOCK_ACQUIRED=TASKFORGE_BACKUP_LOCK_ACQUIRED
readonly LOCK_RELEASED=TASKFORGE_BACKUP_LOCK_RELEASED

if [[ $# -ne 1 ]]; then
    echo "usage: scripts/postgres-backup.sh ABSOLUTE_ARCHIVE_PATH" >&2
    exit 2
fi

archive=$1
if [[ $archive != /* ]]; then
    echo "backup archive path must be absolute" >&2
    exit 2
fi
if [[ -e $archive || -L $archive || -e $archive.sha256 || -L $archive.sha256 ]]; then
    echo "refusing to overwrite an existing backup artifact" >&2
    exit 2
fi

timeout=${TASKFORGE_MIGRATION_LOCK_TIMEOUT_SECONDS:-300}
if [[ ! $timeout =~ ^[0-9]+$ ]] || (( timeout < 1 || timeout > 3600 )); then
    echo "TASKFORGE_MIGRATION_LOCK_TIMEOUT_SECONDS must be from 1 to 3600" >&2
    exit 2
fi

archive_directory=$(dirname -- "$archive")
archive_name=$(basename -- "$archive")
if [[ ! -d $archive_directory ]]; then
    echo "backup destination directory does not exist" >&2
    exit 2
fi

umask 077
temporary_archive=$(mktemp "$archive_directory/.${archive_name}.tmp.XXXXXX")
temporary_checksum=$(mktemp "$archive_directory/.${archive_name}.sha256.tmp.XXXXXX")
control_directory=$(mktemp -d "${TMPDIR:-/tmp}/taskforge-backup-lock.XXXXXX")
control_fifo=$control_directory/control
status_fifo=$control_directory/status
lock_error=$control_directory/error
mkfifo "$control_fifo" "$status_fifo"

lock_pid=
dump_pid=
control_fd=
status_fd=
promotion_started=false
promotion_complete=false
cleanup() {
    status=$?
    if [[ -n $control_fd ]]; then
        exec {control_fd}>&-
    fi
    if [[ -n $status_fd ]]; then
        exec {status_fd}>&-
    fi
    if [[ -n $dump_pid ]] && kill -0 "$dump_pid" 2>/dev/null; then
        kill "$dump_pid" 2>/dev/null || true
    fi
    if [[ -n $dump_pid ]]; then
        wait "$dump_pid" 2>/dev/null || true
    fi
    if [[ -n $lock_pid ]] && kill -0 "$lock_pid" 2>/dev/null; then
        kill "$lock_pid" 2>/dev/null || true
    fi
    if [[ -n $lock_pid ]]; then
        wait "$lock_pid" 2>/dev/null || true
    fi
    if [[ $promotion_started == true && $promotion_complete != true ]]; then
        rm -f -- "$archive" "$archive.sha256"
    fi
    rm -f -- "$temporary_archive" "$temporary_checksum"
    rm -f -- "$control_fifo" "$status_fifo" "$lock_error"
    rmdir -- "$control_directory" 2>/dev/null || true
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM

exec {control_fd}<>"$control_fifo"
exec {status_fd}<>"$status_fifo"

docker compose exec -T -e PGAPPNAME=taskforge-backup-lock postgres \
    sh -eu -c 'exec psql --no-psqlrc --quiet --tuples-only --no-align \
        --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" \
        --dbname "$POSTGRES_DB"' \
    <&"$control_fd" >&"$status_fd" 2>"$lock_error" &
lock_pid=$!

cat >&"$control_fd" <<SQL
SELECT pg_catalog.set_config('taskforge.backup_lock_timeout_seconds', '$timeout', false);
SELECT pg_catalog.set_config('taskforge.backup_lock_backend_pid', pg_catalog.pg_backend_pid()::pg_catalog.text, false);
DO \$block\$
DECLARE
    lock_key bigint;
    lock_deadline timestamp with time zone;
BEGIN
    SELECT (CAST($LOCK_NAMESPACE AS bigint) << 32) | database.oid::bigint
    INTO lock_key
    FROM pg_catalog.pg_database AS database
    WHERE database.datname = pg_catalog.current_database();
    lock_deadline := pg_catalog.clock_timestamp() + pg_catalog.make_interval(
        secs => pg_catalog.current_setting('taskforge.backup_lock_timeout_seconds')::double precision
    );
    LOOP
        EXIT WHEN pg_catalog.pg_try_advisory_lock(lock_key);
        IF pg_catalog.clock_timestamp() >= lock_deadline THEN
            RAISE EXCEPTION 'TaskForge backup lock acquisition timed out';
        END IF;
        PERFORM pg_catalog.pg_sleep(
            LEAST(0.1, GREATEST(0.0, EXTRACT(EPOCH FROM lock_deadline - pg_catalog.clock_timestamp())))
        );
    END LOOP;
END
\$block\$;
\echo $LOCK_ACQUIRED
SQL

wait_for_marker() {
    local expected=$1
    local deadline=$((SECONDS + timeout + 15))
    local line
    while (( SECONDS <= deadline )); do
        if IFS= read -r -t 1 line <&"$status_fd"; then
            if [[ $line == "$expected" ]]; then
                return 0
            fi
        elif ! kill -0 "$lock_pid" 2>/dev/null; then
            break
        fi
    done
    if [[ -s $lock_error ]]; then
        tail -n 1 "$lock_error" >&2
    fi
    return 1
}

if ! wait_for_marker "$LOCK_ACQUIRED"; then
    echo "TaskForge backup could not acquire the migration lock" >&2
    exit 1
fi

docker compose exec -T -e PGAPPNAME=taskforge-backup-dump postgres \
    sh -eu -c 'exec pg_dump --format=custom --username "$POSTGRES_USER" \
        --dbname "$POSTGRES_DB"' >"$temporary_archive" &
dump_pid=$!
while kill -0 "$dump_pid" 2>/dev/null; do
    if ! kill -0 "$lock_pid" 2>/dev/null; then
        kill "$dump_pid" 2>/dev/null || true
        wait "$dump_pid" 2>/dev/null || true
        dump_pid=
        echo "TaskForge backup lost the migration lock during pg_dump" >&2
        exit 1
    fi
    sleep "$LOCK_POLL_SECONDS"
done
if ! wait "$dump_pid"; then
    dump_pid=
    echo "TaskForge PostgreSQL backup failed" >&2
    exit 1
fi
dump_pid=
if ! kill -0 "$lock_pid" 2>/dev/null; then
    echo "TaskForge backup lost the migration lock during pg_dump" >&2
    exit 1
fi

cat >&"$control_fd" <<SQL
SELECT CASE
    WHEN pg_catalog.pg_advisory_unlock(
        (CAST($LOCK_NAMESPACE AS bigint) << 32) |
        (SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database())
    ) THEN '$LOCK_RELEASED'
    ELSE 'TASKFORGE_BACKUP_LOCK_NOT_HELD'
END;
\quit
SQL

if ! wait_for_marker "$LOCK_RELEASED"; then
    echo "TaskForge backup could not verify migration lock release" >&2
    exit 1
fi
wait "$lock_pid"
lock_pid=

if [[ ! -s $temporary_archive ]]; then
    echo "TaskForge PostgreSQL backup artifact is empty" >&2
    exit 1
fi
if ! docker compose exec -T postgres pg_restore --list <"$temporary_archive" >/dev/null; then
    echo "TaskForge PostgreSQL backup archive validation failed" >&2
    exit 1
fi

checksum=$(sha256sum -- "$temporary_archive" | awk '{print $1}')
printf '%s  %s\n' "$checksum" "$archive_name" >"$temporary_checksum"
chmod 0600 "$temporary_archive" "$temporary_checksum"
promotion_started=true
mv -- "$temporary_archive" "$archive"
mv -- "$temporary_checksum" "$archive.sha256"
promotion_complete=true

trap - EXIT HUP INT TERM
rm -f -- "$control_fifo" "$status_fifo" "$lock_error"
rmdir -- "$control_directory"
echo "TaskForge PostgreSQL backup completed archive=$archive"
