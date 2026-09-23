"""Guard and coordination contracts for PostgreSQL operator wrappers."""

from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path

from taskforge.persistence.migration_lock import MIGRATION_LOCK_NAMESPACE

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BACKUP = PROJECT_ROOT / "scripts/postgres-backup.sh"
RESTORE = PROJECT_ROOT / "scripts/postgres-restore.sh"
BOOTSTRAP = PROJECT_ROOT / "docker/postgres/init-taskforge-roles.sh"


def test_backup_uses_the_task3_database_lock_for_the_complete_dump() -> None:
    backup = BACKUP.read_text(encoding="utf-8")
    bootstrap = BOOTSTRAP.read_text(encoding="utf-8")

    assert f"LOCK_NAMESPACE={MIGRATION_LOCK_NAMESPACE}" in backup
    assert "CAST($LOCK_NAMESPACE AS bigint) << 32" in backup
    assert f"CAST({MIGRATION_LOCK_NAMESPACE} AS bigint) << 32" in bootstrap
    assert "pg_try_advisory_lock(lock_key)" in backup
    assert "TASKFORGE_MIGRATION_LOCK_TIMEOUT_SECONDS" in backup
    assert backup.index('wait_for_marker "$LOCK_ACQUIRED"') < backup.index(
        "pg_dump --format=custom"
    )
    assert backup.index("pg_dump --format=custom") < backup.index("pg_advisory_unlock")


def test_custom_archive_ownership_is_suppressed_only_during_restore() -> None:
    backup = BACKUP.read_text(encoding="utf-8")
    restore = RESTORE.read_text(encoding="utf-8")

    assert "pg_dump --format=custom --username" in backup
    assert "pg_dump --format=custom --no-owner" not in backup
    assert "pg_restore --no-owner --exit-on-error --single-transaction" in restore


def test_restore_refuses_nonempty_target_before_pg_restore(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "fixture.taskforge.pgdump"
    archive.write_bytes(b"synthetic-custom-archive")
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    archive.with_name(archive.name + ".sha256").write_text(
        f"{digest}  {archive.name}\n", encoding="utf-8"
    )

    commands = tmp_path / "docker-commands"
    fake_docker = tmp_path / "docker"
    fake_docker.write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        f"printf '%s\\n' \"$*\" >> {commands}\n"
        "input=$(cat)\n"
        'case "$*" in\n'
        "  *'pg_restore --list'*) exit 0 ;;\n"
        '  *\'printf "%s" "$POSTGRES_DB"\'*) printf taskforge ;;\n'
        "  *psql*)\n"
        '    case "$input" in\n'
        "      *'WITH nonempty'*) printf 'nonempty\\n' ;;\n"
        "      *) printf 'ready\\n' ;;\n"
        "    esac\n"
        "    ;;\n"
        "esac\n",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    environment = os.environ.copy()
    environment.update(
        {
            "PATH": f"{tmp_path}:{environment['PATH']}",
            "TASKFORGE_RESTORE_CONFIRM_DATABASE": "taskforge",
        }
    )

    result = subprocess.run(
        ["bash", str(RESTORE), str(archive)],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode == 1
    assert "target database is not clean" in result.stderr
    invoked = commands.read_text(encoding="utf-8")
    assert "pg_restore --list" in invoked
    assert "--single-transaction" not in invoked


def test_restore_rejects_corrupt_archive_before_contacting_postgresql(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "corrupt.taskforge.pgdump"
    archive.write_bytes(b"corrupt-archive")
    archive.with_name(archive.name + ".sha256").write_text(
        f"{'0' * 64}  {archive.name}\n", encoding="utf-8"
    )
    contacted = tmp_path / "docker-contacted"
    fake_docker = tmp_path / "docker"
    fake_docker.write_text(
        f"#!/bin/sh\ntouch {contacted}\nexit 99\n",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    environment = os.environ.copy()
    environment.update(
        {
            "PATH": f"{tmp_path}:{environment['PATH']}",
            "TASKFORGE_RESTORE_CONFIRM_DATABASE": "taskforge",
        }
    )

    result = subprocess.run(
        ["bash", str(RESTORE), str(archive)],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode == 1
    assert "checksum validation failed" in result.stderr
    assert not contacted.exists()


def test_scripts_are_syntactically_valid_and_restore_checks_subscriptions() -> None:
    result = subprocess.run(
        ["bash", "-n", str(BACKUP), str(RESTORE)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    restore = RESTORE.read_text(encoding="utf-8")
    assert "FROM pg_catalog.pg_subscription" in restore
    assert "subdbid" in restore
