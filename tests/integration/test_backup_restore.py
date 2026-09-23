"""Opt-in PostgreSQL-native backup and independent clean-restore validation."""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import shlex
import shutil
import signal
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
from sqlalchemy.engine import URL

from taskforge.claims.authority import TaskClaimResultAuthorityIssuer
from taskforge.claims.service import TaskClaimService
from taskforge.persistence.claims import SQLAlchemyTaskClaimRepository
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.migration_lock import MIGRATION_LOCK_NAMESPACE
from taskforge.persistence.schema_compatibility import EXPECTED_SCHEMA_REVISION
from taskforge.tasks.catalog import (
    INGEST_TASK_TYPE,
    TRANSFORM_TASK_TYPE,
    VALIDATE_TASK_TYPE,
)
from tests.integration.postgresql import asyncpg_dsn
from tests.integration.test_authentication_persistence import settings_for
from tests.integration.test_compose_deployment import (
    ComposeProject,
    _available_port,
    _docker,
    _postgres_scalar,
    _wait_for_health,
)
from tests.integration.test_orchestrator_process import (
    process_environment,
    process_task_contract,
    seed_genuine_retry_pending,
    seed_process_dispatch,
)
from tests.integration.test_task_claim_acquisition import add_worker

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_BACKUP_RESTORE_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_BACKUP_RESTORE_INTEGRATION=1 explicitly",
    ),
]

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BACKUP_SCRIPT = PROJECT_ROOT / "scripts/postgres-backup.sh"
RESTORE_SCRIPT = PROJECT_ROOT / "scripts/postgres-restore.sh"


def _secret() -> str:
    return secrets.token_urlsafe(36)


def _values(tag: str) -> dict[str, str]:
    return {
        "POSTGRES_DB": "taskforge",
        "POSTGRES_OWNER_USER": "taskforge_owner",
        "POSTGRES_OWNER_PASSWORD": _secret(),
        "POSTGRES_USER": "taskforge_runtime",
        "POSTGRES_PASSWORD": _secret(),
        "POSTGRES_PORT": str(_available_port()),
        "RABBITMQ_DEFAULT_USER": "taskforge",
        "RABBITMQ_DEFAULT_PASS": _secret(),
        "RABBITMQ_DEFAULT_VHOST": "taskforge",
        "RABBITMQ_AMQP_PORT": str(_available_port()),
        "RABBITMQ_MANAGEMENT_PORT": str(_available_port()),
        "TASKFORGE_TASK_CLAIM_RESULT_AUTHORITY_SECRET": _secret(),
        "TASKFORGE_IMAGE_TAG": tag,
    }


@dataclass(frozen=True)
class RestoredRecoveryState:
    published_dispatch_id: UUID
    unpublished_dispatch_id: UUID
    expiring_task_run_id: UUID
    retry_task_run_id: UUID
    worker_session_id: UUID
    published_at: str


@dataclass(frozen=True)
class ControlledDump:
    environment: dict[str, str]
    started: Path
    terminated: Path
    pid_file: Path
    holder_pid_file: Path
    control_root: Path


def _script_environment(project: ComposeProject) -> dict[str, str]:
    environment = project.environment()
    environment.update(
        {
            "COMPOSE_PROJECT_NAME": project.name,
            "COMPOSE_ENV_FILES": str(project.env_file),
        }
    )
    return environment


def _run_script(
    project: ComposeProject,
    script: Path,
    archive: Path,
    *,
    check: bool = True,
    environment_overrides: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    environment = _script_environment(project)
    environment.update(environment_overrides or {})
    if script == RESTORE_SCRIPT:
        environment["TASKFORGE_RESTORE_CONFIRM_DATABASE"] = "taskforge"
    result = subprocess.run(
        [str(script), str(archive)],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        check=False,
        text=True,
        timeout=300,
    )
    if check and result.returncode != 0:
        pytest.fail(project.redact(result.stdout + result.stderr))
    return result


def _start_script(
    project: ComposeProject,
    script: Path,
    archive: Path,
    *,
    environment_overrides: dict[str, str],
) -> subprocess.Popen[str]:
    environment = _script_environment(project)
    environment.update(environment_overrides)
    return subprocess.Popen(
        [str(script), str(archive)],
        cwd=PROJECT_ROOT,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )


def _wait_until(description: str, predicate: Callable[[], bool]) -> None:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    pytest.fail(f"timed out waiting for {description}")


def _controlled_dump(
    tmp_path: Path,
    *,
    label: str,
) -> ControlledDump:
    real_docker = shutil.which("docker")
    assert real_docker is not None
    fake_bin = tmp_path / f"{label}-bin"
    fake_bin.mkdir()
    started = tmp_path / f"{label}-dump-started"
    terminated = tmp_path / f"{label}-dump-terminated"
    pid_file = tmp_path / f"{label}-dump-pid"
    holder_pid_file = tmp_path / f"{label}-holder-pid"
    control_root = tmp_path / f"{label}-control"
    control_root.mkdir()
    docker = fake_bin / "docker"
    docker.write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        'case "$*" in\n'
        "  *PGAPPNAME=taskforge-backup-dump*)\n"
        f"    printf '%s\\n' \"$$\" > {shlex.quote(str(pid_file))}\n"
        f"    : > {shlex.quote(str(started))}\n"
        "    tail -f /dev/null &\n"
        "    blocker=$!\n"
        "    terminate() {\n"
        '      kill "$blocker" 2>/dev/null || true\n'
        '      wait "$blocker" 2>/dev/null || true\n'
        f"      : > {shlex.quote(str(terminated))}\n"
        "      exit 143\n"
        "    }\n"
        "    trap terminate HUP INT TERM\n"
        '    wait "$blocker"\n'
        "    exit 99\n"
        "    ;;\n"
        "  *PGAPPNAME=taskforge-backup-lock*)\n"
        f"    printf '%s\\n' \"$$\" > {shlex.quote(str(holder_pid_file))}\n"
        f'    exec {shlex.quote(real_docker)} "$@"\n'
        "    ;;\n"
        "esac\n"
        f'exec {shlex.quote(real_docker)} "$@"\n',
        encoding="utf-8",
    )
    docker.chmod(0o755)
    return ControlledDump(
        {
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "TMPDIR": str(control_root),
        },
        started,
        terminated,
        pid_file,
        holder_pid_file,
        control_root,
    )


def _backup_lock_count(project: ComposeProject) -> int:
    return int(
        _postgres_scalar(
            project,
            "SELECT count(*) FROM pg_catalog.pg_locks AS lock "
            "JOIN pg_catalog.pg_stat_activity AS activity ON activity.pid=lock.pid "
            "WHERE lock.locktype='advisory' AND lock.granted "
            "AND activity.application_name='taskforge-backup-lock'",
        )
    )


def _assert_failed_backup_clean(
    archive: Path,
    controlled: ControlledDump,
) -> None:
    assert not archive.exists()
    assert not archive.with_name(archive.name + ".sha256").exists()
    assert not tuple(archive.parent.glob(f".{archive.name}.tmp.*"))
    assert not tuple(archive.parent.glob(f".{archive.name}.sha256.tmp.*"))
    assert not tuple(controlled.control_root.iterdir())
    dump_pid = int(controlled.pid_file.read_text(encoding="utf-8"))
    with pytest.raises(ProcessLookupError):
        os.kill(dump_pid, 0)
    holder_pid = int(controlled.holder_pid_file.read_text(encoding="utf-8"))
    with pytest.raises(ProcessLookupError):
        os.kill(holder_pid, 0)


def _bootstrap(project: ComposeProject) -> None:
    project.compose(
        (
            "exec",
            "-T",
            "-e",
            "TASKFORGE_MIGRATION_LOCK_TIMEOUT_SECONDS=5",
            "postgres",
            "sh",
            "/docker-entrypoint-initdb.d/10-taskforge-roles.sh",
        )
    )


def _cleanup(project: ComposeProject) -> None:
    project.compose(
        ("down", "--volumes", "--remove-orphans", "--timeout", "10"),
        admin=True,
        check=False,
        timeout=120,
    )


def _hold_migration_lock(project: ComposeProject) -> subprocess.Popen[str]:
    holder = subprocess.Popen(
        (
            "docker",
            "exec",
            "--interactive",
            "--env",
            "PGAPPNAME=taskforge-backup-exclusion-test",
            project.service_container("postgres"),
            "psql",
            "--no-psqlrc",
            "--quiet",
            "--username",
            project.values["POSTGRES_OWNER_USER"],
            "--dbname",
            project.values["POSTGRES_DB"],
        ),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert holder.stdin is not None
    holder.stdin.write(
        "SELECT pg_catalog.pg_advisory_lock("
        f"(CAST({MIGRATION_LOCK_NAMESPACE} AS bigint) << 32) | "
        "(SELECT oid::bigint FROM pg_catalog.pg_database "
        "WHERE datname=pg_catalog.current_database()));\n"
    )
    holder.stdin.flush()
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if holder.poll() is not None:
            _, error = holder.communicate()
            pytest.fail(f"migration-lock holder exited early: {error}")
        held = _postgres_scalar(
            project,
            "SELECT count(*) FROM pg_catalog.pg_locks AS lock "
            "JOIN pg_catalog.pg_stat_activity AS activity "
            "ON activity.pid=lock.pid "
            "WHERE lock.locktype='advisory' AND lock.granted "
            "AND activity.application_name='taskforge-backup-exclusion-test'",
        )
        if held == "1":
            return holder
        time.sleep(0.05)
    holder.terminate()
    holder.wait(timeout=10)
    pytest.fail("migration-lock holder did not acquire the TaskForge lock")


def _release_migration_lock(holder: subprocess.Popen[str]) -> None:
    assert holder.stdin is not None
    holder.stdin.write("\\quit\n")
    holder.stdin.close()
    holder.wait(timeout=10)
    assert holder.returncode == 0


def _owner_url(project: ComposeProject) -> URL:
    return URL.create(
        "postgresql+asyncpg",
        username=project.values["POSTGRES_OWNER_USER"],
        password=project.values["POSTGRES_OWNER_PASSWORD"],
        host="127.0.0.1",
        port=int(project.values["POSTGRES_PORT"]),
        database=project.values["POSTGRES_DB"],
    )


def _runtime_url(project: ComposeProject) -> URL:
    return _owner_url(project).set(
        username=project.values["POSTGRES_USER"],
        password=project.values["POSTGRES_PASSWORD"],
    )


def _run_migration_process(project: ComposeProject) -> subprocess.CompletedProcess[str]:
    environment = project.environment()
    environment.update(
        {
            "PYTHONPATH": str(PROJECT_ROOT / "src"),
            "POSTGRES_HOST": "127.0.0.1",
            "POSTGRES_PORT": project.values["POSTGRES_PORT"],
            "POSTGRES_DB": project.values["POSTGRES_DB"],
            "POSTGRES_OWNER_USER": project.values["POSTGRES_OWNER_USER"],
            "POSTGRES_OWNER_PASSWORD": project.values["POSTGRES_OWNER_PASSWORD"],
            "TASKFORGE_MIGRATION_LOCK_TIMEOUT_SECONDS": "5",
        }
    )
    return subprocess.run(
        [sys.executable, "-m", "taskforge.database_migrations"],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        check=False,
        text=True,
        timeout=300,
    )


def _start_orchestrator(project: ComposeProject) -> subprocess.Popen[str]:
    environment = process_environment(
        _runtime_url(project),
        "amqp://"
        f"{project.values['RABBITMQ_DEFAULT_USER']}:"
        f"{project.values['RABBITMQ_DEFAULT_PASS']}@127.0.0.1:"
        f"{project.values['RABBITMQ_AMQP_PORT']}/"
        f"{project.values['RABBITMQ_DEFAULT_VHOST']}",
        suffix=f"restore-{uuid4().hex}",
    )
    environment.update(
        {
            "TASKFORGE_LOG_LEVEL": "INFO",
            "TASKFORGE_WORKER_STALE_AFTER_SECONDS": "1",
            "TASKFORGE_WORKER_OFFLINE_AFTER_SECONDS": "2",
        }
    )
    return subprocess.Popen(
        [sys.executable, "-m", "taskforge.orchestrator"],
        cwd=PROJECT_ROOT,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _stop_orchestrator(process: subprocess.Popen[str]) -> str:
    if process.poll() is None:
        process.terminate()
    try:
        output, error = process.communicate(timeout=30)
    except subprocess.TimeoutExpired:
        process.kill()
        output, error = process.communicate(timeout=10)
    return output + error


async def _seed_restore_recovery_state(database_url: URL) -> RestoredRecoveryState:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        published = await seed_process_dispatch(
            connection,
            process_task_contract(
                INGEST_TASK_TYPE,
                {"document_id": "restore-published", "content": "alpha"},
            ),
        )
        unpublished = await seed_process_dispatch(
            connection,
            process_task_contract(
                VALIDATE_TASK_TYPE,
                {
                    "document_id": "restore-unpublished",
                    "document": {"value": "ready"},
                    "required_fields": ["value"],
                },
            ),
        )
        published_at = await connection.fetchval(
            "UPDATE task_dispatch_outbox SET published_at=statement_timestamp() "
            "WHERE id=$1 RETURNING published_at::text",
            published.dispatch_id,
        )
        assert isinstance(published_at, str)

        expiring = await seed_process_dispatch(
            connection,
            process_task_contract(
                TRANSFORM_TASK_TYPE,
                {
                    "document_id": "restore-expiring-claim",
                    "content": "  Alpha   Beta  ",
                    "operations": ["strip", "collapse_whitespace"],
                },
            ),
            workflow_policy={
                "retry_policy": {
                    "maximum_attempts": 3,
                    "initial_delay_seconds": 0,
                    "multiplier": 1,
                    "maximum_delay_seconds": 0,
                }
            },
        )
        worker = await add_worker(connection, capability=expiring.required_capability)
        await TaskClaimService(
            SQLAlchemyTaskClaimRepository(sessions, worker_stale_after_seconds=30),
            TaskClaimResultAuthorityIssuer(b"restore-fixture-authority-secret"),
            lease_seconds=5,
        ).claim_task(worker.authenticated, worker.session_id, expiring)
        assert await connection.fetchval(
            "SELECT lease_expires_at > statement_timestamp() "
            "FROM task_attempt_claims WHERE task_attempt_id=$1 "
            "AND terminated_at IS NULL",
            expiring.task_attempt_id,
        )
        assert await connection.fetchval(
            "SELECT last_seen_at > statement_timestamp()-interval '30 seconds' "
            "FROM worker_session_health WHERE worker_session_id=$1",
            worker.session_id,
        )

        retry_task_run_id, _ = await seed_genuine_retry_pending(
            connection,
            sessions,
            process_task_contract(
                TRANSFORM_TASK_TYPE,
                {
                    "document_id": "restore-retry",
                    "content": "  retry me  ",
                    "operations": ["strip"],
                },
            ),
        )
        return RestoredRecoveryState(
            published.dispatch_id,
            unpublished.dispatch_id,
            expiring.task_run_id,
            retry_task_run_id,
            worker.session_id,
            published_at,
        )
    finally:
        await connection.close()
        await engine.dispose()


def _wait_for_restored_reconciliation(
    project: ComposeProject,
    state: RestoredRecoveryState,
    process: subprocess.Popen[str],
) -> None:
    deadline = time.monotonic() + 90
    latest = ""
    while time.monotonic() < deadline:
        if process.poll() is not None:
            output, error = process.communicate()
            pytest.fail(
                project.redact(
                    "orchestrator exited before restored state converged:\n"
                    f"{output}{error}"
                )
            )
        latest = _postgres_scalar(
            project,
            "SELECT json_build_object("
            "'published_preserved', (SELECT published_at::text=$$"
            f"{state.published_at}$$ FROM task_dispatch_outbox WHERE id='"
            f"{state.published_dispatch_id}'::uuid), "
            "'unpublished_reconciled', (SELECT published_at IS NOT NULL "
            "FROM task_dispatch_outbox WHERE id='"
            f"{state.unpublished_dispatch_id}'::uuid), "
            "'claim_recovered', (SELECT status::text='dispatched' FROM task_runs "
            f"WHERE id='{state.expiring_task_run_id}'::uuid), "
            "'retry_progressed', (SELECT status::text='dispatched' FROM task_runs "
            f"WHERE id='{state.retry_task_run_id}'::uuid), "
            "'session_stale', (SELECT ended_at IS NOT NULL FROM worker_sessions "
            f"WHERE id='{state.worker_session_id}'::uuid))::text",
        )
        observed = json.loads(latest)
        if all(
            observed.get(name) is True
            for name in (
                "published_preserved",
                "unpublished_reconciled",
                "claim_recovered",
                "retry_progressed",
                "session_stale",
            )
        ):
            return
        time.sleep(0.1)
    pytest.fail(f"restored state did not converge through ordinary recovery: {latest}")


def test_backup_failure_boundaries_release_processes_and_artifacts(
    tmp_path: Path,
) -> None:
    availability = _docker(("info", "--format", "{{.ServerVersion}}"), check=False)
    if availability.returncode != 0:
        pytest.skip("Docker daemon unavailable; backup failure checks NOT RUN")

    suffix = uuid4().hex
    project = ComposeProject(
        f"taskforge-m22-task5-failures-{suffix}",
        tmp_path / "failure.env",
        _values(f"m22-task5-failures-{suffix}"),
    )
    project.write_environment()
    process: subprocess.Popen[str] | None = None
    try:
        project.compose(("up", "--detach", "postgres"), admin=True, timeout=600)
        _wait_for_health(project, "postgres")
        _bootstrap(project)
        migration = _run_migration_process(project)
        assert migration.returncode == 0, project.redact(
            migration.stdout + migration.stderr
        )

        lost_archive = tmp_path / "lost-holder.taskforge.pgdump"
        lost = _controlled_dump(tmp_path, label="lost-holder")
        process = _start_script(
            project,
            BACKUP_SCRIPT,
            lost_archive,
            environment_overrides=lost.environment,
        )
        _wait_until("controlled dump startup", lost.started.exists)
        _wait_until("backup advisory lock", lambda: _backup_lock_count(project) == 1)
        holder_pid = int(lost.holder_pid_file.read_text(encoding="utf-8"))
        os.kill(holder_pid, signal.SIGTERM)
        _, error = process.communicate(timeout=30)
        assert process.returncode != 0
        assert "lost the migration lock during pg_dump" in error
        _wait_until("controlled dump termination", lost.terminated.exists)
        _wait_until("advisory lock release", lambda: _backup_lock_count(project) == 0)
        _assert_failed_backup_clean(lost_archive, lost)
        process = None

        # Successful bootstrap proves that the shared migration/backup lock is
        # available again after unexpected holder-session loss.
        _bootstrap(project)

        interrupted_archive = tmp_path / "interrupted.taskforge.pgdump"
        interrupted = _controlled_dump(tmp_path, label="interrupted")
        process = _start_script(
            project,
            BACKUP_SCRIPT,
            interrupted_archive,
            environment_overrides=interrupted.environment,
        )
        _wait_until("interruptible dump startup", interrupted.started.exists)
        _wait_until(
            "interruptible backup lock", lambda: _backup_lock_count(project) == 1
        )
        process.send_signal(signal.SIGTERM)
        process.communicate(timeout=30)
        assert process.returncode != 0
        _wait_until("interrupted dump termination", interrupted.terminated.exists)
        _wait_until(
            "interrupted lock release", lambda: _backup_lock_count(project) == 0
        )
        _assert_failed_backup_clean(interrupted_archive, interrupted)
        process = None

        promotion_archive = tmp_path / "promotion.taskforge.pgdump"
        real_mv = shutil.which("mv")
        assert real_mv is not None
        promotion_bin = tmp_path / "promotion-bin"
        promotion_bin.mkdir()
        first_move = tmp_path / "promotion-first-move"
        fake_mv = promotion_bin / "mv"
        fake_mv.write_text(
            "#!/bin/sh\n"
            "set -eu\n"
            f"if [ ! -e {shlex.quote(str(first_move))} ]; then\n"
            f'  {shlex.quote(real_mv)} "$@"\n'
            f"  : > {shlex.quote(str(first_move))}\n"
            "  exit 0\n"
            "fi\n"
            "exit 73\n",
            encoding="utf-8",
        )
        fake_mv.chmod(0o755)
        failed_promotion = _run_script(
            project,
            BACKUP_SCRIPT,
            promotion_archive,
            check=False,
            environment_overrides={"PATH": f"{promotion_bin}:{os.environ['PATH']}"},
        )
        assert failed_promotion.returncode != 0
        assert first_move.exists()
        assert not promotion_archive.exists()
        assert not promotion_archive.with_name(
            promotion_archive.name + ".sha256"
        ).exists()
        assert not tuple(tmp_path.glob(f".{promotion_archive.name}.tmp.*"))
        assert not tuple(tmp_path.glob(f".{promotion_archive.name}.sha256.tmp.*"))

        retry = _run_script(project, BACKUP_SCRIPT, promotion_archive)
        assert "backup completed" in retry.stdout
        assert promotion_archive.is_file()
        assert promotion_archive.with_name(promotion_archive.name + ".sha256").is_file()
    finally:
        if process is not None and process.poll() is None:
            process.send_signal(signal.SIGTERM)
            try:
                process.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.communicate(timeout=10)
        _cleanup(project)


def test_custom_backup_restores_to_independent_clean_postgresql(
    tmp_path: Path,
) -> None:
    availability = _docker(("info", "--format", "{{.ServerVersion}}"), check=False)
    if availability.returncode != 0:
        pytest.skip("Docker daemon unavailable; backup/restore checks NOT RUN")

    suffix = uuid4().hex
    image_tag = f"m22-task5-{suffix}"
    source = ComposeProject(
        f"taskforge-m22-task5-source-{suffix}",
        tmp_path / "source.env",
        _values(image_tag),
    )
    target = ComposeProject(
        f"taskforge-m22-task5-target-{suffix}",
        tmp_path / "target.env",
        _values(image_tag),
    )
    source.write_environment()
    target.write_environment()
    archive = tmp_path / "taskforge.taskforge.pgdump"
    marker_id = uuid4()

    try:
        source.compose(("up", "--detach", "postgres"), admin=True, timeout=600)
        _wait_for_health(source, "postgres")
        _bootstrap(source)
        migration = _run_migration_process(source)
        assert migration.returncode == 0, source.redact(
            migration.stdout + migration.stderr
        )
        assert "action=upgrade" in migration.stdout
        source_system = _postgres_scalar(
            source, "SELECT system_identifier FROM pg_control_system()"
        )
        _postgres_scalar(
            source,
            "INSERT INTO api_principals (id, name) VALUES "
            f"('{marker_id}'::uuid, 'backup-restore-marker') RETURNING id",
        )

        holder = _hold_migration_lock(source)
        try:
            excluded = _run_script(
                source,
                BACKUP_SCRIPT,
                archive,
                check=False,
                environment_overrides={"TASKFORGE_MIGRATION_LOCK_TIMEOUT_SECONDS": "1"},
            )
            assert excluded.returncode == 1
            assert "could not acquire the migration lock" in excluded.stderr
            assert not archive.exists()
        finally:
            _release_migration_lock(holder)

        recovery_state = asyncio.run(_seed_restore_recovery_state(_owner_url(source)))
        backup = _run_script(source, BACKUP_SCRIPT, archive)
        assert "backup completed" in backup.stdout
        assert archive.stat().st_size > 0
        assert archive.stat().st_mode & 0o777 == 0o600

        source.compose(("stop", "postgres"))
        source.compose(("rm", "--force", "postgres"))

        target.compose(("up", "--detach", "postgres"), admin=True, timeout=600)
        _wait_for_health(target, "postgres")
        target_system = _postgres_scalar(
            target, "SELECT system_identifier FROM pg_control_system()"
        )
        assert target_system != source_system

        _bootstrap(target)
        assert (
            _postgres_scalar(
                target,
                "SELECT CASE WHEN ("
                "pg_catalog.pg_get_userbyid(database.datdba)=current_user "
                "AND EXISTS (SELECT FROM pg_catalog.pg_roles WHERE "
                f"rolname='{target.values['POSTGRES_USER']}' "
                "AND NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole "
                "AND NOT rolinherit AND NOT rolreplication "
                "AND NOT rolbypassrls)) THEN 'ready' ELSE 'invalid' END "
                "FROM pg_catalog.pg_database AS "
                "database WHERE database.datname=pg_catalog.current_database()",
            )
            == "ready"
        )
        _postgres_scalar(
            target,
            "CREATE SUBSCRIPTION taskforge_restore_guard "
            "CONNECTION 'host=127.0.0.1 dbname=unreachable' "
            "PUBLICATION taskforge_restore_guard "
            "WITH (connect=false, create_slot=false, enabled=false)",
        )
        subscription_refused = _run_script(target, RESTORE_SCRIPT, archive, check=False)
        assert subscription_refused.returncode == 1
        assert "target database is not clean" in subscription_refused.stderr
        assert (
            _postgres_scalar(
                target, "SELECT to_regclass('public.alembic_version') IS NULL"
            )
            == "t"
        )
        assert (
            _postgres_scalar(
                target,
                "SELECT count(*) FROM pg_catalog.pg_subscription "
                "WHERE subname='taskforge_restore_guard'",
            )
            == "1"
        )
        _postgres_scalar(
            target,
            "ALTER SUBSCRIPTION taskforge_restore_guard SET (slot_name=NONE)",
        )
        _postgres_scalar(target, "DROP SUBSCRIPTION taskforge_restore_guard")
        restored = _run_script(target, RESTORE_SCRIPT, archive)
        assert "restore completed" in restored.stdout
        _bootstrap(target)
        verification = _run_migration_process(target)
        assert verification.returncode == 0, target.redact(
            verification.stdout + verification.stderr
        )
        assert "action=verify" in verification.stdout
        assert (
            _postgres_scalar(target, "SELECT (public.taskforge_schema_revisions())[1]")
            == EXPECTED_SCHEMA_REVISION
        )
        assert (
            _postgres_scalar(
                target,
                "SELECT count(*) FROM api_principals "
                f"WHERE id='{marker_id}'::uuid AND name='backup-restore-marker'",
            )
            == "1"
        )
        assert (
            _postgres_scalar(
                target,
                "SELECT count(*) FROM task_dispatch_outbox WHERE id IN ("
                f"'{recovery_state.published_dispatch_id}'::uuid, "
                f"'{recovery_state.unpublished_dispatch_id}'::uuid)",
            )
            == "2"
        )

        refused = _run_script(target, RESTORE_SCRIPT, archive, check=False)
        assert refused.returncode == 1
        assert "target database is not clean" in refused.stderr
        assert (
            _postgres_scalar(
                target,
                "SELECT count(*) FROM api_principals "
                f"WHERE id='{marker_id}'::uuid AND name='backup-restore-marker'",
            )
            == "1"
        )
        target.compose(("up", "--detach", "rabbitmq"), admin=True, timeout=600)
        _wait_for_health(target, "rabbitmq")
        orchestrator = _start_orchestrator(target)
        try:
            _wait_for_restored_reconciliation(target, recovery_state, orchestrator)
        finally:
            orchestrator_output = _stop_orchestrator(orchestrator)
        assert "dispatch.startup_replay.completed" in orchestrator_output
        assert orchestrator_output.count("dispatch.publish.succeeded") >= 4
    finally:
        _cleanup(source)
        _cleanup(target)
