"""Structural tests for credential-free migration graph validation."""

from __future__ import annotations

from pathlib import Path

from alembic.config import Config

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ALEMBIC_FILE = PROJECT_ROOT / "alembic.ini"
MIGRATIONS_DIRECTORY = PROJECT_ROOT / "migrations"
VERSIONS_DIRECTORY = MIGRATIONS_DIRECTORY / "versions"
MAKEFILE = PROJECT_ROOT / "Makefile"


def test_alembic_uses_the_repository_migration_directory() -> None:
    config = Config(ALEMBIC_FILE)

    assert config.get_main_option("script_location") == str(MIGRATIONS_DIRECTORY)
    assert config.get_main_option("sqlalchemy.url") is None
    assert (MIGRATIONS_DIRECTORY / "env.py").is_file()
    assert (MIGRATIONS_DIRECTORY / "script.py.mako").is_file()
    assert VERSIONS_DIRECTORY.is_dir()


def test_migration_configuration_contains_no_embedded_credentials() -> None:
    configuration = ALEMBIC_FILE.read_text(encoding="utf-8")
    environment = (MIGRATIONS_DIRECTORY / "env.py").read_text(encoding="utf-8")
    combined = f"{configuration}\n{environment}".lower()

    assert "password=" not in combined
    assert "://" not in combined
    assert "replace-with" not in combined
    assert "taskforge_database_url" in combined


def test_migration_graph_has_one_workflow_head_and_locked_validation_commands() -> None:
    revision_files = sorted(VERSIONS_DIRECTORY.glob("*.py"))
    makefile = MAKEFILE.read_text(encoding="utf-8")

    assert [path.name for path in revision_files] == [
        "0001_create_identity_foundation.py",
        "0002_create_workflow_definition_foundation.py",
        "0003_add_workflow_list_index.py",
        "0004_create_workflow_version_snapshots.py",
        "0005_protect_workflow_version_snapshots.py",
        "0006_create_workflow_run_foundation.py",
        "0007_create_task_attempt_dispatch_outbox.py",
        "0008_create_worker_sessions_health.py",
        "0009_create_task_claim_history.py",
        "0010_create_task_claim_events.py",
        "0011_add_task_context_deadlines.py",
        "0012_add_task_execution_timeout.py",
        "0013_add_task_attempt_results.py",
        "0014_add_retry_policy_eligibility.py",
        "0015_add_task_retry_events.py",
        "0016_add_claim_expired_result.py",
        "0017_add_recovery_result_events.py",
        "0018_create_dead_letter_persistence.py",
        "0019_add_dead_letter_redrive_lineage.py",
        "0020_create_workflow_run_cancellation_requests.py",
        "0021_widen_recovered_cancellation_events.py",
        "0022_create_workflow_run_execution_events.py",
        "0023_publish_workflow_run_execution_event_wakeups.py",
        "0024_add_workflow_run_replay_lineage.py",
        "0025_complete_audit_history.py",
        "0026_standardize_audit_semantics.py",
    ]
    assert "migrations-check:\n\tuv run alembic heads --verbose" in makefile
    assert "migration-test:" in makefile
    assert "TASKFORGE_RUN_MIGRATION_INTEGRATION" in makefile
    assert "TASKFORGE_MIGRATION_TEST_DATABASE_URL" in makefile
    assert "check: format-check lint typecheck coverage migrations-check" in makefile


def test_alembic_uses_registered_shared_metadata() -> None:
    environment = (MIGRATIONS_DIRECTORY / "env.py").read_text(encoding="utf-8")

    assert "from taskforge.persistence.schema import metadata" in environment
    assert "target_metadata = metadata" in environment
    assert "async_engine_from_config" in environment
    assert "TASKFORGE_DATABASE_URL" in environment
