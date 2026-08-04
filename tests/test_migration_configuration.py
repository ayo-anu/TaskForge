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


def test_migration_graph_starts_empty_and_has_a_locked_validation_command() -> None:
    revision_files = [
        path for path in VERSIONS_DIRECTORY.iterdir() if path.name != ".gitkeep"
    ]
    makefile = MAKEFILE.read_text(encoding="utf-8")

    assert revision_files == []
    assert "migrations-check:\n\tuv run alembic heads --verbose" in makefile
    assert "check: format-check lint typecheck coverage migrations-check" in makefile
