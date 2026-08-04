"""Structural tests for the local Python quality toolchain."""

from __future__ import annotations

import tomllib
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT_FILE = PROJECT_ROOT / "pyproject.toml"
MAKEFILE = PROJECT_ROOT / "Makefile"
REQUIRED_DEVELOPMENT_TOOLS = {
    "alembic",
    "httpx2",
    "mypy",
    "pytest",
    "pytest-cov",
    "ruff",
}
REQUIRED_MAKE_TARGETS = {
    "install",
    "format",
    "format-check",
    "lint",
    "typecheck",
    "test",
    "coverage",
    "migrations-check",
    "migration-test",
    "check",
    "clean",
}


def load_pyproject() -> dict[str, object]:
    """Load the repository's central Python tool configuration."""
    return tomllib.loads(PYPROJECT_FILE.read_text(encoding="utf-8"))


def test_required_development_tools_are_declared() -> None:
    pyproject = load_pyproject()
    dependency_groups = pyproject["dependency-groups"]
    assert isinstance(dependency_groups, dict)
    development_dependencies = dependency_groups["dev"]
    assert isinstance(development_dependencies, list)

    declared_tools = {
        requirement.partition(">=")[0]
        for requirement in development_dependencies
        if isinstance(requirement, str)
    }

    assert declared_tools == REQUIRED_DEVELOPMENT_TOOLS


def test_sqlalchemy_is_a_runtime_persistence_dependency() -> None:
    pyproject = load_pyproject()
    project = pyproject["project"]
    assert isinstance(project, dict)
    dependencies = project["dependencies"]
    assert isinstance(dependencies, list)

    assert "sqlalchemy>=2,<3" in dependencies


def test_quality_policy_is_centrally_configured() -> None:
    pyproject = load_pyproject()
    tool = pyproject["tool"]
    assert isinstance(tool, dict)

    assert {"ruff", "mypy", "pytest", "coverage"} <= tool.keys()

    mypy = tool["mypy"]
    assert isinstance(mypy, dict)
    assert mypy["strict"] is True
    assert mypy["python_version"] == "3.12"

    coverage = tool["coverage"]
    assert isinstance(coverage, dict)
    coverage_run = coverage["run"]
    coverage_report = coverage["report"]
    assert isinstance(coverage_run, dict)
    assert isinstance(coverage_report, dict)
    assert coverage_run["branch"] is True
    assert coverage_report["fail_under"] == 85


def test_makefile_exposes_consistent_developer_commands() -> None:
    makefile = MAKEFILE.read_text(encoding="utf-8")
    declared_targets = {
        line.partition(":")[0]
        for line in makefile.splitlines()
        if line and not line.startswith(("\t", ".")) and ":" in line
    }

    assert declared_targets == REQUIRED_MAKE_TARGETS
    assert "uv sync --locked --dev" in makefile
    assert "ruff format --check src tests migrations" in makefile
    assert "ruff check src tests migrations" in makefile
    assert "check: format-check lint typecheck coverage migrations-check" in makefile


def test_clean_command_preserves_environment_and_docker_resources() -> None:
    makefile = MAKEFILE.read_text(encoding="utf-8")
    clean_commands = makefile.split("clean:\n", maxsplit=1)[1]

    assert ".venv" not in clean_commands
    assert "docker" not in clean_commands.lower()
    assert ".pytest_cache" in clean_commands
    assert "__pycache__" in clean_commands
    assert "find src tests migrations" in clean_commands
    assert ".coverage" in clean_commands
    assert "htmlcov" in clean_commands
