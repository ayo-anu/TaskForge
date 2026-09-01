"""Structural tests for the local Python quality toolchain."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT_FILE = PROJECT_ROOT / "pyproject.toml"
MAKEFILE = PROJECT_ROOT / "Makefile"
REQUIRED_DEVELOPMENT_TOOLS = {
    "alembic",
    "editables",
    "hatchling",
    "httpx2[ws]",
    "mypy",
    "pytest",
    "pytest-cov",
    "ruff",
}
REQUIRED_MAKE_TARGETS = {
    "install",
    "security",
    "format",
    "format-check",
    "lint",
    "typecheck",
    "test",
    "coverage",
    "privilege-bootstrap",
    "migrations-check",
    "migration-test",
    "claim-test",
    "renewal-test",
    "retry-test",
    "recovery-test",
    "authentication-test",
    "authorization-test",
    "protected-route-test",
    "credential-bootstrap-test",
    "workflow-persistence-test",
    "workflow-route-test",
    "broker-dispatch-test",
    "m21-workload",
    "m21-measurement",
    "m21-contention",
    "m21-profiling",
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
        re.split(r"[<>=!~]", requirement, maxsplit=1)[0]
        for requirement in development_dependencies
        if isinstance(requirement, str)
    }

    assert declared_tools == REQUIRED_DEVELOPMENT_TOOLS
    security_dependencies = dependency_groups["security"]
    assert security_dependencies == ["pip-audit==2.10.1"]

    build_system = pyproject["build-system"]
    assert isinstance(build_system, dict)
    assert build_system["requires"] == ["hatchling==1.32.0", "editables==0.5"]
    assert "hatchling==1.32.0" in development_dependencies
    assert "editables==0.5" in development_dependencies


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
    assert "uv sync --locked --all-groups --no-install-project" in makefile
    assert (
        "uv sync --offline --locked --all-groups --no-build-isolation-package taskforge"
    ) in makefile
    assert "uv sync --locked --all-groups --no-build-isolation\n" not in makefile
    assert (
        "security: install\n\tuv run --no-sync python scripts/security_scan.py"
        in makefile
    )
    assert "ruff format --check src tests migrations" in makefile
    assert "ruff check src tests migrations" in makefile
    assert "TASKFORGE_RUN_BROKER_INTEGRATION=1 is required" in makefile
    assert "TASKFORGE_RUN_CLAIM_INTEGRATION=1 is required" in makefile
    assert "TASKFORGE_RUN_RECOVERY_INTEGRATION=1 is required" in makefile
    assert "TASKFORGE_RUN_M21_CONTENTION=1 is required" in makefile
    assert "TASKFORGE_M21_CONTENTION_DATABASE_URL is required" in makefile
    assert "TASKFORGE_RUN_M21_PROFILING=1 is required" in makefile
    assert "TASKFORGE_M21_PROFILE_OUTPUT is required" in makefile
    assert "tests/integration/test_dispatch_publisher_broker.py" in makefile
    assert "tests/integration/test_dispatch_topology_broker.py" in makefile
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
