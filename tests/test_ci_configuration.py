"""Structural policy tests for the GitHub Actions workflow."""

from __future__ import annotations

import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_FILE = PROJECT_ROOT / ".github" / "workflows" / "ci.yml"
REQUIRED_COMMANDS = (
    "make install",
    "make format-check",
    "make lint",
    "make typecheck",
    "make coverage",
    "make migrations-check",
)


def workflow_text() -> str:
    return WORKFLOW_FILE.read_text(encoding="utf-8")


def test_ci_has_expected_triggers_permissions_and_concurrency() -> None:
    workflow = workflow_text()

    assert re.search(r"(?m)^on:\n  push:\n    branches:\n      - main$", workflow)
    assert re.search(r"(?m)^  pull_request:$", workflow)
    assert re.search(r"(?m)^  workflow_dispatch:$", workflow)
    assert re.search(r"(?m)^permissions:\n  contents: read$", workflow)
    assert "group: ci-${{ github.workflow }}-${{ github.ref }}" in workflow
    assert "cancel-in-progress: true" in workflow


def test_ci_uses_one_bounded_locked_quality_job() -> None:
    workflow = workflow_text()
    declared_jobs = re.findall(r"(?m)^  ([a-z][a-z0-9-]*):\n    name:", workflow)

    assert declared_jobs == ["quality"]
    assert "runs-on: ubuntu-24.04" in workflow
    assert "timeout-minutes: 10" in workflow
    assert "uses: actions/checkout@v5" in workflow
    assert "uses: actions/setup-python@v6" in workflow
    assert 'python-version: "3.12"' in workflow
    assert "check-latest: false" in workflow
    assert "uses: astral-sh/setup-uv@v8" in workflow
    assert 'version: "0.12.1"' in workflow
    assert "enable-cache: true" in workflow
    assert "cache-dependency-glob: uv.lock" in workflow

    command_positions = [
        workflow.index(f"run: {command}") for command in REQUIRED_COMMANDS
    ]
    assert command_positions == sorted(command_positions)


def test_ci_does_not_expand_permissions_services_or_external_state() -> None:
    workflow = workflow_text()
    forbidden_fragments = (
        "services:",
        "secrets.",
        "continue-on-error",
        "actions/upload-artifact",
        "docker ",
        "TASKFORGE_RUN_INTEGRATION",
        "pip install",
        "uv sync",
        "write-all",
        ": write",
    )

    assert all(fragment not in workflow for fragment in forbidden_fragments)
