"""Structural policy tests for the GitHub Actions workflow."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

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
ACTION_REFERENCE = re.compile(
    r"^([a-z0-9_.-]+/[a-z0-9_.-]+)@([0-9a-f]{40}) # (v[^\s]+)$"
)
USES_DIRECTIVE = re.compile(r"(?m)^\s*(?:-\s*)?uses:\s*(.+?)\s*$")
EXPECTED_ACTION_PINS = {
    (
        "actions/checkout",
        "93cb6efe18208431cddfb8368fd83d5badbf9bfd",
        "v5.0.1",
    ),
    (
        "actions/setup-python",
        "83679a892e2d95755f2dac6acb0bfd1e9ac5d548",
        "v6.1.0",
    ),
    (
        "astral-sh/setup-uv",
        "cec208311dfd045dd5311c1add060b2062131d57",
        "v8.0.0",
    ),
}


def workflow_text() -> str:
    return WORKFLOW_FILE.read_text(encoding="utf-8")


def validate_action_references(workflow: str) -> set[tuple[str, str, str]]:
    external_references: set[tuple[str, str, str]] = set()
    for directive in USES_DIRECTIVE.findall(workflow):
        if directive.startswith("./"):
            continue
        match = ACTION_REFERENCE.fullmatch(directive)
        if match is None:
            raise AssertionError(f"mutable or malformed external action: {directive}")
        external_references.add(match.groups())
    return external_references


def test_ci_has_expected_triggers_permissions_and_concurrency() -> None:
    workflow = workflow_text()

    assert re.search(r"(?m)^on:\n  push:\n    branches:\n      - main$", workflow)
    assert re.search(r"(?m)^  pull_request:$", workflow)
    assert re.search(r"(?m)^  workflow_dispatch:$", workflow)
    assert re.search(r'(?m)^  schedule:\n    - cron: "17 5 \* \* 1"$', workflow)
    assert re.search(r"(?m)^permissions:\n  contents: read$", workflow)
    assert "group: ci-${{ github.workflow }}-${{ github.ref }}" in workflow
    assert "cancel-in-progress: true" in workflow


def test_ci_uses_independent_bounded_quality_and_security_jobs() -> None:
    workflow = workflow_text()
    declared_jobs = re.findall(r"(?m)^  ([a-z][a-z0-9-]*):\n    name:", workflow)

    assert declared_jobs == ["quality", "security"]
    assert "runs-on: ubuntu-24.04" in workflow
    assert "timeout-minutes: 10" in workflow
    references = validate_action_references(workflow)
    assert references
    assert references == EXPECTED_ACTION_PINS
    assert 'python-version: "3.12"' in workflow
    assert "check-latest: false" in workflow
    assert 'version: "0.12.1"' in workflow
    assert "enable-cache: true" in workflow
    assert "cache-dependency-glob: uv.lock" in workflow

    command_positions = [
        workflow.index(f"run: {command}") for command in REQUIRED_COMMANDS
    ]
    assert command_positions == sorted(command_positions)
    assert workflow.count("fetch-depth: 0") == 1
    security_job = workflow.split("\n  security:\n", maxsplit=1)[1]
    assert "fetch-depth: 0" in security_job
    assert "run: make security" in security_job


def test_ci_pin_validator_rejects_unmatched_mutable_external_action() -> None:
    mutated_workflow = f"{workflow_text()}\n      - uses: example/action@v1\n"

    with pytest.raises(AssertionError, match="mutable or malformed external action"):
        validate_action_references(mutated_workflow)


def test_ci_does_not_expand_permissions_services_or_external_state() -> None:
    workflow = workflow_text()
    forbidden_fragments = (
        "services:",
        "secrets.",
        "continue-on-error",
        "actions/upload-artifact",
        "TASKFORGE_RUN_INTEGRATION",
        "pip install",
        "uv sync",
        "write-all",
        ": write",
    )

    assert all(fragment not in workflow for fragment in forbidden_fragments)
    assert "continue-on-error" not in workflow
    assert not re.search(r"(?m)^\s+[^#\n]*:\s*write\s*$", workflow)
