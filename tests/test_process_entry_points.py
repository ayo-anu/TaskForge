"""Tests for the initial Taskforge package and process boundaries."""

from __future__ import annotations

import importlib
import os
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

from taskforge.api.__main__ import main as api_main
from taskforge.worker.__main__ import main as worker_main

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = PROJECT_ROOT / "src"
PACKAGE_NAMES = (
    "taskforge",
    "taskforge.api",
    "taskforge.orchestrator",
    "taskforge.worker",
)
PROCESS_MODULES = ("taskforge.api", "taskforge.worker")
PROCESS_ENTRY_POINTS = (api_main, worker_main)


@pytest.mark.parametrize("package_name", PACKAGE_NAMES)
def test_package_boundary_is_importable(package_name: str) -> None:
    module = importlib.import_module(package_name)

    assert module.__name__ == package_name


@pytest.mark.parametrize("entry_point", PROCESS_ENTRY_POINTS)
def test_process_entry_point_returns_success(entry_point: Callable[[], int]) -> None:
    assert entry_point() == 0


@pytest.mark.parametrize("module_name", PROCESS_MODULES)
def test_process_entry_point_exits_cleanly(module_name: str) -> None:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(SOURCE_ROOT)

    result = subprocess.run(
        [sys.executable, "-m", module_name],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode == 0
    assert result.stdout == ""
    assert result.stderr == ""
