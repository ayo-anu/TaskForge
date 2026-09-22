"""Tests for the initial Taskforge package and process boundaries."""

from __future__ import annotations

import importlib
import os
import subprocess
import sys
from pathlib import Path

import pytest

from taskforge.api.__main__ import main as api_main
from taskforge.orchestrator.__main__ import main as orchestrator_main
from taskforge.worker.__main__ import main as worker_main

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = PROJECT_ROOT / "src"
PACKAGE_NAMES = (
    "taskforge",
    "taskforge.api",
    "taskforge.bootstrap",
    "taskforge.orchestrator",
    "taskforge.worker",
)


@pytest.mark.parametrize("package_name", PACKAGE_NAMES)
def test_package_boundary_is_importable(package_name: str) -> None:
    module = importlib.import_module(package_name)

    assert module.__name__ == package_name


def test_worker_process_fails_closed_without_required_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("TASKFORGE_WORKER_CREDENTIAL", raising=False)
    monkeypatch.delenv("TASKFORGE_WORKER_PROFILE", raising=False)

    assert worker_main() == 2


@pytest.mark.parametrize(("error", "expected"), ((None, 0), (RuntimeError(), 1)))
def test_worker_process_returns_stable_runtime_status(
    monkeypatch: pytest.MonkeyPatch, error: Exception | None, expected: int
) -> None:
    class Application:
        def __init__(self, settings: object) -> None:
            del settings

        async def run(self) -> None:
            if error is not None:
                raise error

    monkeypatch.setenv("POSTGRES_PASSWORD", "test-postgres-password")
    monkeypatch.setenv("RABBITMQ_DEFAULT_PASS", "test-rabbitmq-password")
    monkeypatch.setenv("TASKFORGE_WORKER_CREDENTIAL", "configured-for-fake")
    monkeypatch.setenv("TASKFORGE_WORKER_PROFILE", "test-profile")
    monkeypatch.setattr("taskforge.worker.__main__.WorkerApplication", Application)

    assert worker_main() == expected


@pytest.mark.parametrize(("error", "expected"), ((None, 0), (RuntimeError(), 1)))
def test_orchestrator_process_returns_stable_runtime_status(
    monkeypatch: pytest.MonkeyPatch, error: Exception | None, expected: int
) -> None:
    class Application:
        def __init__(self, settings: object) -> None:
            del settings

        async def run(self) -> None:
            if error is not None:
                raise error

    monkeypatch.setenv("POSTGRES_PASSWORD", "test-postgres-password")
    monkeypatch.setenv("RABBITMQ_DEFAULT_PASS", "test-rabbitmq-password")
    monkeypatch.setattr(
        "taskforge.orchestrator.__main__.OrchestratorApplication", Application
    )

    assert orchestrator_main() == expected


def test_orchestrator_process_fails_closed_without_dependency_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    monkeypatch.delenv("TASKFORGE_POSTGRES_PASSWORD", raising=False)
    monkeypatch.delenv("RABBITMQ_DEFAULT_PASS", raising=False)
    monkeypatch.delenv("TASKFORGE_RABBITMQ_PASSWORD", raising=False)

    assert orchestrator_main() == 2


def test_api_entry_point_uses_typed_runtime_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    invocation: dict[str, object] = {}

    def record_uvicorn_invocation(settings: object, **kwargs: object) -> None:
        invocation["settings"] = settings
        invocation.update(kwargs)

    monkeypatch.setenv("POSTGRES_PASSWORD", "test-postgres-password")
    monkeypatch.delenv("RABBITMQ_DEFAULT_PASS", raising=False)
    monkeypatch.delenv("TASKFORGE_RABBITMQ_PASSWORD", raising=False)
    monkeypatch.setenv("TASKFORGE_API_HOST", "127.0.0.2")
    monkeypatch.setenv("TASKFORGE_API_PORT", "8765")
    monkeypatch.setenv("TASKFORGE_LOG_LEVEL", "WARNING")
    monkeypatch.setattr(
        "taskforge.api.__main__.run_api_server",
        record_uvicorn_invocation,
    )

    assert api_main() == 0
    configured = invocation["settings"]
    assert configured.api_host == "127.0.0.2"  # type: ignore[attr-defined]
    assert configured.api_port == 8765  # type: ignore[attr-defined]
    assert configured.log_level == "WARNING"  # type: ignore[attr-defined]
    assert configured.api_graceful_shutdown_timeout_seconds == 30  # type: ignore[attr-defined]
    assert "metrics_runtime" in invocation
    assert "tracing_runtime" in invocation


def test_worker_process_rejects_blank_credential_as_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_PASSWORD", "test-postgres-password")
    monkeypatch.setenv("RABBITMQ_DEFAULT_PASS", "test-rabbitmq-password")
    monkeypatch.setenv("TASKFORGE_WORKER_CREDENTIAL", "")
    monkeypatch.setenv("TASKFORGE_WORKER_PROFILE", "pipeline")

    assert worker_main() == 2


def test_worker_process_module_fails_closed_without_required_settings() -> None:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(SOURCE_ROOT)

    result = subprocess.run(
        [sys.executable, "-m", "taskforge.worker"],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == "taskforge worker configuration is invalid\n"
