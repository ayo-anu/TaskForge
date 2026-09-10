"""Fast structural contracts for the shared Taskforge production image."""

from __future__ import annotations

import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = PROJECT_ROOT / "Dockerfile"
DOCKERIGNORE = PROJECT_ROOT / ".dockerignore"
PINNED_PYTHON_IMAGE = (
    "python:3.12.14-slim-bookworm@"
    "sha256:782412e85d0f0984994c290652577d4018aff08145c85b262bb63dc0c7522254"
)
PINNED_UV_IMAGE = (
    "ghcr.io/astral-sh/uv:0.12.1@"
    "sha256:cf4eedcaa81655197f625739489effcbe71b61ceb1506f332c3facae5deceded"
)


def _dockerfile() -> str:
    return DOCKERFILE.read_text(encoding="utf-8")


def test_build_inputs_are_immutable_and_python_downloads_are_disabled() -> None:
    dockerfile = _dockerfile()

    assert f"FROM {PINNED_UV_IMAGE} AS uv" in dockerfile
    assert re.findall(
        rf"^FROM {re.escape(PINNED_PYTHON_IMAGE)} AS (\w+)$",
        dockerfile,
        re.MULTILINE,
    ) == [
        "builder",
        "runtime",
    ]
    assert "ARG PYTHON_IMAGE" not in dockerfile
    assert "ARG UV_IMAGE" not in dockerfile
    assert "UV_PYTHON_DOWNLOADS=never" in dockerfile


def test_locked_production_install_is_isolated_to_the_builder() -> None:
    dockerfile = _dockerfile()
    builder, runtime = dockerfile.split(f"FROM {PINNED_PYTHON_IMAGE} AS runtime", 1)

    assert "COPY --from=uv /uv /usr/local/bin/uv" in builder
    assert "pip install" not in builder
    assert "uv lock --check" in builder
    assert "uv sync --frozen --no-default-groups --no-install-project" in builder
    assert "uv sync --frozen --no-default-groups --no-editable" in builder
    assert builder.index("COPY src ./src") < builder.rindex("uv sync")
    assert "rm /opt/taskforge/.venv/.lock" in builder
    assert "chmod -R go-w /opt/taskforge/.venv" in builder
    assert "uv sync" not in runtime
    assert "pip install" not in runtime


def test_taskforge_adds_no_operating_system_packages() -> None:
    dockerfile = _dockerfile().lower()

    assert "apt-get" not in dockerfile
    assert "apt " not in dockerfile
    assert "apk " not in dockerfile
    assert "dnf " not in dockerfile
    assert "yum " not in dockerfile


def test_runtime_copies_only_the_production_environment() -> None:
    dockerfile = _dockerfile()
    _builder, runtime = dockerfile.split(f"FROM {PINNED_PYTHON_IMAGE} AS runtime", 1)
    normalized_runtime = runtime.replace("\\\n", " ")

    runtime_filesystem, _api = runtime.split("FROM runtime AS api", 1)

    assert runtime_filesystem.count("COPY ") == 1
    assert "/opt/taskforge/.venv /opt/taskforge/.venv" in " ".join(
        normalized_runtime.split()
    )
    assert "/build" not in runtime_filesystem
    assert "/root/.cache/uv" not in runtime_filesystem
    assert "--chown" not in runtime_filesystem


def test_distinct_non_root_targets_use_direct_role_commands() -> None:
    dockerfile = _dockerfile()
    _builder, runtime = dockerfile.split(f"FROM {PINNED_PYTHON_IMAGE} AS runtime", 1)

    assert "USER 10001:10001" in runtime
    assert "STOPSIGNAL SIGTERM" in runtime
    assert "FROM runtime AS api" in runtime
    assert 'CMD ["python", "-m", "taskforge.api"]' in runtime
    assert "FROM runtime AS worker" in runtime
    assert 'CMD ["python", "-m", "taskforge.worker"]' in runtime
    assert "ENTRYPOINT" not in runtime


def test_build_context_is_an_explicit_production_allowlist() -> None:
    entries = tuple(
        line.strip()
        for line in DOCKERIGNORE.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    )

    assert entries == (
        "**",
        "!Dockerfile",
        "!.dockerignore",
        "!pyproject.toml",
        "!uv.lock",
        "!src/",
        "!src/**",
    )
