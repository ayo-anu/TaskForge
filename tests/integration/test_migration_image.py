"""Real migration-image minimality, ownership, and failure contracts."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_CONTAINER_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_CONTAINER_INTEGRATION=1 explicitly",
    ),
]

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _docker(
    arguments: tuple[str, ...], *, check: bool = True, timeout: float = 600
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ("docker", *arguments),
        cwd=PROJECT_ROOT,
        capture_output=True,
        check=False,
        text=True,
        timeout=timeout,
    )
    if check and result.returncode != 0:
        pytest.fail(
            f"Docker command failed ({result.returncode}): "
            f"{' '.join(arguments[:3])}\n{result.stdout}{result.stderr}"
        )
    return result


def test_real_migration_image_is_minimal_non_root_and_read_only() -> None:
    availability = _docker(("info", "--format", "{{.ServerVersion}}"), check=False)
    if availability.returncode != 0:
        pytest.skip(
            "Docker daemon unavailable; migration-image checks NOT RUN: "
            f"{availability.stderr.strip()}"
        )

    image = f"taskforge-migration:m22-task3-{uuid4().hex}"
    try:
        _docker(("build", "--target", "migration", "--tag", image, "."))
        inspection = cast(
            list[dict[str, Any]],
            json.loads(_docker(("image", "inspect", image)).stdout),
        )[0]
        assert inspection["Config"]["User"] == "10001:10001"
        assert inspection["Config"]["Entrypoint"] is None
        assert inspection["Config"]["Cmd"] == [
            "python",
            "-m",
            "taskforge.database_migrations",
        ]

        script = """
import importlib.metadata
import json
import os
from pathlib import Path

root = Path('/opt/taskforge')
packages = sorted(
    distribution.metadata['Name'].lower()
    for distribution in importlib.metadata.distributions()
)
try:
    (root / 'forbidden-write').write_text('no', encoding='utf-8')
except OSError:
    write_blocked = True
else:
    write_blocked = False
print(json.dumps({
    'uid': os.getuid(),
    'gid': os.getgid(),
    'packages': packages,
    'entries': sorted(path.name for path in root.iterdir()),
    'write_blocked': write_blocked,
    'uv_binary': (root / '.venv/bin/uv').exists(),
}))
"""
        result = _docker(("run", "--rm", "--read-only", image, "python", "-c", script))
        evidence = json.loads(result.stdout)
        assert evidence["uid"] == evidence["gid"] == 10001
        assert {
            "alembic",
            "asyncpg",
            "opentelemetry-api",
            "opentelemetry-exporter-otlp-proto-http",
            "opentelemetry-sdk",
            "pydantic-settings",
            "sqlalchemy",
        } <= set(evidence["packages"])
        assert set(evidence["packages"]).isdisjoint({"aio-pika", "fastapi", "uvicorn"})
        _docker(
            (
                "run",
                "--rm",
                "--read-only",
                image,
                "python",
                "-c",
                "import taskforge.persistence.schema",
            )
        )
        assert evidence["entries"] == [".venv", "alembic.ini", "migrations", "src"]
        assert evidence["write_blocked"] is True
        assert evidence["uv_binary"] is False

        ownership_script = """
import json
import stat
from pathlib import Path

root = Path('/opt/taskforge')
bad = []
for path in (root, *root.rglob('*')):
    details = path.lstat()
    writable = not path.is_symlink() and details.st_mode & (
        stat.S_IWGRP | stat.S_IWOTH
    )
    if details.st_uid != 0 or details.st_gid != 0 or writable:
        bad.append(str(path))
print(json.dumps({
    'bad': bad,
    'build_root': Path('/build').exists(),
    'uv_cache': Path('/root/.cache/uv').exists(),
}))
"""
        ownership = json.loads(
            _docker(
                (
                    "run",
                    "--rm",
                    "--read-only",
                    "--user",
                    "0:0",
                    image,
                    "python",
                    "-c",
                    ownership_script,
                )
            ).stdout
        )
        assert ownership == {"bad": [], "build_root": False, "uv_cache": False}

        owner_secret = "synthetic-owner-secret-must-not-leak"
        failure = _docker(
            (
                "run",
                "--rm",
                "--read-only",
                "--env",
                "POSTGRES_HOST=127.0.0.1",
                "--env",
                "POSTGRES_PORT=1",
                "--env",
                "POSTGRES_DB=taskforge",
                "--env",
                "POSTGRES_OWNER_USER=taskforge_owner",
                "--env",
                f"POSTGRES_OWNER_PASSWORD={owner_secret}",
                "--env",
                "TASKFORGE_MIGRATION_LOCK_TIMEOUT_SECONDS=1",
                image,
            ),
            check=False,
            timeout=30,
        )
        assert failure.returncode == 1
        assert "TaskForge migration failed error_type=" in failure.stderr
        assert owner_secret not in failure.stdout + failure.stderr
    finally:
        _docker(("image", "rm", "--force", image), check=False)
        assert _docker(("image", "inspect", image), check=False).returncode != 0
