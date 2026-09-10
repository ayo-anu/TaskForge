"""Opt-in real-Docker verification of Taskforge production images."""

from __future__ import annotations

import asyncio
import json
import os
import shlex
import shutil
import subprocess
import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from taskforge.identity.provisioning import (
    CredentialIssuanceService,
    IdentityProvisioningService,
)
from taskforge.persistence.provisioning import SQLAlchemyProvisioningRepository
from tests.integration.postgresql import (
    asyncpg_dsn,
    create_database,
    drop_database,
    migration_database_url,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_CONTAINER_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_CONTAINER_INTEGRATION=1 explicitly",
    ),
]

PROJECT_ROOT = Path(__file__).resolve().parents[2]
EXPECTED_COMMANDS = {
    "api": ("python", "-m", "taskforge.api"),
    "orchestrator": ("python", "-m", "taskforge.orchestrator"),
    "worker": ("python", "-m", "taskforge.worker"),
}
PRODUCTION_PACKAGES = {
    "aio-pika",
    "asyncpg",
    "fastapi",
    "opentelemetry-api",
    "opentelemetry-exporter-otlp-proto-http",
    "opentelemetry-sdk",
    "pydantic",
    "pydantic-settings",
    "sqlalchemy",
    "taskforge",
    "uvicorn",
}
TASKFORGE_BUILD_AND_DEVELOPMENT_PACKAGES = {
    "alembic",
    "editables",
    "hatchling",
    "mypy",
    "pip-audit",
    "pytest",
    "pytest-cov",
    "ruff",
    "uv",
}
POSTGRES_OWNER_PASSWORD = "m22-postgres-owner-secret"
POSTGRES_RUNTIME_PASSWORD = "m22-postgres-runtime-secret"
RABBITMQ_PASSWORD = "m22-rabbitmq-secret"
CLAIM_RESULT_AUTHORITY_SECRET = "m22-claim-result-authority-secret"
TEST_SECRET_VALUES = (
    POSTGRES_OWNER_PASSWORD,
    POSTGRES_RUNTIME_PASSWORD,
    RABBITMQ_PASSWORD,
    CLAIM_RESULT_AUTHORITY_SECRET,
)
POSTGRES_IMAGE = "postgres:18.4-bookworm"
RABBITMQ_IMAGE = "rabbitmq:4.3.3-management"
POSTGRES_OWNER = "taskforge_m22_owner"
POSTGRES_ADMIN_DATABASE = "postgres"
POSTGRES_RUNTIME_USER = "taskforge_runtime"
RABBITMQ_USER = "taskforge_m22"
RABBITMQ_VHOST = "taskforge_m22"
PRIVILEGE_BOOTSTRAP = PROJECT_ROOT / "docker/postgres/init-taskforge-roles.sh"
EXPECTED_PIPELINE_CAPABILITIES = (
    "pipeline.ingestion",
    "pipeline.notification",
    "pipeline.processing",
)


@dataclass(frozen=True)
class BuiltImages:
    api: str
    worker: str


@dataclass(frozen=True)
class RealDependencies:
    owner_database_url: URL
    runtime_database_url: URL
    network: str
    postgres_container: str
    rabbitmq_container: str
    worker_identity_id: UUID
    worker_credential: str


def _redacted_arguments(arguments: Sequence[str]) -> tuple[str, ...]:
    rendered: list[str] = []
    redact_next = False
    for argument in arguments:
        if redact_next:
            name, separator, _value = argument.partition("=")
            rendered.append(f"{name}=<redacted>" if separator else "<redacted>")
            redact_next = False
            continue
        rendered.append(argument)
        redact_next = argument in {"--env", "-e"}
    return tuple(rendered)


def _docker(
    arguments: Sequence[str],
    *,
    cwd: Path = PROJECT_ROOT,
    timeout: float = 300,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result: subprocess.CompletedProcess[str] | None = None
    try:
        result = subprocess.run(
            ["docker", *arguments],
            cwd=cwd,
            capture_output=True,
            check=False,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        pass
    display = shlex.join(("docker", *_redacted_arguments(arguments)))
    if result is None:
        pytest.fail(f"{display} timed out after {timeout} seconds")
    if check and result.returncode != 0:
        pytest.fail(
            f"{display} failed ({result.returncode})\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


@pytest.fixture(scope="module")
def images() -> Iterator[BuiltImages]:
    availability = _docker(("info", "--format", "{{.ServerVersion}}"), check=False)
    if availability.returncode != 0:
        pytest.skip(
            "Docker daemon unavailable; real-Docker M22 checks NOT RUN: "
            f"{availability.stderr.strip()}"
        )

    suffix = uuid4().hex
    built = BuiltImages(
        api=f"taskforge-m22-api-test:{suffix}",
        worker=f"taskforge-m22-worker-test:{suffix}",
    )
    _docker(("build", "--target", "api", "--tag", built.api, "."), timeout=600)
    _docker(("build", "--target", "worker", "--tag", built.worker, "."), timeout=600)
    try:
        yield built
    finally:
        _docker(("image", "rm", "--force", built.api, built.worker), check=False)


async def _provision_worker_credential(database_url: URL) -> tuple[UUID, str]:
    engine = create_async_engine(database_url.set(drivername="postgresql+asyncpg"))
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    repository = SQLAlchemyProvisioningRepository(sessions)
    try:
        async with repository.transaction() as transaction:
            worker_id = await IdentityProvisioningService().create_worker_identity(
                transaction,
                name=f"m22-container-worker-{uuid4().hex}",
            )
            generated = await CredentialIssuanceService().issue_worker_credential(
                transaction,
                worker_id=worker_id,
                expires_at=datetime.now(UTC) + timedelta(hours=1),
            )
            await transaction.commit()
        return worker_id, generated.take_presented_value()
    finally:
        await engine.dispose()


async def _drop_unconfigured_runtime_role(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        await connection.execute(f'DROP ROLE "{POSTGRES_RUNTIME_USER}"')
    finally:
        await connection.close()


def _bootstrap_runtime_role(database_url: URL) -> None:
    environment = os.environ.copy()
    environment.update(
        {
            "PGHOST": database_url.host or "",
            "PGPORT": str(database_url.port or 5432),
            "PGPASSWORD": database_url.password or "",
            "POSTGRES_DB": database_url.database or "",
            "POSTGRES_USER": database_url.username or "",
            "TASKFORGE_RUNTIME_USER": POSTGRES_RUNTIME_USER,
            "TASKFORGE_RUNTIME_PASSWORD": POSTGRES_RUNTIME_PASSWORD,
        }
    )
    result = subprocess.run(
        ["sh", str(PRIVILEGE_BOOTSTRAP)],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        pytest.fail(
            "TaskForge runtime-role bootstrap failed "
            f"({result.returncode})\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )


async def _assert_runtime_database_boundary(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        assert await connection.fetchval("SELECT current_user") == POSTGRES_RUNTIME_USER
        assert await connection.fetchval(
            "SELECT has_table_privilege(current_user, 'worker_identities', 'SELECT')"
        )
        assert await connection.fetchval(
            "SELECT has_table_privilege(current_user, 'worker_sessions', 'INSERT')"
        )
        assert not await connection.fetchval(
            "SELECT has_schema_privilege(current_user, 'public', 'CREATE')"
        )
    finally:
        await connection.close()


def _wait_for_dependency(
    container: str,
    command_line: tuple[str, ...],
    *,
    user: str | None = None,
) -> None:
    deadline = time.monotonic() + 60
    latest = ""
    while time.monotonic() < deadline:
        user_arguments = ("--user", user) if user is not None else ()
        result = _docker(
            ("exec", *user_arguments, container, *command_line), check=False
        )
        latest = result.stdout + result.stderr
        if result.returncode == 0:
            return
        if not _inspect(container)["State"]["Running"]:
            pytest.fail(f"dependency container exited:\n{_logs(container)}")
        time.sleep(0.25)
    pytest.fail(f"dependency did not become ready:\n{latest}\n{_logs(container)}")


def _published_port(container: str, private_port: int) -> int:
    result = _docker(("port", container, f"{private_port}/tcp"))
    endpoint = result.stdout.strip().splitlines()[0]
    return int(endpoint.rsplit(":", maxsplit=1)[1])


@pytest.fixture(scope="module")
def real_dependencies() -> Iterator[RealDependencies]:
    suffix = uuid4().hex
    network = f"taskforge-m22-network-{suffix}"
    postgres = f"taskforge-m22-postgres-{suffix}"
    rabbitmq = f"taskforge-m22-rabbitmq-{suffix}"
    database_name = f"taskforge_m21_workload_{uuid4().hex}"
    started: list[str] = []
    _docker(("network", "create", network))
    try:
        _docker(
            (
                "run",
                "--detach",
                "--name",
                postgres,
                "--hostname",
                postgres,
                "--network",
                network,
                "--publish",
                "127.0.0.1::5432",
                "--tmpfs",
                "/var/lib/postgresql:rw",
                "--env",
                f"POSTGRES_USER={POSTGRES_OWNER}",
                "--env",
                f"POSTGRES_PASSWORD={POSTGRES_OWNER_PASSWORD}",
                "--env",
                f"POSTGRES_DB={POSTGRES_ADMIN_DATABASE}",
                POSTGRES_IMAGE,
            ),
            timeout=600,
        )
        started.append(postgres)
        _docker(
            (
                "run",
                "--detach",
                "--name",
                rabbitmq,
                "--hostname",
                rabbitmq,
                "--network",
                network,
                "--tmpfs",
                "/var/lib/rabbitmq:rw,uid=999,gid=999,mode=1777",
                "--env",
                f"RABBITMQ_DEFAULT_USER={RABBITMQ_USER}",
                "--env",
                f"RABBITMQ_DEFAULT_PASS={RABBITMQ_PASSWORD}",
                "--env",
                f"RABBITMQ_DEFAULT_VHOST={RABBITMQ_VHOST}",
                RABBITMQ_IMAGE,
            ),
            timeout=600,
        )
        started.append(rabbitmq)
        _wait_for_dependency(
            postgres,
            ("pg_isready", "--username", POSTGRES_OWNER, "--dbname", "postgres"),
        )
        _wait_for_dependency(
            rabbitmq,
            ("rabbitmq-diagnostics", "-q", "ping"),
            user="999:999",
        )
        _wait_for_dependency(
            rabbitmq,
            ("rabbitmq-diagnostics", "-q", "check_port_connectivity"),
            user="999:999",
        )

        administrative_url = URL.create(
            "postgresql+asyncpg",
            username=POSTGRES_OWNER,
            password=POSTGRES_OWNER_PASSWORD,
            host="127.0.0.1",
            port=_published_port(postgres, 5432),
            database=POSTGRES_ADMIN_DATABASE,
        )
        asyncio.run(create_database(administrative_url, database_name))
        owner_database_url = administrative_url.set(database=database_name)
        asyncio.run(_drop_unconfigured_runtime_role(owner_database_url))
        _bootstrap_runtime_role(owner_database_url)
        rendered = owner_database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        with migration_database_url(rendered):
            command.upgrade(Config("alembic.ini"), "head")
        runtime_database_url = owner_database_url.set(
            username=POSTGRES_RUNTIME_USER,
            password=POSTGRES_RUNTIME_PASSWORD,
        )
        asyncio.run(_assert_runtime_database_boundary(runtime_database_url))
        worker_identity_id, worker_credential = asyncio.run(
            _provision_worker_credential(owner_database_url)
        )
        yield RealDependencies(
            owner_database_url,
            runtime_database_url,
            network,
            postgres,
            rabbitmq,
            worker_identity_id,
            worker_credential,
        )
        asyncio.run(drop_database(administrative_url, database_name))
    finally:
        for container in reversed(started):
            _docker(("rm", "--force", "--volumes", container), check=False)
        _docker(("network", "rm", network), check=False)


def _inspect(image_or_container: str) -> dict[str, Any]:
    result = _docker(("inspect", image_or_container))
    records = cast(list[dict[str, Any]], json.loads(result.stdout))
    assert len(records) == 1
    return records[0]


def _logs(container_id: str) -> str:
    result = _docker(("logs", container_id))
    return result.stdout + result.stderr


def _dependency_environment(
    dependencies: RealDependencies,
    *,
    suffix: str,
    worker: bool = False,
    overrides: dict[str, str] | None = None,
) -> tuple[str, ...]:
    database_url = dependencies.runtime_database_url
    assert database_url.host and database_url.username and database_url.password
    values = {
        "POSTGRES_HOST": dependencies.postgres_container,
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": database_url.database or "postgres",
        "POSTGRES_USER": database_url.username,
        "POSTGRES_PASSWORD": database_url.password,
        "RABBITMQ_HOST": dependencies.rabbitmq_container,
        "RABBITMQ_AMQP_PORT": "5672",
        "RABBITMQ_DEFAULT_USER": RABBITMQ_USER,
        "RABBITMQ_DEFAULT_PASS": RABBITMQ_PASSWORD,
        "RABBITMQ_DEFAULT_VHOST": RABBITMQ_VHOST,
        "TASKFORGE_ENVIRONMENT": "production",
        "TASKFORGE_API_HOST": "127.0.0.1",
        "TASKFORGE_API_PORT": "8000",
        "TASKFORGE_TASK_CLAIM_RESULT_AUTHORITY_SECRET": (CLAIM_RESULT_AUTHORITY_SECRET),
        "TASKFORGE_RABBITMQ_DISPATCH_EXCHANGE_NAME": f"taskforge.m22.{suffix}",
        "TASKFORGE_RABBITMQ_MALFORMED_EXCHANGE_NAME": (
            f"taskforge.m22.malformed.{suffix}"
        ),
        "TASKFORGE_LOG_LEVEL": "INFO",
    }
    if worker:
        values.update(
            {
                "TASKFORGE_WORKER_CREDENTIAL": dependencies.worker_credential,
                "TASKFORGE_WORKER_PROFILE": "pipeline",
            }
        )
    if overrides is not None:
        values.update(overrides)
    return tuple(
        item for name, value in values.items() for item in ("--env", f"{name}={value}")
    )


def _wait_for_api(
    container_id: str,
    *,
    expected_status: int = 200,
    expected_payload: dict[str, object] | None = None,
) -> None:
    probe = """
import json
import urllib.error
import urllib.request

try:
    response = urllib.request.urlopen(
        "http://127.0.0.1:8000/ready", timeout=1
    )
except urllib.error.HTTPError as error:
    response = error
with response:
    print(json.dumps({"status": response.status, "payload": json.load(response)}))
"""
    deadline = time.monotonic() + 20
    latest = ""
    while time.monotonic() < deadline:
        if not _inspect(container_id)["State"]["Running"]:
            pytest.fail(f"API exited during startup:\n{_logs(container_id)}")
        result = _docker(
            ("exec", container_id, "python", "-c", probe),
            timeout=5,
            check=False,
        )
        if result.returncode == 0:
            latest = result.stdout
            response = json.loads(result.stdout)
            if response["status"] == expected_status and (
                expected_payload is None or response["payload"] == expected_payload
            ):
                return
        else:
            latest = result.stdout + result.stderr
        time.sleep(0.05)
    pytest.fail(
        "API did not reach its expected readiness boundary:\n"
        f"latest probe:\n{latest}\ncontainer logs:\n{_logs(container_id)}"
    )


def _run_role_container(
    images: BuiltImages,
    role: str,
    environment: tuple[str, ...],
    *,
    network: str | None = None,
    detach: bool = False,
    timeout: float = 30,
) -> subprocess.CompletedProcess[str]:
    if role not in EXPECTED_COMMANDS:
        raise AssertionError(f"unexpected role {role}")
    image = images.worker if role == "worker" else images.api
    command = EXPECTED_COMMANDS[role] if role == "orchestrator" else ()
    lifecycle_arguments = ("--detach",) if detach else ("--rm",)
    network_arguments = ("--network", network) if network is not None else ()
    return _docker(
        (
            "run",
            *lifecycle_arguments,
            *network_arguments,
            "--read-only",
            *environment,
            image,
            *command,
        ),
        timeout=timeout,
        check=False,
    )


def _assert_role_runtime_failure(
    result: subprocess.CompletedProcess[str],
    role: str,
    *,
    secrets: tuple[str, ...] = (),
) -> None:
    assert result.returncode == 1
    assert result.stderr.endswith(f"taskforge {role} runtime failed\n")
    output = result.stdout + result.stderr
    assert all(secret not in output for secret in (*TEST_SECRET_VALUES, *secrets))


def _wait_for_log_event(container_id: str, event: str) -> None:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        inspection = _inspect(container_id)
        logs = _logs(container_id)
        if event in logs:
            return
        if not inspection["State"]["Running"]:
            pytest.fail(f"container exited before {event}:\n{logs}")
        time.sleep(0.05)
    pytest.fail(f"container did not report {event}")


async def _worker_reached_startup_boundary(dependencies: RealDependencies) -> bool:
    connection = await asyncpg.connect(asyncpg_dsn(dependencies.owner_database_url))
    try:
        session = await connection.fetchrow(
            "SELECT session.id, health.last_sequence, health.accepting_work, "
            "ARRAY(SELECT capability FROM worker_session_capabilities "
            "WHERE worker_session_id=session.id ORDER BY capability) AS capabilities, "
            "EXISTS(SELECT FROM worker_heartbeats heartbeat "
            "WHERE heartbeat.worker_session_id=session.id "
            "AND heartbeat.worker_identity_id=session.worker_identity_id "
            "AND heartbeat.sequence=1 AND heartbeat.accepting_work) "
            "AS initial_heartbeat_accepted "
            "FROM worker_sessions session "
            "JOIN worker_session_health health ON health.worker_session_id=session.id "
            "WHERE session.worker_identity_id=$1 AND session.ended_at IS NULL "
            "ORDER BY session.registered_at DESC, session.id DESC LIMIT 1",
            dependencies.worker_identity_id,
        )
        return bool(
            session is not None
            and session["last_sequence"] >= 1
            and session["accepting_work"]
            and session["initial_heartbeat_accepted"]
            and tuple(session["capabilities"]) == EXPECTED_PIPELINE_CAPABILITIES
        )
    finally:
        await connection.close()


def _wait_for_worker(dependencies: RealDependencies, container_id: str) -> None:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if asyncio.run(_worker_reached_startup_boundary(dependencies)):
            return
        if not _inspect(container_id)["State"]["Running"]:
            pytest.fail(
                f"worker exited during startup:\n{_logs(container_id)}\n"
                f"PostgreSQL logs:\n{_logs(dependencies.postgres_container)}\n"
                f"RabbitMQ logs:\n{_logs(dependencies.rabbitmq_container)}"
            )
        time.sleep(0.05)
    pytest.fail("worker did not reach its registered-session startup boundary")


def test_final_targets_share_root_owned_runtime_and_direct_role_commands(
    images: BuiltImages,
) -> None:
    api = _inspect(images.api)
    worker = _inspect(images.worker)

    assert api["RootFS"]["Layers"] == worker["RootFS"]["Layers"]
    assert api["Config"]["User"] == worker["Config"]["User"] == "10001:10001"
    assert api["Config"]["Entrypoint"] is None
    assert worker["Config"]["Entrypoint"] is None
    assert api["Config"]["Cmd"] == list(EXPECTED_COMMANDS["api"])
    assert worker["Config"]["Cmd"] == list(EXPECTED_COMMANDS["worker"])
    assert api["Config"]["StopSignal"] == "SIGTERM"
    assert worker["Config"]["StopSignal"] == "SIGTERM"

    ownership_script = """
import json
import stat
from pathlib import Path

root = Path("/opt/taskforge/.venv")
bad = []
for path in (root, *root.rglob("*")):
    details = path.lstat()
    writable = not path.is_symlink() and details.st_mode & (
        stat.S_IWGRP | stat.S_IWOTH
    )
    if details.st_uid != 0 or details.st_gid != 0 or writable:
        bad.append(str(path))
print(json.dumps(bad))
"""
    result = _docker(
        (
            "run",
            "--rm",
            "--user",
            "0:0",
            images.api,
            "python",
            "-c",
            ownership_script,
        )
    )
    assert json.loads(result.stdout) == []


def test_runtime_user_is_unprivileged_and_cannot_write_the_install(
    images: BuiltImages,
) -> None:
    script = """
import os
from pathlib import Path

assert os.getuid() == 10001
assert os.getgid() == 10001
target = Path("/opt/taskforge/.venv/lib/python3.12/site-packages/m22-write-test")
try:
    target.write_text("forbidden", encoding="utf-8")
except PermissionError:
    print("read-only-install")
else:
    raise AssertionError("application identity wrote to the production install")
"""
    result = _docker(("run", "--rm", images.api, "python", "-c", script))
    assert result.stdout.strip() == "read-only-install"


def test_locked_packages_and_native_extension_are_installed(
    images: BuiltImages,
) -> None:
    script = """
import importlib.metadata
import json
import platform
from asyncpg.protocol import protocol

packages = sorted(
    distribution.metadata["Name"].lower()
    for distribution in importlib.metadata.distributions()
)
entry_points = sorted(
    (entry_point.group, entry_point.name)
    for entry_point in importlib.metadata.entry_points()
    if entry_point.group.startswith("taskforge.")
)
print(json.dumps({
    "packages": packages,
    "entry_points": entry_points,
    "machine": platform.machine(),
    "native_module": protocol.__file__,
}))
"""
    result = _docker(("run", "--rm", "--read-only", images.api, "python", "-c", script))
    evidence = json.loads(result.stdout)

    packages = set(evidence["packages"])
    assert PRODUCTION_PACKAGES <= packages
    assert packages.isdisjoint(TASKFORGE_BUILD_AND_DEVELOPMENT_PACKAGES)
    assert evidence["machine"] == "x86_64"
    assert evidence["native_module"].endswith("-x86_64-linux-gnu.so")
    assert evidence["entry_points"] == [
        ["taskforge.task_catalog", "taskforge"],
        ["taskforge.worker_profile", "ingestion"],
        ["taskforge.worker_profile", "notification"],
        ["taskforge.worker_profile", "pipeline"],
        ["taskforge.worker_profile", "processing"],
    ]


@pytest.mark.parametrize(
    ("module", "production_factory", "forbidden"),
    (
        (
            "taskforge.api.application",
            "create_production_app",
            (
                "taskforge.orchestrator.application",
                "taskforge.orchestrator.__main__",
                "taskforge.worker.application",
                "taskforge.worker.__main__",
                "taskforge.tasks.handlers",
                "taskforge.tasks.profiles",
            ),
        ),
        (
            "taskforge.orchestrator.application",
            "load_installed_task_catalog",
            (
                "taskforge.api.application",
                "taskforge.api.__main__",
                "taskforge.worker.application",
                "taskforge.worker.__main__",
                "taskforge.tasks.handlers",
                "taskforge.tasks.profiles",
            ),
        ),
    ),
)
def test_api_and_orchestrator_imports_are_isolated(
    images: BuiltImages,
    module: str,
    production_factory: str,
    forbidden: tuple[str, ...],
) -> None:
    script = (
        "import importlib, json, sys; "
        f"module = importlib.import_module({module!r}); "
        f"resolved = getattr(module, {production_factory!r})(); "
        "assert resolved is not None; "
        f"forbidden = {forbidden!r}; "
        "print(json.dumps(sorted(name for name in forbidden if name in sys.modules)))"
    )
    result = _docker(
        (
            "run",
            "--rm",
            "--read-only",
            "--env",
            f"POSTGRES_PASSWORD={POSTGRES_RUNTIME_PASSWORD}",
            "--env",
            f"RABBITMQ_DEFAULT_PASS={RABBITMQ_PASSWORD}",
            images.api,
            "python",
            "-c",
            script,
        )
    )
    assert json.loads(result.stdout) == []


def test_runtime_contains_no_builder_tooling_or_cache(images: BuiltImages) -> None:
    script = """
import json
from pathlib import Path

root = Path("/opt/taskforge")
print(json.dumps({
    "entries": sorted(path.name for path in root.iterdir()),
    "build_root": Path("/build").exists(),
    "uv_cache": Path("/root/.cache/uv").exists(),
    "uv_binary": (root / ".venv/bin/uv").exists(),
}))
"""
    result = _docker(
        ("run", "--rm", "--user", "0:0", images.api, "python", "-c", script)
    )
    assert json.loads(result.stdout) == {
        "entries": [".venv"],
        "build_root": False,
        "uv_cache": False,
        "uv_binary": False,
    }


def test_real_role_startup_pid_one_and_idle_sigterm(
    images: BuiltImages, real_dependencies: RealDependencies
) -> None:
    suffix = uuid4().hex
    specifications = {
        "api": (images.api, (), False),
        "orchestrator": (images.api, EXPECTED_COMMANDS["orchestrator"], False),
        "worker": (images.worker, (), True),
    }
    containers: dict[str, str] = {}
    try:
        for role, (image, command_override, worker) in specifications.items():
            environment = _dependency_environment(
                real_dependencies,
                suffix=suffix,
                worker=worker,
            )
            container_id = _docker(
                (
                    "run",
                    "--detach",
                    "--network",
                    real_dependencies.network,
                    "--read-only",
                    *environment,
                    image,
                    *command_override,
                )
            ).stdout.strip()
            containers[role] = container_id

        _wait_for_api(containers["api"])
        _wait_for_log_event(containers["orchestrator"], "orchestrator.started")
        _wait_for_worker(real_dependencies, containers["worker"])

        for role, container_id in containers.items():
            expected = EXPECTED_COMMANDS[role]
            inspection = _inspect(container_id)
            state = inspection["State"]
            assert state["Running"] is True
            assert inspection["Path"] == "python"
            assert inspection["Args"] == list(expected[1:])
            top = _docker(("top", container_id, "-eo", "pid,ppid,args"))
            process_rows = top.stdout.splitlines()[1:]
            assert len(process_rows) == 1
            pid, _parent_pid, arguments = process_rows[0].split(maxsplit=2)
            assert int(pid) == state["Pid"]
            assert arguments.endswith(" ".join(expected))

        for role in ("worker", "orchestrator", "api"):
            container_id = containers[role]
            _docker(("stop", "--time", "10", container_id), timeout=20)
            state = _inspect(container_id)["State"]
            assert state["Running"] is False
            assert state["ExitCode"] == 0
            assert state["OOMKilled"] is False
    finally:
        for container_id in reversed(tuple(containers.values())):
            _docker(("rm", "--force", container_id), check=False)


@pytest.mark.parametrize("role", ("orchestrator", "worker"))
def test_distinct_role_command_fails_closed_when_dependencies_are_unavailable(
    images: BuiltImages, role: str
) -> None:
    image = images.api if role == "orchestrator" else images.worker
    command_override = EXPECTED_COMMANDS[role] if role == "orchestrator" else ()
    worker_values: tuple[str, ...] = ()
    if role == "worker":
        worker_values = (
            "--env",
            "TASKFORGE_WORKER_CREDENTIAL=tf_worker_v1.00000000-0000-0000-0000-000000000000.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "--env",
            "TASKFORGE_WORKER_PROFILE=pipeline",
        )
    result = _docker(
        (
            "run",
            "--rm",
            "--env",
            f"POSTGRES_PASSWORD={POSTGRES_RUNTIME_PASSWORD}",
            "--env",
            f"RABBITMQ_DEFAULT_PASS={RABBITMQ_PASSWORD}",
            *worker_values,
            "--env",
            "POSTGRES_HOST=127.0.0.1",
            "--env",
            "POSTGRES_PORT=1",
            image,
            *command_override,
        ),
        timeout=30,
        check=False,
    )

    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr.endswith(f"taskforge {role} runtime failed\n")
    assert all(secret not in result.stderr for secret in TEST_SECRET_VALUES)


@pytest.mark.parametrize(
    ("role", "expected_exit"),
    (("api", None), ("orchestrator", 2), ("worker", 2)),
)
def test_role_command_fails_closed_when_required_settings_are_missing(
    images: BuiltImages,
    role: str,
    expected_exit: int | None,
) -> None:
    result = _run_role_container(images, role, ())

    if expected_exit is None:
        assert result.returncode != 0
    else:
        assert result.returncode == expected_exit
        assert result.stderr.endswith(f"taskforge {role} configuration is invalid\n")
    assert result.stdout == ""
    assert all(secret not in result.stderr for secret in TEST_SECRET_VALUES)


def test_worker_unknown_profile_exits_one(
    images: BuiltImages,
    real_dependencies: RealDependencies,
) -> None:
    environment = _dependency_environment(
        real_dependencies,
        suffix=uuid4().hex,
        worker=True,
        overrides={"TASKFORGE_WORKER_PROFILE": "unknown-profile"},
    )
    result = _run_role_container(
        images,
        "worker",
        environment,
        network=real_dependencies.network,
    )

    _assert_role_runtime_failure(
        result,
        "worker",
        secrets=(real_dependencies.worker_credential,),
    )


def test_worker_rejected_real_database_credential_exits_one(
    images: BuiltImages,
    real_dependencies: RealDependencies,
) -> None:
    prefix, credential_id, secret = real_dependencies.worker_credential.split(".")
    replacement = "A" if secret[0] != "A" else "B"
    rejected_credential = ".".join((prefix, credential_id, replacement + secret[1:]))
    environment = _dependency_environment(
        real_dependencies,
        suffix=uuid4().hex,
        worker=True,
        overrides={"TASKFORGE_WORKER_CREDENTIAL": rejected_credential},
    )
    result = _run_role_container(
        images,
        "worker",
        environment,
        network=real_dependencies.network,
    )

    _assert_role_runtime_failure(
        result,
        "worker",
        secrets=(real_dependencies.worker_credential, rejected_credential),
    )


def test_api_database_unavailable_stays_alive_and_reports_not_ready(
    images: BuiltImages,
    real_dependencies: RealDependencies,
) -> None:
    environment = _dependency_environment(
        real_dependencies,
        suffix=uuid4().hex,
        overrides={"POSTGRES_HOST": "127.0.0.1", "POSTGRES_PORT": "1"},
    )
    result = _run_role_container(
        images,
        "api",
        environment,
        network=real_dependencies.network,
        detach=True,
    )
    assert result.returncode == 0
    container_id = result.stdout.strip()
    try:
        _wait_for_api(
            container_id,
            expected_status=503,
            expected_payload={"ready": False, "status": "not_ready"},
        )
        assert _inspect(container_id)["State"]["Running"] is True
        assert all(secret not in _logs(container_id) for secret in TEST_SECRET_VALUES)
    finally:
        _docker(("rm", "--force", container_id), check=False)


@pytest.mark.parametrize("role", ("orchestrator", "worker"))
def test_broker_dependent_role_exits_one_when_rabbitmq_is_unavailable(
    images: BuiltImages,
    real_dependencies: RealDependencies,
    role: str,
) -> None:
    environment = _dependency_environment(
        real_dependencies,
        suffix=uuid4().hex,
        worker=role == "worker",
        overrides={"RABBITMQ_HOST": "127.0.0.1", "RABBITMQ_AMQP_PORT": "1"},
    )
    result = _run_role_container(
        images,
        role,
        environment,
        network=real_dependencies.network,
    )

    _assert_role_runtime_failure(
        result,
        role,
        secrets=(real_dependencies.worker_credential,),
    )


def test_api_starts_normally_when_rabbitmq_is_unavailable(
    images: BuiltImages,
    real_dependencies: RealDependencies,
) -> None:
    environment = _dependency_environment(
        real_dependencies,
        suffix=uuid4().hex,
        overrides={"RABBITMQ_HOST": "127.0.0.1", "RABBITMQ_AMQP_PORT": "1"},
    )
    result = _run_role_container(
        images,
        "api",
        environment,
        network=real_dependencies.network,
        detach=True,
    )
    assert result.returncode == 0
    container_id = result.stdout.strip()
    try:
        _wait_for_api(container_id)
        assert _inspect(container_id)["State"]["Running"] is True
        assert all(secret not in _logs(container_id) for secret in TEST_SECRET_VALUES)
        _docker(("stop", "--time", "10", container_id), timeout=20)
        state = _inspect(container_id)["State"]
        assert state["ExitCode"] == 0
        assert state["OOMKilled"] is False
    finally:
        _docker(("rm", "--force", container_id), check=False)


def test_failure_diagnostics_redact_environment_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arguments = (
        "run",
        "--env",
        "POSTGRES_PASSWORD=must-not-appear",
        "-e",
        "TASKFORGE_WORKER_CREDENTIAL=also-secret",
        "image",
    )
    assert _redacted_arguments(arguments) == (
        "run",
        "--env",
        "POSTGRES_PASSWORD=<redacted>",
        "-e",
        "TASKFORGE_WORKER_CREDENTIAL=<redacted>",
        "image",
    )

    def failed_run(
        *_args: object, **_kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(("docker",), 125, "", "failure")

    monkeypatch.setattr(subprocess, "run", failed_run)
    with pytest.raises(pytest.fail.Exception) as failure:
        _docker(arguments)
    assert "POSTGRES_PASSWORD=<redacted>" in str(failure.value)
    assert "TASKFORGE_WORKER_CREDENTIAL=<redacted>" in str(failure.value)
    assert "must-not-appear" not in str(failure.value)
    assert "also-secret" not in str(failure.value)

    def timed_out_run(
        *_args: object, **_kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(("docker", *arguments), 1)

    monkeypatch.setattr(subprocess, "run", timed_out_run)
    with pytest.raises(pytest.fail.Exception) as timeout_failure:
        _docker(arguments, timeout=1)
    assert "must-not-appear" not in str(timeout_failure.value)
    assert "also-secret" not in str(timeout_failure.value)


def test_source_change_invalidates_the_non_editable_install_layer(
    images: BuiltImages, tmp_path: Path
) -> None:
    del images  # Ensure the warm cache from the canonical images is intentional.
    context = tmp_path / "context"
    context.mkdir()
    for name in ("Dockerfile", ".dockerignore", "pyproject.toml", "uv.lock"):
        shutil.copy2(PROJECT_ROOT / name, context / name)
    shutil.copytree(PROJECT_ROOT / "src", context / "src")

    token = uuid4().hex
    sentinel = context / "src/taskforge/_m22_stale_install_sentinel.py"
    sentinel.write_text(f'TOKEN = "{token}"\n', encoding="utf-8")
    stale_tag = f"taskforge-m22-stale-test:{uuid4().hex}"
    try:
        _docker(
            ("build", "--target", "api", "--tag", stale_tag, "."),
            cwd=context,
            timeout=600,
        )
        result = _docker(
            (
                "run",
                "--rm",
                "--read-only",
                stale_tag,
                "python",
                "-c",
                (
                    "from taskforge._m22_stale_install_sentinel import TOKEN; "
                    "print(TOKEN)"
                ),
            )
        )
        assert result.stdout.strip() == token
    finally:
        _docker(("image", "rm", "--force", stale_tag), check=False)
