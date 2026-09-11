"""Opt-in real-Compose verification of the M22 Task 2 deployment topology."""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import socket
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

import aio_pika
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
from tests.integration.postgresql import asyncpg_dsn, migration_database_url

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_COMPOSE_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_COMPOSE_INTEGRATION=1 explicitly",
    ),
]

PROJECT_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = PROJECT_ROOT / "compose.yaml"
LOCAL_ADMIN_FILE = PROJECT_ROOT / "compose.local-admin.yaml"
RUNTIME_USER = "taskforge_runtime"
EXPECTED_CAPABILITIES = (
    "pipeline.ingestion",
    "pipeline.notification",
    "pipeline.processing",
)
SECRET_NAMES = {
    "POSTGRES_OWNER_PASSWORD",
    "POSTGRES_PASSWORD",
    "RABBITMQ_DEFAULT_PASS",
    "TASKFORGE_TASK_CLAIM_RESULT_AUTHORITY_SECRET",
    "TASKFORGE_WORKER_CREDENTIAL",
}


def _available_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return cast(int, listener.getsockname()[1])


def _synthetic_secret() -> str:
    return secrets.token_urlsafe(36)


@dataclass
class ComposeProject:
    name: str
    env_file: Path
    values: dict[str, str] = field(repr=False)

    @property
    def api_image(self) -> str:
        return f"taskforge-api:{self.values['TASKFORGE_IMAGE_TAG']}"

    @property
    def worker_image(self) -> str:
        return f"taskforge-worker:{self.values['TASKFORGE_IMAGE_TAG']}"

    def write_environment(self) -> None:
        self.env_file.write_text(
            "".join(f"{name}={value}\n" for name, value in self.values.items()),
            encoding="utf-8",
        )

    def environment(self) -> dict[str, str]:
        environment = {
            name: value
            for name, value in os.environ.items()
            if name not in self.values and name not in SECRET_NAMES
        }
        return environment

    def redact(self, value: str) -> str:
        rendered = value
        for name in SECRET_NAMES:
            secret = self.values.get(name, "")
            if secret:
                rendered = rendered.replace(secret, "<redacted>")
        return rendered

    def compose(
        self,
        arguments: tuple[str, ...],
        *,
        admin: bool = False,
        check: bool = True,
        timeout: float = 300,
    ) -> subprocess.CompletedProcess[str]:
        command_line = [
            "docker",
            "compose",
            "--project-name",
            self.name,
            "--env-file",
            str(self.env_file),
            "--file",
            str(COMPOSE_FILE),
        ]
        if admin:
            command_line.extend(("--file", str(LOCAL_ADMIN_FILE)))
        command_line.extend(arguments)
        try:
            result = subprocess.run(
                command_line,
                cwd=PROJECT_ROOT,
                env=self.environment(),
                capture_output=True,
                check=False,
                text=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            pytest.fail(
                f"Docker Compose command timed out after {timeout} seconds: "
                f"{' '.join(arguments)}"
            )
        if check and result.returncode != 0:
            pytest.fail(
                self.redact(
                    "Docker Compose command failed "
                    f"({result.returncode}): {' '.join(arguments)}\n"
                    f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
                )
            )
        return result

    def service_container(self, service: str) -> str:
        result = self.compose(("ps", "--all", "--quiet", service))
        container = result.stdout.strip()
        assert container, f"Compose service {service} has no container"
        return container


def _docker(
    arguments: tuple[str, ...],
    *,
    check: bool = True,
    timeout: float = 60,
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            ("docker", *arguments),
            cwd=PROJECT_ROOT,
            capture_output=True,
            check=False,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        pytest.fail(f"Docker command timed out: {' '.join(arguments)}")
    if check and result.returncode != 0:
        pytest.fail(
            f"Docker command failed ({result.returncode}): {' '.join(arguments)}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def _inspect(container: str) -> dict[str, Any]:
    result = _docker(("inspect", container))
    records = cast(list[dict[str, Any]], json.loads(result.stdout))
    assert len(records) == 1
    return records[0]


def _wait_for_health(project: ComposeProject, service: str) -> None:
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        container = project.service_container(service)
        state = _inspect(container)["State"]
        if state.get("Health", {}).get("Status") == "healthy":
            return
        if not state["Running"]:
            logs = project.compose(("logs", "--no-color", service), check=False)
            pytest.fail(
                project.redact(
                    f"{service} exited before becoming healthy:\n"
                    f"{logs.stdout}{logs.stderr}"
                )
            )
        time.sleep(0.25)
    logs = project.compose(("logs", "--no-color", service), check=False)
    pytest.fail(
        project.redact(f"{service} did not become healthy:\n{logs.stdout}{logs.stderr}")
    )


def _wait_for_api(port: int) -> dict[str, object]:
    deadline = time.monotonic() + 30
    latest = ""
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/ready", timeout=2
            ) as response:
                payload = cast(dict[str, object], json.load(response))
                if response.status == 200 and payload.get("ready") is True:
                    return payload
                latest = f"status={response.status} payload={payload}"
        except (OSError, urllib.error.URLError) as error:
            latest = type(error).__name__
        time.sleep(0.1)
    pytest.fail(f"published API did not become ready: {latest}")


def _wait_for_log(project: ComposeProject, service: str, event: str) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        result = project.compose(("logs", "--no-color", service), check=False)
        if event in result.stdout + result.stderr:
            return
        state = _inspect(project.service_container(service))["State"]
        if not state["Running"]:
            pytest.fail(
                project.redact(f"{service} exited:\n{result.stdout}{result.stderr}")
            )
        time.sleep(0.1)
    pytest.fail(f"{service} did not emit {event}")


async def _provision_worker(database_url: URL) -> tuple[UUID, str]:
    engine = create_async_engine(database_url.set(drivername="postgresql+asyncpg"))
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        repository = SQLAlchemyProvisioningRepository(sessions)
        async with repository.transaction() as transaction:
            worker_id = await IdentityProvisioningService().create_worker_identity(
                transaction, name=f"compose-worker-{uuid4().hex}"
            )
            credential = await CredentialIssuanceService().issue_worker_credential(
                transaction,
                worker_id=worker_id,
                expires_at=datetime.now(UTC) + timedelta(hours=1),
            )
            await transaction.commit()
        return worker_id, credential.take_presented_value()
    finally:
        await engine.dispose()


def _postgres_scalar(project: ComposeProject, statement: str) -> str:
    result = _docker(
        (
            "exec",
            project.service_container("postgres"),
            "psql",
            "--no-psqlrc",
            "--tuples-only",
            "--no-align",
            "--username",
            project.values["POSTGRES_OWNER_USER"],
            "--dbname",
            project.values["POSTGRES_DB"],
            "--command",
            statement,
        )
    )
    return result.stdout.strip()


def _worker_started(project: ComposeProject, worker_id: UUID) -> bool:
    rendered = _postgres_scalar(
        project,
        "SELECT json_build_object("
        "'last_sequence', health.last_sequence, "
        "'accepting_work', health.accepting_work, "
        "'capabilities', ARRAY(SELECT capability "
        "FROM worker_session_capabilities "
        "WHERE worker_session_id=session.id ORDER BY capability), "
        "'initial_heartbeat_accepted', EXISTS("
        "SELECT FROM worker_heartbeats heartbeat "
        "WHERE heartbeat.worker_session_id=session.id "
        "AND heartbeat.worker_identity_id=session.worker_identity_id "
        "AND heartbeat.sequence=1 AND heartbeat.accepting_work))::text "
        "FROM worker_sessions session "
        "JOIN worker_session_health health ON health.worker_session_id=session.id "
        f"WHERE session.worker_identity_id='{worker_id}'::uuid "
        "AND session.ended_at IS NULL "
        "ORDER BY session.registered_at DESC, session.id DESC LIMIT 1",
    )
    if not rendered:
        return False
    row = cast(dict[str, Any], json.loads(rendered))
    return bool(
        row["last_sequence"] >= 1
        and row["accepting_work"]
        and row["initial_heartbeat_accepted"]
        and tuple(row["capabilities"]) == EXPECTED_CAPABILITIES
    )


def _wait_for_worker(project: ComposeProject, worker_id: UUID) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if _worker_started(project, worker_id):
            return
        state = _inspect(project.service_container("worker"))["State"]
        if not state["Running"]:
            logs = project.compose(("logs", "--no-color", "worker"), check=False)
            pytest.fail(project.redact(f"worker exited:\n{logs.stdout}{logs.stderr}"))
        time.sleep(0.1)
    pytest.fail("worker did not reach its registered heartbeat/capability boundary")


async def _assert_runtime_boundary(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        assert await connection.fetchval("SELECT current_user") == RUNTIME_USER
        for table in ("worker_identities", "worker_credentials"):
            assert not await connection.fetchval(
                "SELECT has_table_privilege(current_user, $1, 'UPDATE')", table
            )
        assert await connection.fetchval(
            "SELECT has_function_privilege(current_user, "
            "'public.lock_valid_worker_authority(uuid, uuid)', 'EXECUTE')"
        )
    finally:
        await connection.close()


async def _assert_worker_identity_persisted(database_url: URL, worker_id: UUID) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        assert await connection.fetchval(
            "SELECT EXISTS(SELECT FROM worker_identities WHERE id=$1)", worker_id
        )
    finally:
        await connection.close()


def _assert_private_dependency_topology(project: ComposeProject) -> None:
    expected_networks = {
        "postgres": {f"{project.name}_database"},
        "rabbitmq": {f"{project.name}_broker"},
    }
    for service, networks in expected_networks.items():
        inspection = _inspect(project.service_container(service))
        assert set(inspection["NetworkSettings"]["Networks"]) == networks
        assert inspection["HostConfig"]["PortBindings"] == {}


def _assert_private_persistence_markers(
    project: ComposeProject, worker_id: UUID, queue_name: str
) -> None:
    identity_exists = _postgres_scalar(
        project,
        f"SELECT EXISTS(SELECT FROM worker_identities WHERE id='{worker_id}'::uuid)",
    )
    assert identity_exists == "t"
    queues = _docker(
        (
            "exec",
            project.service_container("rabbitmq"),
            "rabbitmqctl",
            "--quiet",
            "list_queues",
            "--vhost",
            project.values["RABBITMQ_DEFAULT_VHOST"],
            "name",
        )
    )
    assert queue_name in queues.stdout.splitlines()


async def _declare_persistent_queue(
    project: ComposeProject, queue_name: str, *, passive: bool = False
) -> None:
    connection = await aio_pika.connect(
        host="127.0.0.1",
        port=int(project.values["RABBITMQ_AMQP_PORT"]),
        login=project.values["RABBITMQ_DEFAULT_USER"],
        password=project.values["RABBITMQ_DEFAULT_PASS"],
        virtualhost=project.values["RABBITMQ_DEFAULT_VHOST"],
        timeout=5,
    )
    try:
        channel = await connection.channel()
        await channel.declare_queue(
            queue_name,
            durable=True,
            auto_delete=False,
            passive=passive,
            timeout=5,
        )
    finally:
        await connection.close()


def _assert_loopback_bindings(project: ComposeProject) -> None:
    expected = {
        "postgres": {"5432/tcp": project.values["POSTGRES_PORT"]},
        "rabbitmq": {
            "5672/tcp": project.values["RABBITMQ_AMQP_PORT"],
            "15672/tcp": project.values["RABBITMQ_MANAGEMENT_PORT"],
        },
    }
    for service, ports in expected.items():
        bindings = _inspect(project.service_container(service))["HostConfig"][
            "PortBindings"
        ]
        assert set(bindings) == set(ports)
        for container_port, host_port in ports.items():
            assert bindings[container_port] == [
                {"HostIp": "127.0.0.1", "HostPort": host_port}
            ]


def _service_environment(container: str) -> dict[str, str]:
    entries = _inspect(container)["Config"]["Env"]
    return dict(entry.split("=", maxsplit=1) for entry in entries if "=" in entry)


def _assert_last_diagnostic(
    result: subprocess.CompletedProcess[str], expected: str
) -> None:
    lines = [line for line in result.stderr.splitlines() if line]
    assert lines[-1] == expected


def _assert_single_pid_one(container: str, expected: tuple[str, ...]) -> None:
    inspection = _inspect(container)
    state = inspection["State"]
    assert inspection["Path"] == expected[0]
    assert inspection["Args"] == list(expected[1:])
    top = _docker(("top", container, "-eo", "pid,ppid,args"))
    rows = top.stdout.splitlines()[1:]
    assert len(rows) == 1
    pid, _parent_pid, arguments = rows[0].split(maxsplit=2)
    assert int(pid) == state["Pid"]
    assert arguments.endswith(" ".join(expected))


def _probe_from_service(project: ComposeProject, service: str, script: str) -> None:
    result = project.compose(
        ("exec", "-T", service, "python", "-c", script), check=False
    )
    assert result.returncode == 0, project.redact(result.stdout + result.stderr)


def _wait_for_service_probe(project: ComposeProject, service: str, script: str) -> None:
    deadline = time.monotonic() + 10
    latest = ""
    while time.monotonic() < deadline:
        result = project.compose(
            ("exec", "-T", service, "python", "-c", script), check=False
        )
        if result.returncode == 0:
            return
        latest = project.redact(result.stdout + result.stderr)
        time.sleep(0.1)
    pytest.fail(f"{service} could not reach the egress probe: {latest}")


def _assert_no_project_resources(project: ComposeProject) -> None:
    filters = {
        "container": ("ps", "--all", "--quiet"),
        "network": ("network", "ls", "--quiet"),
        "volume": ("volume", "ls", "--quiet"),
    }
    for resource, arguments in filters.items():
        result = _docker(
            (
                *arguments,
                "--filter",
                f"label=com.docker.compose.project={project.name}",
            ),
            check=False,
        )
        assert result.returncode == 0, f"could not inspect leaked {resource} resources"
        assert result.stdout.strip() == "", (
            f"leaked {resource}: {result.stdout.strip()}"
        )
    for image in (project.api_image, project.worker_image):
        assert _docker(("image", "inspect", image), check=False).returncode != 0


def test_real_compose_operator_lifecycle(tmp_path: Path) -> None:
    availability = _docker(("info", "--format", "{{.ServerVersion}}"), check=False)
    if availability.returncode != 0:
        pytest.skip(
            "Docker daemon unavailable; real-Compose M22 checks NOT RUN: "
            f"{availability.stderr.strip()}"
        )
    compose_version = _docker(("compose", "version"), check=False)
    if compose_version.returncode != 0:
        pytest.skip(
            "Docker Compose unavailable; real-Compose M22 checks NOT RUN: "
            f"{compose_version.stderr.strip()}"
        )

    suffix = uuid4().hex
    values = {
        "POSTGRES_DB": "taskforge",
        "POSTGRES_OWNER_USER": "taskforge_owner",
        "POSTGRES_OWNER_PASSWORD": _synthetic_secret(),
        "POSTGRES_USER": RUNTIME_USER,
        "POSTGRES_PASSWORD": _synthetic_secret(),
        "POSTGRES_PORT": str(_available_port()),
        "RABBITMQ_DEFAULT_USER": "taskforge",
        "RABBITMQ_DEFAULT_PASS": _synthetic_secret(),
        "RABBITMQ_DEFAULT_VHOST": "taskforge",
        "RABBITMQ_AMQP_PORT": str(_available_port()),
        "RABBITMQ_MANAGEMENT_PORT": str(_available_port()),
        "TASKFORGE_TASK_CLAIM_RESULT_AUTHORITY_SECRET": _synthetic_secret(),
        "TASKFORGE_API_PUBLISHED_PORT": str(_available_port()),
        "TASKFORGE_IMAGE_TAG": f"m22-task2-{suffix}",
        "TASKFORGE_WORKER_PROFILE": "pipeline",
    }
    project = ComposeProject(
        f"taskforge-m22-task2-{suffix}", tmp_path / "task.env", values
    )
    project.write_environment()
    egress_probe = f"{project.name}-egress-probe"
    cleanup_errors: list[str] = []
    try:
        # The post-provisioned worker credential is absent during dependency startup.
        project.compose(
            ("up", "--detach", "postgres", "rabbitmq"), admin=True, timeout=600
        )
        _wait_for_health(project, "postgres")
        _wait_for_health(project, "rabbitmq")
        assert project.compose(("ps", "--all", "--quiet", "api")).stdout.strip() == ""
        assert (
            project.compose(("ps", "--all", "--quiet", "orchestrator")).stdout.strip()
            == ""
        )
        assert (
            project.compose(("ps", "--all", "--quiet", "worker")).stdout.strip() == ""
        )
        _assert_loopback_bindings(project)

        owner_url = URL.create(
            "postgresql+asyncpg",
            username=values["POSTGRES_OWNER_USER"],
            password=values["POSTGRES_OWNER_PASSWORD"],
            host="127.0.0.1",
            port=int(values["POSTGRES_PORT"]),
            database=values["POSTGRES_DB"],
        )
        runtime_url = owner_url.set(
            username=RUNTIME_USER, password=values["POSTGRES_PASSWORD"]
        )
        rendered_owner_url = owner_url.render_as_string(hide_password=False)
        try:
            with migration_database_url(rendered_owner_url):
                command.upgrade(Config(str(PROJECT_ROOT / "alembic.ini")), "head")
            worker_id, worker_credential = asyncio.run(_provision_worker(owner_url))
            asyncio.run(_assert_runtime_boundary(runtime_url))
        except Exception as error:
            pytest.fail(
                project.redact(
                    "external owner migration/provisioning failed: "
                    f"{type(error).__name__}: {error}"
                )
            )

        # Named volumes survive dependency-container recreation.
        queue_name = f"taskforge.persistence.{suffix}"
        asyncio.run(_declare_persistent_queue(project, queue_name))
        project.compose(("stop", "postgres", "rabbitmq"), admin=True)
        project.compose(("rm", "--force", "postgres", "rabbitmq"), admin=True)
        project.compose(
            ("up", "--detach", "postgres", "rabbitmq"), admin=True, timeout=600
        )
        _wait_for_health(project, "postgres")
        _wait_for_health(project, "rabbitmq")
        asyncio.run(_declare_persistent_queue(project, queue_name, passive=True))
        asyncio.run(_assert_worker_identity_persisted(owner_url, worker_id))

        # A normal base-only convergence removes the opt-in admin attachments while
        # preserving the dependency volumes; no force-recreate is required.
        admin_dependency_containers = {
            service: project.service_container(service)
            for service in ("postgres", "rabbitmq")
        }
        project.compose(("up", "--detach", "postgres", "rabbitmq"), timeout=600)
        _wait_for_health(project, "postgres")
        _wait_for_health(project, "rabbitmq")
        private_dependency_containers = {
            service: project.service_container(service)
            for service in ("postgres", "rabbitmq")
        }
        assert all(
            private_dependency_containers[service]
            != admin_dependency_containers[service]
            for service in admin_dependency_containers
        )
        _assert_private_dependency_topology(project)
        _assert_private_persistence_markers(project, worker_id, queue_name)

        # Both an absent and an explicitly blank credential are configuration errors
        # when the worker is invoked from the private main topology.
        project.compose(("build", "api", "worker"), timeout=600)
        missing = project.compose(
            ("run", "--rm", "--no-deps", "worker"), check=False, timeout=30
        )
        assert missing.returncode == 2
        _assert_last_diagnostic(missing, "taskforge worker configuration is invalid")
        values["TASKFORGE_WORKER_CREDENTIAL"] = ""
        project.write_environment()
        blank = project.compose(
            ("run", "--rm", "--no-deps", "worker"), check=False, timeout=30
        )
        assert blank.returncode == 2
        _assert_last_diagnostic(blank, "taskforge worker configuration is invalid")

        # API starts without RabbitMQ configuration or connectivity.
        project.compose(("stop", "rabbitmq"))
        project.compose(("up", "--detach", "api"), timeout=600)
        assert _wait_for_api(int(values["TASKFORGE_API_PUBLISHED_PORT"]))["ready"]
        api_container = project.service_container("api")
        api_environment = _service_environment(api_container)
        assert api_environment["WEB_CONCURRENCY"] == "1"
        assert not any(name.startswith("RABBITMQ_") for name in api_environment)
        _assert_single_pid_one(api_container, ("python", "-m", "taskforge.api"))

        project.compose(("up", "--detach", "rabbitmq"))
        _wait_for_health(project, "rabbitmq")

        # The real issued credential reaches only the worker via --env-file interpolation.
        values["TASKFORGE_WORKER_CREDENTIAL"] = worker_credential
        project.write_environment()
        project.compose(
            ("up", "--detach", "api", "orchestrator", "worker"),
            timeout=600,
        )
        _wait_for_log(project, "orchestrator", "orchestrator.started")
        _wait_for_worker(project, worker_id)

        application_containers = {
            name: project.service_container(name)
            for name in ("api", "orchestrator", "worker")
        }
        for name, container in application_containers.items():
            environment = _service_environment(container)
            assert environment["POSTGRES_USER"] == RUNTIME_USER
            assert "POSTGRES_OWNER_PASSWORD" not in environment
            assert "POSTGRES_OWNER_USER" not in environment
            assert ("TASKFORGE_WORKER_CREDENTIAL" in environment) is (name == "worker")
            if name != "api":
                assert "WEB_CONCURRENCY" not in environment

        _assert_single_pid_one(
            application_containers["orchestrator"],
            ("python", "-m", "taskforge.orchestrator"),
        )
        _assert_single_pid_one(
            application_containers["worker"], ("python", "-m", "taskforge.worker")
        )

        # Network attachment provides intended dependency access and application egress.
        expected_networks = {
            "api": {"frontend", "database"},
            "orchestrator": {"database", "broker", "application-egress"},
            "worker": {"database", "broker", "application-egress"},
        }
        for name, suffixes in expected_networks.items():
            attached = set(
                _inspect(application_containers[name])["NetworkSettings"]["Networks"]
            )
            assert attached == {
                f"{project.name}_{suffix_name}" for suffix_name in suffixes
            }

        _probe_from_service(
            project,
            "api",
            "import socket; socket.create_connection(('postgres', 5432), 2).close()",
        )
        _probe_from_service(
            project,
            "api",
            "import socket, sys;\n"
            "try: socket.getaddrinfo('rabbitmq', 5672)\n"
            "except socket.gaierror: sys.exit(0)\n"
            "sys.exit(1)",
        )
        _docker(
            (
                "run",
                "--detach",
                "--name",
                egress_probe,
                "--network",
                f"{project.name}_frontend",
                "--network-alias",
                "taskforge-egress-probe",
                "--read-only",
                "--user",
                "10001:10001",
                project.api_image,
                "python",
                "-m",
                "http.server",
                "8765",
            )
        )
        _docker(
            (
                "network",
                "connect",
                "--alias",
                "taskforge-egress-probe",
                f"{project.name}_application-egress",
                egress_probe,
            )
        )
        egress_script = (
            "import urllib.request; "
            "urllib.request.urlopen('http://taskforge-egress-probe:8765', "
            "timeout=2).close()"
        )
        for service in ("api", "orchestrator", "worker"):
            _wait_for_service_probe(project, service, egress_script)

        # Nonempty malformed and rejected credentials remain runtime exit 1.
        for invalid_credential in (
            "not-a-worker-credential",
            worker_credential[:-1] + ("A" if worker_credential[-1] != "A" else "B"),
        ):
            values["TASKFORGE_WORKER_CREDENTIAL"] = invalid_credential
            project.write_environment()
            rejected = project.compose(
                ("run", "--rm", "--no-deps", "worker"),
                check=False,
                timeout=30,
            )
            assert rejected.returncode == 1
            _assert_last_diagnostic(rejected, "taskforge worker runtime failed")
            assert invalid_credential not in rejected.stdout + rejected.stderr

        values["TASKFORGE_WORKER_CREDENTIAL"] = worker_credential
        project.write_environment()
        _docker(("rm", "--force", egress_probe), check=False)

        # Task 2 retains only ordinary idle/safe SIGTERM behavior.
        project.compose(
            ("stop", "--timeout", "10", "worker", "orchestrator", "api"),
        )
        for container in application_containers.values():
            state = _inspect(container)["State"]
            assert state["Running"] is False
            assert state["ExitCode"] == 0
            assert state["OOMKilled"] is False
    finally:
        _docker(("rm", "--force", egress_probe), check=False)
        down = project.compose(
            (
                "down",
                "--volumes",
                "--remove-orphans",
                "--timeout",
                "10",
            ),
            admin=True,
            check=False,
            timeout=120,
        )
        if down.returncode != 0:
            cleanup_errors.append(project.redact(down.stdout + down.stderr))
        for image in (project.api_image, project.worker_image):
            removal = _docker(("image", "rm", "--force", image), check=False)
            if removal.returncode != 0 and "No such image" not in removal.stderr:
                cleanup_errors.append(removal.stderr)
        try:
            _assert_no_project_resources(project)
        except AssertionError as error:
            cleanup_errors.append(str(error))
        if cleanup_errors:
            pytest.fail("Compose cleanup failed:\n" + "\n".join(cleanup_errors))
