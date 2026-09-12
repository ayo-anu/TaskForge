"""Structural policy tests for the Taskforge Compose deployment."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any, cast

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = PROJECT_ROOT / "compose.yaml"
LOCAL_ADMIN_FILE = PROJECT_ROOT / "compose.local-admin.yaml"
ENV_EXAMPLE = PROJECT_ROOT / ".env.example"
SERVICES = {"postgres", "rabbitmq", "api", "orchestrator", "worker"}
OPERATIONS_SERVICES = SERVICES | {"migrate"}
APPLICATION_SERVICES = {"api", "orchestrator", "worker"}
REQUIRED_SECRETS = {
    "POSTGRES_OWNER_PASSWORD": (
        "${POSTGRES_OWNER_PASSWORD:?POSTGRES_OWNER_PASSWORD is required}"
    ),
    "POSTGRES_PASSWORD": "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}",
    "RABBITMQ_DEFAULT_PASS": (
        "${RABBITMQ_DEFAULT_PASS:?RABBITMQ_DEFAULT_PASS is required}"
    ),
    "TASKFORGE_TASK_CLAIM_RESULT_AUTHORITY_SECRET": (
        "${TASKFORGE_TASK_CLAIM_RESULT_AUTHORITY_SECRET:?"
        "TASKFORGE_TASK_CLAIM_RESULT_AUTHORITY_SECRET is required}"
    ),
}
SYNTHETIC_ENVIRONMENT = {
    "POSTGRES_OWNER_PASSWORD": "synthetic-owner-secret",
    "POSTGRES_PASSWORD": "synthetic-runtime-secret",
    "RABBITMQ_DEFAULT_PASS": "synthetic-rabbitmq-secret",
    "TASKFORGE_TASK_CLAIM_RESULT_AUTHORITY_SECRET": (
        "synthetic-claim-result-authority-secret"
    ),
}
EXPECTED_IMAGES = {
    "postgres": (
        "postgres:18.4-bookworm@sha256:"
        "1961f96e6029a02c3812d7cb329a3b03a3ac2bb067058dec17b0f5596aca9296"
    ),
    "rabbitmq": (
        "rabbitmq:4.3.3-management@sha256:"
        "d8b3d416236ae5ba3f6a9f4e4651fdacda01780181a7d83a71b640248d8f0a69"
    ),
}


def _write_environment(path: Path, values: dict[str, str]) -> None:
    path.write_text(
        "".join(f"{name}={value}\n" for name, value in values.items()),
        encoding="utf-8",
    )


def _compose_config(
    env_file: Path,
    *,
    files: tuple[Path, ...] = (COMPOSE_FILE,),
    environment: dict[str, str] | None = None,
    profiles: tuple[str, ...] = (),
) -> subprocess.CompletedProcess[str]:
    clean_environment = {
        name: value
        for name, value in os.environ.items()
        if name not in {*REQUIRED_SECRETS, "TASKFORGE_WORKER_CREDENTIAL"}
    }
    if environment is not None:
        clean_environment.update(environment)
    arguments = ["docker", "compose", "--env-file", str(env_file)]
    for profile in profiles:
        arguments.extend(("--profile", profile))
    for compose_file in files:
        arguments.extend(("--file", str(compose_file)))
    arguments.extend(("config", "--format", "json"))
    return subprocess.run(
        arguments,
        cwd=PROJECT_ROOT,
        env=clean_environment,
        capture_output=True,
        check=False,
        text=True,
    )


@pytest.fixture(scope="module")
def synthetic_env_file(tmp_path_factory: pytest.TempPathFactory) -> Path:
    path = tmp_path_factory.mktemp("compose-configuration") / "synthetic.env"
    _write_environment(path, SYNTHETIC_ENVIRONMENT)
    return path


@pytest.fixture(scope="module")
def compose_configuration(synthetic_env_file: Path) -> dict[str, Any]:
    result = _compose_config(synthetic_env_file)
    assert result.returncode == 0, result.stderr
    return cast(dict[str, Any], json.loads(result.stdout))


def test_compose_defines_exact_runtime_topology(
    compose_configuration: dict[str, Any],
) -> None:
    services = compose_configuration["services"]

    assert set(services) == SERVICES
    assert services["postgres"]["image"] == EXPECTED_IMAGES["postgres"]
    assert services["rabbitmq"]["image"] == EXPECTED_IMAGES["rabbitmq"]
    assert "redis" not in services
    assert all("container_name" not in service for service in services.values())
    assert set(compose_configuration["volumes"]) == {
        "postgres-data",
        "rabbitmq-data",
    }


def test_application_build_targets_commands_and_runtime_security(
    compose_configuration: dict[str, Any],
) -> None:
    services = compose_configuration["services"]
    expected = {
        "api": ("api", ["python", "-m", "taskforge.api"]),
        "orchestrator": ("api", ["python", "-m", "taskforge.orchestrator"]),
        "worker": ("worker", ["python", "-m", "taskforge.worker"]),
    }

    for name, (target, command) in expected.items():
        service = services[name]
        assert service["build"]["target"] == target
        assert service["command"] == command
        assert service["user"] == "10001:10001"
        assert service["read_only"] is True
        assert service["cap_drop"] == ["ALL"]
        assert service["security_opt"] == ["no-new-privileges:true"]
        assert service["stop_signal"] == "SIGTERM"
        assert service["restart"] == "on-failure:5"
        assert service.get("entrypoint") is None
        assert "volumes" not in service
        assert "tmpfs" not in service


def test_networks_isolate_dependencies_while_applications_keep_egress(
    compose_configuration: dict[str, Any],
) -> None:
    networks = compose_configuration["networks"]
    services = compose_configuration["services"]

    assert set(networks) == {
        "frontend",
        "database",
        "broker",
        "application-egress",
    }
    assert networks["database"]["internal"] is True
    assert networks["broker"]["internal"] is True
    assert not networks["frontend"].get("internal", False)
    assert not networks["application-egress"].get("internal", False)
    assert set(services["postgres"]["networks"]) == {"database"}
    assert set(services["rabbitmq"]["networks"]) == {"broker"}
    assert set(services["api"]["networks"]) == {"frontend", "database"}
    assert set(services["orchestrator"]["networks"]) == {
        "database",
        "broker",
        "application-egress",
    }
    assert set(services["worker"]["networks"]) == {
        "database",
        "broker",
        "application-egress",
    }


def test_main_topology_publishes_only_api_on_host_loopback(
    compose_configuration: dict[str, Any],
) -> None:
    services = compose_configuration["services"]

    assert "ports" not in services["postgres"]
    assert "ports" not in services["rabbitmq"]
    assert services["api"]["ports"] == [
        {
            "mode": "ingress",
            "target": 8000,
            "published": "8000",
            "protocol": "tcp",
            "host_ip": "127.0.0.1",
        }
    ]
    assert all("ports" not in services[name] for name in {"orchestrator", "worker"})


def test_local_admin_override_is_explicit_and_loopback_only(
    synthetic_env_file: Path,
) -> None:
    assert LOCAL_ADMIN_FILE.name not in {
        "compose.override.yaml",
        "docker-compose.override.yml",
    }
    result = _compose_config(synthetic_env_file, files=(COMPOSE_FILE, LOCAL_ADMIN_FILE))
    assert result.returncode == 0, result.stderr
    services = json.loads(result.stdout)["services"]

    expected = {
        "postgres": {5432},
        "rabbitmq": {5672, 15672},
    }
    for service_name, targets in expected.items():
        ports = services[service_name]["ports"]
        assert {port["target"] for port in ports} == targets
        assert all(port["host_ip"] == "127.0.0.1" for port in ports)
    assert set(services["postgres"]["networks"]) == {
        "database",
        "postgres-local-admin",
    }
    assert set(services["rabbitmq"]["networks"]) == {
        "broker",
        "rabbitmq-local-admin",
    }


def test_environment_is_role_scoped_and_api_is_single_process(
    compose_configuration: dict[str, Any],
) -> None:
    services = compose_configuration["services"]
    api_environment = services["api"]["environment"]

    assert api_environment["TASKFORGE_API_HOST"] == "0.0.0.0"
    assert api_environment["TASKFORGE_API_PORT"] == "8000"
    assert api_environment["WEB_CONCURRENCY"] == "1"
    assert not any(name.startswith("RABBITMQ_") for name in api_environment)
    for name in {"orchestrator", "worker"}:
        assert "WEB_CONCURRENCY" not in services[name]["environment"]
        assert (
            services[name]["environment"]["RABBITMQ_DEFAULT_PASS"]
            == (SYNTHETIC_ENVIRONMENT["RABBITMQ_DEFAULT_PASS"])
        )
    for name in APPLICATION_SERVICES:
        environment = services[name]["environment"]
        assert environment["POSTGRES_USER"] == "taskforge_runtime"
        assert "POSTGRES_OWNER_USER" not in environment
        assert "POSTGRES_OWNER_PASSWORD" not in environment
        assert "TASKFORGE_DATABASE_URL" not in environment
    assert services["worker"]["environment"]["TASKFORGE_WORKER_CREDENTIAL"] == ""
    assert all(
        "TASKFORGE_WORKER_CREDENTIAL" not in services[name]["environment"]
        for name in {"api", "orchestrator"}
    )


def test_required_secrets_fail_during_interpolation_but_worker_is_post_provisioned(
    tmp_path: Path,
) -> None:
    compose = COMPOSE_FILE.read_text(encoding="utf-8")
    for expression in REQUIRED_SECRETS.values():
        assert expression in compose
    assert "${TASKFORGE_WORKER_CREDENTIAL-}" in compose
    assert "${TASKFORGE_WORKER_CREDENTIAL:?" not in compose
    assert "${TASKFORGE_WORKER_CREDENTIAL:-" not in compose

    for missing in REQUIRED_SECRETS:
        for state, values in (
            (
                "absent",
                {
                    name: value
                    for name, value in SYNTHETIC_ENVIRONMENT.items()
                    if name != missing
                },
            ),
            ("empty", SYNTHETIC_ENVIRONMENT | {missing: ""}),
        ):
            path = tmp_path / f"{state}-{missing.lower()}.env"
            _write_environment(path, values)
            result = _compose_config(path)
            assert result.returncode != 0
            assert missing in result.stderr
            assert all(
                secret not in result.stderr
                for name, secret in SYNTHETIC_ENVIRONMENT.items()
                if name != missing
            )


def test_example_environment_cannot_render_deployable_compose() -> None:
    result = _compose_config(ENV_EXAMPLE)

    assert result.returncode != 0
    assert any(name in result.stderr for name in REQUIRED_SECRETS)


def test_example_environment_contains_only_blank_secret_fields() -> None:
    values: dict[str, str] = {}
    for raw_line in ENV_EXAMPLE.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line and not line.startswith("#") and "=" in line:
            name, value = line.split("=", maxsplit=1)
            values[name] = value

    for name in (*REQUIRED_SECRETS, "TASKFORGE_WORKER_CREDENTIAL"):
        assert name in values
        assert values[name] == ""
    assert "REDIS_PASSWORD" not in values


def test_healthchecks_and_initial_dependency_conditions_are_role_accurate(
    compose_configuration: dict[str, Any],
) -> None:
    services = compose_configuration["services"]
    postgres_health = " ".join(services["postgres"]["healthcheck"]["test"])
    rabbitmq_health = " ".join(services["rabbitmq"]["healthcheck"]["test"])
    api_health = " ".join(services["api"]["healthcheck"]["test"])

    assert "pg_isready" in postgres_health
    assert "check_running" in rabbitmq_health
    assert "check_port_connectivity" in rabbitmq_health
    assert "check_local_alarms" in rabbitmq_health
    assert "http://127.0.0.1:8000/ready" in api_health
    assert "healthcheck" not in services["orchestrator"]
    assert "healthcheck" not in services["worker"]
    assert services["api"]["depends_on"] == {
        "postgres": {
            "condition": "service_healthy",
            "required": True,
        }
    }
    for name in {"orchestrator", "worker"}:
        assert set(services[name]["depends_on"]) == {"postgres", "rabbitmq"}
        assert all(
            dependency["condition"] == "service_healthy"
            for dependency in services[name]["depends_on"].values()
        )


def test_persistent_volumes_and_bootstrap_are_dependency_owned(
    compose_configuration: dict[str, Any],
) -> None:
    services = compose_configuration["services"]
    postgres_volumes = services["postgres"]["volumes"]
    rabbitmq_volumes = services["rabbitmq"]["volumes"]

    assert any(
        volume["type"] == "volume" and volume["target"] == "/var/lib/postgresql"
        for volume in postgres_volumes
    )
    assert any(
        volume["type"] == "bind"
        and volume["target"] == "/docker-entrypoint-initdb.d/10-taskforge-roles.sh"
        and volume["read_only"] is True
        for volume in postgres_volumes
    )
    assert any(
        volume["type"] == "volume" and volume["target"] == "/var/lib/rabbitmq"
        for volume in rabbitmq_volumes
    )
    assert services["rabbitmq"]["hostname"] == "rabbitmq"


def test_compose_introduces_no_migration_or_drain_behavior() -> None:
    rendered = "\n".join(
        (
            COMPOSE_FILE.read_text(encoding="utf-8"),
            LOCAL_ADMIN_FILE.read_text(encoding="utf-8"),
        )
    ).lower()

    assert "alembic" not in rendered
    assert "\n    entrypoint:" not in rendered
    assert "drain" not in rendered


def test_migration_service_is_explicit_one_shot_and_owner_scoped(
    synthetic_env_file: Path,
) -> None:
    result = _compose_config(synthetic_env_file, profiles=("operations",))
    assert result.returncode == 0, result.stderr
    services = json.loads(result.stdout)["services"]
    assert set(services) == OPERATIONS_SERVICES
    migrate = services["migrate"]

    assert migrate["profiles"] == ["operations"]
    assert migrate["build"]["target"] == "migration"
    assert migrate["command"] == ["python", "-m", "taskforge.database_migrations"]
    assert migrate["user"] == "10001:10001"
    assert migrate["read_only"] is True
    assert migrate["restart"] == "no"
    assert migrate["networks"] == {"database": None}
    assert migrate["depends_on"] == {
        "postgres": {"condition": "service_healthy", "required": True}
    }
    assert "healthcheck" not in migrate
    assert "ports" not in migrate
    assert "volumes" not in migrate
    assert "tmpfs" not in migrate
    assert migrate.get("entrypoint") is None
    assert set(migrate["environment"]) == {
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_OWNER_USER",
        "POSTGRES_OWNER_PASSWORD",
        "TASKFORGE_MIGRATION_LOCK_TIMEOUT_SECONDS",
    }
    for application in APPLICATION_SERVICES:
        assert "migrate" not in services[application].get("depends_on", {})
