"""Structural policy tests for the local Docker Compose environment."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any, cast

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = PROJECT_ROOT / "compose.yaml"
ENV_FILE = PROJECT_ROOT / ".env.example"
EXPECTED_IMAGES = {
    "postgres": "postgres:18.4-bookworm",
    "rabbitmq": "rabbitmq:4.3.3-management",
    "redis": "redis:8.8.1-alpine",
}
EXPECTED_VOLUME_TARGETS = {
    "postgres": "/var/lib/postgresql",
    "rabbitmq": "/var/lib/rabbitmq",
    "redis": "/data",
}
EXPECTED_CONTAINER_PORTS = {
    "postgres": {5432},
    "rabbitmq": {5672, 15672},
    "redis": {6379},
}


@pytest.fixture(scope="module")
def compose_configuration() -> dict[str, Any]:
    result = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            str(ENV_FILE),
            "--file",
            str(COMPOSE_FILE),
            "config",
            "--format",
            "json",
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    return cast(dict[str, Any], json.loads(result.stdout))


def test_compose_defines_only_required_services(
    compose_configuration: dict[str, Any],
) -> None:
    services = compose_configuration["services"]

    assert set(services) == set(EXPECTED_IMAGES)
    assert {
        service_name: service["image"] for service_name, service in services.items()
    } == EXPECTED_IMAGES
    assert all("container_name" not in service for service in services.values())


def test_services_use_private_network_and_named_persistent_volumes(
    compose_configuration: dict[str, Any],
) -> None:
    services = compose_configuration["services"]

    assert set(compose_configuration["networks"]) == {"backend"}
    assert set(compose_configuration["volumes"]) == {
        "postgres-data",
        "rabbitmq-data",
        "redis-data",
    }

    for service_name, expected_target in EXPECTED_VOLUME_TARGETS.items():
        service = services[service_name]
        assert set(service["networks"]) == {"backend"}
        assert len(service["volumes"]) == 1
        assert service["volumes"][0]["type"] == "volume"
        assert service["volumes"][0]["target"] == expected_target


def test_published_ports_are_bound_to_loopback(
    compose_configuration: dict[str, Any],
) -> None:
    services = compose_configuration["services"]

    for service_name, expected_ports in EXPECTED_CONTAINER_PORTS.items():
        published_ports = services[service_name]["ports"]
        assert {port["target"] for port in published_ports} == expected_ports
        assert all(port["host_ip"] == "127.0.0.1" for port in published_ports)


def test_every_dependency_has_a_healthcheck(
    compose_configuration: dict[str, Any],
) -> None:
    services = compose_configuration["services"]

    for service in services.values():
        healthcheck = service["healthcheck"]
        assert healthcheck["test"][0] in {"CMD", "CMD-SHELL"}
        assert healthcheck["interval"] == "5s"
        assert healthcheck["timeout"] == "5s"
        assert healthcheck["retries"] == 12


def test_redis_enables_password_authentication_without_embedding_password(
    compose_configuration: dict[str, Any],
) -> None:
    redis = compose_configuration["services"]["redis"]
    redis_command = " ".join(redis["command"])
    healthcheck_command = " ".join(redis["healthcheck"]["test"])

    assert "--requirepass" in redis_command
    assert "$${REDIS_PASSWORD}" in redis_command
    assert 'REDISCLI_AUTH="$${REDIS_PASSWORD}"' in healthcheck_command
    assert "replace-with-local-redis-password" not in redis_command
