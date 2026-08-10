"""Tests for Taskforge environment-backed settings."""

from __future__ import annotations

import os

import pytest
from pydantic import ValidationError

from taskforge.settings import Settings

ENVIRONMENT_PREFIX = "TASKFORGE_"
DEPENDENCY_ENVIRONMENT_VARIABLES = {
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "RABBITMQ_HOST",
    "RABBITMQ_AMQP_PORT",
    "RABBITMQ_DEFAULT_USER",
    "RABBITMQ_DEFAULT_PASS",
    "RABBITMQ_DEFAULT_VHOST",
}


@pytest.fixture(autouse=True)
def isolate_taskforge_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Prevent host Taskforge variables from influencing any settings test."""
    for variable_name in tuple(os.environ):
        if (
            variable_name.startswith(ENVIRONMENT_PREFIX)
            or variable_name in DEPENDENCY_ENVIRONMENT_VARIABLES
        ):
            monkeypatch.delenv(variable_name)
    monkeypatch.setenv("POSTGRES_PASSWORD", "test-postgres-password")
    monkeypatch.setenv("RABBITMQ_DEFAULT_PASS", "test-rabbitmq-password")


def test_settings_have_safe_local_defaults() -> None:
    settings = Settings()

    assert settings.application_name == "taskforge"
    assert settings.environment == "development"
    assert settings.log_level == "INFO"
    assert settings.api_host == "127.0.0.1"
    assert settings.api_port == 8000
    assert settings.readiness_timeout_seconds == 2.0
    assert settings.authentication_timeout_seconds == 2.0
    assert settings.database_pool_size == 5
    assert settings.database_pool_timeout_seconds == 2.0
    assert settings.postgres_host == "127.0.0.1"
    assert settings.postgres_port == 5432
    assert settings.rabbitmq_host == "127.0.0.1"
    assert settings.rabbitmq_port == 5672
    assert settings.rabbitmq_dispatch_exchange_name == "taskforge.dispatch.v1"
    assert settings.rabbitmq_malformed_exchange_name == (
        "taskforge.dispatch.malformed.v1"
    )
    assert settings.rabbitmq_topology_timeout_seconds == 5.0


def test_settings_accept_prefixed_environment_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TASKFORGE_APPLICATION_NAME", "taskforge-test")
    monkeypatch.setenv("TASKFORGE_ENVIRONMENT", "test")
    monkeypatch.setenv("TASKFORGE_LOG_LEVEL", "DEBUG")

    settings = Settings()

    assert settings.application_name == "taskforge-test"
    assert settings.environment == "test"
    assert settings.log_level == "DEBUG"


@pytest.mark.parametrize(
    ("variable_name", "invalid_value"),
    (
        ("TASKFORGE_ENVIRONMENT", "staging"),
        ("TASKFORGE_LOG_LEVEL", "VERBOSE"),
    ),
)
def test_settings_reject_invalid_constrained_values(
    monkeypatch: pytest.MonkeyPatch,
    variable_name: str,
    invalid_value: str,
) -> None:
    monkeypatch.setenv(variable_name, invalid_value)

    with pytest.raises(ValidationError):
        Settings()


def test_settings_ignore_unprefixed_environment_variables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APPLICATION_NAME", "wrong-application")
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("LOG_LEVEL", "ERROR")

    settings = Settings()

    assert settings.application_name == "taskforge"
    assert settings.environment == "development"
    assert settings.log_level == "INFO"


def test_settings_accept_compose_compatible_dependency_variables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_HOST", "postgres.internal")
    monkeypatch.setenv("POSTGRES_PORT", "55432")
    monkeypatch.setenv("POSTGRES_DB", "taskforge_test")
    monkeypatch.setenv("POSTGRES_USER", "postgres-test-user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "postgres-test-secret")
    monkeypatch.setenv("RABBITMQ_HOST", "rabbitmq.internal")
    monkeypatch.setenv("RABBITMQ_AMQP_PORT", "55672")
    monkeypatch.setenv("RABBITMQ_DEFAULT_USER", "rabbitmq-test-user")
    monkeypatch.setenv("RABBITMQ_DEFAULT_PASS", "rabbitmq-test-secret")
    monkeypatch.setenv("RABBITMQ_DEFAULT_VHOST", "taskforge_test")

    settings = Settings()

    assert settings.postgres_host == "postgres.internal"
    assert settings.postgres_port == 55432
    assert settings.postgres_database == "taskforge_test"
    assert settings.postgres_user == "postgres-test-user"
    assert settings.postgres_password.get_secret_value() == "postgres-test-secret"
    assert settings.rabbitmq_host == "rabbitmq.internal"
    assert settings.rabbitmq_port == 55672
    assert settings.rabbitmq_user == "rabbitmq-test-user"
    assert settings.rabbitmq_password.get_secret_value() == "rabbitmq-test-secret"
    assert settings.rabbitmq_vhost == "taskforge_test"


def test_dependency_passwords_are_required(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("POSTGRES_PASSWORD")
    monkeypatch.delenv("RABBITMQ_DEFAULT_PASS")

    with pytest.raises(ValidationError) as error:
        Settings()

    locations = {item["loc"] for item in error.value.errors()}
    assert ("POSTGRES_PASSWORD",) in locations
    assert ("RABBITMQ_DEFAULT_PASS",) in locations


def test_dependency_passwords_are_redacted() -> None:
    settings = Settings()

    rendered = repr(settings)

    assert "test-postgres-password" not in rendered
    assert "test-rabbitmq-password" not in rendered
    assert rendered.count("**********") == 2


@pytest.mark.parametrize(
    ("variable_name", "invalid_value"),
    (
        ("TASKFORGE_API_PORT", "0"),
        ("POSTGRES_PORT", "65536"),
        ("RABBITMQ_AMQP_PORT", "0"),
        ("TASKFORGE_READINESS_TIMEOUT_SECONDS", "0"),
        ("TASKFORGE_READINESS_TIMEOUT_SECONDS", "10.1"),
        ("TASKFORGE_AUTHENTICATION_TIMEOUT_SECONDS", "0"),
        ("TASKFORGE_DATABASE_POOL_SIZE", "0"),
        ("TASKFORGE_DATABASE_POOL_TIMEOUT_SECONDS", "10.1"),
        ("TASKFORGE_RABBITMQ_TOPOLOGY_TIMEOUT_SECONDS", "0"),
        ("TASKFORGE_RABBITMQ_DISPATCH_EXCHANGE_NAME", "amq.reserved"),
        ("TASKFORGE_RABBITMQ_MALFORMED_EXCHANGE_NAME", "Invalid Name"),
    ),
)
def test_settings_reject_invalid_runtime_values(
    monkeypatch: pytest.MonkeyPatch,
    variable_name: str,
    invalid_value: str,
) -> None:
    monkeypatch.setenv(variable_name, invalid_value)

    with pytest.raises(ValidationError):
        Settings()


def test_settings_require_distinct_topology_exchange_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TASKFORGE_RABBITMQ_DISPATCH_EXCHANGE_NAME", "same.exchange")
    monkeypatch.setenv("TASKFORGE_RABBITMQ_MALFORMED_EXCHANGE_NAME", "same.exchange")

    with pytest.raises(ValidationError):
        Settings()
