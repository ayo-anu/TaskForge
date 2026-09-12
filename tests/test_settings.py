"""Tests for Taskforge environment-backed settings."""

from __future__ import annotations

import os
from typing import Any, cast

import pytest
from pydantic import ValidationError

from taskforge.settings import (
    BrokerSettings,
    MigrationSettings,
    OrchestratorSettings,
    OwnerSettings,
    Settings,
    WorkerSettings,
)

ENVIRONMENT_PREFIX = "TASKFORGE_"
DEPENDENCY_ENVIRONMENT_VARIABLES = {
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_OWNER_USER",
    "POSTGRES_OWNER_PASSWORD",
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
    monkeypatch.setenv(
        "TASKFORGE_TASK_CLAIM_RESULT_AUTHORITY_SECRET",
        "test-claim-result-authority-secret",
    )


def test_settings_have_safe_local_defaults() -> None:
    settings = Settings()

    assert settings.application_name == "taskforge"
    assert settings.environment == "development"
    assert settings.log_level == "INFO"
    assert settings.tracing_enabled is False
    assert settings.tracing_exporter == "none"
    assert settings.tracing_otlp_endpoint is None
    assert settings.tracing_sample_ratio == 0.1
    assert settings.metrics_enabled is False
    assert settings.metrics_exporter == "none"
    assert settings.metrics_otlp_endpoint is None
    assert settings.metrics_export_interval_seconds == 60.0
    assert settings.metrics_outbox_staleness_seconds == 120.0
    assert settings.api_host == "127.0.0.1"
    assert settings.api_port == 8000
    assert settings.api_max_request_body_bytes == 10 * 1024 * 1024
    assert settings.readiness_timeout_seconds == 2.0
    assert settings.authentication_timeout_seconds == 2.0
    assert settings.execution_stream_max_connections == 500
    assert settings.execution_stream_allowed_origins == ()
    assert settings.execution_stream_max_session_seconds == 900
    assert settings.execution_stream_queue_size == 100
    assert settings.execution_stream_listener_reconnect_max_seconds == 5.0
    assert settings.database_pool_size == 5
    assert settings.database_pool_timeout_seconds == 2.0
    assert settings.worker_stale_after_seconds == 30
    assert settings.worker_offline_after_seconds == 120
    assert settings.task_claim_lease_seconds == 60
    assert settings.task_claim_result_authority_secret.get_secret_value() == (
        "test-claim-result-authority-secret"
    )
    assert settings.postgres_host == "127.0.0.1"
    assert settings.postgres_port == 5432
    assert not any(name.startswith("rabbitmq_") for name in Settings.model_fields)


def test_orchestrator_settings_have_bounded_defaults() -> None:
    settings = OrchestratorSettings()

    assert settings.orchestrator_batch_size == 100
    assert settings.orchestrator_poll_interval_seconds == 1.0
    assert settings.orchestrator_publication_timeout_seconds == 5.0


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("orchestrator_batch_size", 0),
        ("orchestrator_batch_size", 101),
        ("orchestrator_poll_interval_seconds", 0),
        ("orchestrator_poll_interval_seconds", 61),
        ("orchestrator_publication_timeout_seconds", 0),
        ("orchestrator_publication_timeout_seconds", 31),
    ),
)
def test_orchestrator_settings_reject_values_outside_bounds(
    field: str, value: int
) -> None:
    with pytest.raises(ValidationError):
        OrchestratorSettings(**cast(Any, {field: value}))


def test_settings_accept_prefixed_environment_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TASKFORGE_APPLICATION_NAME", "taskforge-test")
    monkeypatch.setenv("TASKFORGE_ENVIRONMENT", "test")
    monkeypatch.setenv("TASKFORGE_LOG_LEVEL", "DEBUG")

    settings = BrokerSettings()

    assert settings.application_name == "taskforge-test"
    assert settings.environment == "test"
    assert settings.log_level == "DEBUG"


@pytest.mark.parametrize("value", (59, 3601))
def test_execution_stream_max_session_rejects_values_outside_finite_range(
    value: int,
) -> None:
    with pytest.raises(ValidationError):
        Settings(execution_stream_max_session_seconds=value)


@pytest.mark.parametrize("value", (60, 900, 3600))
def test_execution_stream_max_session_accepts_boundaries(value: int) -> None:
    assert (
        Settings(
            execution_stream_max_session_seconds=value
        ).execution_stream_max_session_seconds
        == value
    )


def test_execution_stream_origins_are_canonicalized_and_strict() -> None:
    settings = Settings(
        execution_stream_allowed_origins=(
            "https://Example.COM:443/",
            "http://[0:0:0:0:0:0:0:1]:80",
        )
    )
    assert settings.execution_stream_allowed_origins == (
        "https://example.com",
        "http://[::1]",
    )

    for value in (
        "null",
        "*",
        "https://*.example.com",
        "https://user@example.com",
        "https://example.com/path",
        "https://example.com?query=1",
        "https://example.com#fragment",
        "ftp://example.com",
    ):
        with pytest.raises(ValidationError):
            Settings(execution_stream_allowed_origins=(value,))


def test_tracing_enablement_is_independent_from_export_selection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TASKFORGE_TRACING_ENABLED", "true")
    assert Settings().tracing_exporter == "none"

    monkeypatch.setenv("TASKFORGE_TRACING_EXPORTER", "otlp_http")
    monkeypatch.setenv(
        "TASKFORGE_TRACING_OTLP_ENDPOINT", "http://collector:4318/v1/traces"
    )
    configured = Settings()
    assert configured.tracing_enabled
    assert configured.tracing_exporter == "otlp_http"


def test_metrics_enablement_is_independent_from_export_selection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TASKFORGE_METRICS_ENABLED", "true")
    assert Settings().metrics_exporter == "none"

    monkeypatch.setenv("TASKFORGE_METRICS_EXPORTER", "otlp_http")
    monkeypatch.setenv(
        "TASKFORGE_METRICS_OTLP_ENDPOINT", "http://collector:4318/v1/metrics"
    )
    configured = Settings()
    assert configured.metrics_enabled
    assert configured.metrics_exporter == "otlp_http"


@pytest.mark.parametrize(
    ("enabled", "exporter", "endpoint"),
    (
        ("false", "otlp_http", "http://collector:4318/v1/metrics"),
        ("true", "otlp_http", None),
        ("true", "none", "http://collector:4318/v1/metrics"),
    ),
)
def test_inconsistent_metrics_configuration_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
    enabled: str,
    exporter: str,
    endpoint: str | None,
) -> None:
    monkeypatch.setenv("TASKFORGE_METRICS_ENABLED", enabled)
    monkeypatch.setenv("TASKFORGE_METRICS_EXPORTER", exporter)
    if endpoint is not None:
        monkeypatch.setenv("TASKFORGE_METRICS_OTLP_ENDPOINT", endpoint)
    with pytest.raises(ValidationError):
        Settings()


@pytest.mark.parametrize(
    ("enabled", "exporter", "endpoint"),
    (
        ("false", "otlp_http", "http://collector:4318/v1/traces"),
        ("true", "otlp_http", None),
        ("true", "none", "http://collector:4318/v1/traces"),
    ),
)
def test_inconsistent_tracing_configuration_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
    enabled: str,
    exporter: str,
    endpoint: str | None,
) -> None:
    monkeypatch.setenv("TASKFORGE_TRACING_ENABLED", enabled)
    monkeypatch.setenv("TASKFORGE_TRACING_EXPORTER", exporter)
    if endpoint is not None:
        monkeypatch.setenv("TASKFORGE_TRACING_OTLP_ENDPOINT", endpoint)
    with pytest.raises(ValidationError):
        Settings()


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

    settings = BrokerSettings()

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


def test_api_settings_do_not_require_or_expose_rabbitmq(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("RABBITMQ_DEFAULT_PASS")

    settings = Settings()

    assert not any(name.startswith("rabbitmq_") for name in type(settings).model_fields)
    with pytest.raises(ValidationError) as error:
        BrokerSettings()
    assert ("RABBITMQ_DEFAULT_PASS",) in {item["loc"] for item in error.value.errors()}


def test_runtime_settings_ignore_owner_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_USER", "taskforge_runtime")
    monkeypatch.setenv("POSTGRES_PASSWORD", "runtime-secret")
    monkeypatch.setenv("POSTGRES_OWNER_USER", "taskforge_owner")
    monkeypatch.setenv("POSTGRES_OWNER_PASSWORD", "owner-secret")

    runtime = Settings()
    owner = OwnerSettings()

    assert runtime.postgres_user == "taskforge_runtime"
    assert runtime.postgres_password.get_secret_value() == "runtime-secret"
    assert owner.postgres_user == "taskforge_owner"
    assert owner.postgres_password.get_secret_value() == "owner-secret"
    assert "owner-secret" not in repr(runtime)


def test_dependency_passwords_are_required(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("POSTGRES_PASSWORD")
    monkeypatch.delenv("RABBITMQ_DEFAULT_PASS")

    with pytest.raises(ValidationError) as error:
        Settings()

    locations = {item["loc"] for item in error.value.errors()}
    assert locations == {("POSTGRES_PASSWORD",)}

    with pytest.raises(ValidationError) as broker_error:
        BrokerSettings()
    broker_locations = {item["loc"] for item in broker_error.value.errors()}
    assert broker_locations == {
        ("POSTGRES_PASSWORD",),
        ("RABBITMQ_DEFAULT_PASS",),
    }


def test_dependency_passwords_are_redacted() -> None:
    settings = BrokerSettings()

    rendered = repr(settings)

    assert "test-postgres-password" not in rendered
    assert "test-rabbitmq-password" not in rendered
    assert "test-claim-result-authority-secret" not in rendered
    assert rendered.count("**********") == 3


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
        ("TASKFORGE_TASK_CLAIM_LEASE_SECONDS", "0"),
        ("TASKFORGE_TASK_CLAIM_RESULT_AUTHORITY_SECRET", "too-short"),
    ),
)
def test_settings_reject_invalid_runtime_values(
    monkeypatch: pytest.MonkeyPatch,
    variable_name: str,
    invalid_value: str,
) -> None:
    monkeypatch.setenv(variable_name, invalid_value)

    settings_type = BrokerSettings if "RABBITMQ" in variable_name else Settings
    with pytest.raises(ValidationError):
        settings_type()


def test_settings_require_distinct_topology_exchange_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TASKFORGE_RABBITMQ_DISPATCH_EXCHANGE_NAME", "same.exchange")
    monkeypatch.setenv("TASKFORGE_RABBITMQ_MALFORMED_EXCHANGE_NAME", "same.exchange")

    with pytest.raises(ValidationError):
        BrokerSettings()


def test_settings_require_ordered_worker_health_thresholds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TASKFORGE_WORKER_STALE_AFTER_SECONDS", "30")
    monkeypatch.setenv("TASKFORGE_WORKER_OFFLINE_AFTER_SECONDS", "30")

    with pytest.raises(ValidationError):
        Settings()


def test_production_requires_explicit_claim_result_authority_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TASKFORGE_ENVIRONMENT", "production")
    monkeypatch.delenv("TASKFORGE_TASK_CLAIM_RESULT_AUTHORITY_SECRET")

    with pytest.raises(ValidationError, match="claim result authority secret"):
        Settings()


@pytest.mark.parametrize(
    "host",
    ("127.0.0.1", "127.12.34.56", "::1", "localhost", "0.0.0.0", "::"),
)
def test_production_plaintext_listener_accepts_loopback_or_unspecified_hosts(
    monkeypatch: pytest.MonkeyPatch,
    host: str,
) -> None:
    monkeypatch.setenv("TASKFORGE_ENVIRONMENT", "production")
    monkeypatch.setenv("TASKFORGE_API_HOST", host)

    assert Settings().api_host == host


@pytest.mark.parametrize(
    "host",
    (
        "10.0.0.8",
        "192.168.1.8",
        "203.0.113.8",
        "taskforge.internal",
        "not a host",
        "LOCALHOST",
    ),
)
def test_production_plaintext_listener_rejects_non_loopback_without_dns(
    monkeypatch: pytest.MonkeyPatch,
    host: str,
) -> None:
    monkeypatch.setenv("TASKFORGE_ENVIRONMENT", "production")
    monkeypatch.setenv("TASKFORGE_API_HOST", host)

    with pytest.raises(ValidationError, match="plaintext API listener"):
        Settings()


@pytest.mark.parametrize("environment", ("development", "test"))
def test_non_production_plaintext_listener_behavior_is_unchanged(
    monkeypatch: pytest.MonkeyPatch,
    environment: str,
) -> None:
    monkeypatch.setenv("TASKFORGE_ENVIRONMENT", environment)
    monkeypatch.setenv("TASKFORGE_API_HOST", "0.0.0.0")

    assert Settings().api_host == "0.0.0.0"


def test_worker_settings_require_credential_and_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ValidationError):
        WorkerSettings()
    monkeypatch.setenv("TASKFORGE_WORKER_CREDENTIAL", "not-logged")
    monkeypatch.setenv("TASKFORGE_WORKER_PROFILE", "batch.small")

    settings = WorkerSettings()

    assert settings.worker_credential.get_secret_value() == "not-logged"
    assert settings.worker_profile == "batch.small"
    assert settings.worker_heartbeat_interval_seconds == 10.0
    assert settings.worker_prefetch_count == 1
    assert settings.worker_control_operation_timeout_seconds == 2.0


@pytest.mark.parametrize("credential", (None, ""))
def test_worker_settings_reject_missing_or_empty_credentials(
    monkeypatch: pytest.MonkeyPatch, credential: str | None
) -> None:
    monkeypatch.delenv("TASKFORGE_WORKER_CREDENTIAL", raising=False)
    monkeypatch.setenv("TASKFORGE_WORKER_PROFILE", "pipeline")
    if credential is not None:
        monkeypatch.setenv("TASKFORGE_WORKER_CREDENTIAL", credential)

    with pytest.raises(ValidationError) as error:
        WorkerSettings()

    assert ("worker_credential",) in {item["loc"] for item in error.value.errors()}


@pytest.mark.parametrize(
    ("name", "value"),
    (
        ("TASKFORGE_WORKER_PROFILE", "module:callable"),
        ("TASKFORGE_WORKER_PROFILE", "../profile"),
        ("TASKFORGE_WORKER_HEARTBEAT_INTERVAL_SECONDS", "0"),
        ("TASKFORGE_WORKER_PREFETCH_COUNT", "0"),
        ("TASKFORGE_WORKER_PREFETCH_COUNT", "129"),
        ("TASKFORGE_WORKER_CONTROL_OPERATION_TIMEOUT_SECONDS", "0"),
    ),
)
def test_worker_settings_reject_invalid_values(
    monkeypatch: pytest.MonkeyPatch, name: str, value: str
) -> None:
    monkeypatch.setenv("TASKFORGE_WORKER_CREDENTIAL", "not-logged")
    monkeypatch.setenv("TASKFORGE_WORKER_PROFILE", "batch")
    monkeypatch.setenv(name, value)
    with pytest.raises(ValidationError):
        WorkerSettings()


def test_worker_settings_require_conservative_control_timing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TASKFORGE_WORKER_CREDENTIAL", "not-logged")
    monkeypatch.setenv("TASKFORGE_WORKER_PROFILE", "batch")
    monkeypatch.setenv("TASKFORGE_WORKER_CONTROL_OPERATION_TIMEOUT_SECONDS", "10")
    with pytest.raises(ValidationError):
        WorkerSettings()


def test_migration_settings_accept_only_narrow_owner_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_HOST", "database.internal")
    monkeypatch.setenv("POSTGRES_PORT", "5544")
    monkeypatch.setenv("POSTGRES_DB", "taskforge_deployment")
    monkeypatch.setenv("POSTGRES_OWNER_USER", "taskforge_owner")
    monkeypatch.setenv("POSTGRES_OWNER_PASSWORD", "owner-secret")
    monkeypatch.setenv("TASKFORGE_MIGRATION_LOCK_TIMEOUT_SECONDS", "42")
    monkeypatch.setenv("RABBITMQ_DEFAULT_PASS", "must-be-ignored")

    settings = MigrationSettings()

    assert settings.postgres_host == "database.internal"
    assert settings.postgres_port == 5544
    assert settings.postgres_database == "taskforge_deployment"
    assert settings.postgres_owner_user == "taskforge_owner"
    assert settings.postgres_owner_password.get_secret_value() == "owner-secret"
    assert settings.migration_lock_timeout_seconds == 42
    assert "rabbitmq_password" not in type(settings).model_fields
    assert "postgres_password" not in type(settings).model_fields


@pytest.mark.parametrize("timeout", ("0", "3601", "not-a-number"))
def test_migration_settings_reject_invalid_lock_timeout(
    monkeypatch: pytest.MonkeyPatch, timeout: str
) -> None:
    monkeypatch.setenv("POSTGRES_OWNER_PASSWORD", "owner-secret")
    monkeypatch.setenv("TASKFORGE_MIGRATION_LOCK_TIMEOUT_SECONDS", timeout)

    with pytest.raises(ValidationError):
        MigrationSettings()
