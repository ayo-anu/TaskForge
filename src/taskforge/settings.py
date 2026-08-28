"""Typed application settings loaded from the process environment."""

from __future__ import annotations

from typing import Literal

from pydantic import AliasChoices, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

Environment = Literal["development", "test", "production"]
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
TracingExporter = Literal["none", "otlp_http"]
MetricsExporter = Literal["none", "otlp_http"]
DEVELOPMENT_CLAIM_RESULT_AUTHORITY_SECRET = (
    "taskforge-development-claim-result-authority-secret"
)


class Settings(BaseSettings):
    """Process-neutral Taskforge settings with safe local defaults."""

    model_config = SettingsConfigDict(
        env_file=None,
        env_prefix="TASKFORGE_",
        extra="ignore",
        frozen=True,
        populate_by_name=True,
    )

    application_name: str = "taskforge"
    environment: Environment = "development"
    log_level: LogLevel = "INFO"
    tracing_enabled: bool = False
    tracing_exporter: TracingExporter = "none"
    tracing_otlp_endpoint: str | None = None
    tracing_sample_ratio: float = Field(default=0.1, ge=0, le=1)
    tracing_export_timeout_seconds: float = Field(default=5.0, gt=0, le=30)
    tracing_shutdown_timeout_seconds: float = Field(default=5.0, gt=0, le=30)
    metrics_enabled: bool = False
    metrics_exporter: MetricsExporter = "none"
    metrics_otlp_endpoint: str | None = None
    metrics_export_interval_seconds: float = Field(default=60.0, ge=5, le=300)
    metrics_export_timeout_seconds: float = Field(default=5.0, gt=0, le=30)
    metrics_shutdown_timeout_seconds: float = Field(default=5.0, gt=0, le=30)
    metrics_outbox_staleness_seconds: float = Field(default=120.0, ge=10, le=600)
    api_host: str = "127.0.0.1"
    api_port: int = Field(default=8000, ge=1, le=65535)
    readiness_timeout_seconds: float = Field(default=2.0, gt=0, le=10)
    authentication_timeout_seconds: float = Field(default=2.0, gt=0, le=10)
    database_pool_size: int = Field(default=5, ge=1, le=20)
    database_pool_timeout_seconds: float = Field(default=2.0, gt=0, le=10)
    execution_stream_max_connections: int = Field(default=500, ge=1, le=10_000)
    execution_stream_queue_size: int = Field(default=100, ge=1, le=1_000)
    execution_stream_listener_reconnect_max_seconds: float = Field(
        default=5.0, gt=0, le=60
    )
    worker_stale_after_seconds: int = Field(default=30, ge=1, le=3600)
    worker_offline_after_seconds: int = Field(default=120, ge=2, le=86400)
    task_claim_lease_seconds: int = Field(default=60, ge=1)
    task_cancellation_poll_seconds: float = Field(default=1.0, gt=0, le=60)
    task_claim_result_authority_secret: SecretStr = Field(
        default=SecretStr(DEVELOPMENT_CLAIM_RESULT_AUTHORITY_SECRET), min_length=32
    )

    postgres_host: str = Field(
        default="127.0.0.1",
        validation_alias=AliasChoices("POSTGRES_HOST", "TASKFORGE_POSTGRES_HOST"),
    )
    postgres_port: int = Field(
        default=5432,
        ge=1,
        le=65535,
        validation_alias=AliasChoices("POSTGRES_PORT", "TASKFORGE_POSTGRES_PORT"),
    )
    postgres_database: str = Field(
        default="taskforge",
        validation_alias=AliasChoices("POSTGRES_DB", "TASKFORGE_POSTGRES_DATABASE"),
    )
    postgres_user: str = Field(
        default="taskforge",
        validation_alias=AliasChoices("POSTGRES_USER", "TASKFORGE_POSTGRES_USER"),
    )
    postgres_password: SecretStr = Field(
        validation_alias=AliasChoices(
            "POSTGRES_PASSWORD",
            "TASKFORGE_POSTGRES_PASSWORD",
        ),
    )

    rabbitmq_host: str = Field(
        default="127.0.0.1",
        validation_alias=AliasChoices("RABBITMQ_HOST", "TASKFORGE_RABBITMQ_HOST"),
    )
    rabbitmq_port: int = Field(
        default=5672,
        ge=1,
        le=65535,
        validation_alias=AliasChoices(
            "RABBITMQ_AMQP_PORT",
            "TASKFORGE_RABBITMQ_PORT",
        ),
    )
    rabbitmq_user: str = Field(
        default="taskforge",
        validation_alias=AliasChoices(
            "RABBITMQ_DEFAULT_USER",
            "TASKFORGE_RABBITMQ_USER",
        ),
    )
    rabbitmq_password: SecretStr = Field(
        validation_alias=AliasChoices(
            "RABBITMQ_DEFAULT_PASS",
            "TASKFORGE_RABBITMQ_PASSWORD",
        ),
    )
    rabbitmq_vhost: str = Field(
        default="taskforge",
        validation_alias=AliasChoices(
            "RABBITMQ_DEFAULT_VHOST",
            "TASKFORGE_RABBITMQ_VHOST",
        ),
    )
    rabbitmq_dispatch_exchange_name: str = Field(
        default="taskforge.dispatch.v1",
        pattern=r"^[a-z][a-z0-9._-]{0,254}$",
    )
    rabbitmq_malformed_exchange_name: str = Field(
        default="taskforge.dispatch.malformed.v1",
        pattern=r"^[a-z][a-z0-9._-]{0,254}$",
    )
    rabbitmq_topology_timeout_seconds: float = Field(default=5.0, gt=0, le=30)

    @model_validator(mode="after")
    def validate_rabbitmq_topology_names(self) -> Settings:
        names = (
            self.rabbitmq_dispatch_exchange_name,
            self.rabbitmq_malformed_exchange_name,
        )
        if any(name.startswith("amq.") for name in names):
            raise ValueError("RabbitMQ topology names cannot use the reserved prefix")
        if names[0] == names[1]:
            raise ValueError("RabbitMQ topology exchange names must be distinct")
        return self

    @model_validator(mode="after")
    def validate_tracing_configuration(self) -> Settings:
        if not self.tracing_enabled and self.tracing_exporter != "none":
            raise ValueError("a tracing exporter requires tracing to be enabled")
        if self.tracing_exporter == "otlp_http":
            endpoint = self.tracing_otlp_endpoint
            if endpoint is None or not endpoint.startswith(("http://", "https://")):
                raise ValueError("OTLP/HTTP tracing requires an HTTP(S) endpoint")
        elif self.tracing_otlp_endpoint is not None:
            raise ValueError("a tracing endpoint requires the OTLP/HTTP exporter")
        return self

    @model_validator(mode="after")
    def validate_metrics_configuration(self) -> Settings:
        if not self.metrics_enabled and self.metrics_exporter != "none":
            raise ValueError("a metrics exporter requires metrics to be enabled")
        if self.metrics_exporter == "otlp_http":
            endpoint = self.metrics_otlp_endpoint
            if endpoint is None or not endpoint.startswith(("http://", "https://")):
                raise ValueError("OTLP/HTTP metrics requires an HTTP(S) endpoint")
        elif self.metrics_otlp_endpoint is not None:
            raise ValueError("a metrics endpoint requires the OTLP/HTTP exporter")
        return self

    @model_validator(mode="after")
    def validate_worker_health_thresholds(self) -> Settings:
        if self.worker_offline_after_seconds <= self.worker_stale_after_seconds:
            raise ValueError("worker offline threshold must exceed stale threshold")
        return self

    @model_validator(mode="after")
    def validate_production_claim_authority_secret(self) -> Settings:
        if (
            self.environment == "production"
            and self.task_claim_result_authority_secret.get_secret_value()
            == DEVELOPMENT_CLAIM_RESULT_AUTHORITY_SECRET
        ):
            raise ValueError(
                "production requires an explicit claim result authority secret"
            )
        return self


class OwnerSettings(Settings):
    """Administrative settings whose database credential is the object owner."""

    postgres_user: str = Field(
        default="taskforge_owner",
        validation_alias="POSTGRES_OWNER_USER",
    )
    postgres_password: SecretStr = Field(
        validation_alias="POSTGRES_OWNER_PASSWORD",
    )
