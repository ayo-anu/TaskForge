"""Typed application settings loaded from the process environment."""

from __future__ import annotations

from typing import Literal

from pydantic import AliasChoices, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

Environment = Literal["development", "test", "production"]
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
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
    api_host: str = "127.0.0.1"
    api_port: int = Field(default=8000, ge=1, le=65535)
    readiness_timeout_seconds: float = Field(default=2.0, gt=0, le=10)
    authentication_timeout_seconds: float = Field(default=2.0, gt=0, le=10)
    database_pool_size: int = Field(default=5, ge=1, le=20)
    database_pool_timeout_seconds: float = Field(default=2.0, gt=0, le=10)
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
