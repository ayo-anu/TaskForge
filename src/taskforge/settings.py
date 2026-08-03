"""Typed application settings loaded from the process environment."""

from typing import Literal

from pydantic import AliasChoices, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

Environment = Literal["development", "test", "production"]
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


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
