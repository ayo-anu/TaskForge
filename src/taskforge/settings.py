"""Typed application settings loaded from the process environment."""

from typing import Literal

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
    )

    application_name: str = "taskforge"
    environment: Environment = "development"
    log_level: LogLevel = "INFO"
