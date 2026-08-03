"""Tests for Taskforge environment-backed settings."""

from __future__ import annotations

import os

from pydantic import ValidationError
import pytest

from taskforge.settings import Settings


ENVIRONMENT_PREFIX = "TASKFORGE_"


@pytest.fixture(autouse=True)
def isolate_taskforge_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Prevent host Taskforge variables from influencing any settings test."""
    for variable_name in tuple(os.environ):
        if variable_name.startswith(ENVIRONMENT_PREFIX):
            monkeypatch.delenv(variable_name)


def test_settings_have_safe_local_defaults() -> None:
    settings = Settings()

    assert settings.application_name == "taskforge"
    assert settings.environment == "development"
    assert settings.log_level == "INFO"


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
