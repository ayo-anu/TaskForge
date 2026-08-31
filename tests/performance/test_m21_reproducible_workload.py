"""Opt-in M21 Task 1 workload against real PostgreSQL and RabbitMQ."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config

from tests.integration.postgresql import migration_database_url, temporary_database
from tests.performance.m21_runner import run_m21_workload

pytestmark = [
    pytest.mark.integration,
    pytest.mark.workload,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_M21_WORKLOAD") != "1",
        reason="set TASKFORGE_RUN_M21_WORKLOAD=1 explicitly",
    ),
]


def test_m21_reproducible_workload(tmp_path: Path) -> None:
    amqp_url = os.getenv("TASKFORGE_M21_AMQP_URL")
    if not amqp_url:
        pytest.fail("TASKFORGE_M21_AMQP_URL is required")
    with temporary_database(
        "TASKFORGE_M21_DATABASE_URL", "taskforge_m21_workload"
    ) as url:
        with migration_database_url(url.render_as_string(hide_password=False)):
            command.upgrade(Config("alembic.ini"), "head")
        asyncio.run(run_m21_workload(url, amqp_url, tmp_path / "m21-evidence.json"))
