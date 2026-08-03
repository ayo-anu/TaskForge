"""Opt-in readiness verification against real Task 3 dependencies."""

from __future__ import annotations

import asyncio
import os

import pytest

from taskforge.api.dependencies import build_readiness_coordinator
from taskforge.settings import Settings

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_INTEGRATION=1 to use real dependencies",
    ),
]


def test_required_dependencies_are_ready() -> None:
    readiness = build_readiness_coordinator(Settings())

    async def verify() -> None:
        await readiness.start()
        try:
            assert await readiness.is_ready() is True
        finally:
            await readiness.close()

    asyncio.run(verify())
