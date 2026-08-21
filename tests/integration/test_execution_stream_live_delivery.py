"""Real PostgreSQL LISTEN and durable live-delivery reconciliation."""

from __future__ import annotations

import asyncio
import os
from typing import Any, cast

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from fastapi import WebSocket
from sqlalchemy.engine import URL

from taskforge.api.execution_stream_runtime import (
    ExecutionStreamRuntime,
    SubscriptionState,
)
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.execution_events import (
    SQLAlchemyWorkflowRunExecutionEventRepository,
)
from taskforge.runs.domain import StoredWorkflowRunExecutionEvent
from tests.integration.postgresql import (
    asyncpg_dsn,
    migration_database_url,
    temporary_database,
)
from tests.integration.test_authentication_persistence import settings_for
from tests.integration.test_execution_event_persistence import raw_append, seed_runs

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION=1 explicitly",
    ),
]


class Socket:
    async def send_json(self, message: dict[str, Any]) -> None:
        del message

    async def receive(self) -> dict[str, Any]:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def close(self, *, code: int, reason: str) -> None:
        del code, reason


def serialize(event: StoredWorkflowRunExecutionEvent) -> dict[str, Any]:
    return {"cursor": event.cursor}


async def wait_until_listener_unavailable(runtime: ExecutionStreamRuntime) -> None:
    async with asyncio.timeout(5):
        while runtime.listener_ready:
            await asyncio.sleep(0)


async def verify_multi_instance_and_listener_recovery(database_url: URL) -> None:
    settings = settings_for(database_url)
    first_engine = build_async_engine(settings)
    second_engine = build_async_engine(settings)
    first = ExecutionStreamRuntime(
        settings,
        SQLAlchemyWorkflowRunExecutionEventRepository(
            build_session_factory(first_engine)
        ),
        serialize,
    )
    second = ExecutionStreamRuntime(
        settings,
        SQLAlchemyWorkflowRunExecutionEventRepository(
            build_session_factory(second_engine)
        ),
        serialize,
    )
    setup = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        run_id, other_run_id, _, _ = await seed_runs(setup)
        await asyncio.gather(first.start(), second.start())
        assert first.listener_ready and second.listener_ready
        first_subscription = await first.open_subscription(
            cast(WebSocket, Socket()), run_id, 0, None
        )
        second_subscription = await second.open_subscription(
            cast(WebSocket, Socket()), run_id, 0, None
        )
        unrelated = await first.open_subscription(
            cast(WebSocket, Socket()), other_run_id, 0, None
        )
        first_subscription.state = SubscriptionState.LIVE
        second_subscription.state = SubscriptionState.LIVE
        unrelated.state = SubscriptionState.LIVE

        await raw_append(setup, run_id)
        async with asyncio.timeout(5):
            delivered = await asyncio.gather(
                first_subscription.queue.get(), second_subscription.queue.get()
            )
        assert [item.cursor for item in delivered] == [1, 1]
        assert unrelated.queue.empty()

        listener = first._listener_connection
        assert listener is not None
        backend_pid = cast(Any, listener).get_server_pid()
        await setup.execute("SELECT pg_terminate_backend($1)", backend_pid)
        await wait_until_listener_unavailable(first)
        await raw_append(setup, run_id)
        async with asyncio.timeout(10):
            recovered = await first_subscription.queue.get()
        assert recovered.cursor == 2
        assert first.listener_ready
    finally:
        await asyncio.gather(first.close(), second.close())
        await asyncio.gather(first_engine.dispose(), second_engine.dispose())
        await setup.close()


def test_live_delivery_is_multi_instance_and_recovers_missed_wakeup() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_execution_events"
    ) as database_url:
        with migration_database_url(database_url.render_as_string(hide_password=False)):
            command.upgrade(Config("alembic.ini"), "head")
        asyncio.run(verify_multi_instance_and_listener_recovery(database_url))
