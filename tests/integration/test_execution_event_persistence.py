"""PostgreSQL guarantees for ordered workflow-run execution events."""

from __future__ import annotations

import asyncio
import json
import os
from uuid import UUID, uuid4

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import update
from sqlalchemy.engine import URL

from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.execution_events import (
    SQLAlchemyWorkflowRunExecutionEventRepository,
    append_workflow_run_execution_event,
)
from taskforge.runs.domain import NewWorkflowRunExecutionEvent
from taskforge.runs.persistence_ports import (
    WorkflowRunExecutionEventInvariantViolation,
)
from taskforge.runs.schema import task_runs
from tests.integration.postgresql import (
    asyncpg_dsn,
    migration_database_url,
    temporary_database,
)
from tests.integration.test_authentication_persistence import settings_for

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION=1 explicitly",
    ),
]


async def seed_runs(
    connection: asyncpg.Connection[asyncpg.Record],
) -> tuple[UUID, UUID, UUID, UUID]:
    principal_id, workflow_id, version_id = uuid4(), uuid4(), uuid4()
    first_run, second_run, first_task, second_task = (uuid4() for _ in range(4))
    await connection.execute(
        "INSERT INTO api_principals (id, name) VALUES ($1, $2)",
        principal_id,
        f"execution-events-{uuid4().hex}",
    )
    await connection.execute(
        "INSERT INTO workflow_definitions (id, owner_principal_id, name) "
        "VALUES ($1, $2, 'execution events')",
        workflow_id,
        principal_id,
    )
    await connection.execute(
        "INSERT INTO workflow_versions "
        "(id, workflow_definition_id, version_number, name) "
        "VALUES ($1, $2, 1, 'execution events')",
        version_id,
        workflow_id,
    )
    await connection.execute(
        "INSERT INTO workflow_version_steps "
        "(workflow_version_id, step_identifier, task_type, parameters) VALUES "
        "($1, 'first', 'test.task', '{}'::jsonb), "
        "($1, 'second', 'test.task', '{}'::jsonb)",
        version_id,
    )
    await connection.execute(
        "INSERT INTO workflow_runs (id, workflow_definition_id, "
        "workflow_version_id, requested_by_principal_id, status) VALUES "
        "($1, $3, $4, $5, 'pending'), ($2, $3, $4, $5, 'pending')",
        first_run,
        second_run,
        workflow_id,
        version_id,
        principal_id,
    )
    await connection.execute(
        "INSERT INTO task_runs (id, workflow_run_id, workflow_version_id, "
        "step_identifier, status) VALUES ($1, $3, $5, 'first', 'blocked'), "
        "($2, $4, $5, 'second', 'blocked')",
        first_task,
        second_task,
        first_run,
        second_run,
        version_id,
    )
    return first_run, second_run, first_task, second_task


async def raw_append(
    connection: asyncpg.Connection[asyncpg.Record],
    run_id: UUID,
    *,
    task_id: UUID | None = None,
    event_id: UUID | None = None,
    event_type: str = "workflow_run.status_changed",
) -> asyncpg.Record:
    row = await connection.fetchrow(
        "INSERT INTO workflow_run_execution_events "
        "(id, workflow_run_id, task_run_id, event_type, payload) "
        "VALUES ($1, $2, $3, $4, $5::jsonb) RETURNING id, cursor, occurred_at",
        event_id or uuid4(),
        run_id,
        task_id,
        event_type,
        json.dumps(
            {
                "previous_status": "blocked" if task_id is not None else "pending",
                "status": "running",
            }
        ),
    )
    assert row is not None
    return row


async def wait_until_lock_wait(
    observer: asyncpg.Connection[asyncpg.Record], backend_pid: int
) -> None:
    async with asyncio.timeout(5):
        while not await observer.fetchval(
            "SELECT wait_event_type = 'Lock' FROM pg_stat_activity WHERE pid = $1",
            backend_pid,
        ):
            await asyncio.sleep(0.01)


async def verify_constraints_immutability_and_pagination(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    repository = SQLAlchemyWorkflowRunExecutionEventRepository(sessions)
    try:
        first_run, second_run, first_task, second_task = await seed_runs(connection)
        empty = await repository.inspect_resume_cursor(first_run, None)
        assert (
            empty.earliest_retained_cursor,
            empty.latest_cursor,
            empty.requested_cursor,
            empty.requested_cursor_exists,
        ) == (None, 0, None, None)
        first = await raw_append(connection, first_run)
        second = await raw_append(
            connection,
            first_run,
            task_id=first_task,
            event_type="task_run.status_changed",
        )
        other = await raw_append(connection, second_run)
        assert (first["cursor"], second["cursor"], other["cursor"]) == (1, 2, 1)

        no_cursor = await repository.inspect_resume_cursor(first_run, None)
        beginning = await repository.inspect_resume_cursor(first_run, 0)
        middle = await repository.inspect_resume_cursor(first_run, 1)
        latest = await repository.inspect_resume_cursor(first_run, 2)
        ahead = await repository.inspect_resume_cursor(first_run, 3)
        independent = await repository.inspect_resume_cursor(second_run, 1)
        assert (
            no_cursor.earliest_retained_cursor,
            no_cursor.latest_cursor,
            no_cursor.requested_cursor_exists,
        ) == (1, 2, None)
        assert beginning.requested_cursor_exists is False
        assert middle.requested_cursor_exists is True
        assert latest.requested_cursor_exists is True
        assert ahead.requested_cursor_exists is False
        assert (
            independent.earliest_retained_cursor,
            independent.latest_cursor,
            independent.requested_cursor_exists,
        ) == (1, 1, True)
        with pytest.raises(WorkflowRunExecutionEventInvariantViolation):
            await repository.inspect_resume_cursor(uuid4(), 0)

        first_page = await repository.list_after(first_run, 0, 1)
        second_page = await repository.list_after(first_run, first_page[-1].cursor, 10)
        assert [event.cursor for event in first_page] == [1]
        assert [event.cursor for event in second_page] == [2]
        assert await repository.list_after(first_run, 2, 10) == ()
        assert [
            event.cursor for event in await repository.list_after(second_run, 0, 10)
        ] == [1]

        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await raw_append(connection, first_run, task_id=second_task)
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await raw_append(connection, uuid4())
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                "INSERT INTO workflow_run_execution_events "
                "(id, workflow_run_id, event_type, payload) "
                "VALUES ($1, $2, 'event', '[]'::jsonb)",
                uuid4(),
                first_run,
            )
        with pytest.raises(asyncpg.PostgresError) as assigned:
            await connection.execute(
                "INSERT INTO workflow_run_execution_events "
                "(id, workflow_run_id, cursor, event_type) "
                "VALUES ($1, $2, 99, 'event')",
                uuid4(),
                first_run,
            )
        assert assigned.value.sqlstate == "22023"

        for statement in (
            "UPDATE workflow_run_execution_events SET event_type = event_type "
            "WHERE id = $1",
            "DELETE FROM workflow_run_execution_events WHERE id = $1",
        ):
            with pytest.raises(asyncpg.PostgresError) as immutable:
                await connection.execute(statement, first["id"])
            assert immutable.value.sqlstate == "TF006"
        with pytest.raises(asyncpg.PostgresError) as truncate:
            await connection.execute("TRUNCATE workflow_run_execution_events")
        assert truncate.value.sqlstate == "TF006"
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await connection.execute("DELETE FROM task_runs WHERE id = $1", first_task)

        assert (
            await connection.fetchval(
                "SELECT last_execution_event_cursor FROM workflow_runs WHERE id = $1",
                first_run,
            )
            == 2
        )
    finally:
        await connection.close()
        await engine.dispose()


async def verify_session_atomicity(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    raw = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        run_id, other_run, task_id, other_task = await seed_runs(raw)
        session = sessions()
        await session.begin()
        await session.execute(
            update(task_runs).where(task_runs.c.id == task_id).values(status="runnable")
        )
        stored = await append_workflow_run_execution_event(
            session,
            NewWorkflowRunExecutionEvent(
                uuid4(),
                run_id,
                task_id,
                "task_run.status_changed",
                {"previous_status": "blocked", "status": "runnable"},
            ),
        )
        assert stored.cursor == 1
        assert (
            await raw.fetchval(
                "SELECT count(*) FROM workflow_run_execution_events "
                "WHERE workflow_run_id = $1",
                run_id,
            )
            == 0
        )
        await session.rollback()
        await session.close()
        assert (
            await raw.fetchval(
                "SELECT status::text FROM task_runs WHERE id = $1", task_id
            )
            == "blocked"
        )
        assert (
            await raw.fetchval(
                "SELECT last_execution_event_cursor FROM workflow_runs WHERE id = $1",
                run_id,
            )
            == 0
        )

        with pytest.raises(WorkflowRunExecutionEventInvariantViolation):
            async with sessions.begin() as failing:
                await failing.execute(
                    update(task_runs)
                    .where(task_runs.c.id == task_id)
                    .values(status="runnable")
                )
                await append_workflow_run_execution_event(
                    failing,
                    NewWorkflowRunExecutionEvent(
                        uuid4(),
                        run_id,
                        other_task,
                        "task_run.status_changed",
                        {"previous_status": "blocked", "status": "runnable"},
                    ),
                )
        assert (
            await raw.fetchval(
                "SELECT status::text FROM task_runs WHERE id = $1", task_id
            )
            == "blocked"
        )
        assert (
            await raw.fetchval(
                "SELECT last_execution_event_cursor FROM workflow_runs WHERE id = $1",
                run_id,
            )
            == 0
        )
        assert run_id != other_run
    finally:
        await raw.close()
        await engine.dispose()


async def verify_concurrent_allocation(database_url: URL) -> None:
    setup = await asyncpg.connect(asyncpg_dsn(database_url))
    first = await asyncpg.connect(asyncpg_dsn(database_url))
    second = await asyncpg.connect(asyncpg_dsn(database_url))
    observer = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        first_run, second_run, _, _ = await seed_runs(setup)

        first_tx = first.transaction()
        await first_tx.start()
        held = await raw_append(first, first_run)
        waiting = asyncio.create_task(raw_append(second, first_run))
        await wait_until_lock_wait(observer, second.get_server_pid())
        assert not waiting.done()
        await first_tx.commit()
        following = await waiting
        assert (held["cursor"], following["cursor"]) == (1, 2)

        rollback_tx = first.transaction()
        await rollback_tx.start()
        rolled_back = await raw_append(first, first_run)
        waiting_after_rollback = asyncio.create_task(raw_append(second, first_run))
        await wait_until_lock_wait(observer, second.get_server_pid())
        assert not waiting_after_rollback.done()
        await rollback_tx.rollback()
        replacement = await waiting_after_rollback
        assert rolled_back["cursor"] == replacement["cursor"] == 3

        independent_tx = first.transaction()
        await independent_tx.start()
        await raw_append(first, first_run)
        await second.execute("SET lock_timeout = '250ms'")
        independent = await raw_append(second, second_run)
        assert independent["cursor"] == 1
        await independent_tx.rollback()

        committed = await setup.fetch(
            "SELECT cursor FROM workflow_run_execution_events "
            "WHERE workflow_run_id = $1 ORDER BY cursor",
            first_run,
        )
        assert [row["cursor"] for row in committed] == [1, 2, 3]
    finally:
        await asyncio.gather(
            setup.close(), first.close(), second.close(), observer.close()
        )


async def verify_transactional_wakeups(database_url: URL) -> None:
    setup = await asyncpg.connect(asyncpg_dsn(database_url))
    writer = await asyncpg.connect(asyncpg_dsn(database_url))
    listener = await asyncpg.connect(asyncpg_dsn(database_url))
    notifications: asyncio.Queue[dict[str, str]] = asyncio.Queue()

    def notified(
        connection: asyncpg.Connection[asyncpg.Record],
        pid: int,
        channel: str,
        payload: str,
    ) -> None:
        del connection, pid, channel
        notifications.put_nowait(json.loads(payload))

    try:
        first_run, second_run, _, _ = await seed_runs(setup)
        await listener.add_listener("taskforge_workflow_run_execution_events", notified)

        transaction = writer.transaction()
        await transaction.start()
        await raw_append(writer, first_run)
        await raw_append(writer, first_run)
        assert notifications.empty()
        await transaction.commit()

        # A distinct committed run is a deterministic delimiter: PostgreSQL
        # preserves notification commit order for this listening session.
        await raw_append(setup, second_run)
        observed: list[dict[str, str]] = []
        async with asyncio.timeout(5):
            while True:
                item = await notifications.get()
                observed.append(item)
                if item == {"workflow_run_id": str(second_run)}:
                    break
        assert observed == [
            {"workflow_run_id": str(first_run)},
            {"workflow_run_id": str(second_run)},
        ]
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM workflow_run_execution_events "
                "WHERE workflow_run_id = $1",
                first_run,
            )
            == 2
        )

        rollback = writer.transaction()
        await rollback.start()
        await raw_append(writer, first_run)
        assert notifications.empty()
        await rollback.rollback()
        await raw_append(setup, second_run)
        async with asyncio.timeout(5):
            assert await notifications.get() == {"workflow_run_id": str(second_run)}
        assert notifications.empty()
    finally:
        await asyncio.gather(setup.close(), writer.close(), listener.close())


def test_execution_event_constraints_immutability_and_pagination() -> None:
    with temporary_database(
        "TASKFORGE_WORKFLOW_PERSISTENCE_TEST_DATABASE_URL",
        "taskforge_execution_events",
    ) as database_url:
        rendered = database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        with migration_database_url(rendered):
            command.upgrade(Config("alembic.ini"), "head")
        asyncio.run(verify_constraints_immutability_and_pagination(database_url))


def test_execution_event_append_is_transaction_atomic() -> None:
    with temporary_database(
        "TASKFORGE_WORKFLOW_PERSISTENCE_TEST_DATABASE_URL",
        "taskforge_execution_events",
    ) as database_url:
        rendered = database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        with migration_database_url(rendered):
            command.upgrade(Config("alembic.ini"), "head")
        asyncio.run(verify_session_atomicity(database_url))


def test_execution_event_concurrent_cursor_allocation_is_commit_safe() -> None:
    with temporary_database(
        "TASKFORGE_WORKFLOW_PERSISTENCE_TEST_DATABASE_URL",
        "taskforge_execution_events",
    ) as database_url:
        rendered = database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        with migration_database_url(rendered):
            command.upgrade(Config("alembic.ini"), "head")
        asyncio.run(verify_concurrent_allocation(database_url))


def test_execution_event_wakeups_are_transactional_and_run_scoped() -> None:
    with temporary_database(
        "TASKFORGE_WORKFLOW_PERSISTENCE_TEST_DATABASE_URL",
        "taskforge_execution_events",
    ) as database_url:
        rendered = database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        with migration_database_url(rendered):
            command.upgrade(Config("alembic.ini"), "head")
        asyncio.run(verify_transactional_wakeups(database_url))
