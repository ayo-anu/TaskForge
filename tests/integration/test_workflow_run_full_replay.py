"""Opt-in full workflow replay verification against PostgreSQL."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable, Coroutine
from typing import Any
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import func, insert, select, text, update
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.identity.authorization import OwnerFilter
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.runs import SQLAlchemyWorkflowRunRepository
from taskforge.runs.domain import (
    ExplicitWorkflowVersion,
    WorkflowRunReplayNotEligible,
    WorkflowRunStatus,
    create_workflow_run_input,
)
from taskforge.runs.schema import (
    task_attempts,
    task_runs,
    workflow_run_cancellation_requests,
    workflow_run_execution_events,
    workflow_run_inputs,
    workflow_run_replays,
    workflow_runs,
)
from taskforge.runs.service import (
    WorkflowRunNotFound,
    WorkflowRunService,
    WorkflowRunServiceUnavailable,
)
from taskforge.workflows.schema import workflow_definitions
from tests.integration.postgresql import migration_database_url, temporary_database
from tests.integration.test_authentication_persistence import settings_for
from tests.integration.test_workflow_run_creation import seed_workflow

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION=1 explicitly",
    ),
]

Verifier = Callable[[URL], Coroutine[Any, Any, None]]


async def create_source(
    service: WorkflowRunService,
    sessions: async_sessionmaker[AsyncSession],
    workflow_id: UUID,
    owner_id: UUID,
    status: WorkflowRunStatus,
) -> UUID:
    created = await service.create_run(
        workflow_id,
        owner_principal_id=owner_id,
        requested_by_principal_id=owner_id,
        selection=ExplicitWorkflowVersion(1),
        input_snapshot=create_workflow_run_input(
            {"ordinary": {"batch": 7}},
            {"database_password": {"secret_ref": "vault://taskforge/database"}},
        ),
    )
    async with sessions.begin() as session:
        await session.execute(
            update(workflow_runs)
            .where(workflow_runs.c.id == created.id)
            .values(status=status.value)
        )
    return created.id


async def projection(
    sessions: async_sessionmaker[AsyncSession], run_id: UUID
) -> tuple[Any, Any, tuple[Any, ...], int, int, int]:
    async with sessions() as session:
        run = (
            await session.execute(
                select(workflow_runs).where(workflow_runs.c.id == run_id)
            )
        ).one()
        input_row = (
            await session.execute(
                select(workflow_run_inputs).where(
                    workflow_run_inputs.c.workflow_run_id == run_id
                )
            )
        ).one()
        tasks = (
            await session.execute(
                select(task_runs.c.id, task_runs.c.step_identifier, task_runs.c.status)
                .where(task_runs.c.workflow_run_id == run_id)
                .order_by(task_runs.c.step_identifier)
            )
        ).all()
        attempts = await session.scalar(
            select(func.count())
            .select_from(task_attempts.join(task_runs))
            .where(task_runs.c.workflow_run_id == run_id)
        )
        cancellations = await session.scalar(
            select(func.count())
            .select_from(workflow_run_cancellation_requests)
            .where(workflow_run_cancellation_requests.c.workflow_run_id == run_id)
        )
        events = await session.scalar(
            select(func.count())
            .select_from(workflow_run_execution_events)
            .where(workflow_run_execution_events.c.workflow_run_id == run_id)
        )
    return (
        run,
        input_row,
        tuple(tasks),
        int(attempts or 0),
        int(cancellations or 0),
        int(events or 0),
    )


async def replay_counts(
    sessions: async_sessionmaker[AsyncSession],
) -> tuple[int, int, int, int]:
    async with sessions() as session:
        values = []
        for table in (
            workflow_runs,
            workflow_run_inputs,
            task_runs,
            workflow_run_replays,
        ):
            value = await session.scalar(select(func.count()).select_from(table))
            values.append(int(value or 0))
    return values[0], values[1], values[2], values[3]


async def verify_terminal_sources_and_exact_version(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    service = WorkflowRunService(SQLAlchemyWorkflowRunRepository(sessions))
    try:
        owner_id, _, workflow_id, version_one_id, version_two_id = await seed_workflow(
            sessions
        )
        source_ids = {
            status: await create_source(
                service, sessions, workflow_id, owner_id, status
            )
            for status in WorkflowRunStatus
        }
        async with sessions.begin() as session:
            await session.execute(
                update(workflow_definitions)
                .where(workflow_definitions.c.id == workflow_id)
                .values(status="archived")
            )
        for status in (
            WorkflowRunStatus.SUCCEEDED,
            WorkflowRunStatus.FAILED,
            WorkflowRunStatus.CANCELLED,
        ):
            replay = await service.create_full_replay(
                source_ids[status],
                OwnerFilter.only(owner_id),
                requested_by_principal_id=owner_id,
            )
            assert replay.run.id != source_ids[status]
            assert replay.run.workflow_version_id == version_one_id
            assert replay.run.workflow_version_id != version_two_id
            assert replay.run.status is WorkflowRunStatus.PENDING
            assert replay.run.task_count == 2
            target_run, target_input, target_tasks, _, _, _ = await projection(
                sessions, replay.run.id
            )
            source_projection = await projection(sessions, source_ids[status])
            source_input, source_tasks = source_projection[1], source_projection[2]
            assert target_run.workflow_definition_id == workflow_id
            assert target_input.workflow_run_id == replay.run.id
            assert target_input.workflow_run_id != source_input.workflow_run_id
            assert target_input.payload == source_input.payload
            assert target_input.input_references == source_input.input_references
            assert [(row.step_identifier, row.status) for row in target_tasks] == [
                ("leaf", "blocked"),
                ("root", "runnable"),
            ]
            assert not {row.id for row in source_tasks}.intersection(
                row.id for row in target_tasks
            )
            async with sessions() as session:
                lineage = (
                    await session.execute(
                        select(workflow_run_replays).where(
                            workflow_run_replays.c.workflow_run_id == replay.run.id
                        )
                    )
                ).one()
                assert lineage.source_workflow_run_id == source_ids[status]
                assert lineage.mode == "full"
                assert lineage.requested_scope == {}
        for status in (
            WorkflowRunStatus.PENDING,
            WorkflowRunStatus.RUNNING,
            WorkflowRunStatus.CANCELLING,
        ):
            with pytest.raises(WorkflowRunReplayNotEligible):
                await service.create_full_replay(
                    source_ids[status],
                    OwnerFilter.only(owner_id),
                    requested_by_principal_id=owner_id,
                )
    finally:
        await engine.dispose()


async def verify_preservation_and_owner_visibility(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    service = WorkflowRunService(SQLAlchemyWorkflowRunRepository(sessions))
    try:
        owner_id, other_owner_id, workflow_id, _, _ = await seed_workflow(sessions)
        failed_source = await create_source(
            service, sessions, workflow_id, owner_id, WorkflowRunStatus.FAILED
        )
        cancelled_source = await create_source(
            service, sessions, workflow_id, owner_id, WorkflowRunStatus.CANCELLED
        )
        async with sessions.begin() as session:
            failed_task_id = await session.scalar(
                select(task_runs.c.id)
                .where(task_runs.c.workflow_run_id == failed_source)
                .order_by(task_runs.c.step_identifier)
                .limit(1)
            )
            assert failed_task_id is not None
            await session.execute(
                insert(task_attempts).values(
                    id=uuid4(), task_run_id=failed_task_id, attempt_number=1
                )
            )
            await session.execute(
                insert(workflow_run_cancellation_requests).values(
                    workflow_run_id=cancelled_source,
                    requested_by_principal_id=owner_id,
                    idempotency_key_digest="a" * 64,
                    request_fingerprint="b" * 64,
                    reason="source cancellation history",
                )
            )
        source_before = {
            failed_source: await projection(sessions, failed_source),
            cancelled_source: await projection(sessions, cancelled_source),
        }
        assert source_before[failed_source][3] == 1
        assert source_before[cancelled_source][4] == 1
        targets = []
        for source_id in source_before:
            replay = await service.create_full_replay(
                source_id,
                OwnerFilter.only(owner_id),
                requested_by_principal_id=owner_id,
            )
            targets.append(replay.run.id)
            _, target_input, _, attempts, cancellations, events = await projection(
                sessions, replay.run.id
            )
            assert attempts == cancellations == events == 0
            assert target_input.payload == source_before[source_id][1].payload
            assert (
                target_input.input_references
                == source_before[source_id][1].input_references
            )
            rendered = repr(replay)
            assert "ordinary" not in rendered
            assert "secret_ref" not in rendered
            async with sessions() as session:
                lineage = (
                    await session.execute(
                        select(workflow_run_replays).where(
                            workflow_run_replays.c.workflow_run_id == replay.run.id
                        )
                    )
                ).one()
                assert lineage.requested_scope == {}
                assert "secret_ref" not in str(lineage.requested_scope)
        assert {
            source_id: await projection(sessions, source_id)
            for source_id in source_before
        } == source_before
        with pytest.raises(WorkflowRunNotFound):
            await service.create_full_replay(
                failed_source,
                OwnerFilter.only(other_owner_id),
                requested_by_principal_id=other_owner_id,
            )
        unrestricted = await service.create_full_replay(
            failed_source,
            OwnerFilter.all_owners(),
            requested_by_principal_id=other_owner_id,
        )
        assert unrestricted.run.requested_by_principal_id == other_owner_id
        assert len(set(targets)) == 2
    finally:
        await engine.dispose()


async def verify_concurrent_requests(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    service = WorkflowRunService(SQLAlchemyWorkflowRunRepository(sessions))
    try:
        owner_id, _, workflow_id, version_one_id, _ = await seed_workflow(sessions)
        source_id = await create_source(
            service, sessions, workflow_id, owner_id, WorkflowRunStatus.FAILED
        )
        concurrent = await asyncio.gather(
            *(
                service.create_full_replay(
                    source_id,
                    OwnerFilter.only(owner_id),
                    requested_by_principal_id=owner_id,
                )
                for _ in range(3)
            )
        )
        assert len({item.run.id for item in concurrent}) == 3
        assert all(
            item.run.workflow_version_id == version_one_id for item in concurrent
        )
        source_task_ids = {row.id for row in (await projection(sessions, source_id))[2]}
        async with sessions() as session:
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(workflow_run_replays)
                    .where(
                        workflow_run_replays.c.workflow_run_id.in_(
                            item.run.id for item in concurrent
                        )
                    )
                )
                == 3
            )
        for item in concurrent:
            target_tasks = (await projection(sessions, item.run.id))[2]
            assert len(target_tasks) == 2
            assert not source_task_ids.intersection(row.id for row in target_tasks)
    finally:
        await engine.dispose()


async def verify_terminalization_serialization(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    service = WorkflowRunService(SQLAlchemyWorkflowRunRepository(sessions))
    try:
        owner_id, _, workflow_id, version_one_id, _ = await seed_workflow(sessions)
        source_id = await create_source(
            service, sessions, workflow_id, owner_id, WorkflowRunStatus.PENDING
        )
        blocker = sessions()
        await blocker.begin()
        try:
            await blocker.execute(
                update(workflow_runs)
                .where(workflow_runs.c.id == source_id)
                .values(status=WorkflowRunStatus.SUCCEEDED.value)
            )
            pending_replay = asyncio.create_task(
                service.create_full_replay(
                    source_id,
                    OwnerFilter.only(owner_id),
                    requested_by_principal_id=owner_id,
                )
            )
            await asyncio.sleep(0)
            assert not pending_replay.done()
            await blocker.commit()
            replay = await asyncio.wait_for(pending_replay, timeout=5)
        finally:
            await blocker.close()
        assert replay.run.workflow_version_id == version_one_id
    finally:
        await engine.dispose()


async def verify_atomic_rollback(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    service = WorkflowRunService(SQLAlchemyWorkflowRunRepository(sessions))
    try:
        owner_id, _, workflow_id, _, _ = await seed_workflow(sessions)
        source_id = await create_source(
            service, sessions, workflow_id, owner_id, WorkflowRunStatus.SUCCEEDED
        )
        before_failure = await replay_counts(sessions)
        async with sessions.begin() as session:
            await session.execute(
                text(
                    "CREATE FUNCTION reject_test_full_replay() RETURNS trigger "
                    "LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected'; END; $$"
                )
            )
            await session.execute(
                text(
                    "CREATE TRIGGER trg_reject_test_full_replay BEFORE INSERT ON "
                    "workflow_run_replays FOR EACH ROW EXECUTE FUNCTION "
                    "reject_test_full_replay()"
                )
            )
        try:
            with pytest.raises(WorkflowRunServiceUnavailable):
                await service.create_full_replay(
                    source_id,
                    OwnerFilter.only(owner_id),
                    requested_by_principal_id=owner_id,
                )
            assert await replay_counts(sessions) == before_failure
        finally:
            async with sessions.begin() as session:
                await session.execute(
                    text(
                        "DROP TRIGGER trg_reject_test_full_replay ON workflow_run_replays"
                    )
                )
                await session.execute(text("DROP FUNCTION reject_test_full_replay()"))
    finally:
        await engine.dispose()


def run_postgresql_case(database_prefix: str, verifier: Verifier) -> None:
    with temporary_database(
        "TASKFORGE_WORKFLOW_PERSISTENCE_TEST_DATABASE_URL", database_prefix
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        configuration = Config("alembic.ini")
        with migration_database_url(alembic_url):
            command.upgrade(configuration, "head")
        asyncio.run(verifier(database_url))


def test_full_replay_terminal_sources_use_exact_historical_version() -> None:
    run_postgresql_case(
        "taskforge_workflow_replay_mig", verify_terminal_sources_and_exact_version
    )


def test_full_replay_preserves_source_and_enforces_owner_visibility() -> None:
    run_postgresql_case(
        "taskforge_workflow_replay_mig", verify_preservation_and_owner_visibility
    )


def test_full_replay_concurrent_requests_are_independent() -> None:
    run_postgresql_case("taskforge_workflow_replay_mig", verify_concurrent_requests)


def test_full_replay_serializes_with_source_terminalization() -> None:
    run_postgresql_case(
        "taskforge_workflow_replay_mig", verify_terminalization_serialization
    )


def test_full_replay_rolls_back_atomically() -> None:
    run_postgresql_case("taskforge_workflow_replay_mig", verify_atomic_rollback)
