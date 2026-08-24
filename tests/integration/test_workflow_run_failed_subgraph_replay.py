"""Opt-in failed-subgraph workflow replay verification against PostgreSQL."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable, Coroutine
from typing import Any
from uuid import UUID

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
    FailedSubgraphReplaySelectionInvalid,
    WorkflowRunReplayNotEligible,
    WorkflowRunStatus,
    create_workflow_run_input,
)
from taskforge.runs.schema import (
    task_attempt_claims,
    task_attempt_results,
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
    WorkflowRunReplayInvariantError,
    WorkflowRunService,
    WorkflowRunServiceUnavailable,
)
from taskforge.workflows.schema import (
    workflow_definitions,
    workflow_version_steps,
    workflow_versions,
)
from tests.integration.postgresql import migration_database_url, temporary_database
from tests.integration.test_authentication_persistence import settings_for
from tests.integration.test_workflow_run_creation import seed_workflow
from tests.integration.test_workflow_run_dependency_failure_propagation import (
    seed_failure_graph,
    set_statuses,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION=1 explicitly",
    ),
]

Verifier = Callable[[URL], Coroutine[Any, Any, None]]


async def create_failure_source(
    service: WorkflowRunService,
    sessions: async_sessionmaker[AsyncSession],
    workflow_id: UUID,
    owner_id: UUID,
    *,
    status: WorkflowRunStatus = WorkflowRunStatus.FAILED,
) -> UUID:
    created = await service.create_run(
        workflow_id,
        owner_principal_id=owner_id,
        requested_by_principal_id=owner_id,
        selection=ExplicitWorkflowVersion(1),
        input_snapshot=create_workflow_run_input(
            {"ordinary": {"batch": 7}},
            {"database_password": {"secret_ref": "vault://database"}},
        ),
    )
    await set_statuses(
        sessions,
        created.id,
        a="succeeded",
        b="failed",
        c="skipped",
        independent="succeeded",
        left="failed",
        right="failed",
        join="skipped",
    )
    async with sessions.begin() as session:
        await session.execute(
            update(workflow_runs)
            .where(workflow_runs.c.id == created.id)
            .values(status=status.value)
        )
    return created.id


async def task_projection(
    sessions: async_sessionmaker[AsyncSession], run_id: UUID
) -> tuple[Any, ...]:
    async with sessions() as session:
        return tuple(
            (
                await session.execute(
                    select(
                        task_runs.c.id,
                        task_runs.c.step_identifier,
                        task_runs.c.status,
                    )
                    .where(task_runs.c.workflow_run_id == run_id)
                    .order_by(task_runs.c.step_identifier)
                )
            ).all()
        )


async def execution_counts(
    sessions: async_sessionmaker[AsyncSession], run_id: UUID
) -> tuple[int, ...]:
    async with sessions() as session:
        attempts = await session.scalar(
            select(func.count())
            .select_from(task_attempts.join(task_runs))
            .where(task_runs.c.workflow_run_id == run_id)
        )
        results = await session.scalar(
            select(func.count())
            .select_from(task_attempt_results.join(task_attempts).join(task_runs))
            .where(task_runs.c.workflow_run_id == run_id)
        )
        claims = await session.scalar(
            select(func.count())
            .select_from(task_attempt_claims.join(task_attempts).join(task_runs))
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
    return tuple(
        int(value or 0) for value in (attempts, results, claims, cancellations, events)
    )


async def replay_row_counts(
    sessions: async_sessionmaker[AsyncSession],
) -> tuple[int, int, int, int]:
    values: list[int] = []
    async with sessions() as session:
        for table in (
            workflow_runs,
            workflow_run_inputs,
            task_runs,
            workflow_run_replays,
        ):
            value = await session.scalar(select(func.count()).select_from(table))
            values.append(int(value or 0))
    return values[0], values[1], values[2], values[3]


async def verify_graph_and_exact_version(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    service = WorkflowRunService(SQLAlchemyWorkflowRunRepository(sessions))
    try:
        owner_id, workflow_id, version_id = await seed_failure_graph(sessions)
        source_id = await create_failure_source(
            service, sessions, workflow_id, owner_id
        )
        source_before = await task_projection(sessions, source_id)
        async with sessions.begin() as session:
            newer_version_id = UUID(int=1)
            await session.execute(
                insert(workflow_versions).values(
                    id=newer_version_id,
                    workflow_definition_id=workflow_id,
                    version_number=2,
                    name="newer unrelated graph",
                )
            )
            await session.execute(
                insert(workflow_version_steps).values(
                    workflow_version_id=newer_version_id,
                    step_identifier="new_step",
                    task_type="test.task",
                    parameters={},
                )
            )
            await session.execute(
                update(workflow_definitions)
                .where(workflow_definitions.c.id == workflow_id)
                .values(status="archived")
            )
        replay = await service.create_failed_subgraph_replay(
            source_id,
            OwnerFilter.only(owner_id),
            requested_by_principal_id=owner_id,
            failed_step_identifiers=("right", "b", "left"),
        )
        assert replay.run.workflow_version_id == version_id
        assert replay.run.workflow_version_id != newer_version_id
        assert replay.canonical_failed_step_identifiers == ("b", "left", "right")
        assert replay.selected_step_identifiers == (
            "b",
            "c",
            "left",
            "right",
            "join",
        )
        target = await task_projection(sessions, replay.run.id)
        assert [(row.step_identifier, row.status) for row in target] == [
            ("a", "succeeded"),
            ("b", "runnable"),
            ("c", "blocked"),
            ("independent", "succeeded"),
            ("join", "blocked"),
            ("left", "runnable"),
            ("right", "runnable"),
        ]
        assert not {row.id for row in source_before}.intersection(
            row.id for row in target
        )
        assert await execution_counts(sessions, replay.run.id) == (0, 0, 0, 0, 0)
        assert await task_projection(sessions, source_id) == source_before
        async with sessions() as session:
            lineage = (
                await session.execute(
                    select(workflow_run_replays).where(
                        workflow_run_replays.c.workflow_run_id == replay.run.id
                    )
                )
            ).one()
            source_input = (
                await session.execute(
                    select(workflow_run_inputs).where(
                        workflow_run_inputs.c.workflow_run_id == source_id
                    )
                )
            ).one()
            target_input = (
                await session.execute(
                    select(workflow_run_inputs).where(
                        workflow_run_inputs.c.workflow_run_id == replay.run.id
                    )
                )
            ).one()
        assert lineage.mode == "failed_subgraph"
        assert lineage.source_workflow_run_id == source_id
        assert lineage.requested_scope == {
            "failed_step_identifiers": ["b", "left", "right"]
        }
        assert source_input.workflow_run_id != target_input.workflow_run_id
        assert source_input.payload == target_input.payload
        assert source_input.input_references == target_input.input_references
        rendered = repr(replay)
        assert "ordinary" not in rendered and "vault://database" not in rendered
    finally:
        await engine.dispose()


async def verify_eligibility_integrity_and_authorization(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    service = WorkflowRunService(SQLAlchemyWorkflowRunRepository(sessions))
    try:
        owner_id, other_id, simple_id, _, _ = await seed_workflow(sessions)
        simple = await service.create_run(
            simple_id,
            owner_principal_id=owner_id,
            requested_by_principal_id=owner_id,
            selection=ExplicitWorkflowVersion(1),
            input_snapshot=create_workflow_run_input({}, {}),
        )
        await set_statuses(sessions, simple.id, root="succeeded", leaf="failed")
        async with sessions.begin() as session:
            await session.execute(
                update(workflow_runs)
                .where(workflow_runs.c.id == simple.id)
                .values(status="failed")
            )
        accepted = await service.create_failed_subgraph_replay(
            simple.id,
            OwnerFilter.only(owner_id),
            requested_by_principal_id=owner_id,
            failed_step_identifiers=("leaf",),
        )
        assert accepted.selected_step_identifiers == ("leaf",)
        with pytest.raises(WorkflowRunNotFound):
            await service.create_failed_subgraph_replay(
                simple.id,
                OwnerFilter.only(other_id),
                requested_by_principal_id=other_id,
                failed_step_identifiers=("leaf",),
            )
        unrestricted = await service.create_failed_subgraph_replay(
            simple.id,
            OwnerFilter.all_owners(),
            requested_by_principal_id=other_id,
            failed_step_identifiers=("leaf",),
        )
        assert unrestricted.run.requested_by_principal_id == other_id

        await set_statuses(sessions, simple.id, root="failed", leaf="failed")
        with pytest.raises(WorkflowRunReplayInvariantError):
            await service.create_failed_subgraph_replay(
                simple.id,
                OwnerFilter.only(owner_id),
                requested_by_principal_id=owner_id,
                failed_step_identifiers=("root",),
            )

        owner, workflow_id, _ = await seed_failure_graph(sessions)
        cancelled = await create_failure_source(
            service,
            sessions,
            workflow_id,
            owner,
            status=WorkflowRunStatus.CANCELLED,
        )
        valid_cancelled = await service.create_failed_subgraph_replay(
            cancelled,
            OwnerFilter.only(owner),
            requested_by_principal_id=owner,
            failed_step_identifiers=("b", "left", "right"),
        )
        assert valid_cancelled.run.task_count == 7
        await set_statuses(sessions, cancelled, independent="cancelled")
        with pytest.raises(FailedSubgraphReplaySelectionInvalid):
            await service.create_failed_subgraph_replay(
                cancelled,
                OwnerFilter.only(owner),
                requested_by_principal_id=owner,
                failed_step_identifiers=("b", "left", "right"),
            )

        for status in (
            WorkflowRunStatus.PENDING,
            WorkflowRunStatus.RUNNING,
            WorkflowRunStatus.CANCELLING,
            WorkflowRunStatus.SUCCEEDED,
        ):
            if status is WorkflowRunStatus.SUCCEEDED:
                await set_statuses(
                    sessions, simple.id, root="succeeded", leaf="succeeded"
                )
            async with sessions.begin() as session:
                await session.execute(
                    update(workflow_runs)
                    .where(workflow_runs.c.id == simple.id)
                    .values(status=status.value)
                )
            with pytest.raises(WorkflowRunReplayNotEligible):
                await service.create_failed_subgraph_replay(
                    simple.id,
                    OwnerFilter.only(owner_id),
                    requested_by_principal_id=owner_id,
                    failed_step_identifiers=("leaf",),
                )
    finally:
        await engine.dispose()


async def verify_concurrency_and_terminalization(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    service = WorkflowRunService(SQLAlchemyWorkflowRunRepository(sessions))
    try:
        owner_id, _, workflow_id, _, _ = await seed_workflow(sessions)
        source = await service.create_run(
            workflow_id,
            owner_principal_id=owner_id,
            requested_by_principal_id=owner_id,
            selection=ExplicitWorkflowVersion(1),
            input_snapshot=create_workflow_run_input({}, {}),
        )
        blocker = sessions()
        await blocker.begin()
        try:
            await blocker.execute(
                update(task_runs)
                .where(
                    task_runs.c.workflow_run_id == source.id,
                    task_runs.c.step_identifier == "root",
                )
                .values(status="succeeded")
            )
            await blocker.execute(
                update(task_runs)
                .where(
                    task_runs.c.workflow_run_id == source.id,
                    task_runs.c.step_identifier == "leaf",
                )
                .values(status="failed")
            )
            await blocker.execute(
                update(workflow_runs)
                .where(workflow_runs.c.id == source.id)
                .values(status="failed")
            )
            waiting = asyncio.create_task(
                service.create_failed_subgraph_replay(
                    source.id,
                    OwnerFilter.only(owner_id),
                    requested_by_principal_id=owner_id,
                    failed_step_identifiers=("leaf",),
                )
            )
            await asyncio.sleep(0)
            assert not waiting.done()
            await blocker.commit()
            first = await asyncio.wait_for(waiting, 5)
        finally:
            await blocker.close()
        concurrent = await asyncio.gather(
            *(
                service.create_failed_subgraph_replay(
                    source.id,
                    OwnerFilter.only(owner_id),
                    requested_by_principal_id=owner_id,
                    failed_step_identifiers=("leaf",),
                )
                for _ in range(3)
            )
        )
        assert len({first.run.id, *(item.run.id for item in concurrent)}) == 4
        assert all(item.run.task_count == 2 for item in concurrent)
    finally:
        await engine.dispose()


async def verify_atomic_rollback(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    service = WorkflowRunService(SQLAlchemyWorkflowRunRepository(sessions))
    try:
        owner_id, _, workflow_id, _, _ = await seed_workflow(sessions)
        source = await service.create_run(
            workflow_id,
            owner_principal_id=owner_id,
            requested_by_principal_id=owner_id,
            selection=ExplicitWorkflowVersion(1),
            input_snapshot=create_workflow_run_input({}, {}),
        )
        await set_statuses(sessions, source.id, root="succeeded", leaf="failed")
        async with sessions.begin() as session:
            await session.execute(
                update(workflow_runs)
                .where(workflow_runs.c.id == source.id)
                .values(status="failed")
            )
        before = await replay_row_counts(sessions)
        async with sessions.begin() as session:
            await session.execute(
                text(
                    "CREATE FUNCTION reject_failed_replay() RETURNS trigger "
                    "LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected'; END; $$"
                )
            )
            await session.execute(
                text(
                    "CREATE TRIGGER trg_reject_failed_replay BEFORE INSERT ON "
                    "workflow_run_replays FOR EACH ROW EXECUTE FUNCTION reject_failed_replay()"
                )
            )
        try:
            with pytest.raises(WorkflowRunServiceUnavailable):
                await service.create_failed_subgraph_replay(
                    source.id,
                    OwnerFilter.only(owner_id),
                    requested_by_principal_id=owner_id,
                    failed_step_identifiers=("leaf",),
                )
            after = await replay_row_counts(sessions)
            assert after == before
        finally:
            async with sessions.begin() as session:
                await session.execute(
                    text(
                        "DROP TRIGGER trg_reject_failed_replay ON workflow_run_replays"
                    )
                )
                await session.execute(text("DROP FUNCTION reject_failed_replay()"))
    finally:
        await engine.dispose()


def run_case(verifier: Verifier) -> None:
    with temporary_database(
        "TASKFORGE_WORKFLOW_PERSISTENCE_TEST_DATABASE_URL",
        "taskforge_workflow_replay_mig",
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        configuration = Config("alembic.ini")
        with migration_database_url(alembic_url):
            command.upgrade(configuration, "head")
        asyncio.run(verifier(database_url))


def test_failed_subgraph_graph_exact_version_and_history() -> None:
    run_case(verify_graph_and_exact_version)


def test_failed_subgraph_eligibility_integrity_and_authorization() -> None:
    run_case(verify_eligibility_integrity_and_authorization)


def test_failed_subgraph_concurrency_and_terminalization() -> None:
    run_case(verify_concurrency_and_terminalization)


def test_failed_subgraph_atomic_rollback() -> None:
    run_case(verify_atomic_rollback)
