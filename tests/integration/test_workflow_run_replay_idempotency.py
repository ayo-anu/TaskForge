"""Opt-in replay-scoped idempotency verification against PostgreSQL."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable, Coroutine
from typing import Any
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import func, insert, select, text
from sqlalchemy.engine import URL

from taskforge.identity.authorization import OwnerFilter
from taskforge.identity.schema import api_principals
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.runs import SQLAlchemyWorkflowRunRepository
from taskforge.runs.domain import (
    ExplicitWorkflowVersion,
    WorkflowReplayIdempotencyConflict,
    WorkflowReplayMode,
    create_workflow_replay_idempotency,
    create_workflow_run_input,
)
from taskforge.runs.schema import (
    task_runs,
    workflow_run_execution_events,
    workflow_run_idempotency,
    workflow_run_inputs,
    workflow_run_replays,
    workflow_runs,
)
from taskforge.runs.service import (
    WorkflowRunReplayInvariantError,
    WorkflowRunService,
    WorkflowRunServiceUnavailable,
)
from tests.integration.postgresql import migration_database_url, temporary_database
from tests.integration.test_authentication_persistence import settings_for
from tests.integration.test_workflow_run_dependency_failure_propagation import (
    seed_failure_graph,
)
from tests.integration.test_workflow_run_failed_subgraph_replay import (
    create_failure_source,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION=1 explicitly",
    ),
]

KEY = "replay-key-00001"
Verifier = Callable[[URL], Coroutine[Any, Any, None]]


async def counts(sessions: Any) -> tuple[int, int, int, int, int, int]:
    async with sessions() as session:
        values: list[int] = []
        for table in (
            workflow_runs,
            workflow_run_inputs,
            task_runs,
            workflow_run_replays,
            workflow_run_idempotency,
            workflow_run_execution_events,
        ):
            value = await session.scalar(select(func.count()).select_from(table))
            values.append(int(value or 0))
        return values[0], values[1], values[2], values[3], values[4], values[5]


async def seeded_failed_source(database_url: URL) -> tuple[Any, Any, Any, Any, Any]:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    service = WorkflowRunService(SQLAlchemyWorkflowRunRepository(sessions))
    owner, workflow, _ = await seed_failure_graph(sessions)
    source = await create_failure_source(service, sessions, workflow, owner)
    return engine, sessions, service, owner, source


async def verify_same_intent_and_conflicts(database_url: URL) -> None:
    engine, _sessions, service, owner, source = await seeded_failed_source(database_url)
    try:
        winning_correlation_id = uuid4()
        full = await service.create_idempotent_full_replay(
            source,
            OwnerFilter.only(owner),
            requested_by_principal_id=owner,
            idempotency_key=KEY,
            correlation_id=winning_correlation_id,
        )
        repeated = await service.create_idempotent_full_replay(
            source,
            OwnerFilter.only(owner),
            requested_by_principal_id=owner,
            idempotency_key=KEY,
            correlation_id=uuid4(),
        )
        assert repeated.run.id == full.run.id
        async with _sessions() as session:
            events = (
                await session.execute(
                    select(workflow_run_execution_events).where(
                        workflow_run_execution_events.c.workflow_run_id == full.run.id
                    )
                )
            ).all()
        assert len(events) == 1
        assert events[0].payload["correlation_id"] == str(winning_correlation_id)

        with pytest.raises(WorkflowReplayIdempotencyConflict):
            await service.create_idempotent_failed_subgraph_replay(
                source,
                OwnerFilter.only(owner),
                requested_by_principal_id=owner,
                failed_step_identifiers=["b"],
                idempotency_key=KEY,
                correlation_id=uuid4(),
            )

        failed = await service.create_idempotent_failed_subgraph_replay(
            source,
            OwnerFilter.only(owner),
            requested_by_principal_id=owner,
            failed_step_identifiers=["right", "b", "left"],
            idempotency_key="failed-replay-key",
            correlation_id=uuid4(),
        )
        reordered = await service.create_idempotent_failed_subgraph_replay(
            source,
            OwnerFilter.only(owner),
            requested_by_principal_id=owner,
            failed_step_identifiers=["left", "right", "b"],
            idempotency_key="failed-replay-key",
            correlation_id=uuid4(),
        )
        assert reordered.run.id == failed.run.id
        assert reordered.selected_step_identifiers == (
            "b",
            "c",
            "left",
            "right",
            "join",
        )
        with pytest.raises(WorkflowReplayIdempotencyConflict):
            await service.create_idempotent_failed_subgraph_replay(
                source,
                OwnerFilter.only(owner),
                requested_by_principal_id=owner,
                failed_step_identifiers=["b"],
                idempotency_key="failed-replay-key",
                correlation_id=uuid4(),
            )
    finally:
        await engine.dispose()


async def verify_concurrency_and_scope_isolation(database_url: URL) -> None:
    engine, sessions, service, owner, source = await seeded_failed_source(database_url)
    try:
        identical = await asyncio.gather(
            *(
                service.create_idempotent_full_replay(
                    source,
                    OwnerFilter.only(owner),
                    requested_by_principal_id=owner,
                    idempotency_key="concurrent-replay",
                    correlation_id=uuid4(),
                )
                for _ in range(2)
            )
        )
        assert identical[0].run.id == identical[1].run.id
        async with sessions() as session:
            lineage_count = await session.scalar(
                select(func.count())
                .select_from(workflow_run_replays)
                .where(workflow_run_replays.c.workflow_run_id == identical[0].run.id)
            )
            task_count = await session.scalar(
                select(func.count())
                .select_from(task_runs)
                .where(task_runs.c.workflow_run_id == identical[0].run.id)
            )
            event_count = await session.scalar(
                select(func.count())
                .select_from(workflow_run_execution_events)
                .where(
                    workflow_run_execution_events.c.workflow_run_id
                    == identical[0].run.id
                )
            )
        assert lineage_count == 1
        assert task_count == 7
        assert event_count == 1

        async with sessions() as session:
            workflow_id = await session.scalar(
                select(workflow_runs.c.workflow_definition_id).where(
                    workflow_runs.c.id == source
                )
            )
        assert workflow_id is not None
        other_source = await create_failure_source(
            service, sessions, workflow_id, owner
        )
        other_source_replay = await service.create_idempotent_full_replay(
            other_source,
            OwnerFilter.only(owner),
            requested_by_principal_id=owner,
            idempotency_key="concurrent-replay",
            correlation_id=uuid4(),
        )
        assert other_source_replay.run.id != identical[0].run.id

        identical_failed = await asyncio.gather(
            *(
                service.create_idempotent_failed_subgraph_replay(
                    source,
                    OwnerFilter.only(owner),
                    requested_by_principal_id=owner,
                    failed_step_identifiers=["right", "b", "left"],
                    idempotency_key="concurrent-failed",
                    correlation_id=uuid4(),
                )
                for _ in range(2)
            )
        )
        assert identical_failed[0].run.id == identical_failed[1].run.id
        async with sessions() as session:
            failed_event_count = await session.scalar(
                select(func.count())
                .select_from(workflow_run_execution_events)
                .where(
                    workflow_run_execution_events.c.workflow_run_id
                    == identical_failed[0].run.id
                )
            )
        assert failed_event_count == 1

        before_conflict = await counts(sessions)
        outcomes = await asyncio.gather(
            service.create_idempotent_full_replay(
                source,
                OwnerFilter.only(owner),
                requested_by_principal_id=owner,
                idempotency_key="conflicting-key-1",
                correlation_id=uuid4(),
            ),
            service.create_idempotent_failed_subgraph_replay(
                source,
                OwnerFilter.only(owner),
                requested_by_principal_id=owner,
                failed_step_identifiers=["b", "left", "right"],
                idempotency_key="conflicting-key-1",
                correlation_id=uuid4(),
            ),
            return_exceptions=True,
        )
        assert (
            sum(
                isinstance(value, WorkflowReplayIdempotencyConflict)
                for value in outcomes
            )
            == 1
        )
        winners = [value for value in outcomes if not isinstance(value, BaseException)]
        assert len(winners) == 1
        winner = winners[0]
        after_conflict = await counts(sessions)
        assert tuple(
            after - before
            for before, after in zip(before_conflict, after_conflict, strict=True)
        ) == (1, 1, 7, 1, 1, 1)
        async with sessions() as session:
            winner_lineage_count = await session.scalar(
                select(func.count())
                .select_from(workflow_run_replays)
                .where(workflow_run_replays.c.workflow_run_id == winner.run.id)
            )
            winner_idempotency_count = await session.scalar(
                select(func.count())
                .select_from(workflow_run_idempotency)
                .where(workflow_run_idempotency.c.workflow_run_id == winner.run.id)
            )
            winner_event_count = await session.scalar(
                select(func.count())
                .select_from(workflow_run_execution_events)
                .where(
                    workflow_run_execution_events.c.workflow_run_id == winner.run.id,
                    workflow_run_execution_events.c.event_type
                    == "workflow_run.replay_created",
                )
            )
        assert winner_lineage_count == 1
        assert winner_idempotency_count == 1
        assert winner_event_count == 1

        other_principal = uuid4()
        async with sessions.begin() as session:
            await session.execute(
                insert(api_principals).values(id=other_principal, name="replay-admin")
            )
        independent = await service.create_idempotent_full_replay(
            source,
            OwnerFilter.all_owners(),
            requested_by_principal_id=other_principal,
            idempotency_key="concurrent-replay",
            correlation_id=uuid4(),
        )
        assert independent.run.id != identical[0].run.id
    finally:
        await engine.dispose()


async def verify_corrupt_lineage_is_rejected(database_url: URL) -> None:
    engine, sessions, service, owner, source = await seeded_failed_source(database_url)
    try:
        async with sessions() as session:
            workflow_id = await session.scalar(
                select(workflow_runs.c.workflow_definition_id).where(
                    workflow_runs.c.id == source
                )
            )
        assert workflow_id is not None

        async def attach_idempotency(
            target_id: Any,
            key: str,
            mode: WorkflowReplayMode,
            scope: dict[str, object],
        ) -> None:
            expected = create_workflow_replay_idempotency(
                key,
                source_workflow_run_id=source,
                requested_by_principal_id=owner,
                mode=mode,
                requested_scope=scope,
            )
            async with sessions.begin() as session:
                await session.execute(
                    insert(workflow_run_idempotency).values(
                        principal_id=owner,
                        workflow_definition_id=workflow_id,
                        idempotency_key_digest=expected.key_digest,
                        request_fingerprint=expected.request_fingerprint,
                        workflow_run_id=target_id,
                    )
                )

        await attach_idempotency(
            source, "missing-lineage-1", WorkflowReplayMode.FULL, {}
        )
        with pytest.raises(WorkflowRunReplayInvariantError):
            await service.create_idempotent_full_replay(
                source,
                OwnerFilter.only(owner),
                requested_by_principal_id=owner,
                idempotency_key="missing-lineage-1",
                correlation_id=uuid4(),
            )

        other_source = await create_failure_source(
            service, sessions, workflow_id, owner
        )
        wrong_source = await service.create_full_replay(
            other_source,
            OwnerFilter.only(owner),
            requested_by_principal_id=owner,
            correlation_id=uuid4(),
        )
        await attach_idempotency(
            wrong_source.run.id, "wrong-source-key", WorkflowReplayMode.FULL, {}
        )
        with pytest.raises(WorkflowRunReplayInvariantError):
            await service.create_idempotent_full_replay(
                source,
                OwnerFilter.only(owner),
                requested_by_principal_id=owner,
                idempotency_key="wrong-source-key",
                correlation_id=uuid4(),
            )

        wrong_mode = await service.create_full_replay(
            source,
            OwnerFilter.only(owner),
            requested_by_principal_id=owner,
            correlation_id=uuid4(),
        )
        await attach_idempotency(
            wrong_mode.run.id,
            "wrong-mode-key-1",
            WorkflowReplayMode.FAILED_SUBGRAPH,
            {"failed_step_identifiers": ["b"]},
        )
        with pytest.raises(WorkflowRunReplayInvariantError):
            await service.create_idempotent_failed_subgraph_replay(
                source,
                OwnerFilter.only(owner),
                requested_by_principal_id=owner,
                failed_step_identifiers=["b"],
                idempotency_key="wrong-mode-key-1",
                correlation_id=uuid4(),
            )

        wrong_scope = await service.create_failed_subgraph_replay(
            source,
            OwnerFilter.only(owner),
            requested_by_principal_id=owner,
            failed_step_identifiers=["b", "left", "right"],
            correlation_id=uuid4(),
        )
        await attach_idempotency(
            wrong_scope.run.id,
            "wrong-scope-key1",
            WorkflowReplayMode.FAILED_SUBGRAPH,
            {"failed_step_identifiers": ["b"]},
        )
        with pytest.raises(WorkflowRunReplayInvariantError):
            await service.create_idempotent_failed_subgraph_replay(
                source,
                OwnerFilter.only(owner),
                requested_by_principal_id=owner,
                failed_step_identifiers=["b"],
                idempotency_key="wrong-scope-key1",
                correlation_id=uuid4(),
            )

        malformed = await service.create_run(
            workflow_id,
            owner_principal_id=owner,
            requested_by_principal_id=owner,
            selection=ExplicitWorkflowVersion(1),
            input_snapshot=create_workflow_run_input({}, {}),
        )
        malformed_key = "malformed-scope1"
        expected = create_workflow_replay_idempotency(
            malformed_key,
            source_workflow_run_id=source,
            requested_by_principal_id=owner,
            mode=WorkflowReplayMode.FAILED_SUBGRAPH,
            requested_scope={"failed_step_identifiers": ["b"]},
        )
        async with sessions.begin() as session:
            await session.execute(
                insert(workflow_run_replays).values(
                    workflow_run_id=malformed.id,
                    source_workflow_run_id=source,
                    mode="failed_subgraph",
                    requested_scope={"failed_step_identifiers": "b"},
                )
            )
            await session.execute(
                insert(workflow_run_idempotency).values(
                    principal_id=owner,
                    workflow_definition_id=workflow_id,
                    idempotency_key_digest=expected.key_digest,
                    request_fingerprint=expected.request_fingerprint,
                    workflow_run_id=malformed.id,
                )
            )
        with pytest.raises(WorkflowRunReplayInvariantError):
            await service.create_idempotent_failed_subgraph_replay(
                source,
                OwnerFilter.only(owner),
                requested_by_principal_id=owner,
                failed_step_identifiers=["b"],
                idempotency_key=malformed_key,
                correlation_id=uuid4(),
            )
    finally:
        await engine.dispose()


async def verify_idempotency_failure_rolls_back(database_url: URL) -> None:
    engine, sessions, service, owner, source = await seeded_failed_source(database_url)
    try:
        before = await counts(sessions)
        async with sessions.begin() as session:
            await session.execute(
                text(
                    "CREATE FUNCTION reject_replay_idempotency() RETURNS trigger "
                    "LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'reject'; END $$"
                )
            )
            await session.execute(
                text(
                    "CREATE TRIGGER trg_reject_replay_idempotency BEFORE INSERT ON "
                    "workflow_run_idempotency FOR EACH ROW EXECUTE FUNCTION "
                    "reject_replay_idempotency()"
                )
            )
        with pytest.raises(WorkflowRunServiceUnavailable):
            await service.create_idempotent_full_replay(
                source,
                OwnerFilter.only(owner),
                requested_by_principal_id=owner,
                idempotency_key=KEY,
                correlation_id=uuid4(),
            )
        assert await counts(sessions) == before
    finally:
        await engine.dispose()


async def verify_keyed_event_failure_rolls_back(database_url: URL) -> None:
    engine, sessions, service, owner, source = await seeded_failed_source(database_url)
    key = "event-failure-replay-key"
    try:
        before = await counts(sessions)
        async with sessions.begin() as session:
            await session.execute(
                text(
                    "CREATE FUNCTION reject_keyed_replay_event() RETURNS trigger "
                    "LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'reject event'; "
                    "END $$"
                )
            )
            await session.execute(
                text(
                    "CREATE TRIGGER trg_reject_keyed_replay_event BEFORE INSERT ON "
                    "workflow_run_execution_events FOR EACH ROW WHEN "
                    "(NEW.event_type = 'workflow_run.replay_created') EXECUTE "
                    "FUNCTION reject_keyed_replay_event()"
                )
            )
        try:
            with pytest.raises(WorkflowRunServiceUnavailable):
                await service.create_idempotent_full_replay(
                    source,
                    OwnerFilter.only(owner),
                    requested_by_principal_id=owner,
                    idempotency_key=key,
                    correlation_id=uuid4(),
                )
            assert await counts(sessions) == before
        finally:
            async with sessions.begin() as session:
                await session.execute(
                    text(
                        "DROP TRIGGER trg_reject_keyed_replay_event ON "
                        "workflow_run_execution_events"
                    )
                )
                await session.execute(text("DROP FUNCTION reject_keyed_replay_event()"))

        replay = await service.create_idempotent_full_replay(
            source,
            OwnerFilter.only(owner),
            requested_by_principal_id=owner,
            idempotency_key=key,
            correlation_id=uuid4(),
        )
        after_retry = await counts(sessions)
        assert tuple(
            after - before_count
            for before_count, after in zip(before, after_retry, strict=True)
        ) == (1, 1, 7, 1, 1, 1)
        async with sessions() as session:
            event_count = await session.scalar(
                select(func.count())
                .select_from(workflow_run_execution_events)
                .where(
                    workflow_run_execution_events.c.workflow_run_id == replay.run.id,
                    workflow_run_execution_events.c.event_type
                    == "workflow_run.replay_created",
                )
            )
        assert event_count == 1
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


def test_replay_idempotency_same_intent_and_conflicts() -> None:
    run_case(verify_same_intent_and_conflicts)


def test_replay_idempotency_concurrency_and_scope_isolation() -> None:
    run_case(verify_concurrency_and_scope_isolation)


def test_replay_idempotency_rejects_corrupt_lineage() -> None:
    run_case(verify_corrupt_lineage_is_rejected)


def test_replay_idempotency_rolls_back_atomically() -> None:
    run_case(verify_idempotency_failure_rolls_back)


def test_keyed_replay_event_failure_rolls_back_everything() -> None:
    run_case(verify_keyed_event_failure_rolls_back)
