"""Opt-in atomic task dispatch creation verification against PostgreSQL."""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from uuid import UUID, uuid4

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import insert, select, text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.dispatch.service import (
    DispatchedTask,
    TaskDispatchNotEligible,
    TaskDispatchService,
    TaskDispatchServiceUnavailable,
)
from taskforge.identity.schema import api_principals
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.dispatch import SQLAlchemyTaskDispatchRepository
from taskforge.runs.schema import (
    task_attempts,
    task_dispatch_outbox,
    task_runs,
    workflow_run_execution_events,
    workflow_run_inputs,
    workflow_runs,
)
from taskforge.workflows.schema import (
    workflow_definitions,
    workflow_version_steps,
    workflow_versions,
)
from taskforge.workflows.task_types import (
    JSONMapping,
    TaskTypeDefinition,
    TaskTypeRegistry,
    WorkflowValidationIssue,
)
from tests.integration.postgresql import (
    ExpectedStatusExecutionEvent,
    assert_status_execution_events,
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


@dataclass(frozen=True)
class AcceptParameters:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        del parameters
        return ()


async def seed_runnable_task(
    sessions: async_sessionmaker[AsyncSession],
) -> tuple[UUID, UUID, UUID]:
    owner_id, workflow_id, version_id = uuid4(), uuid4(), uuid4()
    workflow_run_id, task_run_id = uuid4(), uuid4()
    async with sessions.begin() as session:
        await session.execute(
            insert(api_principals).values(id=owner_id, name=f"owner-{uuid4().hex}")
        )
        await session.execute(
            insert(workflow_definitions).values(
                id=workflow_id,
                owner_principal_id=owner_id,
                name="dispatch workflow",
                status="enabled",
            )
        )
        await session.execute(
            insert(workflow_versions).values(
                id=version_id,
                workflow_definition_id=workflow_id,
                version_number=1,
                name="one",
            )
        )
        await session.execute(
            insert(workflow_version_steps).values(
                workflow_version_id=version_id,
                step_identifier="extract",
                task_type="document.extract",
                parameters={"page": 2, "source": "version-step"},
            )
        )
        await session.execute(
            insert(workflow_runs).values(
                id=workflow_run_id,
                workflow_definition_id=workflow_id,
                workflow_version_id=version_id,
                requested_by_principal_id=owner_id,
                status="pending",
            )
        )
        await session.execute(
            insert(workflow_run_inputs).values(
                workflow_run_id=workflow_run_id,
                payload={"input_sentinel": "must-not-leak"},
                input_references={"reference_sentinel": "must-not-leak"},
            )
        )
        await session.execute(
            insert(task_runs).values(
                id=task_run_id,
                workflow_run_id=workflow_run_id,
                workflow_version_id=version_id,
                step_identifier="extract",
                status="runnable",
            )
        )
    return workflow_run_id, task_run_id, version_id


async def verify_dispatch_creation(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    registry = TaskTypeRegistry(
        (
            TaskTypeDefinition(
                "document.extract", "document-workers", AcceptParameters()
            ),
        )
    )
    service = TaskDispatchService(SQLAlchemyTaskDispatchRepository(sessions), registry)
    try:
        workflow_run_id, task_run_id, _ = await seed_runnable_task(sessions)

        with pytest.raises(TaskDispatchNotEligible):
            await service.dispatch_task(uuid4(), task_run_id)

        async def dispatch_once() -> DispatchedTask | TaskDispatchNotEligible:
            try:
                return await service.dispatch_task(workflow_run_id, task_run_id)
            except TaskDispatchNotEligible as error:
                return error

        outcomes = await asyncio.gather(dispatch_once(), dispatch_once())
        winners = [
            outcome for outcome in outcomes if not isinstance(outcome, Exception)
        ]
        losers = [outcome for outcome in outcomes if isinstance(outcome, Exception)]
        assert len(winners) == len(losers) == 1
        winner = winners[0]

        async with sessions() as session:
            attempts = (await session.execute(select(task_attempts))).all()
            outboxes = (await session.execute(select(task_dispatch_outbox))).all()
            task_status = await session.scalar(
                select(task_runs.c.status).where(task_runs.c.id == task_run_id)
            )

        assert len(attempts) == len(outboxes) == 1
        attempt, outbox = attempts[0], outboxes[0]
        assert attempt.id == winner.task_attempt_id
        assert attempt.task_run_id == task_run_id
        assert attempt.attempt_number == winner.attempt_number == 1
        assert outbox.id == winner.dispatch_id
        assert outbox.task_attempt_id == attempt.id
        assert outbox.route == "capability.document-workers"
        assert outbox.payload["task_payload"] == {
            "page": 2,
            "source": "version-step",
        }
        assert outbox.payload["references"] == {}
        assert "input_sentinel" not in str(outbox.payload)
        assert "reference_sentinel" not in str(outbox.payload)
        assert task_status == "dispatched"

        raw = await asyncpg.connect(asyncpg_dsn(database_url))
        try:
            await assert_status_execution_events(
                raw,
                workflow_run_id,
                (ExpectedStatusExecutionEvent(task_run_id, "runnable", "dispatched"),),
            )
        finally:
            await raw.close()

        with pytest.raises(TaskDispatchNotEligible):
            await service.dispatch_task(workflow_run_id, task_run_id)
        async with sessions() as session:
            assert len((await session.execute(select(task_attempts))).all()) == 1
            assert len((await session.execute(select(task_dispatch_outbox))).all()) == 1

        rollback_run_id, rollback_task_id, _ = await seed_runnable_task(sessions)
        async with sessions.begin() as session:
            await session.execute(
                text(
                    "CREATE FUNCTION reject_dispatch_execution_event() RETURNS "
                    "trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.task_run_id = "
                    f"'{rollback_task_id}'::uuid THEN RAISE EXCEPTION "
                    "'forced execution event failure'; END IF; RETURN NEW; END $$"
                )
            )
            await session.execute(
                text(
                    "CREATE TRIGGER reject_dispatch_execution_event_trigger "
                    "BEFORE INSERT ON workflow_run_execution_events FOR EACH ROW "
                    "EXECUTE FUNCTION reject_dispatch_execution_event()"
                )
            )
        with pytest.raises(TaskDispatchServiceUnavailable):
            await service.dispatch_task(rollback_run_id, rollback_task_id)
        async with sessions() as session:
            assert (
                await session.scalar(
                    select(task_runs.c.status).where(task_runs.c.id == rollback_task_id)
                )
                == "runnable"
            )
            assert not (
                await session.execute(
                    select(task_attempts.c.id).where(
                        task_attempts.c.task_run_id == rollback_task_id
                    )
                )
            ).all()
            assert not (
                await session.execute(
                    select(workflow_run_execution_events.c.id).where(
                        workflow_run_execution_events.c.workflow_run_id
                        == rollback_run_id
                    )
                )
            ).all()
            assert (
                await session.scalar(
                    select(workflow_runs.c.last_execution_event_cursor).where(
                        workflow_runs.c.id == rollback_run_id
                    )
                )
                == 0
            )
            assert not (
                await session.execute(
                    select(task_dispatch_outbox.c.id)
                    .select_from(
                        task_dispatch_outbox.join(
                            task_attempts,
                            task_attempts.c.id
                            == task_dispatch_outbox.c.task_attempt_id,
                        )
                    )
                    .where(task_attempts.c.task_run_id == rollback_task_id)
                )
            ).all()
        async with sessions.begin() as session:
            await session.execute(
                text(
                    "DROP TRIGGER reject_dispatch_execution_event_trigger ON "
                    "workflow_run_execution_events"
                )
            )
            await session.execute(
                text("DROP FUNCTION reject_dispatch_execution_event()")
            )
    finally:
        await engine.dispose()


def test_task_dispatch_creation_is_atomic_scoped_and_concurrent() -> None:
    with temporary_database(
        "TASKFORGE_WORKFLOW_PERSISTENCE_TEST_DATABASE_URL",
        "taskforge_task_dispatch",
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        configuration = Config("alembic.ini")
        with migration_database_url(alembic_url):
            command.upgrade(configuration, "head")
        asyncio.run(verify_dispatch_creation(database_url))
