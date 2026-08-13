"""Real PostgreSQL worker claim/start/replay verification."""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from taskforge.claims.authority import TaskClaimResultAuthorityIssuer
from taskforge.claims.domain import TaskClaimOutcome
from taskforge.claims.service import TaskClaimService
from taskforge.dispatch.envelope import serialize_dispatch_envelope
from taskforge.dispatch.transport import DispatchTransportMetadata
from taskforge.persistence.claims import SQLAlchemyTaskClaimRepository
from taskforge.persistence.database import build_session_factory
from taskforge.persistence.task_start import SQLAlchemyTaskStartRepository
from taskforge.worker.consumer_ports import BrokerDispatchDelivery
from taskforge.worker.execution import WorkerExecutionConsumer
from taskforge.worker.handlers import (
    TaskContext,
    TaskHandlerDefinition,
    TaskHandlerRegistry,
)
from taskforge.worker.start import TaskStartOutcome, TaskStartRequest, TaskStartService
from taskforge.workflows.task_types import (
    JSONMapping,
    TaskTypeDefinition,
    TaskTypeRegistry,
    WorkflowValidationIssue,
)
from tests.integration.postgresql import (
    asyncpg_dsn,
    migration_database_url,
    temporary_database,
)
from tests.integration.test_task_claim_acquisition import (
    add_dispatched_task,
    add_worker,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_CLAIM_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_CLAIM_INTEGRATION=1 explicitly",
    ),
]


@dataclass(frozen=True)
class AcceptParameters:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        del parameters
        return ()


class Control:
    def __init__(self, body: bytes, metadata: DispatchTransportMetadata) -> None:
        self._delivery = BrokerDispatchDelivery(body, metadata, True)
        self.disposed = False

    @property
    def delivery(self) -> BrokerDispatchDelivery:
        return self._delivery

    async def acknowledge(self) -> None:
        self.disposed = True

    async def reject(self, *, requeue: bool) -> None:
        del requeue
        self.disposed = True


async def exercise(database_url: URL) -> None:
    setup = await asyncpg.connect(asyncpg_dsn(database_url))
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
    )
    sessions = build_session_factory(engine)
    claim_service = TaskClaimService(
        SQLAlchemyTaskClaimRepository(sessions, worker_stale_after_seconds=30),
        TaskClaimResultAuthorityIssuer(b"a" * 32),
        lease_seconds=60,
    )
    start_service = TaskStartService(SQLAlchemyTaskStartRepository(sessions))
    try:
        worker = await add_worker(setup)
        dispatch = await add_dispatched_task(setup)
        issued = await claim_service.claim_task(
            worker.authenticated, worker.session_id, dispatch
        )
        request = TaskStartRequest(
            dispatch.task_run_id, dispatch.task_attempt_id, issued.claim.generation
        )

        first, second = await asyncio.gather(
            start_service.start_task(worker.authenticated, worker.session_id, request),
            start_service.start_task(worker.authenticated, worker.session_id, request),
        )
        assert {first.outcome, second.outcome} == {
            TaskStartOutcome.STARTED,
            TaskStartOutcome.REPLAYED_RUNNING,
        }
        assert (
            await setup.fetchval(
                "SELECT status::text FROM task_runs WHERE id = $1",
                dispatch.task_run_id,
            )
            == "running"
        )
        replay = await claim_service.claim_task(
            worker.authenticated, worker.session_id, dispatch
        )
        assert replay.outcome is TaskClaimOutcome.REPLAYED_ACTIVE
        assert replay.claim.generation == issued.claim.generation

        events: list[str] = []

        async def handler(invocation: TaskContext) -> object:
            assert invocation.task_attempt_id == dispatch.task_attempt_id
            # This write would block if the start transaction retained the task row lock.
            await asyncio.wait_for(
                setup.execute(
                    "UPDATE task_runs SET updated_at = updated_at WHERE id = $1",
                    dispatch.task_run_id,
                ),
                timeout=2,
            )
            events.append("handler")
            return None

        task_types = TaskTypeRegistry(
            (TaskTypeDefinition("test.task", "test-capability", AcceptParameters()),)
        )
        handlers = TaskHandlerRegistry(
            (TaskHandlerDefinition("test.task", "test-capability", handler),),
            task_types,
        )
        control = Control(
            serialize_dispatch_envelope(dispatch),
            DispatchTransportMetadata(
                str(dispatch.dispatch_id),
                dispatch.route,
                "application/json",
                "utf-8",
            ),
        )
        consumer = WorkerExecutionConsumer(
            claim_service,
            start_service,
            handlers,
            worker.authenticated,
            worker.session_id,
        )
        await consumer.consume(control)
        assert events == ["handler"]
        assert not control.disposed
    finally:
        await setup.close()
        await engine.dispose()


def test_real_postgresql_task_start_is_guarded_replayable_and_committed() -> None:
    with temporary_database(
        "TASKFORGE_CLAIM_TEST_DATABASE_URL", "taskforge_claim_acquisition"
    ) as database_url:
        configuration = Config("alembic.ini")
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        with migration_database_url(alembic_url):
            command.upgrade(configuration, "head")
        asyncio.run(exercise(database_url))
