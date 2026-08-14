"""Real PostgreSQL due-retry scanner and concurrency verification."""

from __future__ import annotations

import asyncio
import json
import os
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta
from types import TracebackType
from typing import Protocol
from uuid import UUID, uuid4

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from taskforge.dispatch.envelope import (
    create_dispatch_envelope,
    dispatch_envelope_to_mapping,
)
from taskforge.persistence.database import build_session_factory
from taskforge.persistence.retries import SQLAlchemyRetryTransitionRepository
from taskforge.retries.persistence_ports import (
    DueRetryDispatchTransaction,
    DueRetryDispatchTransactionContext,
    DueRetryPreparation,
    PreparedDueRetryDispatch,
)
from taskforge.retries.scanner import (
    DueRetryScanInvariantError,
    DueRetryScanner,
    DueRetryScanServiceUnavailable,
)
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
    add_worker,
    wait_for_lock_waiter,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_RETRY_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_RETRY_INTEGRATION=1 explicitly",
    ),
]


@dataclass(frozen=True)
class AcceptParameters:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        del parameters
        return ()


@dataclass(frozen=True)
class ScheduledTaskFacts:
    task_run_id: UUID
    predecessor_attempt_id: UUID
    scheduled_attempt_id: UUID
    predecessor_dispatch_id: UUID
    attempt_number: int
    next_eligible_at: datetime


@dataclass(frozen=True)
class ScheduledWorkflowFacts:
    workflow_run_id: UUID
    worker_session_id: UUID
    tasks: tuple[ScheduledTaskFacts, ...]


def registry() -> TaskTypeRegistry:
    return TaskTypeRegistry(
        (TaskTypeDefinition("test.task", "test-capability", AcceptParameters()),)
    )


async def add_scheduled_workflow(
    connection: asyncpg.Connection[asyncpg.Record],
    *,
    eligible_at: tuple[datetime, ...],
    scheduled_attempt_ids: tuple[UUID, ...] | None = None,
) -> ScheduledWorkflowFacts:
    worker = await add_worker(connection)
    principal_id, workflow_id, version_id, workflow_run_id = (
        uuid4(),
        uuid4(),
        uuid4(),
        uuid4(),
    )
    await connection.execute(
        "INSERT INTO api_principals (id, name) VALUES ($1, $2)",
        principal_id,
        f"scanner-owner-{uuid4().hex}",
    )
    await connection.execute(
        "INSERT INTO workflow_definitions (id, owner_principal_id, name) "
        "VALUES ($1, $2, $3)",
        workflow_id,
        principal_id,
        f"scanner-workflow-{uuid4().hex}",
    )
    await connection.execute(
        "INSERT INTO workflow_versions "
        "(id, workflow_definition_id, version_number, name) "
        "VALUES ($1, $2, 1, 'scanner-v1')",
        version_id,
        workflow_id,
    )
    task_facts: list[ScheduledTaskFacts] = []
    for index, eligibility in enumerate(eligible_at):
        step = f"step-{index}"
        task_run_id, predecessor_id, predecessor_dispatch_id = (
            uuid4(),
            uuid4(),
            uuid4(),
        )
        scheduled_id = (
            scheduled_attempt_ids[index]
            if scheduled_attempt_ids is not None
            else uuid4()
        )
        parameters: JSONMapping = {"step": step, "source": "immutable"}
        predecessor = create_dispatch_envelope(
            dispatch_id=predecessor_dispatch_id,
            task_attempt_id=predecessor_id,
            task_run_id=task_run_id,
            workflow_run_id=workflow_run_id,
            attempt_number=1,
            task_type="test.task",
            required_capability="test-capability",
            task_payload=parameters,
            references={"stable": step},
            correlation_id=f"correlation-{index}",
            trace_context={
                "traceparent": (
                    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
                )
            },
        )
        await connection.execute(
            "INSERT INTO workflow_version_steps "
            "(workflow_version_id, step_identifier, task_type, parameters) "
            "VALUES ($1, $2, 'test.task', $3::jsonb)",
            version_id,
            step,
            json.dumps(parameters),
        )
        if index == 0:
            await connection.execute(
                "INSERT INTO workflow_runs "
                "(id, workflow_definition_id, workflow_version_id, "
                "requested_by_principal_id, status) "
                "VALUES ($1, $2, $3, $4, 'running')",
                workflow_run_id,
                workflow_id,
                version_id,
                principal_id,
            )
        await connection.execute(
            "INSERT INTO task_runs "
            "(id, workflow_run_id, workflow_version_id, step_identifier, status) "
            "VALUES ($1, $2, $3, $4, 'retry_scheduled')",
            task_run_id,
            workflow_run_id,
            version_id,
            step,
        )
        await connection.execute(
            "INSERT INTO task_attempts (id, task_run_id, attempt_number) "
            "VALUES ($1, $2, 1), ($3, $2, 2)",
            predecessor_id,
            task_run_id,
            scheduled_id,
        )
        await connection.execute(
            "UPDATE task_attempts SET next_eligible_at = $2 WHERE id = $1",
            scheduled_id,
            eligibility,
        )
        await connection.execute(
            "INSERT INTO task_dispatch_outbox "
            "(id, task_attempt_id, route, payload) VALUES ($1, $2, $3, $4::jsonb)",
            predecessor_dispatch_id,
            predecessor_id,
            predecessor.route,
            json.dumps(dispatch_envelope_to_mapping(predecessor)),
        )
        await connection.execute(
            "INSERT INTO task_attempt_claims "
            "(task_attempt_id, generation, worker_session_id, lease_expires_at, "
            "terminated_at) VALUES ($1, 1, $2, statement_timestamp() + "
            "interval '1 minute', statement_timestamp())",
            predecessor_id,
            worker.session_id,
        )
        await connection.execute(
            "INSERT INTO task_attempt_results "
            "(task_attempt_id, claim_generation, dispatch_id, result_kind, "
            "failure_kind, output, result_fingerprint) VALUES "
            "($1, 1, $2, 'retryable_failure', 'handler_reported', NULL, $3)",
            predecessor_id,
            predecessor_dispatch_id,
            f"{index + 1:064x}",
        )
        task_facts.append(
            ScheduledTaskFacts(
                task_run_id,
                predecessor_id,
                scheduled_id,
                predecessor_dispatch_id,
                2,
                eligibility,
            )
        )
    return ScheduledWorkflowFacts(workflow_run_id, worker.session_id, tuple(task_facts))


class ScannerRepository(Protocol):
    def due_dispatch_transaction(self) -> DueRetryDispatchTransactionContext: ...


class PausingTransaction:
    def __init__(
        self,
        context: DueRetryDispatchTransactionContext,
        locked: asyncio.Event,
        release: asyncio.Event,
    ) -> None:
        self._context = context
        self._transaction: DueRetryDispatchTransaction | None = None
        self._locked = locked
        self._release = release

    async def __aenter__(self) -> PausingTransaction:
        self._transaction = await self._context.__aenter__()
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self._context.__aexit__(exception_type, exception, traceback)

    async def prepare_next_due(self) -> DueRetryPreparation:
        prepared = await self._required().prepare_next_due()
        if isinstance(prepared, PreparedDueRetryDispatch):
            self._locked.set()
            await self._release.wait()
        return prepared

    async def persist_dispatch(
        self,
        prepared: PreparedDueRetryDispatch,
        outbox_id: UUID,
        route: str,
        payload: dict[str, object],
    ) -> None:
        await self._required().persist_dispatch(prepared, outbox_id, route, payload)

    def _required(self) -> DueRetryDispatchTransaction:
        if self._transaction is None:
            raise RuntimeError("pausing transaction is not active")
        return self._transaction


class PausingRepository:
    def __init__(
        self,
        delegate: ScannerRepository,
        locked: asyncio.Event,
        release: asyncio.Event,
    ) -> None:
        self._delegate = delegate
        self._locked = locked
        self._release = release

    def due_dispatch_transaction(self) -> PausingTransaction:
        return PausingTransaction(
            self._delegate.due_dispatch_transaction(), self._locked, self._release
        )


async def assert_attempt_counts(
    connection: asyncpg.Connection[asyncpg.Record],
    facts: ScheduledWorkflowFacts,
) -> None:
    for task in facts.tasks:
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM task_attempts WHERE task_run_id = $1",
                task.task_run_id,
            )
            == 2
        )


async def exercise_scanner(database_url: URL) -> None:
    setup = await asyncpg.connect(asyncpg_dsn(database_url))
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        ),
        pool_size=6,
    )
    repository = SQLAlchemyRetryTransitionRepository(build_session_factory(engine))
    scanner = DueRetryScanner(repository, registry())
    try:
        now = await setup.fetchval("SELECT statement_timestamp()")
        ordered_ids = (UUID(int=101), UUID(int=102), UUID(int=103), UUID(int=104))
        ordered = tuple(
            [
                await add_scheduled_workflow(
                    setup,
                    eligible_at=(eligibility,),
                    scheduled_attempt_ids=(attempt_id,),
                )
                for eligibility, attempt_id in zip(
                    (
                        now - timedelta(seconds=40),
                        now - timedelta(seconds=40),
                        now - timedelta(seconds=20),
                        now + timedelta(hours=1),
                    ),
                    ordered_ids,
                    strict=True,
                )
            ]
        )
        first = await scanner.scan_due_retries(batch_size=2)
        assert first.examined == first.dispatched == 2
        assert first.dispatched_attempt_ids == ordered_ids[:2]
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM task_runs WHERE status = 'retry_scheduled'"
            )
            == 2
        )
        second = await scanner.scan_due_retries(batch_size=2)
        assert second.dispatched_attempt_ids == (ordered_ids[2],)
        assert second.examined == 1
        assert (
            await setup.fetchval(
                "SELECT status::text FROM task_runs WHERE id = $1",
                ordered[3].tasks[0].task_run_id,
            )
            == "retry_scheduled"
        )
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM task_dispatch_outbox WHERE task_attempt_id = $1",
                ordered[3].tasks[0].scheduled_attempt_id,
            )
            == 0
        )
        rerun = await scanner.scan_due_retries(batch_size=10)
        assert rerun.examined == rerun.dispatched == rerun.skipped == 0
        for facts in ordered:
            await assert_attempt_counts(setup, facts)
        for facts in ordered[:3]:
            task = facts.tasks[0]
            row = await setup.fetchrow(
                "SELECT tr.status::text, ta.attempt_number, ta.next_eligible_at, "
                "count(o.id) FROM task_runs tr JOIN task_attempts ta ON "
                "ta.task_run_id = tr.id LEFT JOIN task_dispatch_outbox o ON "
                "o.task_attempt_id = ta.id WHERE tr.id = $1 AND ta.id = $2 "
                "GROUP BY tr.status, ta.attempt_number, ta.next_eligible_at",
                task.task_run_id,
                task.scheduled_attempt_id,
            )
            assert tuple(row) == ("dispatched", 2, task.next_eligible_at, 1)
            assert tuple(
                await setup.fetchrow(
                    "SELECT event_type, failed_attempt_number, "
                    "retry_attempt_number, next_eligible_at, decision_reason "
                    "FROM task_retry_events WHERE task_run_id = $1",
                    task.task_run_id,
                )
            ) == ("retry_dispatched", None, 2, None, None)

        await exercise_single_candidate_concurrency(setup, scanner, repository)
        await exercise_shared_workflow_skip_locked(setup, scanner, repository)
        await exercise_cancellation_wins(database_url, setup, scanner)
        await exercise_state_change_after_discovery(database_url, setup, scanner)
        await exercise_corruption_and_rollback(setup, scanner)
    finally:
        await setup.close()
        await engine.dispose()


async def exercise_single_candidate_concurrency(
    setup: asyncpg.Connection[asyncpg.Record],
    scanner: DueRetryScanner,
    repository: SQLAlchemyRetryTransitionRepository,
) -> None:
    now = await setup.fetchval("SELECT statement_timestamp()")
    attempt_id = UUID(int=151)
    facts = await add_scheduled_workflow(
        setup,
        eligible_at=(now - timedelta(seconds=1),),
        scheduled_attempt_ids=(attempt_id,),
    )
    locked, release = asyncio.Event(), asyncio.Event()
    scanner_a = DueRetryScanner(
        PausingRepository(repository, locked, release), registry()
    )
    pending_a = asyncio.create_task(scanner_a.scan_due_retries(batch_size=1))
    await locked.wait()
    try:
        result_b = await asyncio.wait_for(
            scanner.scan_due_retries(batch_size=1), timeout=2
        )
        assert result_b.examined == result_b.dispatched == 0
    finally:
        release.set()
    result_a = await pending_a
    assert result_a.dispatched_attempt_ids == (attempt_id,)
    assert (
        await setup.fetchval(
            "SELECT count(*) FROM task_dispatch_outbox WHERE task_attempt_id = $1",
            attempt_id,
        )
        == 1
    )
    await assert_attempt_counts(setup, facts)


async def exercise_shared_workflow_skip_locked(
    setup: asyncpg.Connection[asyncpg.Record],
    scanner: DueRetryScanner,
    repository: SQLAlchemyRetryTransitionRepository,
) -> None:
    now = await setup.fetchval("SELECT statement_timestamp()")
    w1_ids = (UUID(int=201), UUID(int=202))
    w2_id = UUID(int=203)
    w1 = await add_scheduled_workflow(
        setup,
        eligible_at=(now - timedelta(seconds=30), now - timedelta(seconds=20)),
        scheduled_attempt_ids=w1_ids,
    )
    w2 = await add_scheduled_workflow(
        setup,
        eligible_at=(now - timedelta(seconds=10),),
        scheduled_attempt_ids=(w2_id,),
    )
    locked, release = asyncio.Event(), asyncio.Event()
    scanner_a = DueRetryScanner(
        PausingRepository(repository, locked, release), registry()
    )
    pending_a = asyncio.create_task(scanner_a.scan_due_retries(batch_size=1))
    await locked.wait()
    try:
        result_b = await asyncio.wait_for(
            scanner.scan_due_retries(batch_size=1), timeout=2
        )
        assert result_b.dispatched_attempt_ids == (w2_id,)
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM task_dispatch_outbox WHERE task_attempt_id = ANY($1)",
                list(w1_ids),
            )
            == 0
        )
    finally:
        release.set()
    result_a = await pending_a
    assert result_a.dispatched_attempt_ids == (w1_ids[0],)
    remaining = await scanner.scan_due_retries(batch_size=1)
    assert remaining.dispatched_attempt_ids == (w1_ids[1],)
    await assert_attempt_counts(setup, w1)
    await assert_attempt_counts(setup, w2)


async def exercise_cancellation_wins(
    database_url: URL,
    setup: asyncpg.Connection[asyncpg.Record],
    scanner: DueRetryScanner,
) -> None:
    now = await setup.fetchval("SELECT statement_timestamp()")
    facts = await add_scheduled_workflow(
        setup, eligible_at=(now - timedelta(seconds=1),)
    )
    blocker = await asyncpg.connect(asyncpg_dsn(database_url))
    transaction = blocker.transaction()
    await transaction.start()
    try:
        await blocker.execute(
            "SELECT id FROM workflow_runs WHERE id = $1 FOR UPDATE",
            facts.workflow_run_id,
        )
        result = await asyncio.wait_for(
            scanner.scan_due_retries(batch_size=1), timeout=2
        )
        assert result.examined == 0
        await blocker.execute(
            "UPDATE workflow_runs SET status = 'cancelling' WHERE id = $1",
            facts.workflow_run_id,
        )
        await transaction.commit()
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM task_dispatch_outbox WHERE task_attempt_id = $1",
                facts.tasks[0].scheduled_attempt_id,
            )
            == 0
        )
    finally:
        with suppress(Exception):
            await transaction.rollback()
        await blocker.close()


async def exercise_state_change_after_discovery(
    database_url: URL,
    setup: asyncpg.Connection[asyncpg.Record],
    scanner: DueRetryScanner,
) -> None:
    now = await setup.fetchval("SELECT statement_timestamp()")
    facts = await add_scheduled_workflow(
        setup, eligible_at=(now - timedelta(seconds=1),)
    )
    blocker = await asyncpg.connect(asyncpg_dsn(database_url))
    observer = await asyncpg.connect(asyncpg_dsn(database_url))
    transaction = blocker.transaction()
    await transaction.start()
    pending: asyncio.Task[object] | None = None
    try:
        await blocker.execute(
            "SELECT id FROM task_runs WHERE id = $1 FOR UPDATE",
            facts.tasks[0].task_run_id,
        )
        pending = asyncio.create_task(scanner.scan_due_retries(batch_size=1))
        await wait_for_lock_waiter(observer)
        await blocker.execute(
            "UPDATE task_runs SET status = 'cancelled' WHERE id = $1",
            facts.tasks[0].task_run_id,
        )
        await transaction.commit()
        result = await pending
        assert result.examined == result.skipped == 1
        assert result.dispatched == 0
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM task_dispatch_outbox WHERE task_attempt_id = $1",
                facts.tasks[0].scheduled_attempt_id,
            )
            == 0
        )
    finally:
        if pending is not None and not pending.done():
            pending.cancel()
            with suppress(BaseException):
                await pending
        with suppress(Exception):
            await transaction.rollback()
        await blocker.close()
        await observer.close()


async def exercise_corruption_and_rollback(
    setup: asyncpg.Connection[asyncpg.Record], scanner: DueRetryScanner
) -> None:
    now = await setup.fetchval("SELECT statement_timestamp()")

    existing_outbox = await add_scheduled_workflow(
        setup, eligible_at=(now - timedelta(seconds=4),)
    )
    await setup.execute(
        "INSERT INTO task_dispatch_outbox (id, task_attempt_id, route, payload) "
        "VALUES ($1, $2, 'capability.test', '{}'::jsonb)",
        uuid4(),
        existing_outbox.tasks[0].scheduled_attempt_id,
    )
    with pytest.raises(DueRetryScanInvariantError):
        await scanner.scan_due_retries(batch_size=1)
    await setup.execute(
        "UPDATE task_runs SET status = 'cancelled' WHERE id = $1",
        existing_outbox.tasks[0].task_run_id,
    )

    claimed = await add_scheduled_workflow(
        setup, eligible_at=(now - timedelta(seconds=3),)
    )
    await setup.execute(
        "INSERT INTO task_attempt_claims "
        "(task_attempt_id, generation, worker_session_id, lease_expires_at) "
        "VALUES ($1, 1, $2, statement_timestamp() + interval '1 minute')",
        claimed.tasks[0].scheduled_attempt_id,
        claimed.worker_session_id,
    )
    await setup.execute(
        "UPDATE task_attempts SET next_eligible_at = statement_timestamp() - "
        "interval '10 seconds' WHERE id = $1",
        claimed.tasks[0].scheduled_attempt_id,
    )
    with pytest.raises(DueRetryScanInvariantError):
        await scanner.scan_due_retries(batch_size=1)
    await setup.execute(
        "UPDATE task_runs SET status = 'cancelled' WHERE id = $1",
        claimed.tasks[0].task_run_id,
    )

    resulted = await add_scheduled_workflow(
        setup, eligible_at=(now - timedelta(seconds=2),)
    )
    result_dispatch_id = uuid4()
    await setup.execute(
        "INSERT INTO task_dispatch_outbox (id, task_attempt_id, route, payload) "
        "VALUES ($1, $2, 'capability.test', '{}'::jsonb)",
        result_dispatch_id,
        resulted.tasks[0].scheduled_attempt_id,
    )
    await setup.execute(
        "INSERT INTO task_attempt_claims "
        "(task_attempt_id, generation, worker_session_id, lease_expires_at) "
        "VALUES ($1, 1, $2, statement_timestamp() + interval '1 minute')",
        resulted.tasks[0].scheduled_attempt_id,
        resulted.worker_session_id,
    )
    await setup.execute(
        "INSERT INTO task_attempt_results "
        "(task_attempt_id, claim_generation, dispatch_id, result_kind, output, "
        "result_fingerprint) VALUES ($1, 1, $2, 'success', 'null'::jsonb, $3)",
        resulted.tasks[0].scheduled_attempt_id,
        result_dispatch_id,
        "f" * 64,
    )
    with pytest.raises(DueRetryScanInvariantError):
        await scanner.scan_due_retries(batch_size=1)
    await setup.execute(
        "UPDATE task_runs SET status = 'cancelled' WHERE id = $1",
        resulted.tasks[0].task_run_id,
    )

    invalid_envelope = await add_scheduled_workflow(
        setup, eligible_at=(now - timedelta(seconds=1),)
    )
    await setup.execute(
        "UPDATE task_dispatch_outbox SET payload = '{\"invalid\": true}'::jsonb "
        "WHERE id = $1",
        invalid_envelope.tasks[0].predecessor_dispatch_id,
    )
    with pytest.raises(DueRetryScanInvariantError):
        await scanner.scan_due_retries(batch_size=1)
    await setup.execute(
        "UPDATE task_runs SET status = 'cancelled' WHERE id = $1",
        invalid_envelope.tasks[0].task_run_id,
    )

    rollback = await add_scheduled_workflow(
        setup,
        eligible_at=(now,),
    )
    await setup.execute(
        "CREATE FUNCTION reject_retry_dispatch_event() RETURNS trigger "
        "LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION "
        "'forced retry dispatch event rollback'; END $$"
    )
    await setup.execute(
        "CREATE TRIGGER reject_retry_dispatch_event_trigger BEFORE INSERT ON "
        "task_retry_events FOR EACH ROW EXECUTE FUNCTION "
        "reject_retry_dispatch_event()"
    )
    try:
        with pytest.raises(DueRetryScanServiceUnavailable):
            await scanner.scan_due_retries(batch_size=1)
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM task_dispatch_outbox WHERE task_attempt_id = $1",
                rollback.tasks[0].scheduled_attempt_id,
            )
            == 0
        )
        assert (
            await setup.fetchval(
                "SELECT status::text FROM task_runs WHERE id = $1",
                rollback.tasks[0].task_run_id,
            )
            == "retry_scheduled"
        )
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM task_retry_events WHERE task_run_id = $1",
                rollback.tasks[0].task_run_id,
            )
            == 0
        )
        await assert_attempt_counts(setup, rollback)
    finally:
        await setup.execute(
            "DROP TRIGGER reject_retry_dispatch_event_trigger ON task_retry_events"
        )
        await setup.execute("DROP FUNCTION reject_retry_dispatch_event()")


def test_real_postgresql_due_retry_scanner() -> None:
    with temporary_database(
        "TASKFORGE_RETRY_TEST_DATABASE_URL", "taskforge_retry_scanner"
    ) as database_url:
        configuration = Config("alembic.ini")
        with migration_database_url(database_url.render_as_string(hide_password=False)):
            command.upgrade(configuration, "head")
        asyncio.run(exercise_scanner(database_url))
