"""Real PostgreSQL retry-transition and concurrency tests."""

from __future__ import annotations

import asyncio
import json
import os
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID, uuid4

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from taskforge.dead_letters.domain import DeadLetterReason
from taskforge.persistence.database import build_session_factory
from taskforge.persistence.dead_letters import (
    DeadLetterInsertOutcome,
    DeadLetterPersistenceInvariantViolation,
    ensure_dead_letter,
)
from taskforge.persistence.retries import SQLAlchemyRetryTransitionRepository
from taskforge.retries.persistence_ports import (
    RetryTransitionPersistenceInvariantViolation,
)
from taskforge.retries.service import (
    RetryTransitionInvariantError,
    RetryTransitionOutcome,
    RetryTransitionService,
    RetryTransitionServiceUnavailable,
)
from tests.integration.postgresql import (
    ExpectedStatusExecutionEvent,
    assert_status_execution_events,
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


def retry_policy(
    *,
    maximum_attempts: int = 4,
    initial_delay_seconds: int = 10,
    multiplier: float = 2,
    maximum_delay_seconds: int = 300,
) -> dict[str, object]:
    return {
        "maximum_attempts": maximum_attempts,
        "initial_delay_seconds": initial_delay_seconds,
        "multiplier": multiplier,
        "maximum_delay_seconds": maximum_delay_seconds,
    }


@dataclass(frozen=True)
class RetryFacts:
    workflow_run_id: UUID
    task_run_id: UUID
    failed_attempt_id: UUID
    completed_at: datetime
    correlation_id: str


async def add_retry_pending_task(
    connection: asyncpg.Connection[asyncpg.Record],
    *,
    workflow_policy: dict[str, object] | None = None,
    step_policy: dict[str, object] | None = None,
    result_kind: str = "retryable_failure",
    failure_kind: str | None = "handler_reported",
    include_result_event: bool = True,
) -> RetryFacts:
    worker = await add_worker(connection)
    principal_id, workflow_id, version_id, run_id = (
        uuid4(),
        uuid4(),
        uuid4(),
        uuid4(),
    )
    task_id, attempt_id, dispatch_id = uuid4(), uuid4(), uuid4()
    await connection.execute(
        "INSERT INTO api_principals (id, name) VALUES ($1, $2)",
        principal_id,
        f"retry-owner-{uuid4().hex}",
    )
    await connection.execute(
        "INSERT INTO workflow_definitions (id, owner_principal_id, name) "
        "VALUES ($1, $2, $3)",
        workflow_id,
        principal_id,
        f"retry-workflow-{uuid4().hex}",
    )
    await connection.execute(
        "INSERT INTO workflow_versions "
        "(id, workflow_definition_id, version_number, name, execution_policy) "
        "VALUES ($1, $2, 1, 'v1', $3::jsonb)",
        version_id,
        workflow_id,
        json.dumps(workflow_policy) if workflow_policy is not None else None,
    )
    await connection.execute(
        "INSERT INTO workflow_version_steps "
        "(workflow_version_id, step_identifier, task_type, parameters, "
        "execution_policy) VALUES ($1, 'step', 'test.task', '{}'::jsonb, $2::jsonb)",
        version_id,
        json.dumps(step_policy) if step_policy is not None else None,
    )
    await connection.execute(
        "INSERT INTO workflow_runs "
        "(id, workflow_definition_id, workflow_version_id, "
        "requested_by_principal_id, status) VALUES ($1, $2, $3, $4, 'running')",
        run_id,
        workflow_id,
        version_id,
        principal_id,
    )
    await connection.execute(
        "INSERT INTO task_runs "
        "(id, workflow_run_id, workflow_version_id, step_identifier, status) "
        "VALUES ($1, $2, $3, 'step', 'retry_pending')",
        task_id,
        run_id,
        version_id,
    )
    await connection.execute(
        "INSERT INTO task_attempts (id, task_run_id, attempt_number) "
        "VALUES ($1, $2, 1)",
        attempt_id,
        task_id,
    )
    await connection.execute(
        "INSERT INTO task_dispatch_outbox (id, task_attempt_id, route, payload) "
        "VALUES ($1, $2, 'capability.test', '{}'::jsonb)",
        dispatch_id,
        attempt_id,
    )
    await connection.execute(
        "INSERT INTO task_attempt_claims "
        "(task_attempt_id, generation, worker_session_id, lease_expires_at, "
        "terminated_at) VALUES ($1, 1, $2, statement_timestamp() + "
        "interval '1 minute', statement_timestamp())",
        attempt_id,
        worker.session_id,
    )
    output = "'null'::jsonb" if result_kind == "success" else "NULL"
    result_fingerprint = "a" * 64
    completed_at = await connection.fetchval(
        "INSERT INTO task_attempt_results "
        "(task_attempt_id, claim_generation, dispatch_id, result_kind, "
        "failure_kind, output, result_fingerprint) VALUES "
        f"($1, 1, $2, $3, $4, {output}, $5) RETURNING completed_at",
        attempt_id,
        dispatch_id,
        result_kind,
        failure_kind,
        result_fingerprint,
    )
    correlation_id = f"retry-result-{attempt_id}"
    if include_result_event:
        await connection.execute(
            "INSERT INTO task_result_events "
            "(id, task_attempt_id, claim_generation, worker_session_id, "
            "worker_identity_id, correlation_id, dispatch_id, event_type, "
            "result_kind, result_fingerprint) VALUES "
            "($1, $2, 1, $3, $4, $5, $6, 'result_accepted', $7, $8)",
            uuid4(),
            attempt_id,
            worker.session_id,
            worker.authenticated.worker_identity_id,
            correlation_id,
            dispatch_id,
            result_kind,
            result_fingerprint,
        )
    return RetryFacts(run_id, task_id, attempt_id, completed_at, correlation_id)


async def scheduled_shape(
    connection: asyncpg.Connection[asyncpg.Record], task_run_id: UUID
) -> tuple[asyncpg.Record, ...]:
    return tuple(
        await connection.fetch(
            "SELECT ta.id, ta.attempt_number, ta.next_eligible_at, "
            "EXISTS (SELECT FROM task_attempt_results r WHERE "
            "r.task_attempt_id = ta.id) AS has_result, "
            "EXISTS (SELECT FROM task_attempt_claims c WHERE "
            "c.task_attempt_id = ta.id) AS has_claim, "
            "EXISTS (SELECT FROM task_dispatch_outbox d WHERE "
            "d.task_attempt_id = ta.id) AS has_outbox "
            "FROM task_attempts ta WHERE ta.task_run_id = $1 "
            "ORDER BY ta.attempt_number",
            task_run_id,
        )
    )


async def exercise_retry_transitions(database_url: URL) -> None:
    setup = await asyncpg.connect(asyncpg_dsn(database_url))
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        ),
        pool_size=4,
    )
    sessions = build_session_factory(engine)
    service = RetryTransitionService(SQLAlchemyRetryTransitionRepository(sessions))
    try:
        corrupt = await add_retry_pending_task(
            setup,
            workflow_policy={"retry_policy": retry_policy()},
            include_result_event=False,
        )
        with pytest.raises(RetryTransitionInvariantError) as raised:
            await service.transition_retry(corrupt.task_run_id)
        assert isinstance(
            raised.value.__cause__, RetryTransitionPersistenceInvariantViolation
        )
        assert (
            await setup.fetchval(
                "SELECT status::text FROM task_runs WHERE id = $1", corrupt.task_run_id
            )
            == "retry_pending"
        )
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM task_attempts WHERE task_run_id = $1",
                corrupt.task_run_id,
            )
            == 1
        )
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM task_retry_events WHERE task_run_id = $1",
                corrupt.task_run_id,
            )
            == 0
        )
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM workflow_run_execution_events "
                "WHERE workflow_run_id = $1",
                corrupt.workflow_run_id,
            )
            == 0
        )

        workflow = await add_retry_pending_task(
            setup, workflow_policy={"retry_policy": retry_policy()}
        )
        scheduled = await service.transition_retry(workflow.task_run_id)
        assert scheduled.outcome is RetryTransitionOutcome.SCHEDULED
        assert scheduled.next_eligible_at == workflow.completed_at + timedelta(
            seconds=10
        )
        shape = await scheduled_shape(setup, workflow.task_run_id)
        assert len(shape) == 2
        assert shape[1][1:] == (2, scheduled.next_eligible_at, False, False, False)
        assert tuple(
            await setup.fetchrow(
                "SELECT event_type, failed_attempt_number, retry_attempt_number, "
                "next_eligible_at, decision_reason, correlation_id "
                "FROM task_retry_events "
                "WHERE task_run_id = $1",
                workflow.task_run_id,
            )
        ) == (
            "retry_scheduled",
            1,
            2,
            scheduled.next_eligible_at,
            None,
            workflow.correlation_id,
        )
        replayed = await service.transition_retry(workflow.task_run_id)
        assert replayed.outcome is RetryTransitionOutcome.ALREADY_SCHEDULED
        assert replayed.scheduled_attempt_id == scheduled.scheduled_attempt_id
        assert len(await scheduled_shape(setup, workflow.task_run_id)) == 2
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM dead_letter_items d JOIN task_attempts a "
                "ON a.id = d.source_task_attempt_id WHERE a.task_run_id = $1",
                workflow.task_run_id,
            )
            == 0
        )
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM task_retry_events WHERE task_run_id = $1",
                workflow.task_run_id,
            )
            == 1
        )
        await assert_status_execution_events(
            setup,
            workflow.workflow_run_id,
            (
                ExpectedStatusExecutionEvent(
                    workflow.task_run_id, "retry_pending", "retry_scheduled"
                ),
            ),
        )

        override = await add_retry_pending_task(
            setup,
            workflow_policy={"retry_policy": retry_policy(initial_delay_seconds=100)},
            step_policy={
                "retry_policy": retry_policy(initial_delay_seconds=7, multiplier=3)
            },
        )
        overridden = await service.transition_retry(override.task_run_id)
        assert overridden.next_eligible_at == override.completed_at + timedelta(
            seconds=7
        )

        zero = await add_retry_pending_task(
            setup,
            workflow_policy={
                "retry_policy": retry_policy(
                    initial_delay_seconds=0, maximum_delay_seconds=0
                )
            },
        )
        zero_result = await service.transition_retry(zero.task_run_id)
        assert zero_result.next_eligible_at == zero.completed_at

        no_policy = await add_retry_pending_task(setup)
        no_policy_result = await service.transition_retry(no_policy.task_run_id)
        assert no_policy_result.outcome is RetryTransitionOutcome.FAILED_NO_POLICY
        assert no_policy_result.dead_letter_created is True
        assert (
            await setup.fetchval(
                "SELECT status::text FROM task_runs WHERE id = $1",
                no_policy.task_run_id,
            )
            == "failed"
        )
        assert len(await scheduled_shape(setup, no_policy.task_run_id)) == 1
        assert (
            await setup.fetchval(
                "SELECT decision_reason FROM task_retry_events WHERE task_run_id = $1",
                no_policy.task_run_id,
            )
            == "no_policy"
        )
        assert tuple(
            await setup.fetchrow(
                "SELECT d.reason, s.status FROM dead_letter_items d "
                "JOIN dead_letter_status s ON s.dead_letter_item_id = d.id "
                "WHERE d.source_task_attempt_id = $1",
                no_policy.failed_attempt_id,
            )
        ) == ("retry_exhausted", "open")

        exhausted = await add_retry_pending_task(
            setup,
            workflow_policy={"retry_policy": retry_policy(maximum_attempts=1)},
        )
        exhausted_result = await service.transition_retry(exhausted.task_run_id)
        assert exhausted_result.outcome is RetryTransitionOutcome.FAILED_EXHAUSTED
        assert exhausted_result.dead_letter_created is True
        assert (
            await service.transition_retry(exhausted.task_run_id)
        ).outcome is RetryTransitionOutcome.NOT_ELIGIBLE
        assert len(await scheduled_shape(setup, exhausted.task_run_id)) == 1
        assert (
            await setup.fetchval(
                "SELECT decision_reason FROM task_retry_events WHERE task_run_id = $1",
                exhausted.task_run_id,
            )
            == "exhausted"
        )
        assert tuple(
            await setup.fetchrow(
                "SELECT d.task_run_id, d.source_task_attempt_id, d.reason, s.status "
                "FROM dead_letter_items d JOIN dead_letter_status s "
                "ON s.dead_letter_item_id = d.id "
                "WHERE d.source_task_attempt_id = $1",
                exhausted.failed_attempt_id,
            )
        ) == (
            exhausted.task_run_id,
            exhausted.failed_attempt_id,
            "retry_exhausted",
            "open",
        )
        async with sessions.begin() as session:
            assert (
                await ensure_dead_letter(
                    session,
                    item_id=uuid4(),
                    task_run_id=exhausted.task_run_id,
                    source_task_attempt_id=exhausted.failed_attempt_id,
                    reason=DeadLetterReason.RETRY_EXHAUSTED,
                )
                is DeadLetterInsertOutcome.ALREADY_PRESENT
            )
            with pytest.raises(DeadLetterPersistenceInvariantViolation):
                await ensure_dead_letter(
                    session,
                    item_id=uuid4(),
                    task_run_id=exhausted.task_run_id,
                    source_task_attempt_id=exhausted.failed_attempt_id,
                    reason=DeadLetterReason.PERMANENT_FAILURE,
                )

        invalid_result = await add_retry_pending_task(
            setup,
            workflow_policy={"retry_policy": retry_policy()},
            result_kind="success",
            failure_kind=None,
        )
        with pytest.raises(RetryTransitionInvariantError):
            await service.transition_retry(invalid_result.task_run_id)
        assert len(await scheduled_shape(setup, invalid_result.task_run_id)) == 1
        assert (
            await setup.fetchval(
                "SELECT status::text FROM task_runs WHERE id = $1",
                invalid_result.task_run_id,
            )
            == "retry_pending"
        )
        async with sessions.begin() as session:
            with pytest.raises(DeadLetterPersistenceInvariantViolation):
                await ensure_dead_letter(
                    session,
                    item_id=uuid4(),
                    task_run_id=invalid_result.task_run_id,
                    source_task_attempt_id=invalid_result.failed_attempt_id,
                    reason=DeadLetterReason.RETRY_EXHAUSTED,
                )

        malformed = await add_retry_pending_task(
            setup,
            workflow_policy={"retry_policy": retry_policy()},
            step_policy={"retry_policy": {"maximum_attempts": 4}},
        )
        with pytest.raises(RetryTransitionInvariantError):
            await service.transition_retry(malformed.task_run_id)
        assert len(await scheduled_shape(setup, malformed.task_run_id)) == 1

        stale = await add_retry_pending_task(
            setup, workflow_policy={"retry_policy": retry_policy()}
        )
        await setup.execute(
            "INSERT INTO task_attempts (id, task_run_id, attempt_number) "
            "VALUES ($1, $2, 2)",
            uuid4(),
            stale.task_run_id,
        )
        with pytest.raises(RetryTransitionInvariantError):
            await service.transition_retry(stale.task_run_id)
        assert len(await scheduled_shape(setup, stale.task_run_id)) == 2

        await _exercise_duplicate_race(database_url, setup, service)
        await _exercise_cancellation_wins(database_url, setup, service)
        await _exercise_rollback(setup, service)
    finally:
        await setup.close()
        await engine.dispose()


async def _exercise_duplicate_race(
    database_url: URL,
    setup: asyncpg.Connection[asyncpg.Record],
    service: RetryTransitionService,
) -> None:
    facts = await add_retry_pending_task(
        setup, workflow_policy={"retry_policy": retry_policy(maximum_attempts=1)}
    )
    blocker = await asyncpg.connect(asyncpg_dsn(database_url))
    observer = await asyncpg.connect(asyncpg_dsn(database_url))
    transaction = blocker.transaction()
    await transaction.start()
    pending: tuple[asyncio.Task[object], asyncio.Task[object]] | None = None
    try:
        await blocker.execute(
            "SELECT id FROM workflow_runs WHERE id = $1 FOR UPDATE",
            facts.workflow_run_id,
        )
        pending = (
            asyncio.create_task(service.transition_retry(facts.task_run_id)),
            asyncio.create_task(service.transition_retry(facts.task_run_id)),
        )
        await wait_for_lock_waiter(observer, minimum=2)
        await transaction.commit()
        results = await asyncio.gather(*pending)
        assert {result.outcome for result in results} == {
            RetryTransitionOutcome.FAILED_EXHAUSTED,
            RetryTransitionOutcome.NOT_ELIGIBLE,
        }
        assert len(await scheduled_shape(setup, facts.task_run_id)) == 1
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM dead_letter_items "
                "WHERE source_task_attempt_id = $1",
                facts.failed_attempt_id,
            )
            == 1
        )
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM dead_letter_status s JOIN dead_letter_items d "
                "ON d.id = s.dead_letter_item_id "
                "WHERE d.source_task_attempt_id = $1",
                facts.failed_attempt_id,
            )
            == 1
        )
    finally:
        if pending is not None:
            for task in pending:
                if not task.done():
                    task.cancel()
            with suppress(BaseException):
                await asyncio.gather(*pending)
        with suppress(Exception):
            await transaction.rollback()
        await blocker.close()
        await observer.close()


async def _exercise_cancellation_wins(
    database_url: URL,
    setup: asyncpg.Connection[asyncpg.Record],
    service: RetryTransitionService,
) -> None:
    facts = await add_retry_pending_task(
        setup, workflow_policy={"retry_policy": retry_policy()}
    )
    blocker = await asyncpg.connect(asyncpg_dsn(database_url))
    observer = await asyncpg.connect(asyncpg_dsn(database_url))
    transaction = blocker.transaction()
    await transaction.start()
    pending: asyncio.Task[object] | None = None
    try:
        await blocker.execute(
            "SELECT id FROM workflow_runs WHERE id = $1 FOR UPDATE",
            facts.workflow_run_id,
        )
        pending = asyncio.create_task(service.transition_retry(facts.task_run_id))
        await wait_for_lock_waiter(observer)
        await blocker.execute(
            "UPDATE workflow_runs SET status = 'cancelling' WHERE id = $1",
            facts.workflow_run_id,
        )
        await transaction.commit()
        result = await pending
        assert result.outcome is RetryTransitionOutcome.NOT_ELIGIBLE
        assert len(await scheduled_shape(setup, facts.task_run_id)) == 1
        assert (
            await setup.fetchval(
                "SELECT status::text FROM task_runs WHERE id = $1", facts.task_run_id
            )
            == "retry_pending"
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


async def _exercise_rollback(
    setup: asyncpg.Connection[asyncpg.Record], service: RetryTransitionService
) -> None:
    facts = await add_retry_pending_task(
        setup, workflow_policy={"retry_policy": retry_policy()}
    )
    await setup.execute(
        "CREATE FUNCTION reject_retry_event() RETURNS trigger LANGUAGE plpgsql "
        "AS $$ BEGIN RAISE EXCEPTION 'forced retry event rollback'; END $$"
    )
    await setup.execute(
        "CREATE TRIGGER reject_retry_event_trigger BEFORE INSERT ON task_retry_events "
        "FOR EACH ROW EXECUTE FUNCTION reject_retry_event()"
    )
    try:
        with pytest.raises(RetryTransitionServiceUnavailable):
            await service.transition_retry(facts.task_run_id)
        assert len(await scheduled_shape(setup, facts.task_run_id)) == 1
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM task_retry_events WHERE task_run_id = $1",
                facts.task_run_id,
            )
            == 0
        )
        assert (
            await setup.fetchval(
                "SELECT status::text FROM task_runs WHERE id = $1", facts.task_run_id
            )
            == "retry_pending"
        )
    finally:
        await setup.execute(
            "DROP TRIGGER reject_retry_event_trigger ON task_retry_events"
        )
        await setup.execute("DROP FUNCTION reject_retry_event()")

    exhausted = await add_retry_pending_task(
        setup, workflow_policy={"retry_policy": retry_policy(maximum_attempts=1)}
    )
    await setup.execute(
        "CREATE FUNCTION reject_retry_dead_letter_status() RETURNS trigger "
        "LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION "
        "'forced dead-letter status rollback'; END $$"
    )
    await setup.execute(
        "CREATE TRIGGER reject_retry_dead_letter_status_trigger BEFORE INSERT ON "
        "dead_letter_status FOR EACH ROW EXECUTE FUNCTION "
        "reject_retry_dead_letter_status()"
    )
    try:
        with pytest.raises(RetryTransitionServiceUnavailable):
            await service.transition_retry(exhausted.task_run_id)
        assert (
            await setup.fetchval(
                "SELECT status::text FROM task_runs WHERE id = $1",
                exhausted.task_run_id,
            )
            == "retry_pending"
        )
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM task_retry_events WHERE task_run_id = $1",
                exhausted.task_run_id,
            )
            == 0
        )
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM dead_letter_items "
                "WHERE source_task_attempt_id = $1",
                exhausted.failed_attempt_id,
            )
            == 0
        )
    finally:
        await setup.execute(
            "DROP TRIGGER reject_retry_dead_letter_status_trigger ON dead_letter_status"
        )
        await setup.execute("DROP FUNCTION reject_retry_dead_letter_status()")


def test_real_postgresql_retry_transitions() -> None:
    with temporary_database(
        "TASKFORGE_RETRY_TEST_DATABASE_URL", "taskforge_retry_transition"
    ) as database_url:
        configuration = Config("alembic.ini")
        with migration_database_url(database_url.render_as_string(hide_password=False)):
            command.upgrade(configuration, "head")
        asyncio.run(exercise_retry_transitions(database_url))
