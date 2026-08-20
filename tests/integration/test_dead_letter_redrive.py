"""Real PostgreSQL redrive materialization, idempotency, and rollback tests."""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from uuid import uuid4

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL

from taskforge.dead_letters.domain import (
    CreatedDeadLetterRedrive,
    DeadLetterRedriveIdempotency,
    DeadLetterRedriveIdempotencyConflict,
    create_dead_letter_redrive_idempotency,
)
from taskforge.dead_letters.persistence_ports import (
    DeadLetterPersistenceUnavailable,
    DeadLetterRedriveLimitExceeded,
    DeadLetterRedriveNotEligible,
)
from taskforge.dispatch.service import TaskDispatchService
from taskforge.identity.authorization import OwnerFilter
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.dispatch import SQLAlchemyTaskDispatchRepository
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
from tests.integration.test_authentication_persistence import settings_for
from tests.integration.test_dead_letter_migrations import Facts, seed_facts

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_DEAD_LETTER_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_DEAD_LETTER_INTEGRATION=1 explicitly",
    ),
]


@dataclass(frozen=True)
class AcceptParameters:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        del parameters
        return ()


async def _seed(connection: asyncpg.Connection) -> Facts:
    facts = await seed_facts(connection)
    await connection.execute(
        "INSERT INTO workflow_run_inputs (workflow_run_id, payload, input_references) "
        "SELECT workflow_run_id, '{\"safe\": 1}'::jsonb, "
        '\'{"secret": "vault://source"}\'::jsonb FROM task_runs WHERE id = $1',
        facts.task_run,
    )
    return facts


def _idempotency(
    facts: Facts, key: str, reason: str | None = None
) -> DeadLetterRedriveIdempotency:
    return create_dead_letter_redrive_idempotency(
        key,
        dead_letter_item_id=facts.item,
        requested_by_principal_id=facts.principal,
        reason=reason,
    )


async def verify_materialization_and_idempotency(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        facts = await _seed(connection)
    finally:
        await connection.close()
    engine = build_async_engine(settings_for(database_url))
    from taskforge.persistence.dead_letter_operations import (
        SQLAlchemyDeadLetterRepository,
    )

    repository = SQLAlchemyDeadLetterRepository(build_session_factory(engine))
    owner = OwnerFilter.only(facts.principal)
    idempotency = _idempotency(facts, "redrive-key-0001", "corrected")

    async def command() -> CreatedDeadLetterRedrive | None:
        return await repository.redrive(
            facts.item,
            owner,
            operator_principal_id=facts.principal,
            idempotency=idempotency,
            reason="corrected",
            correlation_id=uuid4(),
        )

    try:
        first, replay = await asyncio.gather(command(), command())
        assert first is not None and replay is not None
        assert first == replay
        assert first.target_workflow_run_id != first.source_workflow_run_id
        detail = await repository.get_item(facts.item, owner)
        assert detail is not None and detail.redrive is not None
        assert detail.redrive.target_workflow_run_id == first.target_workflow_run_id
        assert detail.redrive.target_workflow_run_status == "pending"

        check = await asyncpg.connect(asyncpg_dsn(database_url))
        try:
            assert (
                await check.fetchval(
                    "SELECT count(*) FROM dead_letter_redrive_requests "
                    "WHERE dead_letter_item_id = $1",
                    facts.item,
                )
                == 1
            )
            target = await check.fetchrow(
                "SELECT status, workflow_version_id FROM workflow_runs WHERE id = $1",
                first.target_workflow_run_id,
            )
            assert tuple(target.values()) == ("pending", first.workflow_version_id)
            copied_input = await check.fetchrow(
                "SELECT payload, input_references FROM workflow_run_inputs "
                "WHERE workflow_run_id = $1",
                first.target_workflow_run_id,
            )
            assert json.loads(copied_input["payload"]) == {"safe": 1}
            assert json.loads(copied_input["input_references"]) == {
                "secret": "vault://source"
            }
            tasks = await check.fetch(
                "SELECT id, status FROM task_runs WHERE workflow_run_id = $1 "
                "ORDER BY step_identifier",
                first.target_workflow_run_id,
            )
            assert len(tasks) == 2
            assert all(row["status"] == "runnable" for row in tasks)
            assert all(
                row["id"] not in {facts.task_run, facts.other_task_run} for row in tasks
            )
            assert (
                await check.fetchval(
                    "SELECT count(*) FROM task_attempts WHERE task_run_id = ANY($1::uuid[])",
                    [row["id"] for row in tasks],
                )
                == 0
            )
            assert (
                await check.fetchval(
                    "SELECT count(*) FROM task_dispatch_outbox outbox JOIN task_attempts "
                    "attempt ON attempt.id = outbox.task_attempt_id "
                    "WHERE attempt.task_run_id = ANY($1::uuid[])",
                    [row["id"] for row in tasks],
                )
                == 0
            )
        finally:
            await check.close()

        dispatched = await TaskDispatchService(
            SQLAlchemyTaskDispatchRepository(build_session_factory(engine)),
            TaskTypeRegistry(
                (TaskTypeDefinition("test.task", "test.task", AcceptParameters()),)
            ),
        ).dispatch_task(first.target_workflow_run_id, tasks[0]["id"])
        assert dispatched.attempt_number == 1
        dispatch_check = await asyncpg.connect(asyncpg_dsn(database_url))
        try:
            assert (
                await dispatch_check.fetchval(
                    "SELECT count(*) FROM task_dispatch_outbox WHERE id = $1",
                    dispatched.dispatch_id,
                )
                == 1
            )
        finally:
            await dispatch_check.close()

        with pytest.raises(DeadLetterRedriveLimitExceeded):
            await repository.redrive(
                facts.item,
                owner,
                operator_principal_id=facts.principal,
                idempotency=_idempotency(facts, "redrive-key-0002"),
                reason=None,
                correlation_id=uuid4(),
            )
        with pytest.raises(DeadLetterRedriveIdempotencyConflict):
            await repository.redrive(
                facts.item,
                owner,
                operator_principal_id=facts.principal,
                idempotency=_idempotency(
                    facts, "redrive-key-0001", "different request"
                ),
                reason="different request",
                correlation_id=uuid4(),
            )

        async def different_key(key: str) -> CreatedDeadLetterRedrive | None:
            idempotency = create_dead_letter_redrive_idempotency(
                key,
                dead_letter_item_id=facts.other_item,
                requested_by_principal_id=facts.principal,
                reason=None,
            )
            return await repository.redrive(
                facts.other_item,
                owner,
                operator_principal_id=facts.principal,
                idempotency=idempotency,
                reason=None,
                correlation_id=uuid4(),
            )

        raced = await asyncio.gather(
            different_key("different-key-0001"),
            different_key("different-key-0002"),
            return_exceptions=True,
        )
        assert sum(isinstance(value, CreatedDeadLetterRedrive) for value in raced) == 1
        assert (
            sum(isinstance(value, DeadLetterRedriveLimitExceeded) for value in raced)
            == 1
        )

        generation_connection = await asyncpg.connect(asyncpg_dsn(database_url))
        try:
            target_task = await generation_connection.fetchval(
                "SELECT task.id FROM task_runs task WHERE task.workflow_run_id = $1 "
                "AND NOT EXISTS (SELECT 1 FROM task_attempts attempt "
                "WHERE attempt.task_run_id = task.id) ORDER BY task.id LIMIT 1",
                first.target_workflow_run_id,
            )
            worker_session = await generation_connection.fetchval(
                "SELECT id FROM worker_sessions ORDER BY id LIMIT 1"
            )
            attempt, dispatch, item = uuid4(), uuid4(), uuid4()
            await generation_connection.execute(
                "UPDATE task_runs SET status = 'failed' WHERE workflow_run_id = $1",
                first.target_workflow_run_id,
            )
            await generation_connection.execute(
                "UPDATE workflow_runs SET status = 'failed' WHERE id = $1",
                first.target_workflow_run_id,
            )
            await generation_connection.execute(
                "INSERT INTO task_attempts (id, task_run_id, attempt_number) "
                "VALUES ($1, $2, 1)",
                attempt,
                target_task,
            )
            await generation_connection.execute(
                "INSERT INTO task_dispatch_outbox (id, task_attempt_id, route, payload) "
                "VALUES ($1, $2, 'test.task', '{}'::jsonb)",
                dispatch,
                attempt,
            )
            await generation_connection.execute(
                "INSERT INTO task_attempt_claims (task_attempt_id, generation, "
                "worker_session_id, lease_expires_at, terminated_at) VALUES "
                "($1, 1, $2, statement_timestamp() + interval '1 minute', "
                "statement_timestamp())",
                attempt,
                worker_session,
            )
            await generation_connection.execute(
                "INSERT INTO task_attempt_results (task_attempt_id, claim_generation, "
                "dispatch_id, result_kind, failure_kind, result_fingerprint) VALUES "
                "($1, 1, $2, 'permanent_failure', 'handler_reported', $3)",
                attempt,
                dispatch,
                "9" * 64,
            )
            await generation_connection.execute(
                "INSERT INTO dead_letter_items "
                "(id, task_run_id, source_task_attempt_id, reason) VALUES "
                "($1, $2, $3, 'permanent_failure')",
                item,
                target_task,
                attempt,
            )
            await generation_connection.execute(
                "INSERT INTO dead_letter_status (dead_letter_item_id) VALUES ($1)",
                item,
            )
        finally:
            await generation_connection.close()

        second_generation = await repository.redrive(
            item,
            owner,
            operator_principal_id=facts.principal,
            idempotency=create_dead_letter_redrive_idempotency(
                "second-generation",
                dead_letter_item_id=item,
                requested_by_principal_id=facts.principal,
                reason=None,
            ),
            reason=None,
            correlation_id=uuid4(),
        )
        assert second_generation is not None
        assert second_generation.source_workflow_run_id == first.target_workflow_run_id
        chain = await asyncpg.connect(asyncpg_dsn(database_url))
        try:
            assert (
                await chain.fetchval(
                    "SELECT count(*) FROM dead_letter_redrive_requests first_request "
                    "JOIN dead_letter_items second_item ON second_item.id = $1 "
                    "JOIN task_runs second_source_task ON second_source_task.id = "
                    "second_item.task_run_id JOIN dead_letter_redrive_requests "
                    "second_request ON second_request.dead_letter_item_id = second_item.id "
                    "WHERE first_request.target_workflow_run_id = "
                    "second_source_task.workflow_run_id",
                    item,
                )
                == 1
            )
        finally:
            await chain.close()
    finally:
        await engine.dispose()


async def verify_eligibility_ownership_and_rollback(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        hidden = await _seed(connection)
        resolved = await _seed(connection)
        unsettled = await _seed(connection)
        rollback = await _seed(connection)
        await connection.execute(
            "UPDATE dead_letter_status SET status = 'resolved' "
            "WHERE dead_letter_item_id = $1",
            resolved.item,
        )
        await connection.execute(
            "UPDATE workflow_runs SET status = 'running' WHERE id = "
            "(SELECT workflow_run_id FROM task_runs WHERE id = $1)",
            unsettled.task_run,
        )
        before_runs = await connection.fetchval("SELECT count(*) FROM workflow_runs")
        await connection.execute(
            "CREATE FUNCTION reject_redrive_request() RETURNS trigger LANGUAGE "
            "plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected redrive failure'; END $$"
        )
        await connection.execute(
            "CREATE TRIGGER reject_redrive_request BEFORE INSERT ON "
            "dead_letter_redrive_requests FOR EACH ROW EXECUTE FUNCTION "
            "reject_redrive_request()"
        )
    finally:
        await connection.close()

    engine = build_async_engine(settings_for(database_url))
    from taskforge.persistence.dead_letter_operations import (
        SQLAlchemyDeadLetterRepository,
    )

    repository = SQLAlchemyDeadLetterRepository(build_session_factory(engine))

    async def redrive(
        facts: Facts, owner: OwnerFilter
    ) -> CreatedDeadLetterRedrive | None:
        return await repository.redrive(
            facts.item,
            owner,
            operator_principal_id=facts.principal,
            idempotency=_idempotency(facts, f"key-{facts.item.hex}"),
            reason=None,
            correlation_id=uuid4(),
        )

    try:
        assert await redrive(hidden, OwnerFilter.only(hidden.other_principal)) is None
        with pytest.raises(DeadLetterRedriveNotEligible):
            await redrive(resolved, OwnerFilter.only(resolved.principal))
        with pytest.raises(DeadLetterRedriveNotEligible):
            await redrive(unsettled, OwnerFilter.only(unsettled.principal))
        with pytest.raises(DeadLetterPersistenceUnavailable):
            await redrive(rollback, OwnerFilter.only(rollback.principal))

        check = await asyncpg.connect(asyncpg_dsn(database_url))
        try:
            assert (
                await check.fetchval("SELECT count(*) FROM workflow_runs")
                == before_runs
            )
            assert (
                await check.fetchval(
                    "SELECT count(*) FROM dead_letter_redrive_requests"
                )
                == 0
            )
        finally:
            await check.close()
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    "verification",
    (verify_materialization_and_idempotency, verify_eligibility_ownership_and_rollback),
)
def test_dead_letter_redrive(
    verification: Callable[[URL], Coroutine[object, object, None]],
) -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_dead_letter_redrive"
    ) as database_url:
        config = Config("alembic.ini")
        with migration_database_url(database_url.render_as_string(hide_password=False)):
            command.upgrade(config, "head")
        asyncio.run(verification(database_url))
