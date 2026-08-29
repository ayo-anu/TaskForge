"""Real PostgreSQL task claim acquisition and concurrency verification."""

from __future__ import annotations

import asyncio
import json
import os
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from taskforge.claims.authority import TaskClaimResultAuthorityIssuer
from taskforge.claims.domain import (
    IssuedTaskClaim,
    TaskClaimOutcome,
    TaskClaimRejected,
    TaskClaimRejectionReason,
)
from taskforge.claims.persistence_ports import (
    TaskClaimAttemptStale,
    TaskClaimWorkerUnavailable,
)
from taskforge.claims.service import (
    TaskClaimService,
    TaskClaimServiceInvariantError,
    TaskClaimServiceUnavailable,
)
from taskforge.dispatch.envelope import (
    DispatchEnvelope,
    create_dispatch_envelope,
    deserialize_dispatch_envelope,
    dispatch_envelope_to_mapping,
)
from taskforge.dispatch.persistence_ports import NewTaskAttempt, NewTaskDispatchOutbox
from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.persistence.claims import SQLAlchemyTaskClaimRepository
from taskforge.persistence.database import build_session_factory
from taskforge.persistence.dispatch import SQLAlchemyTaskDispatchRepository
from tests.integration.postgresql import (
    ExpectedStatusExecutionEvent,
    assert_status_execution_events,
    asyncpg_dsn,
    migration_database_url,
    temporary_database,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_CLAIM_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_CLAIM_INTEGRATION=1 explicitly",
    ),
]


@dataclass(frozen=True)
class WorkerFacts:
    authenticated: AuthenticatedWorker
    session_id: UUID


async def add_worker(
    connection: asyncpg.Connection[asyncpg.Record],
    *,
    capability: str = "test-capability",
    expired: bool = False,
) -> WorkerFacts:
    identity_id, credential_id, session_id = uuid4(), uuid4(), uuid4()
    await connection.execute(
        "INSERT INTO worker_identities (id, name) VALUES ($1, $2)",
        identity_id,
        f"claim-worker-{uuid4().hex}",
    )
    now = datetime.now(UTC)
    created_at = now - timedelta(seconds=2) if expired else now
    expires_at = now - timedelta(seconds=1) if expired else None
    await connection.execute(
        "INSERT INTO worker_credentials "
        "(id, worker_identity_id, credential_verifier, created_at, expires_at) "
        "VALUES ($1, $2, 'unused', $3, $4)",
        credential_id,
        identity_id,
        created_at,
        expires_at,
    )
    registered_at = await connection.fetchval(
        "INSERT INTO worker_sessions (id, worker_identity_id) VALUES ($1, $2) "
        "RETURNING registered_at",
        session_id,
        identity_id,
    )
    await connection.execute(
        "INSERT INTO worker_session_health "
        "(worker_session_id, last_seen_at, accepting_work, availability_changed_at) "
        "VALUES ($1, $2, true, $2)",
        session_id,
        registered_at,
    )
    await connection.execute(
        "INSERT INTO worker_session_capabilities (worker_session_id, capability) "
        "VALUES ($1, $2)",
        session_id,
        capability,
    )
    return WorkerFacts(AuthenticatedWorker(identity_id, credential_id), session_id)


async def add_dispatched_task(
    connection: asyncpg.Connection[asyncpg.Record],
    *,
    status: str = "dispatched",
    attempt_number: int = 1,
    persist_dispatch: bool = True,
    workflow_policy: str | None = None,
) -> DispatchEnvelope:
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
        f"claim-owner-{uuid4().hex}",
    )
    await connection.execute(
        "INSERT INTO workflow_definitions (id, owner_principal_id, name) "
        "VALUES ($1, $2, $3)",
        workflow_id,
        principal_id,
        f"claim-workflow-{uuid4().hex}",
    )
    await connection.execute(
        "INSERT INTO workflow_versions "
        "(id, workflow_definition_id, version_number, name, execution_policy) "
        "VALUES ($1, $2, 1, 'v1', $3::jsonb)",
        version_id,
        workflow_id,
        workflow_policy,
    )
    await connection.execute(
        "INSERT INTO workflow_version_steps "
        "(workflow_version_id, step_identifier, task_type, parameters) "
        "VALUES ($1, 'step', 'test.task', '{}'::jsonb)",
        version_id,
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
        "VALUES ($1, $2, $3, 'step', $4)",
        task_id,
        run_id,
        version_id,
        status,
    )
    envelope = create_dispatch_envelope(
        dispatch_id=dispatch_id,
        task_attempt_id=attempt_id,
        task_run_id=task_id,
        workflow_run_id=run_id,
        attempt_number=attempt_number,
        task_type="test.task",
        required_capability="test-capability",
        task_payload={},
        references={},
    )
    if persist_dispatch:
        await connection.execute(
            "INSERT INTO task_attempts (id, task_run_id, attempt_number) "
            "VALUES ($1, $2, $3)",
            attempt_id,
            task_id,
            attempt_number,
        )
        await connection.execute(
            "INSERT INTO task_dispatch_outbox (id, task_attempt_id, route, payload) "
            "VALUES ($1, $2, $3, $4::jsonb)",
            dispatch_id,
            attempt_id,
            envelope.route,
            json.dumps(dispatch_envelope_to_mapping(envelope)),
        )
    return envelope


async def wait_for_lock_waiter(
    observer: asyncpg.Connection[asyncpg.Record], *, minimum: int = 1
) -> None:
    for _ in range(200):
        count = await observer.fetchval(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() "
            "AND pid <> pg_backend_pid() AND wait_event_type = 'Lock'"
        )
        if count >= minimum:
            return
        await asyncio.sleep(0.01)
    pytest.fail("expected PostgreSQL lock waiter was not observed")


async def run_serialized_claim_race(
    database_url: URL,
    service: TaskClaimService,
    dispatch: DispatchEnvelope,
    contenders: tuple[WorkerFacts, WorkerFacts],
) -> tuple[IssuedTaskClaim | BaseException, IssuedTaskClaim | BaseException]:
    blocker = await asyncpg.connect(asyncpg_dsn(database_url))
    observer = await asyncpg.connect(asyncpg_dsn(database_url))
    transaction = blocker.transaction()
    await transaction.start()
    pending: (
        tuple[asyncio.Task[IssuedTaskClaim], asyncio.Task[IssuedTaskClaim]] | None
    ) = None
    try:
        await blocker.execute(
            "SELECT id FROM task_runs WHERE id = $1 FOR UPDATE", dispatch.task_run_id
        )
        pending = (
            asyncio.create_task(
                service.claim_task(
                    contenders[0].authenticated,
                    contenders[0].session_id,
                    dispatch,
                )
            ),
            asyncio.create_task(
                service.claim_task(
                    contenders[1].authenticated,
                    contenders[1].session_id,
                    dispatch,
                )
            ),
        )
        await wait_for_lock_waiter(observer, minimum=2)
        await transaction.commit()
        first, second = await asyncio.gather(*pending, return_exceptions=True)
        return first, second
    finally:
        if pending is not None:
            for task in pending:
                if not task.done():
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
        if blocker.is_in_transaction():
            await transaction.rollback()
        await blocker.close()
        await observer.close()


async def exercise_database_outage_during_claim(
    setup: asyncpg.Connection[asyncpg.Record],
    database_url: URL,
    service: TaskClaimService,
) -> None:
    dispatch = await add_dispatched_task(setup)
    worker = await add_worker(setup)
    blocker = await asyncpg.connect(asyncpg_dsn(database_url))
    observer = await asyncpg.connect(asyncpg_dsn(database_url))
    transaction = blocker.transaction()
    pending: asyncio.Task[IssuedTaskClaim] | None = None
    await transaction.start()
    try:
        await blocker.execute(
            "SELECT id FROM task_runs WHERE id = $1 FOR UPDATE", dispatch.task_run_id
        )
        pending = asyncio.create_task(
            service.claim_task(worker.authenticated, worker.session_id, dispatch)
        )
        await wait_for_lock_waiter(observer)
        waiter_pid = await observer.fetchval(
            "SELECT pid FROM pg_stat_activity WHERE datname = current_database() "
            "AND pid <> ALL($1::int[]) AND wait_event_type = 'Lock' "
            "ORDER BY query_start LIMIT 1",
            [blocker.get_server_pid(), observer.get_server_pid()],
        )
        assert isinstance(waiter_pid, int)
        assert await observer.fetchval("SELECT pg_terminate_backend($1)", waiter_pid)
        with pytest.raises(TaskClaimServiceUnavailable):
            await pending
    finally:
        if pending is not None and not pending.done():
            pending.cancel()
            with suppress(asyncio.CancelledError):
                await pending
        if blocker.is_in_transaction():
            await transaction.rollback()
        await blocker.close()
        await observer.close()


async def exercise_claim_acquisition(database_url: URL) -> None:
    dsn = asyncpg_dsn(database_url)
    setup = await asyncpg.connect(dsn)
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
    )
    repository = SQLAlchemyTaskClaimRepository(
        build_session_factory(engine), worker_stale_after_seconds=30
    )
    service = TaskClaimService(
        repository,
        TaskClaimResultAuthorityIssuer(b"claim-acquisition-integration-secret"),
        lease_seconds=60,
    )

    cancellation_worker = await add_worker(setup)
    cancelling_dispatch = await add_dispatched_task(setup)
    await setup.execute(
        "UPDATE workflow_runs SET status = 'cancelling' WHERE id = $1",
        cancelling_dispatch.workflow_run_id,
    )
    with pytest.raises(TaskClaimRejected) as rejected:
        await service.claim_task(
            cancellation_worker.authenticated,
            cancellation_worker.session_id,
            cancelling_dispatch,
        )
    assert rejected.value.reason is TaskClaimRejectionReason.OBSOLETE_TASK
    assert (
        await setup.fetchval(
            "SELECT status::text FROM task_runs WHERE id = $1",
            cancelling_dispatch.task_run_id,
        )
        == "dispatched"
    )
    try:
        first_worker = await add_worker(setup)
        second_worker = await add_worker(setup)
        dispatch = await add_dispatched_task(setup)

        first, second = await run_serialized_claim_race(
            database_url,
            service,
            dispatch,
            (first_worker, second_worker),
        )
        results = (first, second)
        assert (
            sum(
                getattr(result, "outcome", None) is TaskClaimOutcome.ACQUIRED_ACTIVE
                for result in results
            )
            == 1
        )
        rejection = next(result for result in results if isinstance(result, Exception))
        assert isinstance(rejection, TaskClaimRejected)
        assert rejection.reason is TaskClaimRejectionReason.ALREADY_AUTHORITATIVE
        winner = first_worker if not isinstance(first, Exception) else second_worker

        await setup.execute(
            "UPDATE worker_session_health SET accepting_work = false, "
            "last_seen_at = statement_timestamp() - interval '1 hour', "
            "availability_changed_at = statement_timestamp() - interval '1 hour' "
            "WHERE worker_session_id = $1",
            winner.session_id,
        )
        await setup.execute(
            "DELETE FROM worker_session_capabilities WHERE worker_session_id = $1",
            winner.session_id,
        )
        replay = await service.claim_task(
            winner.authenticated, winner.session_id, dispatch
        )
        assert replay.outcome is TaskClaimOutcome.REPLAYED_ACTIVE
        assert replay.result_authority is not None
        await assert_status_execution_events(
            setup,
            dispatch.workflow_run_id,
            (
                ExpectedStatusExecutionEvent(
                    dispatch.task_run_id, "dispatched", "claimed"
                ),
            ),
        )
        original = next(
            result for result in results if not isinstance(result, Exception)
        )
        assert isinstance(original, IssuedTaskClaim)
        assert replay.claim == original.claim

        await setup.execute(
            "UPDATE task_attempt_claims SET lease_expires_at = acquired_at + "
            "interval '1 microsecond' WHERE task_attempt_id = $1",
            dispatch.task_attempt_id,
        )
        expired = await service.claim_task(
            winner.authenticated, winner.session_id, dispatch
        )
        assert expired.outcome is TaskClaimOutcome.REPLAYED_EXPIRED
        assert expired.claim.generation == original.claim.generation
        assert expired.result_authority is None

        await exercise_service_rejections(setup, service)
        await exercise_database_outage_during_claim(setup, database_url, service)

        duplicate_dispatch = await add_dispatched_task(setup)
        duplicate_worker = await add_worker(setup)
        duplicate_results = await run_serialized_claim_race(
            database_url,
            service,
            duplicate_dispatch,
            (duplicate_worker, duplicate_worker),
        )
        duplicate_first, duplicate_second = duplicate_results
        assert isinstance(duplicate_first, IssuedTaskClaim)
        assert isinstance(duplicate_second, IssuedTaskClaim)
        assert {duplicate_first.outcome, duplicate_second.outcome} == {
            TaskClaimOutcome.ACQUIRED_ACTIVE,
            TaskClaimOutcome.REPLAYED_ACTIVE,
        }
        assert duplicate_first.claim == duplicate_second.claim

        history_dispatch = await add_dispatched_task(setup)
        history_worker = await add_worker(setup)
        history_contender = await add_worker(setup)
        await setup.execute(
            "INSERT INTO task_attempt_claims "
            "(task_attempt_id, generation, worker_session_id, lease_expires_at, "
            "terminated_at) VALUES ($1, 7, $2, statement_timestamp() + "
            "interval '1 minute', statement_timestamp())",
            history_dispatch.task_attempt_id,
            history_worker.session_id,
        )
        history_results = await run_serialized_claim_race(
            database_url,
            service,
            history_dispatch,
            (history_worker, history_contender),
        )
        history_result = next(
            result for result in history_results if isinstance(result, IssuedTaskClaim)
        )
        assert history_result.claim.generation == 8
        assert (
            sum(
                isinstance(result, TaskClaimRejected)
                and result.reason is TaskClaimRejectionReason.ALREADY_AUTHORITATIVE
                for result in history_results
            )
            == 1
        )

        unavailable_dispatch = await add_dispatched_task(setup)
        unavailable_worker = await add_worker(setup)
        await setup.execute(
            "UPDATE worker_session_health SET accepting_work = false "
            "WHERE worker_session_id = $1",
            unavailable_worker.session_id,
        )
        with pytest.raises(TaskClaimWorkerUnavailable):
            await repository.acquire_claim(
                unavailable_worker.authenticated,
                unavailable_worker.session_id,
                unavailable_dispatch,
                lease_seconds=60,
            )
        assert not await setup.fetchval(
            "SELECT EXISTS (SELECT FROM task_attempt_claims WHERE task_attempt_id = $1)",
            unavailable_dispatch.task_attempt_id,
        )

        stale_dispatch = await add_dispatched_task(setup)
        await setup.execute(
            "INSERT INTO task_attempts (id, task_run_id, attempt_number) "
            "VALUES ($1, $2, 2)",
            uuid4(),
            stale_dispatch.task_run_id,
        )
        with pytest.raises(TaskClaimAttemptStale):
            await repository.acquire_claim(
                history_worker.authenticated,
                history_worker.session_id,
                stale_dispatch,
                lease_seconds=60,
            )

        await exercise_authority_lifecycle(setup, service, database_url)
        await exercise_health_mutation_race(setup, service, database_url)

        rollback_dispatch = await add_dispatched_task(setup)
        rollback_worker = await add_worker(setup)
        await setup.execute(
            "CREATE FUNCTION reject_claimed_transition() RETURNS trigger "
            "LANGUAGE plpgsql AS $$ BEGIN IF NEW.id = '"
            + str(rollback_dispatch.task_run_id)
            + "'::uuid AND NEW.status = 'claimed' THEN RAISE EXCEPTION "
            "'forced claim rollback'; END IF; RETURN NEW; END $$"
        )
        await setup.execute(
            "CREATE TRIGGER reject_claimed_transition_trigger BEFORE UPDATE ON "
            "task_runs FOR EACH ROW EXECUTE FUNCTION reject_claimed_transition()"
        )
        with pytest.raises(TaskClaimServiceUnavailable):
            await service.claim_task(
                rollback_worker.authenticated,
                rollback_worker.session_id,
                rollback_dispatch,
            )
        assert not await setup.fetchval(
            "SELECT EXISTS (SELECT FROM task_attempt_claims WHERE task_attempt_id = $1)",
            rollback_dispatch.task_attempt_id,
        )
        assert not await setup.fetchval(
            "SELECT EXISTS (SELECT FROM task_claim_events WHERE task_attempt_id = $1)",
            rollback_dispatch.task_attempt_id,
        )
        assert (
            await setup.fetchval(
                "SELECT status::text FROM task_runs WHERE id = $1",
                rollback_dispatch.task_run_id,
            )
            == "dispatched"
        )
        await setup.execute(
            "DROP TRIGGER reject_claimed_transition_trigger ON task_runs"
        )
        await setup.execute("DROP FUNCTION reject_claimed_transition()")

        await exercise_session_and_capability_races(setup, service, database_url)
        await exercise_dispatch_and_independent_races(setup, service, database_url)
    finally:
        await setup.close()
        await engine.dispose()


async def acquisition_state(
    setup: asyncpg.Connection[asyncpg.Record], dispatch: DispatchEnvelope
) -> asyncpg.Record:
    state = await setup.fetchrow(
        "SELECT task_runs.status::text AS status, task_runs.updated_at, "
        "count(task_attempt_claims.generation) AS claim_count "
        "FROM task_attempts JOIN task_runs "
        "ON task_runs.id = task_attempts.task_run_id "
        "LEFT JOIN task_attempt_claims ON task_attempt_claims.task_attempt_id = "
        "task_attempts.id WHERE task_attempts.id = $1 "
        "GROUP BY task_runs.id, task_runs.status, task_runs.updated_at",
        dispatch.task_attempt_id,
    )
    assert state is not None
    return state


async def assert_service_rejection_does_not_mutate(
    setup: asyncpg.Connection[asyncpg.Record],
    service: TaskClaimService,
    worker: WorkerFacts,
    dispatch: DispatchEnvelope,
    expected_reason: TaskClaimRejectionReason,
) -> None:
    before = await acquisition_state(setup, dispatch)
    with pytest.raises(TaskClaimRejected) as raised:
        await service.claim_task(worker.authenticated, worker.session_id, dispatch)
    after = await acquisition_state(setup, dispatch)

    assert raised.value.reason is expected_reason
    assert str(raised.value) == "task claim acquisition rejected"
    assert before == after


async def exercise_service_rejections(
    setup: asyncpg.Connection[asyncpg.Record], service: TaskClaimService
) -> None:
    unknown_task = await add_dispatched_task(setup)
    unknown_task_mapping = dispatch_envelope_to_mapping(unknown_task)
    unknown_task_mapping["task_run_id"] = str(uuid4())
    await assert_service_rejection_does_not_mutate(
        setup,
        service,
        await add_worker(setup),
        deserialize_dispatch_envelope(json.dumps(unknown_task_mapping).encode()),
        TaskClaimRejectionReason.INVALID_DISPATCH,
    )

    unknown_dispatch = await add_dispatched_task(setup)
    unknown_dispatch_mapping = dispatch_envelope_to_mapping(unknown_dispatch)
    unknown_dispatch_mapping["dispatch_id"] = str(uuid4())
    await assert_service_rejection_does_not_mutate(
        setup,
        service,
        await add_worker(setup),
        deserialize_dispatch_envelope(json.dumps(unknown_dispatch_mapping).encode()),
        TaskClaimRejectionReason.INVALID_DISPATCH,
    )

    stale_worker = await add_worker(setup)
    stale_dispatch = await add_dispatched_task(setup)
    await setup.execute(
        "INSERT INTO task_attempts (id, task_run_id, attempt_number) "
        "VALUES ($1, $2, 2)",
        uuid4(),
        stale_dispatch.task_run_id,
    )
    await assert_service_rejection_does_not_mutate(
        setup,
        service,
        stale_worker,
        stale_dispatch,
        TaskClaimRejectionReason.STALE_ATTEMPT,
    )

    mismatched_dispatch = await add_dispatched_task(setup)
    mismatched_delivery = create_dispatch_envelope(
        dispatch_id=mismatched_dispatch.dispatch_id,
        task_attempt_id=mismatched_dispatch.task_attempt_id,
        task_run_id=mismatched_dispatch.task_run_id,
        workflow_run_id=mismatched_dispatch.workflow_run_id,
        attempt_number=mismatched_dispatch.attempt_number,
        task_type=mismatched_dispatch.task_type,
        required_capability=mismatched_dispatch.required_capability,
        task_payload={"mismatched": True},
        references={},
    )
    await assert_service_rejection_does_not_mutate(
        setup,
        service,
        stale_worker,
        mismatched_delivery,
        TaskClaimRejectionReason.INVALID_DISPATCH,
    )

    corrupt_dispatch = await add_dispatched_task(setup, status="claimed")
    corrupt_worker = await add_worker(setup)
    with pytest.raises(TaskClaimServiceInvariantError):
        await service.claim_task(
            corrupt_worker.authenticated,
            corrupt_worker.session_id,
            corrupt_dispatch,
        )

    deadline_dispatch = await add_dispatched_task(setup)
    deadline_tampering = create_dispatch_envelope(
        dispatch_id=deadline_dispatch.dispatch_id,
        task_attempt_id=deadline_dispatch.task_attempt_id,
        task_run_id=deadline_dispatch.task_run_id,
        workflow_run_id=deadline_dispatch.workflow_run_id,
        attempt_number=deadline_dispatch.attempt_number,
        task_type=deadline_dispatch.task_type,
        required_capability=deadline_dispatch.required_capability,
        task_payload={},
        references={},
        deadline_at=datetime(2030, 1, 1, tzinfo=UTC),
    )
    await assert_service_rejection_does_not_mutate(
        setup,
        service,
        stale_worker,
        deadline_tampering,
        TaskClaimRejectionReason.INVALID_DISPATCH,
    )

    timeout_dispatch = await add_dispatched_task(setup)
    timeout_tampering = create_dispatch_envelope(
        dispatch_id=timeout_dispatch.dispatch_id,
        task_attempt_id=timeout_dispatch.task_attempt_id,
        task_run_id=timeout_dispatch.task_run_id,
        workflow_run_id=timeout_dispatch.workflow_run_id,
        attempt_number=timeout_dispatch.attempt_number,
        task_type=timeout_dispatch.task_type,
        required_capability=timeout_dispatch.required_capability,
        task_payload={},
        references={},
        execution_timeout_seconds=30,
    )
    await assert_service_rejection_does_not_mutate(
        setup,
        service,
        stale_worker,
        timeout_tampering,
        TaskClaimRejectionReason.INVALID_DISPATCH,
    )

    downgraded_mapping = dispatch_envelope_to_mapping(deadline_dispatch)
    downgraded_mapping["schema_version"] = 1
    downgraded_mapping.pop("deadline_at")
    downgraded_mapping.pop("execution_timeout_seconds")
    downgraded = deserialize_dispatch_envelope(
        json.dumps(downgraded_mapping).encode("utf-8")
    )
    await assert_service_rejection_does_not_mutate(
        setup,
        service,
        stale_worker,
        downgraded,
        TaskClaimRejectionReason.INVALID_DISPATCH,
    )

    version_two_mapping = dispatch_envelope_to_mapping(deadline_dispatch)
    version_two_mapping["schema_version"] = 2
    version_two_mapping.pop("execution_timeout_seconds")
    version_two = deserialize_dispatch_envelope(
        json.dumps(version_two_mapping).encode("utf-8")
    )
    await assert_service_rejection_does_not_mutate(
        setup,
        service,
        stale_worker,
        version_two,
        TaskClaimRejectionReason.INVALID_DISPATCH,
    )

    historical_dispatch = await add_dispatched_task(setup)
    historical_mapping = dispatch_envelope_to_mapping(historical_dispatch)
    historical_mapping["schema_version"] = 1
    historical_mapping.pop("deadline_at")
    historical_mapping.pop("execution_timeout_seconds")
    historical_v1 = deserialize_dispatch_envelope(
        json.dumps(historical_mapping).encode("utf-8")
    )
    await setup.execute(
        "UPDATE task_dispatch_outbox SET payload = $2::jsonb WHERE id = $1",
        historical_dispatch.dispatch_id,
        json.dumps(historical_mapping),
    )
    historical_worker = await add_worker(setup)
    historical_claim = await service.claim_task(
        historical_worker.authenticated,
        historical_worker.session_id,
        historical_v1,
    )
    assert historical_claim.outcome is TaskClaimOutcome.ACQUIRED_ACTIVE

    historical_v2_dispatch = await add_dispatched_task(setup)
    historical_v2_mapping = dispatch_envelope_to_mapping(historical_v2_dispatch)
    historical_v2_mapping["schema_version"] = 2
    historical_v2_mapping.pop("execution_timeout_seconds")
    historical_v2 = deserialize_dispatch_envelope(
        json.dumps(historical_v2_mapping).encode("utf-8")
    )
    await setup.execute(
        "UPDATE task_dispatch_outbox SET payload = $2::jsonb WHERE id = $1",
        historical_v2_dispatch.dispatch_id,
        json.dumps(historical_v2_mapping),
    )
    historical_v2_worker = await add_worker(setup)
    historical_v2_claim = await service.claim_task(
        historical_v2_worker.authenticated,
        historical_v2_worker.session_id,
        historical_v2,
    )
    assert historical_v2_claim.outcome is TaskClaimOutcome.ACQUIRED_ACTIVE

    nonclaimable_worker = await add_worker(setup)
    nonclaimable_dispatch = await add_dispatched_task(setup, status="succeeded")
    await assert_service_rejection_does_not_mutate(
        setup,
        service,
        nonclaimable_worker,
        nonclaimable_dispatch,
        TaskClaimRejectionReason.OBSOLETE_TASK,
    )

    disabled_worker = await add_worker(setup)
    disabled_dispatch = await add_dispatched_task(setup)
    await setup.execute(
        "UPDATE worker_identities SET disabled_at = statement_timestamp() "
        "WHERE id = $1",
        disabled_worker.authenticated.worker_identity_id,
    )
    await assert_service_rejection_does_not_mutate(
        setup,
        service,
        disabled_worker,
        disabled_dispatch,
        TaskClaimRejectionReason.WORKER_AUTHORITY_REJECTED,
    )

    unavailable_worker = await add_worker(setup)
    unavailable_dispatch = await add_dispatched_task(setup)
    await setup.execute(
        "UPDATE worker_session_health SET accepting_work = false "
        "WHERE worker_session_id = $1",
        unavailable_worker.session_id,
    )
    await assert_service_rejection_does_not_mutate(
        setup,
        service,
        unavailable_worker,
        unavailable_dispatch,
        TaskClaimRejectionReason.WORKER_UNAVAILABLE,
    )

    owner = await add_worker(setup)
    contender = await add_worker(setup)
    owned_dispatch = await add_dispatched_task(setup)
    acquired = await service.claim_task(
        owner.authenticated, owner.session_id, owned_dispatch
    )
    assert acquired.outcome is TaskClaimOutcome.ACQUIRED_ACTIVE
    await assert_service_rejection_does_not_mutate(
        setup,
        service,
        contender,
        owned_dispatch,
        TaskClaimRejectionReason.ALREADY_AUTHORITATIVE,
    )


async def exercise_session_and_capability_races(
    setup: asyncpg.Connection[asyncpg.Record],
    service: TaskClaimService,
    database_url: URL,
) -> None:
    for terminate in (True, False):
        worker = await add_worker(setup)
        dispatch = await add_dispatched_task(setup)
        blocker = await asyncpg.connect(asyncpg_dsn(database_url))
        observer = await asyncpg.connect(asyncpg_dsn(database_url))
        transaction = blocker.transaction()
        await transaction.start()
        pending: asyncio.Task[IssuedTaskClaim] | None = None
        try:
            await blocker.execute(
                "SELECT id FROM worker_sessions WHERE id = $1 FOR NO KEY UPDATE",
                worker.session_id,
            )
            pending = asyncio.create_task(
                service.claim_task(
                    worker.authenticated,
                    worker.session_id,
                    dispatch,
                )
            )
            await wait_for_lock_waiter(observer)
            if terminate:
                await blocker.execute(
                    "UPDATE worker_sessions SET ended_at = statement_timestamp() "
                    "WHERE id = $1",
                    worker.session_id,
                )
            else:
                await blocker.execute(
                    "DELETE FROM worker_session_capabilities "
                    "WHERE worker_session_id = $1",
                    worker.session_id,
                )
            await transaction.commit()
            with pytest.raises(TaskClaimRejected) as raised:
                await pending
            expected_reason = (
                TaskClaimRejectionReason.WORKER_SESSION_INACTIVE
                if terminate
                else TaskClaimRejectionReason.CAPABILITY_MISMATCH
            )
            assert raised.value.reason is expected_reason
        finally:
            if pending is not None and not pending.done():
                pending.cancel()
                with suppress(asyncio.CancelledError):
                    await pending
            if blocker.is_in_transaction():
                await transaction.rollback()
            await blocker.close()
            await observer.close()


async def exercise_authority_lifecycle(
    setup: asyncpg.Connection[asyncpg.Record],
    service: TaskClaimService,
    database_url: URL,
) -> None:
    disabled_worker = await add_worker(setup)
    disabled_dispatch = await add_dispatched_task(setup)
    await setup.execute(
        "UPDATE worker_identities SET disabled_at = statement_timestamp() WHERE id = $1",
        disabled_worker.authenticated.worker_identity_id,
    )
    with pytest.raises(TaskClaimRejected) as disabled:
        await service.claim_task(
            disabled_worker.authenticated,
            disabled_worker.session_id,
            disabled_dispatch,
        )
    assert disabled.value.reason is TaskClaimRejectionReason.WORKER_AUTHORITY_REJECTED

    revoked_worker = await add_worker(setup)
    revoked_dispatch = await add_dispatched_task(setup)
    await setup.execute(
        "UPDATE worker_credentials SET revoked_at = statement_timestamp() WHERE id = $1",
        revoked_worker.authenticated.credential_id,
    )
    with pytest.raises(TaskClaimRejected) as revoked:
        await service.claim_task(
            revoked_worker.authenticated,
            revoked_worker.session_id,
            revoked_dispatch,
        )
    assert revoked.value.reason is TaskClaimRejectionReason.WORKER_AUTHORITY_REJECTED

    expired_worker = await add_worker(setup, expired=True)
    expired_dispatch = await add_dispatched_task(setup)
    with pytest.raises(TaskClaimRejected) as expired:
        await service.claim_task(
            expired_worker.authenticated,
            expired_worker.session_id,
            expired_dispatch,
        )
    assert expired.value.reason is TaskClaimRejectionReason.WORKER_AUTHORITY_REJECTED

    replay_worker = await add_worker(setup)
    replay_dispatch = await add_dispatched_task(setup)
    acquired = await service.claim_task(
        replay_worker.authenticated,
        replay_worker.session_id,
        replay_dispatch,
    )
    assert acquired.outcome is TaskClaimOutcome.ACQUIRED_ACTIVE
    await setup.execute(
        "UPDATE worker_identities SET disabled_at = statement_timestamp() WHERE id = $1",
        replay_worker.authenticated.worker_identity_id,
    )
    with pytest.raises(TaskClaimRejected) as disabled_replay:
        await service.claim_task(
            replay_worker.authenticated,
            replay_worker.session_id,
            replay_dispatch,
        )
    assert (
        disabled_replay.value.reason
        is TaskClaimRejectionReason.WORKER_AUTHORITY_REJECTED
    )

    racing_worker = await add_worker(setup)
    racing_dispatch = await add_dispatched_task(setup)
    blocker = await asyncpg.connect(asyncpg_dsn(database_url))
    observer = await asyncpg.connect(asyncpg_dsn(database_url))
    transaction = blocker.transaction()
    await transaction.start()
    pending: asyncio.Task[IssuedTaskClaim] | None = None
    try:
        await blocker.execute(
            "SELECT id FROM worker_identities WHERE id = $1 FOR UPDATE",
            racing_worker.authenticated.worker_identity_id,
        )
        pending = asyncio.create_task(
            service.claim_task(
                racing_worker.authenticated,
                racing_worker.session_id,
                racing_dispatch,
            )
        )
        await wait_for_lock_waiter(observer)
        await blocker.execute(
            "UPDATE worker_identities SET disabled_at = statement_timestamp() "
            "WHERE id = $1",
            racing_worker.authenticated.worker_identity_id,
        )
        await transaction.commit()
        with pytest.raises(TaskClaimRejected) as rejected:
            await pending
        assert (
            rejected.value.reason is TaskClaimRejectionReason.WORKER_AUTHORITY_REJECTED
        )
    finally:
        if pending is not None and not pending.done():
            pending.cancel()
            with suppress(asyncio.CancelledError):
                await pending
        if blocker.is_in_transaction():
            await transaction.rollback()
        await blocker.close()
        await observer.close()


async def exercise_health_mutation_race(
    setup: asyncpg.Connection[asyncpg.Record],
    service: TaskClaimService,
    database_url: URL,
) -> None:
    worker = await add_worker(setup)
    dispatch = await add_dispatched_task(setup)
    blocker = await asyncpg.connect(asyncpg_dsn(database_url))
    observer = await asyncpg.connect(asyncpg_dsn(database_url))
    transaction = blocker.transaction()
    await transaction.start()
    pending: asyncio.Task[IssuedTaskClaim] | None = None
    try:
        await blocker.execute(
            "SELECT worker_session_id FROM worker_session_health "
            "WHERE worker_session_id = $1 FOR UPDATE",
            worker.session_id,
        )
        pending = asyncio.create_task(
            service.claim_task(
                worker.authenticated,
                worker.session_id,
                dispatch,
            )
        )
        await wait_for_lock_waiter(observer)
        await blocker.execute(
            "UPDATE worker_session_health SET accepting_work = false, "
            "last_seen_at = statement_timestamp(), "
            "availability_changed_at = statement_timestamp() WHERE worker_session_id = $1",
            worker.session_id,
        )
        await transaction.commit()
        with pytest.raises(TaskClaimRejected) as rejected:
            await pending
        assert rejected.value.reason is TaskClaimRejectionReason.WORKER_UNAVAILABLE
        assert not await setup.fetchval(
            "SELECT EXISTS (SELECT FROM task_attempt_claims WHERE task_attempt_id = $1)",
            dispatch.task_attempt_id,
        )
    finally:
        if pending is not None and not pending.done():
            pending.cancel()
            with suppress(asyncio.CancelledError):
                await pending
        if blocker.is_in_transaction():
            await transaction.rollback()
        await blocker.close()
        await observer.close()


async def exercise_dispatch_and_independent_races(
    setup: asyncpg.Connection[asyncpg.Record],
    service: TaskClaimService,
    database_url: URL,
) -> None:
    worker = await add_worker(setup)
    dispatch = await add_dispatched_task(
        setup, status="runnable", persist_dispatch=False
    )
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
    )
    dispatch_repository = SQLAlchemyTaskDispatchRepository(
        build_session_factory(engine)
    )
    observer = await asyncpg.connect(asyncpg_dsn(database_url))
    pending: asyncio.Task[IssuedTaskClaim] | None = None
    try:
        async with dispatch_repository.dispatch_transaction() as transaction:
            prepared = await transaction.prepare_dispatch(
                dispatch.workflow_run_id, dispatch.task_run_id
            )
            assert prepared is not None
            await transaction.persist_dispatch(
                prepared,
                NewTaskAttempt(
                    dispatch.task_attempt_id,
                    dispatch.task_run_id,
                    dispatch.attempt_number,
                ),
                NewTaskDispatchOutbox(
                    dispatch.dispatch_id,
                    dispatch.task_attempt_id,
                    dispatch.route,
                    dispatch_envelope_to_mapping(dispatch),
                ),
            )
            pending = asyncio.create_task(
                service.claim_task(
                    worker.authenticated,
                    worker.session_id,
                    dispatch,
                )
            )
            await wait_for_lock_waiter(observer)
            assert not await setup.fetchval(
                "SELECT EXISTS (SELECT FROM task_attempts WHERE id = $1)",
                dispatch.task_attempt_id,
            )
            await transaction.commit()
        assert pending is not None
        assert (await pending).outcome is TaskClaimOutcome.ACQUIRED_ACTIVE
    finally:
        if pending is not None and not pending.done():
            pending.cancel()
            with suppress(asyncio.CancelledError):
                await pending
        await observer.close()
        await engine.dispose()

    blocked_dispatch = await add_dispatched_task(setup)
    free_dispatch = await add_dispatched_task(setup)
    blocker = await asyncpg.connect(asyncpg_dsn(database_url))
    observer = await asyncpg.connect(asyncpg_dsn(database_url))
    transaction = blocker.transaction()
    await transaction.start()
    blocked: asyncio.Task[IssuedTaskClaim] | None = None
    try:
        await blocker.execute(
            "SELECT id FROM task_runs WHERE id = $1 FOR UPDATE",
            blocked_dispatch.task_run_id,
        )
        blocked = asyncio.create_task(
            service.claim_task(
                worker.authenticated,
                worker.session_id,
                blocked_dispatch,
            )
        )
        await wait_for_lock_waiter(observer)
        independent = await asyncio.wait_for(
            service.claim_task(
                worker.authenticated,
                worker.session_id,
                free_dispatch,
            ),
            timeout=2,
        )
        assert independent.outcome is TaskClaimOutcome.ACQUIRED_ACTIVE
        await transaction.commit()
        assert (await blocked).outcome is TaskClaimOutcome.ACQUIRED_ACTIVE
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM task_claim_events WHERE task_attempt_id = "
                "ANY($1::uuid[])",
                [blocked_dispatch.task_attempt_id, free_dispatch.task_attempt_id],
            )
            == 2
        )
    finally:
        if blocked is not None and not blocked.done():
            blocked.cancel()
            with suppress(asyncio.CancelledError):
                await blocked
        if blocker.is_in_transaction():
            await transaction.rollback()
        await blocker.close()
        await observer.close()


def test_real_postgresql_claim_acquisition_and_concurrency() -> None:
    with temporary_database(
        "TASKFORGE_CLAIM_TEST_DATABASE_URL", "taskforge_claim_acquisition"
    ) as database_url:
        configuration = Config("alembic.ini")
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        with migration_database_url(alembic_url):
            command.upgrade(configuration, "head")
        asyncio.run(exercise_claim_acquisition(database_url))
