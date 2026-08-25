"""Real PostgreSQL claim-event atomicity and current-claim inspection."""

from __future__ import annotations

import asyncio
import os
from datetime import timedelta
from uuid import UUID, uuid4

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from taskforge.claims.authority import TaskClaimResultAuthorityIssuer
from taskforge.claims.domain import (
    TaskClaimLeaseStatus,
    TaskClaimOutcome,
    TaskClaimRejected,
    TaskClaimRenewalOutcome,
)
from taskforge.claims.persistence_ports import (
    TaskClaimInspectionNotFound,
    TaskClaimPersistenceUnavailable,
    TaskClaimRenewalExpired,
)
from taskforge.claims.service import TaskClaimService, TaskClaimServiceUnavailable
from taskforge.identity.authorization import OwnerFilter
from taskforge.persistence.claims import (
    SQLAlchemyTaskClaimInspectionRepository,
    SQLAlchemyTaskClaimRepository,
)
from taskforge.persistence.database import build_session_factory
from tests.integration.postgresql import (
    asyncpg_dsn,
    migration_database_url,
    temporary_database,
)
from tests.integration.test_task_claim_acquisition import (
    add_dispatched_task,
    add_worker,
    run_serialized_claim_race,
)
from tests.integration.test_task_claim_renewal import (
    add_current_claim,
    concurrent_same_owner_renewal,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_CLAIM_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_CLAIM_INTEGRATION=1 explicitly",
    ),
]


async def event_count(
    connection: asyncpg.Connection[asyncpg.Record],
    task_attempt_id: UUID,
    event_type: str | None = None,
) -> int:
    if event_type is None:
        return int(
            await connection.fetchval(
                "SELECT count(*) FROM task_claim_events WHERE task_attempt_id = $1",
                task_attempt_id,
            )
        )
    return int(
        await connection.fetchval(
            "SELECT count(*) FROM task_claim_events WHERE task_attempt_id = $1 "
            "AND event_type = $2",
            task_attempt_id,
            event_type,
        )
    )


async def install_event_failure_trigger(
    connection: asyncpg.Connection[asyncpg.Record], event_type: str
) -> None:
    await connection.execute(
        "CREATE FUNCTION reject_selected_claim_event() RETURNS trigger "
        "LANGUAGE plpgsql AS $$ BEGIN IF NEW.event_type = '"
        + event_type
        + "' THEN RAISE EXCEPTION 'forced event failure'; END IF; "
        "RETURN NEW; END $$"
    )
    await connection.execute(
        "CREATE TRIGGER reject_selected_claim_event_trigger BEFORE INSERT ON "
        "task_claim_events FOR EACH ROW EXECUTE FUNCTION "
        "reject_selected_claim_event()"
    )


async def remove_event_failure_trigger(
    connection: asyncpg.Connection[asyncpg.Record],
) -> None:
    await connection.execute(
        "DROP TRIGGER reject_selected_claim_event_trigger ON task_claim_events"
    )
    await connection.execute("DROP FUNCTION reject_selected_claim_event()")


async def exercise_acquisition_events(
    setup: asyncpg.Connection[asyncpg.Record],
    database_url: URL,
    service: TaskClaimService,
) -> None:
    worker = await add_worker(setup)
    dispatch = await add_dispatched_task(setup)
    acquired = await service.claim_task(
        worker.authenticated, worker.session_id, dispatch
    )
    assert acquired.outcome is TaskClaimOutcome.ACQUIRED_ACTIVE
    event = await setup.fetchrow(
        "SELECT occurred_at, previous_lease_expires_at, lease_expires_at "
        "FROM task_claim_events WHERE task_attempt_id = $1",
        dispatch.task_attempt_id,
    )
    assert event is not None
    assert event["occurred_at"] == acquired.claim.acquired_at
    assert event["previous_lease_expires_at"] is None
    assert event["lease_expires_at"] == acquired.claim.lease_expires_at

    replay = await service.claim_task(worker.authenticated, worker.session_id, dispatch)
    assert replay.outcome is TaskClaimOutcome.REPLAYED_ACTIVE
    assert await event_count(setup, dispatch.task_attempt_id) == 1

    duplicate_dispatch = await add_dispatched_task(setup)
    duplicate_worker = await add_worker(setup)
    duplicate_results = await run_serialized_claim_race(
        database_url,
        service,
        duplicate_dispatch,
        (duplicate_worker, duplicate_worker),
    )
    assert {getattr(result, "outcome", None) for result in duplicate_results} == {
        TaskClaimOutcome.ACQUIRED_ACTIVE,
        TaskClaimOutcome.REPLAYED_ACTIVE,
    }
    assert await event_count(setup, duplicate_dispatch.task_attempt_id) == 1

    contested_dispatch = await add_dispatched_task(setup)
    contenders = (await add_worker(setup), await add_worker(setup))
    await run_serialized_claim_race(
        database_url, service, contested_dispatch, contenders
    )
    assert await event_count(setup, contested_dispatch.task_attempt_id) == 1

    rejected_dispatch = await add_dispatched_task(setup, status="succeeded")
    with pytest.raises(TaskClaimRejected):
        await service.claim_task(
            worker.authenticated, worker.session_id, rejected_dispatch
        )
    assert await event_count(setup, rejected_dispatch.task_attempt_id) == 0


async def exercise_acquisition_event_failure(
    setup: asyncpg.Connection[asyncpg.Record], service: TaskClaimService
) -> None:
    worker = await add_worker(setup)
    dispatch = await add_dispatched_task(setup)
    await install_event_failure_trigger(setup, "claim_acquired")
    try:
        with pytest.raises(TaskClaimServiceUnavailable):
            await service.claim_task(worker.authenticated, worker.session_id, dispatch)
    finally:
        await remove_event_failure_trigger(setup)
    assert await event_count(setup, dispatch.task_attempt_id) == 0
    assert not await setup.fetchval(
        "SELECT EXISTS (SELECT FROM task_attempt_claims WHERE task_attempt_id = $1)",
        dispatch.task_attempt_id,
    )
    assert (
        await setup.fetchval(
            "SELECT status::text FROM task_runs WHERE id = $1", dispatch.task_run_id
        )
        == "dispatched"
    )


async def exercise_renewal_events(
    setup: asyncpg.Connection[asyncpg.Record],
    database_url: URL,
    repository: SQLAlchemyTaskClaimRepository,
) -> None:
    worker = await add_worker(setup)
    request = await add_current_claim(
        setup, worker, lease_interval=timedelta(seconds=5)
    )
    renewed = await repository.renew_claim(
        worker.authenticated, request, lease_seconds=60
    )
    assert renewed.outcome is TaskClaimRenewalOutcome.RENEWED
    event = await setup.fetchrow(
        "SELECT occurred_at, previous_lease_expires_at, lease_expires_at "
        "FROM task_claim_events WHERE task_attempt_id = $1",
        request.task_attempt_id,
    )
    assert event is not None
    assert event["previous_lease_expires_at"] == request.expected_lease_expires_at
    assert event["lease_expires_at"] == renewed.claim.lease_expires_at
    assert event["occurred_at"] <= event["lease_expires_at"]

    replay = await repository.renew_claim(
        worker.authenticated, request, lease_seconds=60
    )
    assert replay.outcome is TaskClaimRenewalOutcome.REPLAYED
    assert await event_count(setup, request.task_attempt_id) == 1

    unchanged_worker = await add_worker(setup)
    unchanged = await add_current_claim(
        setup, unchanged_worker, lease_interval=timedelta(minutes=2)
    )
    result = await repository.renew_claim(
        unchanged_worker.authenticated, unchanged, lease_seconds=60
    )
    assert result.outcome is TaskClaimRenewalOutcome.ACTIVE_UNCHANGED
    assert await event_count(setup, unchanged.task_attempt_id) == 0

    race_worker = await add_worker(setup)
    race_request = await add_current_claim(
        setup, race_worker, lease_interval=timedelta(seconds=5)
    )
    race_results = await concurrent_same_owner_renewal(
        setup, database_url, repository, race_worker, race_request
    )
    assert {result.outcome for result in race_results} == {
        TaskClaimRenewalOutcome.RENEWED,
        TaskClaimRenewalOutcome.REPLAYED,
    }
    assert await event_count(setup, race_request.task_attempt_id) == 1

    expired_worker = await add_worker(setup)
    expired = await add_current_claim(
        setup, expired_worker, lease_interval=timedelta(seconds=1)
    )
    await setup.execute(
        "UPDATE task_attempt_claims SET lease_expires_at = acquired_at + "
        "interval '1 microsecond' WHERE task_attempt_id = $1",
        expired.task_attempt_id,
    )
    with pytest.raises(TaskClaimRenewalExpired):
        await repository.renew_claim(
            expired_worker.authenticated, expired, lease_seconds=60
        )
    assert await event_count(setup, expired.task_attempt_id) == 0


async def exercise_renewal_event_failure(
    setup: asyncpg.Connection[asyncpg.Record],
    repository: SQLAlchemyTaskClaimRepository,
) -> None:
    worker = await add_worker(setup)
    request = await add_current_claim(
        setup, worker, lease_interval=timedelta(seconds=5)
    )
    before = await setup.fetchrow(
        "SELECT lease_expires_at, xmin::text AS xmin FROM task_attempt_claims "
        "WHERE task_attempt_id = $1",
        request.task_attempt_id,
    )
    await install_event_failure_trigger(setup, "lease_renewed")
    try:
        with pytest.raises(TaskClaimPersistenceUnavailable):
            await repository.renew_claim(
                worker.authenticated, request, lease_seconds=60
            )
    finally:
        await remove_event_failure_trigger(setup)
    after = await setup.fetchrow(
        "SELECT lease_expires_at, xmin::text AS xmin FROM task_attempt_claims "
        "WHERE task_attempt_id = $1",
        request.task_attempt_id,
    )
    assert before == after
    assert await event_count(setup, request.task_attempt_id) == 0


async def exercise_atomic_visibility(
    setup: asyncpg.Connection[asyncpg.Record], database_url: URL
) -> None:
    worker = await add_worker(setup)
    dispatch = await add_dispatched_task(setup)
    writer = await asyncpg.connect(asyncpg_dsn(database_url))
    observer = await asyncpg.connect(asyncpg_dsn(database_url))
    transaction = writer.transaction()
    await transaction.start()
    committed = False
    try:
        row = await writer.fetchrow(
            "INSERT INTO task_attempt_claims "
            "(task_attempt_id, generation, worker_session_id, lease_expires_at) "
            "VALUES ($1, 1, $2, statement_timestamp() + interval '1 minute') "
            "RETURNING acquired_at, lease_expires_at",
            dispatch.task_attempt_id,
            worker.session_id,
        )
        assert row is not None
        await writer.execute(
            "UPDATE task_runs SET status = 'claimed' WHERE id = $1",
            dispatch.task_run_id,
        )
        await writer.execute(
            "INSERT INTO task_claim_events "
            "(id, task_attempt_id, generation, worker_identity_id, "
            "worker_session_id, event_type, occurred_at, lease_expires_at) "
            "VALUES ($1, $2, 1, $5, $6, 'claim_acquired', $3, $4)",
            uuid4(),
            dispatch.task_attempt_id,
            row["acquired_at"],
            row["lease_expires_at"],
            worker.authenticated.worker_identity_id,
            worker.session_id,
        )
        observation_sql = (
            "SELECT count(*) FROM task_attempt_claims c JOIN task_claim_events e "
            "ON (e.task_attempt_id, e.generation) = "
            "(c.task_attempt_id, c.generation) WHERE c.task_attempt_id = $1"
        )
        assert await observer.fetchval(observation_sql, dispatch.task_attempt_id) == 0
        await transaction.commit()
        committed = True
        assert await observer.fetchval(observation_sql, dispatch.task_attempt_id) == 1
    finally:
        if not committed and writer.is_in_transaction():
            await transaction.rollback()
        await writer.close()
        await observer.close()


async def exercise_inspection(
    setup: asyncpg.Connection[asyncpg.Record],
    inspection: SQLAlchemyTaskClaimInspectionRepository,
    service: TaskClaimService,
) -> None:
    worker = await add_worker(setup)
    dispatch = await add_dispatched_task(setup)
    await service.claim_task(worker.authenticated, worker.session_id, dispatch)
    owner_id = await setup.fetchval(
        "SELECT wd.owner_principal_id FROM workflow_definitions wd "
        "JOIN workflow_runs wr ON wr.workflow_definition_id = wd.id "
        "WHERE wr.id = $1",
        dispatch.workflow_run_id,
    )
    assert isinstance(owner_id, UUID)
    before_xmin = await setup.fetchval(
        "SELECT xmin::text FROM task_attempt_claims WHERE task_attempt_id = $1",
        dispatch.task_attempt_id,
    )
    inspected = await inspection.get_current_claim(
        dispatch.task_attempt_id, OwnerFilter.only(owner_id)
    )
    assert inspected.task_attempt_id == dispatch.task_attempt_id
    assert inspected.task_run_id == dispatch.task_run_id
    assert inspected.workflow_run_id == dispatch.workflow_run_id
    assert inspected.worker_identity_id == worker.authenticated.worker_identity_id
    assert inspected.worker_session_id == worker.session_id
    assert inspected.lease_status is TaskClaimLeaseStatus.UNEXPIRED
    assert inspected.lease_expires_at > inspected.observed_at
    assert (
        await setup.fetchval(
            "SELECT xmin::text FROM task_attempt_claims WHERE task_attempt_id = $1",
            dispatch.task_attempt_id,
        )
        == before_xmin
    )
    with pytest.raises(TaskClaimInspectionNotFound):
        await inspection.get_current_claim(
            dispatch.task_attempt_id, OwnerFilter.only(uuid4())
        )
    with pytest.raises(TaskClaimInspectionNotFound):
        await inspection.get_current_claim(uuid4(), OwnerFilter.all_owners())

    await setup.execute(
        "UPDATE task_attempt_claims SET lease_expires_at = "
        "statement_timestamp() WHERE task_attempt_id = $1",
        dispatch.task_attempt_id,
    )
    expired = await inspection.get_current_claim(
        dispatch.task_attempt_id, OwnerFilter.all_owners()
    )
    assert expired.lease_status is TaskClaimLeaseStatus.EXPIRED
    assert expired.lease_expires_at <= expired.observed_at


async def exercise_claim_events(database_url: URL) -> None:
    setup = await asyncpg.connect(asyncpg_dsn(database_url))
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
    )
    sessions = build_session_factory(engine)
    repository = SQLAlchemyTaskClaimRepository(sessions, worker_stale_after_seconds=30)
    service = TaskClaimService(
        repository,
        TaskClaimResultAuthorityIssuer(b"claim-event-integration-secret-value"),
        lease_seconds=60,
    )
    inspection = SQLAlchemyTaskClaimInspectionRepository(sessions)
    try:
        await exercise_acquisition_events(setup, database_url, service)
        await exercise_acquisition_event_failure(setup, service)
        await exercise_renewal_events(setup, database_url, repository)
        await exercise_renewal_event_failure(setup, repository)
        await exercise_atomic_visibility(setup, database_url)
        await exercise_inspection(setup, inspection, service)
    finally:
        await setup.close()
        await engine.dispose()


def test_real_postgresql_claim_events_and_inspection() -> None:
    with temporary_database(
        "TASKFORGE_CLAIM_TEST_DATABASE_URL", "taskforge_claim_events"
    ) as database_url:
        configuration = Config("alembic.ini")
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        with migration_database_url(alembic_url):
            command.upgrade(configuration, "head")
        asyncio.run(exercise_claim_events(database_url))
