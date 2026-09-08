"""Real PostgreSQL claim renewal, fencing, and concurrency verification."""

from __future__ import annotations

import asyncio
import os
from contextlib import suppress
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from taskforge.claims.authority import TaskClaimResultAuthorityIssuer
from taskforge.claims.domain import (
    TaskClaimOutcome,
    TaskClaimRenewalOutcome,
    TaskClaimRenewalRequest,
    TaskClaimRenewalResult,
)
from taskforge.claims.persistence_ports import (
    TaskClaimAuthorityRejected,
    TaskClaimRenewalExpired,
    TaskClaimRenewalStale,
    TaskClaimRenewalTaskInactive,
    TaskClaimSessionInactive,
    TaskClaimSessionUnavailable,
)
from taskforge.claims.service import TaskClaimService
from taskforge.dispatch.envelope import serialize_dispatch_envelope
from taskforge.dispatch.transport import DispatchTransportMetadata
from taskforge.persistence.claims import SQLAlchemyTaskClaimRepository
from taskforge.persistence.database import build_session_factory
from taskforge.persistence.recovery import SQLAlchemyExpiredClaimRecoveryRepository
from taskforge.persistence.task_cancellation import SQLAlchemyTaskCancellationObserver
from taskforge.persistence.task_start import SQLAlchemyTaskStartRepository
from taskforge.recovery.domain import ExpiredClaimCandidate
from taskforge.recovery.service import ExpiredClaimRecoveryService
from taskforge.worker.cancellation import TaskCancellationObservationOutcome
from taskforge.worker.claim_renewal import ClaimRenewalSupervisor
from taskforge.worker.consumer_ports import BrokerDispatchDelivery
from taskforge.worker.execution import WorkerExecutionConsumer
from taskforge.worker.handlers import (
    TaskContext,
    TaskHandlerDefinition,
    TaskHandlerRegistry,
)
from taskforge.worker.start import TaskStartService
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
    WorkerFacts,
    add_dispatched_task,
    add_worker,
    wait_for_lock_waiter,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_CLAIM_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_CLAIM_INTEGRATION=1 explicitly",
    ),
]


class AcceptParameters:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        del parameters
        return ()


class RecoveryControl:
    def __init__(self, dispatch: Any) -> None:
        self._delivery = BrokerDispatchDelivery(
            serialize_dispatch_envelope(dispatch),
            DispatchTransportMetadata(
                str(dispatch.dispatch_id),
                dispatch.route,
                "application/json",
                "utf-8",
            ),
            False,
        )
        self.actions: list[str] = []

    @property
    def delivery(self) -> BrokerDispatchDelivery:
        return self._delivery

    async def acknowledge(self) -> None:
        self.actions.append("ack")

    async def reject(self, *, requeue: bool) -> None:
        self.actions.append(f"reject:{requeue}")


class MustNotSubmitResult:
    def __init__(self) -> None:
        self.called = False

    async def submit_result(self, *args: Any) -> Any:
        del args
        self.called = True
        raise AssertionError("stale handler result must not be submitted")


def execution_registry(handler: Any) -> TaskHandlerRegistry:
    task_types = TaskTypeRegistry(
        (TaskTypeDefinition("test.task", "test-capability", AcceptParameters()),)
    )
    return TaskHandlerRegistry(
        (TaskHandlerDefinition("test.task", "test-capability", handler),),
        task_types,
    )


async def add_current_claim(
    connection: asyncpg.Connection[asyncpg.Record],
    worker: WorkerFacts,
    *,
    task_status: str = "claimed",
    generation: int = 1,
    lease_interval: timedelta = timedelta(seconds=30),
) -> TaskClaimRenewalRequest:
    dispatch = await add_dispatched_task(connection, status=task_status)
    row = await connection.fetchrow(
        "INSERT INTO task_attempt_claims "
        "(task_attempt_id, generation, worker_session_id, lease_expires_at) "
        "VALUES ($1, $2, $3, statement_timestamp() + $4::interval) "
        "RETURNING lease_expires_at",
        dispatch.task_attempt_id,
        generation,
        worker.session_id,
        lease_interval,
    )
    assert row is not None
    return TaskClaimRenewalRequest(
        dispatch.task_attempt_id,
        generation,
        worker.session_id,
        row["lease_expires_at"],
    )


async def claim_task_id(
    connection: asyncpg.Connection[asyncpg.Record], task_attempt_id: UUID
) -> UUID:
    task_id = await connection.fetchval(
        "SELECT task_run_id FROM task_attempts WHERE id = $1", task_attempt_id
    )
    assert isinstance(task_id, UUID)
    return task_id


async def cancel_and_await(task: asyncio.Task[object] | None) -> None:
    if task is not None and not task.done():
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


async def concurrent_same_owner_renewal(
    connection: asyncpg.Connection[asyncpg.Record],
    database_url: URL,
    repository: SQLAlchemyTaskClaimRepository,
    worker: WorkerFacts,
    request: TaskClaimRenewalRequest,
) -> tuple[TaskClaimRenewalResult, TaskClaimRenewalResult]:
    blocker = await asyncpg.connect(asyncpg_dsn(database_url))
    observer = await asyncpg.connect(asyncpg_dsn(database_url))
    transaction = blocker.transaction()
    await transaction.start()
    pending: (
        tuple[
            asyncio.Task[TaskClaimRenewalResult], asyncio.Task[TaskClaimRenewalResult]
        ]
        | None
    ) = None
    try:
        await blocker.execute(
            "SELECT id FROM task_runs WHERE id = $1 FOR UPDATE",
            await claim_task_id(connection, request.task_attempt_id),
        )
        pending = (
            asyncio.create_task(
                repository.renew_claim(worker.authenticated, request, lease_seconds=60)
            ),
            asyncio.create_task(
                repository.renew_claim(worker.authenticated, request, lease_seconds=60)
            ),
        )
        await wait_for_lock_waiter(observer, minimum=2)
        await transaction.commit()
        first, second = await asyncio.wait_for(asyncio.gather(*pending), timeout=5)
        return first, second
    finally:
        if pending is not None:
            await cancel_and_await(pending[0])
            await cancel_and_await(pending[1])
        if blocker.is_in_transaction():
            await transaction.rollback()
        await blocker.close()
        await observer.close()


async def lifecycle_race(
    connection: asyncpg.Connection[asyncpg.Record],
    database_url: URL,
    repository: SQLAlchemyTaskClaimRepository,
    worker: WorkerFacts,
    request: TaskClaimRenewalRequest,
    *,
    lock_sql: str,
    lock_id: UUID,
    mutation_sql: str,
    expected: type[Exception],
) -> None:
    blocker = await asyncpg.connect(asyncpg_dsn(database_url))
    observer = await asyncpg.connect(asyncpg_dsn(database_url))
    transaction = blocker.transaction()
    await transaction.start()
    pending: asyncio.Task[TaskClaimRenewalResult] | None = None
    try:
        await blocker.execute(lock_sql, lock_id)
        pending = asyncio.create_task(
            repository.renew_claim(worker.authenticated, request, lease_seconds=60)
        )
        await wait_for_lock_waiter(observer)
        await blocker.execute(mutation_sql, lock_id)
        await transaction.commit()
        with pytest.raises(expected):
            await asyncio.wait_for(pending, timeout=5)
    finally:
        await cancel_and_await(pending)
        if blocker.is_in_transaction():
            await transaction.rollback()
        await blocker.close()
        await observer.close()


async def exercise_renewal(database_url: URL) -> None:
    dsn = asyncpg_dsn(database_url)
    setup = await asyncpg.connect(dsn)
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
    )
    sessions = build_session_factory(engine)
    repository = SQLAlchemyTaskClaimRepository(sessions, worker_stale_after_seconds=30)
    try:
        worker = await add_worker(setup)
        request = await add_current_claim(
            setup, worker, lease_interval=timedelta(seconds=5)
        )
        renewed = await repository.renew_claim(
            worker.authenticated, request, lease_seconds=60
        )
        assert renewed.outcome is TaskClaimRenewalOutcome.RENEWED
        assert renewed.claim.lease_expires_at > request.expected_lease_expires_at

        replayed = await repository.renew_claim(
            worker.authenticated, request, lease_seconds=60
        )
        assert replayed.outcome is TaskClaimRenewalOutcome.REPLAYED
        assert replayed.claim.lease_expires_at == renewed.claim.lease_expires_at

        rapid_request = TaskClaimRenewalRequest(
            request.task_attempt_id,
            request.generation,
            request.worker_session_id,
            renewed.claim.lease_expires_at,
        )
        rapid = await repository.renew_claim(
            worker.authenticated, rapid_request, lease_seconds=60
        )
        assert rapid.claim.lease_expires_at >= renewed.claim.lease_expires_at
        assert (
            rapid.claim.lease_expires_at
            < renewed.claim.lease_expires_at + timedelta(seconds=2)
        )

        unchanged_worker = await add_worker(setup)
        unchanged_request = await add_current_claim(
            setup, unchanged_worker, lease_interval=timedelta(minutes=2)
        )
        before = await setup.fetchrow(
            "SELECT lease_expires_at, xmin::text AS xmin FROM task_attempt_claims "
            "WHERE task_attempt_id = $1 AND generation = $2",
            unchanged_request.task_attempt_id,
            unchanged_request.generation,
        )
        unchanged = await repository.renew_claim(
            unchanged_worker.authenticated, unchanged_request, lease_seconds=60
        )
        after = await setup.fetchrow(
            "SELECT lease_expires_at, xmin::text AS xmin FROM task_attempt_claims "
            "WHERE task_attempt_id = $1 AND generation = $2",
            unchanged_request.task_attempt_id,
            unchanged_request.generation,
        )
        assert unchanged.outcome is TaskClaimRenewalOutcome.ACTIVE_UNCHANGED
        assert before == after

        running_worker = await add_worker(setup)
        running_request = await add_current_claim(
            setup,
            running_worker,
            task_status="running",
            lease_interval=timedelta(seconds=5),
        )
        assert (
            await repository.renew_claim(
                running_worker.authenticated, running_request, lease_seconds=60
            )
        ).outcome is TaskClaimRenewalOutcome.RENEWED

        cancelling_worker = await add_worker(setup)
        cancelling_request = await add_current_claim(
            setup,
            cancelling_worker,
            task_status="running",
            lease_interval=timedelta(seconds=30),
        )
        run = await setup.fetchrow(
            "SELECT wr.id, wr.requested_by_principal_id FROM workflow_runs wr "
            "JOIN task_runs tr ON tr.workflow_run_id = wr.id "
            "JOIN task_attempts ta ON ta.task_run_id = tr.id WHERE ta.id = $1",
            cancelling_request.task_attempt_id,
        )
        assert run is not None
        requested_at = await setup.fetchval(
            "INSERT INTO workflow_run_cancellation_requests "
            "(workflow_run_id, requested_by_principal_id, idempotency_key_digest, "
            "request_fingerprint) VALUES ($1, $2, $3, $4) RETURNING requested_at",
            run["id"],
            run["requested_by_principal_id"],
            "a" * 64,
            "b" * 64,
        )
        await setup.execute(
            "UPDATE workflow_runs SET status = 'cancelling' WHERE id = $1", run["id"]
        )
        denied = await repository.renew_claim(
            cancelling_worker.authenticated, cancelling_request, lease_seconds=60
        )
        assert denied.outcome is TaskClaimRenewalOutcome.CANCELLATION_REQUESTED
        assert denied.cancellation_requested_at == requested_at
        assert denied.claim.lease_expires_at == (
            cancelling_request.expected_lease_expires_at
        )

        await exercise_rejections(setup, repository)
        await exercise_assignment_changes(setup, repository)
        await exercise_same_owner_race(
            setup, database_url, repository, await add_worker(setup)
        )
        await exercise_lifecycle_races(setup, database_url, repository)
        await exercise_task_and_termination_races(setup, database_url, repository)
        await exercise_independent_renewal(setup, database_url, repository)
        await exercise_result_authority(setup, repository)
        await exercise_reason_preserving_observation(setup, sessions)
        await exercise_running_handler_recovery(setup, sessions)
    finally:
        await setup.close()
        await engine.dispose()


async def exercise_rejections(
    setup: asyncpg.Connection[asyncpg.Record],
    repository: SQLAlchemyTaskClaimRepository,
) -> None:
    worker = await add_worker(setup)
    expired = await add_current_claim(
        setup, worker, lease_interval=timedelta(seconds=1)
    )
    await setup.execute(
        "UPDATE task_attempt_claims SET lease_expires_at = statement_timestamp() "
        "WHERE task_attempt_id = $1 AND generation = $2",
        expired.task_attempt_id,
        expired.generation,
    )
    with pytest.raises(TaskClaimRenewalExpired):
        await repository.renew_claim(worker.authenticated, expired, lease_seconds=60)

    terminated_worker = await add_worker(setup)
    terminated = await add_current_claim(setup, terminated_worker)
    await setup.execute(
        "UPDATE task_attempt_claims SET terminated_at = statement_timestamp() "
        "WHERE task_attempt_id = $1 AND generation = $2",
        terminated.task_attempt_id,
        terminated.generation,
    )
    with pytest.raises(TaskClaimRenewalStale):
        await repository.renew_claim(
            terminated_worker.authenticated, terminated, lease_seconds=60
        )

    stale_worker = await add_worker(setup)
    stale = await add_current_claim(setup, stale_worker)
    wrong_generation = TaskClaimRenewalRequest(
        stale.task_attempt_id,
        stale.generation + 1,
        stale.worker_session_id,
        stale.expected_lease_expires_at,
    )
    with pytest.raises(TaskClaimRenewalStale):
        await repository.renew_claim(
            stale_worker.authenticated, wrong_generation, lease_seconds=60
        )
    wrong_session = TaskClaimRenewalRequest(
        stale.task_attempt_id,
        stale.generation,
        uuid4(),
        stale.expected_lease_expires_at,
    )
    with pytest.raises(TaskClaimSessionUnavailable):
        await repository.renew_claim(
            stale_worker.authenticated, wrong_session, lease_seconds=60
        )
    await setup.execute(
        "UPDATE task_attempt_claims SET terminated_at = statement_timestamp() "
        "WHERE task_attempt_id = $1 AND generation = $2",
        stale.task_attempt_id,
        stale.generation,
    )
    newer_worker = await add_worker(setup)
    await setup.execute(
        "INSERT INTO task_attempt_claims "
        "(task_attempt_id, generation, worker_session_id, lease_expires_at) "
        "VALUES ($1, $2, $3, statement_timestamp() + interval '1 minute')",
        stale.task_attempt_id,
        stale.generation + 1,
        newer_worker.session_id,
    )
    with pytest.raises(TaskClaimRenewalStale):
        await repository.renew_claim(
            stale_worker.authenticated, stale, lease_seconds=60
        )

    inactive_worker = await add_worker(setup)
    for status in (
        "blocked",
        "runnable",
        "dispatched",
        "retry_scheduled",
        "succeeded",
        "failed",
        "skipped",
        "cancelled",
    ):
        inactive = await add_current_claim(setup, inactive_worker, task_status=status)
        with pytest.raises(TaskClaimRenewalTaskInactive):
            await repository.renew_claim(
                inactive_worker.authenticated, inactive, lease_seconds=60
            )


async def exercise_assignment_changes(
    setup: asyncpg.Connection[asyncpg.Record],
    repository: SQLAlchemyTaskClaimRepository,
) -> None:
    worker = await add_worker(setup)
    request = await add_current_claim(
        setup, worker, lease_interval=timedelta(seconds=5)
    )
    await setup.execute(
        "UPDATE worker_session_health SET accepting_work = false, "
        "last_seen_at = statement_timestamp() - interval '1 hour', "
        "availability_changed_at = statement_timestamp() - interval '1 hour' "
        "WHERE worker_session_id = $1",
        worker.session_id,
    )
    await setup.execute(
        "DELETE FROM worker_session_capabilities WHERE worker_session_id = $1",
        worker.session_id,
    )
    assert (
        await repository.renew_claim(worker.authenticated, request, lease_seconds=60)
    ).outcome is TaskClaimRenewalOutcome.RENEWED


async def exercise_same_owner_race(
    setup: asyncpg.Connection[asyncpg.Record],
    database_url: URL,
    repository: SQLAlchemyTaskClaimRepository,
    worker: WorkerFacts,
) -> None:
    request = await add_current_claim(
        setup, worker, lease_interval=timedelta(seconds=5)
    )
    first, second = await concurrent_same_owner_renewal(
        setup, database_url, repository, worker, request
    )
    assert {first.outcome, second.outcome} == {
        TaskClaimRenewalOutcome.RENEWED,
        TaskClaimRenewalOutcome.REPLAYED,
    }
    assert first.claim.lease_expires_at == second.claim.lease_expires_at


async def exercise_lifecycle_races(
    setup: asyncpg.Connection[asyncpg.Record],
    database_url: URL,
    repository: SQLAlchemyTaskClaimRepository,
) -> None:
    session_worker = await add_worker(setup)
    session_request = await add_current_claim(setup, session_worker)
    await lifecycle_race(
        setup,
        database_url,
        repository,
        session_worker,
        session_request,
        lock_sql="SELECT id FROM worker_sessions WHERE id = $1 FOR UPDATE",
        lock_id=session_worker.session_id,
        mutation_sql="UPDATE worker_sessions SET ended_at = statement_timestamp() WHERE id = $1",
        expected=TaskClaimSessionInactive,
    )

    identity_worker = await add_worker(setup)
    identity_request = await add_current_claim(setup, identity_worker)
    await lifecycle_race(
        setup,
        database_url,
        repository,
        identity_worker,
        identity_request,
        lock_sql="SELECT id FROM worker_identities WHERE id = $1 FOR UPDATE",
        lock_id=identity_worker.authenticated.worker_identity_id,
        mutation_sql="UPDATE worker_identities SET disabled_at = statement_timestamp() WHERE id = $1",
        expected=TaskClaimAuthorityRejected,
    )

    credential_worker = await add_worker(setup)
    credential_request = await add_current_claim(setup, credential_worker)
    await lifecycle_race(
        setup,
        database_url,
        repository,
        credential_worker,
        credential_request,
        lock_sql="SELECT id FROM worker_credentials WHERE id = $1 FOR UPDATE",
        lock_id=credential_worker.authenticated.credential_id,
        mutation_sql="UPDATE worker_credentials SET revoked_at = statement_timestamp() WHERE id = $1",
        expected=TaskClaimAuthorityRejected,
    )

    expired_worker = await add_worker(setup, expired=True)
    expired_request = await add_current_claim(setup, expired_worker)
    with pytest.raises(TaskClaimAuthorityRejected):
        await repository.renew_claim(
            expired_worker.authenticated, expired_request, lease_seconds=60
        )


async def exercise_task_and_termination_races(
    setup: asyncpg.Connection[asyncpg.Record],
    database_url: URL,
    repository: SQLAlchemyTaskClaimRepository,
) -> None:
    worker = await add_worker(setup)
    request = await add_current_claim(setup, worker)
    task_id = await claim_task_id(setup, request.task_attempt_id)
    await lifecycle_race(
        setup,
        database_url,
        repository,
        worker,
        request,
        lock_sql="SELECT id FROM task_runs WHERE id = $1 FOR UPDATE",
        lock_id=task_id,
        mutation_sql="UPDATE task_runs SET status = 'failed' WHERE id = $1",
        expected=TaskClaimRenewalTaskInactive,
    )

    terminated_worker = await add_worker(setup)
    terminated = await add_current_claim(setup, terminated_worker)
    await lifecycle_race(
        setup,
        database_url,
        repository,
        terminated_worker,
        terminated,
        lock_sql=(
            "SELECT task_attempt_id FROM task_attempt_claims "
            "WHERE task_attempt_id = $1 FOR UPDATE"
        ),
        lock_id=terminated.task_attempt_id,
        mutation_sql=(
            "UPDATE task_attempt_claims SET terminated_at = statement_timestamp() "
            "WHERE task_attempt_id = $1"
        ),
        expected=TaskClaimRenewalStale,
    )


async def exercise_independent_renewal(
    setup: asyncpg.Connection[asyncpg.Record],
    database_url: URL,
    repository: SQLAlchemyTaskClaimRepository,
) -> None:
    worker = await add_worker(setup)
    blocked_request = await add_current_claim(
        setup, worker, lease_interval=timedelta(seconds=5)
    )
    free_request = await add_current_claim(
        setup, worker, lease_interval=timedelta(seconds=5)
    )
    blocker = await asyncpg.connect(asyncpg_dsn(database_url))
    observer = await asyncpg.connect(asyncpg_dsn(database_url))
    transaction = blocker.transaction()
    await transaction.start()
    pending: asyncio.Task[TaskClaimRenewalResult] | None = None
    try:
        await blocker.execute(
            "SELECT id FROM task_runs WHERE id = $1 FOR UPDATE",
            await claim_task_id(setup, blocked_request.task_attempt_id),
        )
        pending = asyncio.create_task(
            repository.renew_claim(
                worker.authenticated, blocked_request, lease_seconds=60
            )
        )
        await wait_for_lock_waiter(observer)
        independent = await asyncio.wait_for(
            repository.renew_claim(
                worker.authenticated, free_request, lease_seconds=60
            ),
            timeout=2,
        )
        assert independent.outcome is TaskClaimRenewalOutcome.RENEWED
        await transaction.commit()
        assert (await asyncio.wait_for(pending, timeout=5)).outcome is (
            TaskClaimRenewalOutcome.RENEWED
        )
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM task_claim_events WHERE task_attempt_id = "
                "ANY($1::uuid[]) AND event_type = 'lease_renewed'",
                [blocked_request.task_attempt_id, free_request.task_attempt_id],
            )
            == 2
        )
    finally:
        await cancel_and_await(pending)
        if blocker.is_in_transaction():
            await transaction.rollback()
        await blocker.close()
        await observer.close()


async def exercise_result_authority(
    setup: asyncpg.Connection[asyncpg.Record],
    repository: SQLAlchemyTaskClaimRepository,
) -> None:
    worker = await add_worker(setup)
    dispatch = await add_dispatched_task(setup)
    issuer = TaskClaimResultAuthorityIssuer(b"result-authority-test-secret-value")
    service = TaskClaimService(repository, issuer, lease_seconds=60)
    acquired = await service.claim_task(
        worker.authenticated, worker.session_id, dispatch
    )
    replayed = await service.claim_task(
        worker.authenticated, worker.session_id, dispatch
    )
    assert acquired.outcome is TaskClaimOutcome.ACQUIRED_ACTIVE
    assert replayed.outcome is TaskClaimOutcome.REPLAYED_ACTIVE
    assert acquired.result_authority == replayed.result_authority
    assert acquired.result_authority is not None
    assert issuer.verify(
        acquired.result_authority,
        worker_identity_id=worker.authenticated.worker_identity_id,
        worker_session_id=worker.session_id,
        task_attempt_id=dispatch.task_attempt_id,
        generation=acquired.claim.generation,
    )
    await setup.execute(
        "UPDATE task_attempt_claims SET lease_expires_at = acquired_at + "
        "interval '1 microsecond' WHERE task_attempt_id = $1",
        dispatch.task_attempt_id,
    )
    expired = await service.claim_task(
        worker.authenticated, worker.session_id, dispatch
    )
    assert expired.outcome is TaskClaimOutcome.REPLAYED_EXPIRED
    assert expired.result_authority is None


async def exercise_reason_preserving_observation(
    setup: asyncpg.Connection[asyncpg.Record], sessions: Any
) -> None:
    observer = SQLAlchemyTaskCancellationObserver(sessions)
    worker = await add_worker(setup)
    request = await add_current_claim(
        setup, worker, task_status="running", lease_interval=timedelta(seconds=30)
    )
    task = await setup.fetchrow(
        "SELECT ta.task_run_id, tr.workflow_run_id FROM task_attempts ta "
        "JOIN task_runs tr ON tr.id = ta.task_run_id WHERE ta.id = $1",
        request.task_attempt_id,
    )
    assert task is not None

    active = await observer.observe_cancellation(
        worker.authenticated,
        worker.session_id,
        task["workflow_run_id"],
        task["task_run_id"],
        request.task_attempt_id,
        request.generation,
    )
    assert active.outcome is TaskCancellationObservationOutcome.ACTIVE

    lease_expires_at = await setup.fetchval(
        "UPDATE task_attempt_claims SET lease_expires_at = statement_timestamp() "
        "WHERE task_attempt_id = $1 AND generation = $2 RETURNING lease_expires_at",
        request.task_attempt_id,
        request.generation,
    )
    expired = await observer.observe_cancellation(
        worker.authenticated,
        worker.session_id,
        task["workflow_run_id"],
        task["task_run_id"],
        request.task_attempt_id,
        request.generation,
    )
    assert expired.outcome is (
        TaskCancellationObservationOutcome.CLAIM_EXPIRED_AWAITING_RECOVERY
    )

    observed_at = await setup.fetchval("SELECT statement_timestamp()")
    assert isinstance(lease_expires_at, datetime)
    assert observed_at is not None
    candidate = ExpiredClaimCandidate(
        request.task_attempt_id,
        task["task_run_id"],
        task["workflow_run_id"],
        1,
        request.generation,
        worker.session_id,
        lease_expires_at,
        observed_at,
    )
    await ExpiredClaimRecoveryService(
        SQLAlchemyExpiredClaimRecoveryRepository(sessions)
    ).recover_expired_claim(candidate)
    recovered = await observer.observe_cancellation(
        worker.authenticated,
        worker.session_id,
        task["workflow_run_id"],
        task["task_run_id"],
        request.task_attempt_id,
        request.generation,
    )
    assert recovered.outcome is TaskCancellationObservationOutcome.CLAIM_RECOVERED

    await setup.execute(
        "UPDATE worker_credentials SET revoked_at = statement_timestamp() "
        "WHERE id = $1",
        worker.authenticated.credential_id,
    )
    rejected = await observer.observe_cancellation(
        worker.authenticated,
        worker.session_id,
        task["workflow_run_id"],
        task["task_run_id"],
        request.task_attempt_id,
        request.generation,
    )
    assert rejected.outcome is (
        TaskCancellationObservationOutcome.WORKER_AUTHORITY_REJECTED
    )


async def exercise_running_handler_recovery(
    setup: asyncpg.Connection[asyncpg.Record], sessions: Any
) -> None:
    worker = await add_worker(setup)
    dispatch = await add_dispatched_task(setup)
    observer = SQLAlchemyTaskCancellationObserver(sessions)
    claim_service = TaskClaimService(
        SQLAlchemyTaskClaimRepository(sessions, worker_stale_after_seconds=30),
        TaskClaimResultAuthorityIssuer(b"running-handler-recovery-authority"),
        lease_seconds=60,
    )
    results = MustNotSubmitResult()
    control = RecoveryControl(dispatch)
    handler_started = asyncio.Event()
    handler_cancelled = asyncio.Event()

    async def handler(context: TaskContext) -> object:
        del context
        handler_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            handler_cancelled.set()
            raise
        return None

    consumer = WorkerExecutionConsumer(
        claim_service,
        TaskStartService(SQLAlchemyTaskStartRepository(sessions)),
        results,
        execution_registry(handler),
        worker.authenticated,
        worker.session_id,
        observer,
        ClaimRenewalSupervisor(
            claim_service,
            observer,
            worker.authenticated,
            worker.session_id,
            lease_seconds=60,
            operation_timeout_seconds=0.5,
            observation_poll_seconds=0.01,
        ),
        cancellation_poll_seconds=0.01,
    )
    consuming = asyncio.create_task(consumer.consume(control))
    try:
        await asyncio.wait_for(handler_started.wait(), timeout=3)
        claim = await setup.fetchrow(
            "UPDATE task_attempt_claims SET lease_expires_at = "
            "statement_timestamp() WHERE task_attempt_id = $1 "
            "RETURNING generation, lease_expires_at",
            dispatch.task_attempt_id,
        )
        assert claim is not None

        await asyncio.wait_for(handler_cancelled.wait(), timeout=3)
        assert not consuming.done()
        assert not results.called
        assert control.actions == []

        observed_at = await setup.fetchval("SELECT statement_timestamp()")
        assert isinstance(observed_at, datetime)
        await ExpiredClaimRecoveryService(
            SQLAlchemyExpiredClaimRecoveryRepository(sessions)
        ).recover_expired_claim(
            ExpiredClaimCandidate(
                dispatch.task_attempt_id,
                dispatch.task_run_id,
                dispatch.workflow_run_id,
                dispatch.attempt_number,
                claim["generation"],
                worker.session_id,
                claim["lease_expires_at"],
                observed_at,
            )
        )

        await asyncio.wait_for(consuming, timeout=3)
        assert not results.called
        assert control.actions == ["ack"]
    finally:
        if not consuming.done():
            consuming.cancel()
            with suppress(asyncio.CancelledError):
                await consuming


def test_real_postgresql_claim_renewal_and_concurrency() -> None:
    with temporary_database(
        "TASKFORGE_CLAIM_TEST_DATABASE_URL", "taskforge_claim_renewal"
    ) as database_url:
        configuration = Config("alembic.ini")
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        with migration_database_url(alembic_url):
            command.upgrade(configuration, "head")
        asyncio.run(exercise_renewal(database_url))
