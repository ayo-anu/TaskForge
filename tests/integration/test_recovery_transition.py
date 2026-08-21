"""Real PostgreSQL tests for atomic expired-claim recovery."""

from __future__ import annotations

import asyncio
import json
import os
from collections import Counter
from datetime import datetime, timedelta
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
    TaskClaimRenewalOutcome,
    TaskClaimRenewalRequest,
    TaskClaimResultAuthority,
)
from taskforge.claims.persistence_ports import TaskClaimRenewalRecovered
from taskforge.claims.service import TaskClaimService
from taskforge.dispatch.envelope import (
    DispatchEnvelope,
    create_dispatch_envelope,
    dispatch_envelope_to_mapping,
)
from taskforge.persistence.claims import SQLAlchemyTaskClaimRepository
from taskforge.persistence.database import build_session_factory
from taskforge.persistence.recovery import (
    SQLAlchemyExpiredClaimRecoveryRepository,
)
from taskforge.persistence.retries import SQLAlchemyRetryTransitionRepository
from taskforge.persistence.runs import SQLAlchemyWorkflowRunRepository
from taskforge.persistence.task_results import SQLAlchemyTaskResultRepository
from taskforge.recovery.domain import (
    ExpiredClaimCandidate,
    PreparedExpiredClaimRecovery,
)
from taskforge.recovery.progression import (
    ExpiredClaimRecoveryProgressionService,
    ExpiredClaimRecoveryProgressionUnavailable,
)
from taskforge.recovery.service import (
    ExpiredClaimRecoveryOutcome,
    ExpiredClaimRecoveryService,
    ExpiredClaimRecoveryServiceUnavailable,
)
from taskforge.retries.domain import RetryNotScheduledReason
from taskforge.retries.persistence_ports import NewScheduledRetryAttempt
from taskforge.retries.scanner import DueRetryScanner
from taskforge.runs.domain import WorkflowRunStatus
from taskforge.runs.service import WorkflowRunService
from taskforge.worker.result_submission import (
    TaskResultStale,
    TaskResultSubmissionRequest,
    TaskResultSubmissionService,
)
from taskforge.worker.results import (
    TaskExecutionFailureKind,
    TaskExecutionResult,
    TaskExecutionResultKind,
    task_result_fingerprint,
)
from tests.integration.postgresql import (
    ExpectedStatusExecutionEvent,
    assert_status_execution_events,
    asyncpg_dsn,
    migration_database_url,
    temporary_database,
)
from tests.integration.test_recovery_scanner import add_claim, add_session
from tests.integration.test_retry_scanner import registry
from tests.integration.test_task_claim_acquisition import (
    WorkerFacts,
    add_dispatched_task,
    add_worker,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_RECOVERY_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_RECOVERY_INTEGRATION=1 explicitly",
    ),
]

_AUTHORITY_SECRET = b"recovery-result-race-secret-value"


async def recoverable_candidate(
    connection: asyncpg.Connection[asyncpg.Record],
    *,
    maximum_attempts: int | None = 3,
    task_status: str = "running",
    run_status: str = "running",
    initial_delay_seconds: int = 7,
) -> ExpiredClaimCandidate:
    now = await connection.fetchval("SELECT statement_timestamp()")
    assert isinstance(now, datetime)
    worker = await add_session(connection, last_seen_at=now)
    workflow_policy = None
    if maximum_attempts is not None:
        workflow_policy = (
            '{"retry_policy":{"maximum_attempts":'
            f"{maximum_attempts},"
            f'"initial_delay_seconds":{initial_delay_seconds},"multiplier":2,'
            '"maximum_delay_seconds":60}}'
        )
    facts = await add_claim(
        connection,
        worker.session_id,
        lease_expires_at=now - timedelta(seconds=1),
        task_status=task_status,
        run_status=run_status,
        workflow_policy=workflow_policy,
    )
    dispatch_id = uuid4()
    task = await connection.fetchrow(
        "SELECT tr.workflow_run_id, tr.step_identifier FROM task_runs tr "
        "WHERE tr.id = $1",
        facts.task_run_id,
    )
    assert task is not None
    predecessor = create_dispatch_envelope(
        dispatch_id=dispatch_id,
        task_attempt_id=facts.attempt_id,
        task_run_id=facts.task_run_id,
        workflow_run_id=task["workflow_run_id"],
        attempt_number=1,
        task_type="test.task",
        required_capability="test-capability",
        task_payload={},
        references={},
    )
    await connection.execute(
        "INSERT INTO task_dispatch_outbox (id, task_attempt_id, route, payload) "
        "VALUES ($1, $2, $3, $4::jsonb)",
        dispatch_id,
        facts.attempt_id,
        predecessor.route,
        json.dumps(dispatch_envelope_to_mapping(predecessor)),
    )
    observed_at = await connection.fetchval("SELECT statement_timestamp()")
    assert isinstance(observed_at, datetime)
    workflow_run_id = await connection.fetchval(
        "SELECT workflow_run_id FROM task_runs WHERE id = $1", facts.task_run_id
    )
    assert isinstance(workflow_run_id, UUID)
    if run_status == "cancelling":
        requester = await connection.fetchval(
            "SELECT requested_by_principal_id FROM workflow_runs WHERE id = $1",
            workflow_run_id,
        )
        await connection.execute(
            "INSERT INTO workflow_run_cancellation_requests "
            "(workflow_run_id, requested_by_principal_id, reason, "
            "idempotency_key_digest, request_fingerprint) VALUES "
            "($1, $2, 'recovery test cancellation', $3, $4)",
            workflow_run_id,
            requester,
            "a" * 64,
            "b" * 64,
        )
    return ExpiredClaimCandidate(
        facts.attempt_id,
        facts.task_run_id,
        workflow_run_id,
        1,
        1,
        facts.session_id,
        facts.lease_expires_at,
        observed_at,
    )


async def shape(
    connection: asyncpg.Connection[asyncpg.Record], candidate: ExpiredClaimCandidate
) -> asyncpg.Record:
    row = await connection.fetchrow(
        "SELECT tr.status::text, c.terminated_at, r.failure_kind, r.completed_at, "
        "(SELECT count(*) FROM task_attempts a WHERE a.task_run_id = tr.id) "
        "AS attempt_count, (SELECT max(a.attempt_number) FROM task_attempts a "
        "WHERE a.task_run_id = tr.id) AS latest_attempt, "
        "(SELECT max(a.next_eligible_at) FROM task_attempts a WHERE "
        "a.task_run_id = tr.id) AS next_eligible_at, "
        "(SELECT count(*) FROM task_retry_events e WHERE e.task_run_id = tr.id) "
        "AS event_count FROM task_runs tr JOIN task_attempt_claims c ON "
        "c.task_attempt_id = $1 LEFT JOIN task_attempt_results r ON "
        "r.task_attempt_id = $1 WHERE tr.id = $2",
        candidate.task_attempt_id,
        candidate.task_run_id,
    )
    assert row is not None
    return row


async def wait_until_lock_blocked(
    monitor: asyncpg.Connection[asyncpg.Record], application_name: str
) -> None:
    for _ in range(100):
        blocked = await monitor.fetchval(
            "SELECT wait_event_type = 'Lock' FROM pg_stat_activity WHERE "
            "datname = current_database() AND application_name = $1",
            application_name,
        )
        if blocked:
            return
        await asyncio.sleep(0.01)
    pytest.fail(f"{application_name} did not reach the expected row lock")


async def wait_until_expired(
    connection: asyncpg.Connection[asyncpg.Record], lease_expires_at: datetime
) -> datetime:
    for _ in range(100):
        observed_at = await connection.fetchval("SELECT statement_timestamp()")
        assert isinstance(observed_at, datetime)
        if observed_at >= lease_expires_at:
            return observed_at
        await asyncio.sleep(0.01)
    pytest.fail("claim did not reach its PostgreSQL expiry boundary")


async def late_renewal(database_url: URL, candidate: ExpiredClaimCandidate) -> str:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        await connection.execute("SET application_name = 'late-recovery-renewal'")
        result = await connection.execute(
            "UPDATE task_attempt_claims SET lease_expires_at = "
            "statement_timestamp() + interval '1 minute' WHERE "
            "task_attempt_id = $1 AND generation = $2 AND terminated_at IS NULL "
            "AND lease_expires_at = $3",
            candidate.task_attempt_id,
            candidate.generation,
            candidate.lease_expires_at,
        )
        assert isinstance(result, str)
        return result
    finally:
        await connection.close()


async def production_result_race_candidate(
    connection: asyncpg.Connection[asyncpg.Record],
    claim_service: TaskClaimService,
) -> tuple[ExpiredClaimCandidate, WorkerFacts, DispatchEnvelope, IssuedTaskClaim]:
    worker = await add_worker(connection)
    dispatch = await add_dispatched_task(connection)
    issued = await claim_service.claim_task(
        worker.authenticated, worker.session_id, dispatch
    )
    lease_expires_at = await connection.fetchval(
        "UPDATE task_attempt_claims SET lease_expires_at = acquired_at + "
        "interval '1 microsecond' WHERE task_attempt_id = $1 AND generation = $2 "
        "RETURNING lease_expires_at",
        dispatch.task_attempt_id,
        issued.claim.generation,
    )
    observed_at = await connection.fetchval("SELECT statement_timestamp()")
    workflow_run_id = await connection.fetchval(
        "SELECT workflow_run_id FROM task_runs WHERE id = $1", dispatch.task_run_id
    )
    assert isinstance(lease_expires_at, datetime)
    assert isinstance(observed_at, datetime)
    assert isinstance(workflow_run_id, UUID)
    candidate = ExpiredClaimCandidate(
        dispatch.task_attempt_id,
        dispatch.task_run_id,
        workflow_run_id,
        dispatch.attempt_number,
        issued.claim.generation,
        worker.session_id,
        lease_expires_at,
        observed_at,
    )
    return candidate, worker, dispatch, issued


async def exercise_recovery(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg"), pool_size=6
    )
    service = ExpiredClaimRecoveryService(
        SQLAlchemyExpiredClaimRecoveryRepository(build_session_factory(engine))
    )
    issuer = TaskClaimResultAuthorityIssuer(_AUTHORITY_SECRET)
    claim_service = TaskClaimService(
        SQLAlchemyTaskClaimRepository(
            build_session_factory(engine), worker_stale_after_seconds=30
        ),
        issuer,
        lease_seconds=60,
    )
    result_service = TaskResultSubmissionService(
        SQLAlchemyTaskResultRepository(build_session_factory(engine)), issuer
    )
    run_service = WorkflowRunService(
        SQLAlchemyWorkflowRunRepository(build_session_factory(engine))
    )
    progression = ExpiredClaimRecoveryProgressionService(service, run_service)
    due_scanner = DueRetryScanner(
        SQLAlchemyRetryTransitionRepository(build_session_factory(engine)), registry()
    )
    try:
        retryable = await recoverable_candidate(connection)
        recovered = await service.recover_expired_claim(retryable)
        assert recovered.outcome is ExpiredClaimRecoveryOutcome.RETRY_SCHEDULED
        retry_shape = await shape(connection, retryable)
        assert tuple(retry_shape)[:3] == (
            "retry_scheduled",
            recovered.recovered_at,
            "claim_expired",
        )
        assert retry_shape[3] == recovered.recovered_at
        assert retry_shape[4:6] == (2, 2)
        assert retry_shape[6] == recovered.recovered_at + timedelta(seconds=7)
        assert retry_shape[7] == 1
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM task_result_events WHERE task_attempt_id = $1 "
                "AND event_type = 'result_recovered'",
                retryable.task_attempt_id,
            )
            == 1
        )
        assert await connection.fetchval(
            "SELECT result_fingerprint FROM task_attempt_results WHERE "
            "task_attempt_id = $1",
            retryable.task_attempt_id,
        ) == task_result_fingerprint(
            result_kind=TaskExecutionResultKind.RETRYABLE_FAILURE,
            failure_kind=TaskExecutionFailureKind.CLAIM_EXPIRED,
            output=None,
        )

        duplicate = await service.recover_expired_claim(retryable)
        assert duplicate.outcome is ExpiredClaimRecoveryOutcome.ALREADY_RECOVERED
        assert (await shape(connection, retryable))[4:] == retry_shape[4:]
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM task_result_events WHERE task_attempt_id = $1 "
                "AND event_type = 'result_recovered'",
                retryable.task_attempt_id,
            )
            == 1
        )
        await assert_status_execution_events(
            connection,
            retryable.workflow_run_id,
            (
                ExpectedStatusExecutionEvent(
                    retryable.task_run_id, "running", "retry_scheduled"
                ),
            ),
        )
        original_dispatch = await connection.fetchval(
            "SELECT id FROM task_dispatch_outbox WHERE task_attempt_id = $1",
            retryable.task_attempt_id,
        )
        with pytest.raises(asyncpg.UniqueViolationError):
            await connection.execute(
                "INSERT INTO task_attempt_results (task_attempt_id, "
                "claim_generation, dispatch_id, result_kind, failure_kind, "
                "output, result_fingerprint) VALUES "
                "($1, 1, $2, 'success', NULL, 'null'::jsonb, $3)",
                retryable.task_attempt_id,
                original_dispatch,
                "f" * 64,
            )
        assert (await shape(connection, retryable))[2] == "claim_expired"

        no_policy = await recoverable_candidate(connection, maximum_attempts=None)
        no_policy_result = await service.recover_expired_claim(no_policy)
        assert no_policy_result.outcome is ExpiredClaimRecoveryOutcome.FAILED_NO_POLICY
        assert (await shape(connection, no_policy))[0:8:4] == ("failed", 1)
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM task_result_events WHERE task_attempt_id = $1 "
                "AND event_type = 'result_recovered'",
                no_policy.task_attempt_id,
            )
            == 1
        )

        exhausted = await recoverable_candidate(connection, maximum_attempts=1)
        exhausted_result = await service.recover_expired_claim(exhausted)
        assert exhausted_result.outcome is ExpiredClaimRecoveryOutcome.FAILED_EXHAUSTED
        assert (await shape(connection, exhausted))[4:6] == (1, 1)
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM task_result_events WHERE task_attempt_id = $1 "
                "AND event_type = 'result_recovered'",
                exhausted.task_attempt_id,
            )
            == 1
        )

        renewed = await recoverable_candidate(connection)
        await connection.execute(
            "UPDATE task_attempt_claims SET lease_expires_at = "
            "statement_timestamp() + interval '1 minute' WHERE "
            "task_attempt_id = $1 AND generation = 1",
            renewed.task_attempt_id,
        )
        renewed_result = await service.recover_expired_claim(renewed)
        assert (
            renewed_result.outcome
            is ExpiredClaimRecoveryOutcome.CANDIDATE_NO_LONGER_EXPIRED
        )
        assert (await shape(connection, renewed))[:3] == ("running", None, None)

        superseded = await recoverable_candidate(connection)
        await connection.execute(
            "INSERT INTO task_attempts (id, task_run_id, attempt_number) "
            "VALUES ($1, $2, 2)",
            uuid4(),
            superseded.task_run_id,
        )
        assert (
            await service.recover_expired_claim(superseded)
        ).outcome is ExpiredClaimRecoveryOutcome.ATTEMPT_NO_LONGER_LATEST
        assert (await shape(connection, superseded))[:3] == ("running", None, None)

        cancelling = await recoverable_candidate(connection, run_status="cancelling")
        assert (
            await service.recover_expired_claim(cancelling)
        ).outcome is ExpiredClaimRecoveryOutcome.CANCELLED
        cancelling_shape = await shape(connection, cancelling)
        assert cancelling_shape[:3] == ("cancelled", cancelling_shape[1], None)
        assert cancelling_shape[1] is not None
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM task_result_events WHERE task_attempt_id = $1 "
                "AND event_type = 'result_recovered' AND result_kind = 'cancellation'",
                cancelling.task_attempt_id,
            )
            == 1
        )
        assert (
            await service.recover_expired_claim(cancelling)
        ).outcome is ExpiredClaimRecoveryOutcome.ALREADY_RECOVERED

        (
            recovered_cancel,
            recovered_worker,
            recovered_dispatch,
            recovered_claim,
        ) = await production_result_race_candidate(connection, claim_service)
        requester = await connection.fetchval(
            "SELECT requested_by_principal_id FROM workflow_runs WHERE id = $1",
            recovered_cancel.workflow_run_id,
        )
        await connection.execute(
            "INSERT INTO workflow_run_cancellation_requests "
            "(workflow_run_id, requested_by_principal_id, idempotency_key_digest, "
            "request_fingerprint) VALUES ($1, $2, $3, $4)",
            recovered_cancel.workflow_run_id,
            requester,
            "c" * 64,
            "d" * 64,
        )
        await connection.execute(
            "UPDATE workflow_runs SET status = 'cancelling' WHERE id = $1",
            recovered_cancel.workflow_run_id,
        )
        assert (
            await service.recover_expired_claim(recovered_cancel)
        ).outcome is ExpiredClaimRecoveryOutcome.CANCELLED
        recovered_authority = recovered_claim.result_authority
        assert isinstance(recovered_authority, TaskClaimResultAuthority)
        # The semantic cancellation fingerprint matches, but recovered provenance
        # has already ended this authority generation, so a worker submission is
        # stale rather than an identical replay.
        with pytest.raises(TaskResultStale):
            await result_service.submit_result(
                recovered_worker.authenticated,
                recovered_worker.session_id,
                TaskResultSubmissionRequest(
                    recovered_dispatch.dispatch_id,
                    recovered_dispatch.task_run_id,
                    recovered_dispatch.task_attempt_id,
                    recovered_claim.claim.generation,
                    recovered_authority,
                    TaskExecutionResult.cancellation(),
                ),
            )

        result_first = await recoverable_candidate(connection)
        dispatch_id = await connection.fetchval(
            "SELECT id FROM task_dispatch_outbox WHERE task_attempt_id = $1",
            result_first.task_attempt_id,
        )
        await connection.execute(
            "INSERT INTO task_attempt_results (task_attempt_id, claim_generation, "
            "dispatch_id, result_kind, failure_kind, output, result_fingerprint) "
            "VALUES ($1, 1, $2, 'success', NULL, 'null'::jsonb, $3)",
            result_first.task_attempt_id,
            dispatch_id,
            "a" * 64,
        )
        result_outcome = await service.recover_expired_claim(result_first)
        assert (
            result_outcome.outcome
            is ExpiredClaimRecoveryOutcome.RESULT_ALREADY_ACCEPTED
        )

        (
            production_result_first,
            first_worker,
            first_dispatch,
            first_claim,
        ) = await production_result_race_candidate(connection, claim_service)
        await connection.execute(
            "UPDATE task_attempt_claims SET lease_expires_at = "
            "statement_timestamp() + interval '1 minute' WHERE "
            "task_attempt_id = $1 AND generation = $2",
            production_result_first.task_attempt_id,
            production_result_first.generation,
        )
        await connection.execute(
            "UPDATE task_runs SET status = 'running' WHERE id = $1",
            production_result_first.task_run_id,
        )
        first_authority = first_claim.result_authority
        assert isinstance(first_authority, TaskClaimResultAuthority)
        await result_service.submit_result(
            first_worker.authenticated,
            first_worker.session_id,
            TaskResultSubmissionRequest(
                first_dispatch.dispatch_id,
                first_dispatch.task_run_id,
                first_dispatch.task_attempt_id,
                first_claim.claim.generation,
                first_authority,
                TaskExecutionResult.success("worker-won"),
            ),
        )
        assert (
            await service.recover_expired_claim(production_result_first)
        ).outcome is ExpiredClaimRecoveryOutcome.RESULT_ALREADY_ACCEPTED
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT FROM task_result_events WHERE "
            "task_attempt_id = $1 AND event_type = 'result_recovered')",
            production_result_first.task_attempt_id,
        )

        boundary = await recoverable_candidate(connection)
        boundary_time = await connection.fetchval("SELECT statement_timestamp()")
        assert isinstance(boundary_time, datetime)
        await connection.execute(
            "UPDATE task_attempt_claims SET lease_expires_at = $2 WHERE "
            "task_attempt_id = $1",
            boundary.task_attempt_id,
            boundary_time,
        )
        boundary = ExpiredClaimCandidate(
            boundary.task_attempt_id,
            boundary.task_run_id,
            boundary.workflow_run_id,
            1,
            1,
            boundary.worker_session_id,
            boundary_time,
            boundary_time,
        )
        assert (
            await service.recover_expired_claim(boundary)
        ).outcome is ExpiredClaimRecoveryOutcome.RETRY_SCHEDULED

        concurrent_candidate = await recoverable_candidate(connection)
        concurrent = await asyncio.gather(
            service.recover_expired_claim(concurrent_candidate),
            service.recover_expired_claim(concurrent_candidate),
        )
        assert {item.outcome for item in concurrent} == {
            ExpiredClaimRecoveryOutcome.RETRY_SCHEDULED,
            ExpiredClaimRecoveryOutcome.ALREADY_RECOVERED,
        }
        concurrent_shape = await shape(connection, concurrent_candidate)
        assert concurrent_shape[4:] == (
            2,
            2,
            concurrent[0].next_eligible_at or concurrent[1].next_eligible_at,
            1,
        )

        lock_winner = await recoverable_candidate(connection)
        repository = SQLAlchemyExpiredClaimRecoveryRepository(
            build_session_factory(engine)
        )
        context = repository.recovery_transaction()
        transaction = await context.__aenter__()
        try:
            prepared = await transaction.prepare_recovery(lock_winner)
            assert isinstance(prepared, PreparedExpiredClaimRecovery)
            renewal = asyncio.create_task(late_renewal(database_url, lock_winner))
            await wait_until_lock_blocked(connection, "late-recovery-renewal")
            assert not renewal.done()
            replacement = NewScheduledRetryAttempt(
                uuid4(),
                lock_winner.task_run_id,
                2,
                prepared.recovered_at + timedelta(seconds=7),
            )
            await transaction.schedule_retry(prepared, replacement)
        except BaseException as error:
            await context.__aexit__(type(error), error, error.__traceback__)
            raise
        else:
            await context.__aexit__(None, None, None)
        assert await renewal == "UPDATE 0"
        assert (await shape(connection, lock_winner))[:3] == (
            "retry_scheduled",
            prepared.recovered_at,
            "claim_expired",
        )

        (
            renewal_race,
            renewal_worker,
            _,
            _,
        ) = await production_result_race_candidate(connection, claim_service)
        renewal_engine = create_async_engine(
            database_url.set(drivername="postgresql+asyncpg"),
            connect_args={"server_settings": {"application_name": "recovered-renewal"}},
        )
        renewal_service = TaskClaimService(
            SQLAlchemyTaskClaimRepository(
                build_session_factory(renewal_engine), worker_stale_after_seconds=30
            ),
            issuer,
            lease_seconds=60,
        )
        renewal_context = repository.recovery_transaction()
        renewal_transaction = await renewal_context.__aenter__()
        try:
            renewal_prepared = await renewal_transaction.prepare_recovery(renewal_race)
            assert isinstance(renewal_prepared, PreparedExpiredClaimRecovery)
            renewal_request = TaskClaimRenewalRequest(
                renewal_race.task_attempt_id,
                renewal_race.generation,
                renewal_race.worker_session_id,
                renewal_race.lease_expires_at,
            )
            renewal_submission = asyncio.create_task(
                renewal_service.renew_claim(
                    renewal_worker.authenticated, renewal_request
                )
            )
            await wait_until_lock_blocked(connection, "recovered-renewal")
            assert not renewal_submission.done()
            await renewal_transaction.exhaust(
                renewal_prepared, RetryNotScheduledReason.NO_POLICY
            )
        except BaseException as error:
            await renewal_context.__aexit__(type(error), error, error.__traceback__)
            raise
        else:
            await renewal_context.__aexit__(None, None, None)
        with pytest.raises(TaskClaimRenewalRecovered):
            await renewal_submission
        await renewal_engine.dispose()
        assert (
            await connection.fetchval(
                "SELECT lease_expires_at FROM task_attempt_claims WHERE "
                "task_attempt_id = $1 AND generation = $2",
                renewal_race.task_attempt_id,
                renewal_race.generation,
            )
            == renewal_race.lease_expires_at
        )

        renewal_winner_worker = await add_worker(connection)
        renewal_winner_dispatch = await add_dispatched_task(connection)
        renewal_winner_claim = await claim_service.claim_task(
            renewal_winner_worker.authenticated,
            renewal_winner_worker.session_id,
            renewal_winner_dispatch,
        )
        short_expiry = await connection.fetchval(
            "UPDATE task_attempt_claims SET lease_expires_at = "
            "statement_timestamp() + interval '250 milliseconds' WHERE "
            "task_attempt_id = $1 AND generation = $2 RETURNING lease_expires_at",
            renewal_winner_dispatch.task_attempt_id,
            renewal_winner_claim.claim.generation,
        )
        assert isinstance(short_expiry, datetime)
        await connection.execute("SELECT pg_advisory_lock(13000401)")
        await connection.execute(
            "CREATE FUNCTION pause_winning_renewal() RETURNS trigger LANGUAGE "
            "plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(13000401); "
            "RETURN NEW; END $$"
        )
        await connection.execute(
            "CREATE TRIGGER trg_pause_winning_renewal BEFORE UPDATE OF "
            "lease_expires_at ON task_attempt_claims FOR EACH ROW EXECUTE "
            "FUNCTION pause_winning_renewal()"
        )
        renewal_winner_engine = create_async_engine(
            database_url.set(drivername="postgresql+asyncpg"),
            connect_args={"server_settings": {"application_name": "winning-renewal"}},
        )
        renewal_winner_service = TaskClaimService(
            SQLAlchemyTaskClaimRepository(
                build_session_factory(renewal_winner_engine),
                worker_stale_after_seconds=30,
            ),
            issuer,
            lease_seconds=60,
        )
        winning_renewal = asyncio.create_task(
            renewal_winner_service.renew_claim(
                renewal_winner_worker.authenticated,
                TaskClaimRenewalRequest(
                    renewal_winner_dispatch.task_attempt_id,
                    renewal_winner_claim.claim.generation,
                    renewal_winner_worker.session_id,
                    short_expiry,
                ),
            )
        )
        await wait_until_lock_blocked(connection, "winning-renewal")
        observed_at = await wait_until_expired(connection, short_expiry)
        renewal_winner_candidate = ExpiredClaimCandidate(
            renewal_winner_dispatch.task_attempt_id,
            renewal_winner_dispatch.task_run_id,
            renewal_winner_dispatch.workflow_run_id,
            renewal_winner_dispatch.attempt_number,
            renewal_winner_claim.claim.generation,
            renewal_winner_worker.session_id,
            short_expiry,
            observed_at,
        )
        blocked_recovery_engine = create_async_engine(
            database_url.set(drivername="postgresql+asyncpg"),
            connect_args={
                "server_settings": {"application_name": "renewal-blocked-recovery"}
            },
        )
        blocked_recovery_service = ExpiredClaimRecoveryService(
            SQLAlchemyExpiredClaimRecoveryRepository(
                build_session_factory(blocked_recovery_engine)
            )
        )
        blocked_recovery = asyncio.create_task(
            blocked_recovery_service.recover_expired_claim(renewal_winner_candidate)
        )
        await wait_until_lock_blocked(connection, "renewal-blocked-recovery")
        assert not winning_renewal.done()
        assert not blocked_recovery.done()
        assert await connection.fetchval("SELECT pg_advisory_unlock(13000401)") is True
        renewed_receipt = await winning_renewal
        recovery_after_renewal = await blocked_recovery
        assert renewed_receipt.outcome is TaskClaimRenewalOutcome.RENEWED
        assert (
            recovery_after_renewal.outcome
            is ExpiredClaimRecoveryOutcome.CANDIDATE_NO_LONGER_EXPIRED
        )
        renewal_winner_state = await connection.fetchrow(
            "SELECT c.lease_expires_at, c.terminated_at, "
            "(SELECT count(*) FROM task_attempt_results r WHERE "
            "r.task_attempt_id = c.task_attempt_id AND "
            "r.failure_kind = 'claim_expired') AS recovery_results, "
            "(SELECT count(*) FROM task_result_events e WHERE "
            "e.task_attempt_id = c.task_attempt_id AND "
            "e.event_type = 'result_recovered') AS recovery_events, "
            "(SELECT count(*) FROM task_attempts a WHERE a.task_run_id = $2) "
            "AS attempt_count, (SELECT count(*) FROM task_retry_events e "
            "WHERE e.task_run_id = $2) AS retry_events FROM "
            "task_attempt_claims c WHERE c.task_attempt_id = $1 AND "
            "c.generation = $3",
            renewal_winner_candidate.task_attempt_id,
            renewal_winner_candidate.task_run_id,
            renewal_winner_candidate.generation,
        )
        assert renewal_winner_state is not None
        assert renewal_winner_state["lease_expires_at"] > short_expiry
        assert renewal_winner_state["terminated_at"] is None
        assert tuple(renewal_winner_state)[2:] == (0, 0, 1, 0)
        await renewal_winner_engine.dispose()
        await blocked_recovery_engine.dispose()
        await connection.execute(
            "DROP TRIGGER trg_pause_winning_renewal ON task_attempt_claims"
        )
        await connection.execute("DROP FUNCTION pause_winning_renewal()")

        (
            result_race,
            result_worker,
            result_dispatch,
            result_claim,
        ) = await production_result_race_candidate(connection, claim_service)
        result_engine = create_async_engine(
            database_url.set(drivername="postgresql+asyncpg"),
            connect_args={
                "server_settings": {"application_name": "late-recovery-result"}
            },
        )
        result_service = TaskResultSubmissionService(
            SQLAlchemyTaskResultRepository(build_session_factory(result_engine)), issuer
        )
        result_context = repository.recovery_transaction()
        result_transaction = await result_context.__aenter__()
        try:
            result_prepared = await result_transaction.prepare_recovery(result_race)
            assert isinstance(result_prepared, PreparedExpiredClaimRecovery)
            authority = result_claim.result_authority
            assert isinstance(authority, TaskClaimResultAuthority)
            submitted = asyncio.create_task(
                result_service.submit_result(
                    result_worker.authenticated,
                    result_worker.session_id,
                    TaskResultSubmissionRequest(
                        result_dispatch.dispatch_id,
                        result_dispatch.task_run_id,
                        result_dispatch.task_attempt_id,
                        result_claim.claim.generation,
                        authority,
                        TaskExecutionResult.success("late"),
                    ),
                )
            )
            await wait_until_lock_blocked(connection, "late-recovery-result")
            assert not submitted.done()
            await result_transaction.exhaust(
                result_prepared, RetryNotScheduledReason.NO_POLICY
            )
        except BaseException as error:
            await result_context.__aexit__(type(error), error, error.__traceback__)
            raise
        else:
            await result_context.__aexit__(None, None, None)
        with pytest.raises(TaskResultStale):
            await submitted
        result_race_shape = await shape(connection, result_race)
        assert result_race_shape[:6] == (
            "failed",
            result_prepared.recovered_at,
            "claim_expired",
            result_prepared.recovered_at,
            1,
            1,
        )
        assert result_race_shape[7] == 1
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM task_attempt_results WHERE task_attempt_id = $1 "
                "AND failure_kind = 'claim_expired'",
                result_race.task_attempt_id,
            )
            == 1
        )
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM task_result_events WHERE task_attempt_id = $1 "
                "AND event_type = 'result_stale_rejected'",
                result_race.task_attempt_id,
            )
            == 1
        )
        for late_value in ("late", "different-late-result"):
            with pytest.raises(TaskResultStale):
                await result_service.submit_result(
                    result_worker.authenticated,
                    result_worker.session_id,
                    TaskResultSubmissionRequest(
                        result_dispatch.dispatch_id,
                        result_dispatch.task_run_id,
                        result_dispatch.task_attempt_id,
                        result_claim.claim.generation,
                        authority,
                        TaskExecutionResult.success(late_value),
                    ),
                )
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM task_result_events WHERE task_attempt_id = $1 "
                "AND event_type = 'result_stale_rejected'",
                result_race.task_attempt_id,
            )
            == 3
        )
        stale_fingerprints = await connection.fetch(
            "SELECT result_fingerprint FROM task_result_events WHERE "
            "task_attempt_id = $1 AND event_type = 'result_stale_rejected'",
            result_race.task_attempt_id,
        )
        assert Counter(row["result_fingerprint"] for row in stale_fingerprints) == (
            Counter(
                task_result_fingerprint(
                    result_kind=TaskExecutionResultKind.SUCCESS,
                    failure_kind=None,
                    output=value,
                )
                for value in ("late", "late", "different-late-result")
            )
        )
        await result_engine.dispose()

        event_rollback = await recoverable_candidate(connection)
        await connection.execute(
            "CREATE FUNCTION reject_recovery_result_event() RETURNS trigger "
            "LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION "
            "'injected recovery event failure'; END $$"
        )
        await connection.execute(
            "CREATE TRIGGER trg_inject_recovery_event_failure BEFORE INSERT ON "
            "task_result_events FOR EACH ROW WHEN "
            "(NEW.event_type = 'result_recovered') EXECUTE FUNCTION "
            "reject_recovery_result_event()"
        )
        with pytest.raises(ExpiredClaimRecoveryServiceUnavailable):
            await service.recover_expired_claim(event_rollback)
        assert (await shape(connection, event_rollback)) == (
            "running",
            None,
            None,
            None,
            1,
            1,
            None,
            0,
        )
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT FROM task_result_events WHERE task_attempt_id = $1)",
            event_rollback.task_attempt_id,
        )
        await connection.execute(
            "DROP TRIGGER trg_inject_recovery_event_failure ON task_result_events"
        )
        await connection.execute("DROP FUNCTION reject_recovery_result_event()")

        rollback = await recoverable_candidate(connection)
        await connection.execute(
            "CREATE FUNCTION reject_recovery_retry_event() RETURNS trigger "
            "LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected failure'; END $$"
        )
        await connection.execute(
            "CREATE TRIGGER trg_inject_recovery_failure BEFORE INSERT ON "
            "task_retry_events FOR EACH ROW EXECUTE FUNCTION "
            "reject_recovery_retry_event()"
        )
        with pytest.raises(ExpiredClaimRecoveryServiceUnavailable):
            await service.recover_expired_claim(rollback)
        assert (await shape(connection, rollback)) == (
            "running",
            None,
            None,
            None,
            1,
            1,
            None,
            0,
        )
        await connection.execute(
            "DROP TRIGGER trg_inject_recovery_failure ON task_retry_events"
        )
        await connection.execute("DROP FUNCTION reject_recovery_retry_event()")

        dead_letter_rollback = await recoverable_candidate(
            connection, maximum_attempts=1
        )
        await connection.execute(
            "CREATE FUNCTION reject_recovery_dead_letter_status() RETURNS trigger "
            "LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION "
            "'injected dead-letter status failure'; END $$"
        )
        await connection.execute(
            "CREATE TRIGGER trg_inject_recovery_dead_letter_status BEFORE INSERT ON "
            "dead_letter_status FOR EACH ROW EXECUTE FUNCTION "
            "reject_recovery_dead_letter_status()"
        )
        with pytest.raises(ExpiredClaimRecoveryServiceUnavailable):
            await service.recover_expired_claim(dead_letter_rollback)
        assert (await shape(connection, dead_letter_rollback)) == (
            "running",
            None,
            None,
            None,
            1,
            1,
            None,
            0,
        )
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT FROM dead_letter_items "
            "WHERE source_task_attempt_id = $1)",
            dead_letter_rollback.task_attempt_id,
        )
        await connection.execute(
            "DROP TRIGGER trg_inject_recovery_dead_letter_status ON dead_letter_status"
        )
        await connection.execute("DROP FUNCTION reject_recovery_dead_letter_status()")

        due = await recoverable_candidate(connection, initial_delay_seconds=0)
        scheduled = await progression.recover_and_progress(due)
        assert scheduled.recovery.outcome is ExpiredClaimRecoveryOutcome.RETRY_SCHEDULED
        assert scheduled.reconciliation is None
        dispatched = await asyncio.gather(
            due_scanner.scan_due_retries(batch_size=1),
            due_scanner.scan_due_retries(batch_size=1),
        )
        assert sum(item.dispatched for item in dispatched) == 1
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM task_attempts WHERE task_run_id = $1",
                due.task_run_id,
            )
            == 2
        )
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM task_dispatch_outbox o JOIN task_attempts a "
                "ON a.id = o.task_attempt_id WHERE a.task_run_id = $1 "
                "AND a.attempt_number = 2",
                due.task_run_id,
            )
            == 1
        )
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM task_retry_events WHERE task_run_id = $1",
                due.task_run_id,
            )
            == 2
        )
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT FROM dead_letter_items d JOIN task_attempts a "
            "ON a.id = d.source_task_attempt_id WHERE a.task_run_id = $1)",
            due.task_run_id,
        )

        cancelled = await recoverable_candidate(connection, initial_delay_seconds=0)
        assert (
            await service.recover_expired_claim(cancelled)
        ).outcome is ExpiredClaimRecoveryOutcome.RETRY_SCHEDULED
        await connection.execute(
            "UPDATE workflow_runs SET status = 'cancelling' WHERE id = $1",
            cancelled.workflow_run_id,
        )
        cancellation_scan = await due_scanner.scan_due_retries(batch_size=1)
        assert cancelled.task_attempt_id not in cancellation_scan.dispatched_attempt_ids
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM task_dispatch_outbox o JOIN task_attempts a "
                "ON a.id = o.task_attempt_id WHERE a.task_run_id = $1 "
                "AND a.attempt_number = 2",
                cancelled.task_run_id,
            )
            == 0
        )

        failed = await recoverable_candidate(connection, maximum_attempts=1)
        progressed = await progression.recover_and_progress(failed)
        assert (
            progressed.recovery.outcome is ExpiredClaimRecoveryOutcome.FAILED_EXHAUSTED
        )
        assert progressed.reconciliation is not None
        assert progressed.reconciliation.final_status is WorkflowRunStatus.FAILED
        assert tuple(
            await connection.fetchrow(
                "SELECT d.task_run_id, d.reason, s.status FROM dead_letter_items d "
                "JOIN dead_letter_status s ON s.dead_letter_item_id = d.id "
                "WHERE d.source_task_attempt_id = $1",
                failed.task_attempt_id,
            )
        ) == (failed.task_run_id, "retry_exhausted", "open")

        no_policy_progression = await recoverable_candidate(
            connection, maximum_attempts=None
        )
        no_policy_progressed = await progression.recover_and_progress(
            no_policy_progression
        )
        assert (
            no_policy_progressed.recovery.outcome
            is ExpiredClaimRecoveryOutcome.FAILED_NO_POLICY
        )
        assert no_policy_progressed.reconciliation is not None
        assert (
            no_policy_progressed.reconciliation.final_status is WorkflowRunStatus.FAILED
        )
        assert (
            await connection.fetchval(
                "SELECT decision_reason FROM task_retry_events "
                "WHERE task_run_id = $1 AND event_type = 'retry_not_scheduled'",
                no_policy_progression.task_run_id,
            )
            == "no_policy"
        )
        assert tuple(
            await connection.fetchrow(
                "SELECT d.reason, s.status FROM dead_letter_items d "
                "JOIN dead_letter_status s ON s.dead_letter_item_id = d.id "
                "WHERE d.source_task_attempt_id = $1",
                no_policy_progression.task_attempt_id,
            )
        ) == ("retry_exhausted", "open")

        progression_failure = await recoverable_candidate(
            connection, maximum_attempts=1
        )
        await connection.execute(
            "CREATE FUNCTION reject_recovery_progression() RETURNS trigger "
            "LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION "
            "'injected progression failure'; END $$"
        )
        await connection.execute(
            "CREATE TRIGGER trg_inject_progression_failure BEFORE UPDATE OF status ON "
            "workflow_runs FOR EACH ROW EXECUTE FUNCTION "
            "reject_recovery_progression()"
        )
        with pytest.raises(ExpiredClaimRecoveryProgressionUnavailable) as captured:
            await progression.recover_and_progress(progression_failure)
        assert (
            captured.value.recovery_receipt.outcome
            is ExpiredClaimRecoveryOutcome.FAILED_EXHAUSTED
        )
        assert (
            await connection.fetchval(
                "SELECT status::text FROM task_runs WHERE id = $1",
                progression_failure.task_run_id,
            )
            == "failed"
        )
        assert (
            await connection.fetchval(
                "SELECT status::text FROM workflow_runs WHERE id = $1",
                progression_failure.workflow_run_id,
            )
            == "running"
        )
        await connection.execute(
            "DROP TRIGGER trg_inject_progression_failure ON workflow_runs"
        )
        await connection.execute("DROP FUNCTION reject_recovery_progression()")
        replayed = await progression.recover_and_progress(progression_failure)
        assert (
            replayed.recovery.outcome is ExpiredClaimRecoveryOutcome.ALREADY_RECOVERED
        )
        assert replayed.reconciliation is not None
        assert replayed.reconciliation.final_status is WorkflowRunStatus.FAILED
        repeated = await progression.recover_and_progress(progression_failure)
        assert (
            repeated.recovery.outcome is ExpiredClaimRecoveryOutcome.ALREADY_RECOVERED
        )
        assert repeated.reconciliation is not None
        assert repeated.reconciliation.workflow_transition_count == 0
    finally:
        await engine.dispose()
        await connection.close()


def test_real_postgresql_expired_claim_recovery() -> None:
    with temporary_database(
        "TASKFORGE_RECOVERY_TEST_DATABASE_URL", "taskforge_recovery_transition"
    ) as database_url:
        config = Config("alembic.ini")
        with migration_database_url(database_url.render_as_string(hide_password=False)):
            command.upgrade(config, "head")
        asyncio.run(exercise_recovery(database_url))
