"""Real PostgreSQL authoritative result persistence and contention tests."""

from __future__ import annotations

import asyncio
import os

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from taskforge.claims.authority import TaskClaimResultAuthorityIssuer
from taskforge.claims.domain import IssuedTaskClaim, TaskClaimResultAuthority
from taskforge.claims.service import TaskClaimService
from taskforge.dispatch.envelope import DispatchEnvelope
from taskforge.persistence.claims import SQLAlchemyTaskClaimRepository
from taskforge.persistence.database import build_session_factory
from taskforge.persistence.task_results import SQLAlchemyTaskResultRepository
from taskforge.rate_limits import AllowAllRateLimiter
from taskforge.worker.result_submission import (
    TaskResultConflict,
    TaskResultServiceUnavailable,
    TaskResultStale,
    TaskResultSubmissionOutcome,
    TaskResultSubmissionRequest,
    TaskResultSubmissionService,
)
from taskforge.worker.results import TaskExecutionResult, TaskExecutionResultKind
from tests.integration.postgresql import (
    ExpectedStatusExecutionEvent,
    assert_status_execution_events,
    asyncpg_dsn,
    migration_database_url,
    temporary_database,
)
from tests.integration.test_task_claim_acquisition import (
    WorkerFacts,
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

_SECRET = b"task-result-integration-secret-value"


async def claimed_running_task(
    setup: asyncpg.Connection[asyncpg.Record],
    claim_service: TaskClaimService,
    *,
    worker: WorkerFacts | None = None,
) -> tuple[WorkerFacts, DispatchEnvelope, IssuedTaskClaim]:
    worker = worker or await add_worker(setup)
    dispatch = await add_dispatched_task(setup)
    issued = await claim_service.claim_task(
        worker.authenticated, worker.session_id, dispatch
    )
    await setup.execute(
        "UPDATE task_runs SET status = 'running' WHERE id = $1",
        dispatch.task_run_id,
    )
    return worker, dispatch, issued


def submission(
    dispatch: DispatchEnvelope, issued: IssuedTaskClaim, result: TaskExecutionResult
) -> TaskResultSubmissionRequest:
    authority = issued.result_authority
    assert isinstance(authority, TaskClaimResultAuthority)
    return TaskResultSubmissionRequest(
        dispatch.dispatch_id,
        dispatch.task_run_id,
        dispatch.task_attempt_id,
        issued.claim.generation,
        authority,
        result,
    )


async def exercise_results(database_url: URL) -> None:
    setup = await asyncpg.connect(asyncpg_dsn(database_url))
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
    )
    sessions = build_session_factory(engine)
    issuer = TaskClaimResultAuthorityIssuer(_SECRET)
    claim_service = TaskClaimService(
        SQLAlchemyTaskClaimRepository(sessions, worker_stale_after_seconds=30),
        issuer,
        lease_seconds=60,
    )
    result_service = TaskResultSubmissionService(
        SQLAlchemyTaskResultRepository(sessions),
        issuer,
        rate_limiter=AllowAllRateLimiter(),
    )
    try:
        worker, dispatch, issued = await claimed_running_task(setup, claim_service)
        accepted_request = submission(
            dispatch, issued, TaskExecutionResult.success({"answer": 42})
        )
        accepted = await result_service.submit_result(
            worker.authenticated, worker.session_id, accepted_request
        )
        assert accepted.outcome is TaskResultSubmissionOutcome.ACCEPTED
        state = await setup.fetchrow(
            "SELECT tr.status::text, tar.result_kind, tar.output, "
            "tac.terminated_at IS NOT NULL AS terminated "
            "FROM task_runs tr JOIN task_attempts ta ON ta.task_run_id = tr.id "
            "JOIN task_attempt_results tar ON tar.task_attempt_id = ta.id "
            "JOIN task_attempt_claims tac ON tac.task_attempt_id = ta.id "
            "AND tac.generation = tar.claim_generation WHERE ta.id = $1",
            dispatch.task_attempt_id,
        )
        assert tuple(state) == ("succeeded", "success", '{"answer": 42}', True)
        await assert_status_execution_events(
            setup,
            dispatch.workflow_run_id,
            (
                ExpectedStatusExecutionEvent(
                    dispatch.task_run_id, "dispatched", "claimed"
                ),
                ExpectedStatusExecutionEvent(
                    dispatch.task_run_id, "running", "succeeded"
                ),
            ),
        )

        retry_worker, retry_dispatch, retry_claim = await claimed_running_task(
            setup, claim_service
        )
        run = await setup.fetchrow(
            "SELECT id, requested_by_principal_id FROM workflow_runs WHERE id = $1",
            retry_dispatch.workflow_run_id,
        )
        assert run is not None
        await setup.execute(
            "INSERT INTO workflow_run_cancellation_requests "
            "(workflow_run_id, requested_by_principal_id, idempotency_key_digest, "
            "request_fingerprint) VALUES ($1, $2, $3, $4)",
            run["id"],
            run["requested_by_principal_id"],
            "a" * 64,
            "b" * 64,
        )
        await setup.execute(
            "UPDATE workflow_runs SET status = 'cancelling' WHERE id = $1", run["id"]
        )
        retryable = await result_service.submit_result(
            retry_worker.authenticated,
            retry_worker.session_id,
            submission(
                retry_dispatch,
                retry_claim,
                TaskExecutionResult.retryable_handler_reported(),
            ),
        )
        assert retryable.outcome is TaskResultSubmissionOutcome.ACCEPTED
        retry_shape = await setup.fetchrow(
            "SELECT tr.status::text, tar.result_kind, tar.failure_kind, "
            "(SELECT count(*) FROM task_attempts next WHERE next.task_run_id = tr.id) "
            "FROM task_runs tr JOIN task_attempts ta ON ta.task_run_id = tr.id "
            "JOIN task_attempt_results tar ON tar.task_attempt_id = ta.id "
            "WHERE ta.id = $1",
            retry_dispatch.task_attempt_id,
        )
        assert tuple(retry_shape) == (
            "cancelled",
            "retryable_failure",
            "handler_reported",
            1,
        )

        replay = await result_service.submit_result(
            worker.authenticated, worker.session_id, accepted_request
        )
        assert replay.outcome is TaskResultSubmissionOutcome.REPLAYED_IDENTICAL
        with pytest.raises(TaskResultConflict):
            await result_service.submit_result(
                worker.authenticated,
                worker.session_id,
                submission(
                    dispatch, issued, TaskExecutionResult.success({"answer": 43})
                ),
            )
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM task_result_events WHERE task_attempt_id = $1",
                dispatch.task_attempt_id,
            )
            == 3
        )
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM task_attempts WHERE task_run_id = $1",
                dispatch.task_run_id,
            )
            == 1
        )
        for statement in (
            "UPDATE task_attempt_results SET result_kind = 'success' "
            "WHERE task_attempt_id = $1",
            "DELETE FROM task_attempt_results WHERE task_attempt_id = $1",
        ):
            with pytest.raises(asyncpg.PostgresError) as immutable:
                await setup.execute(statement, dispatch.task_attempt_id)
            assert immutable.value.sqlstate == "TF004"

        constraint_cases = (
            ("success", None, "'null'::jsonb", True),
            ("success", None, "NULL", False),
            ("retryable_failure", "handler_reported", "NULL", True),
            ("retryable_failure", "handler_reported", "'{}'::jsonb", False),
        )
        for index, (result_kind, failure_kind, output_sql, should_succeed) in enumerate(
            constraint_cases, start=1
        ):
            _, constraint_dispatch, constraint_claim = await claimed_running_task(
                setup, claim_service
            )
            insert = (
                "INSERT INTO task_attempt_results "
                "(task_attempt_id, claim_generation, dispatch_id, result_kind, "
                "failure_kind, output, result_fingerprint) VALUES "
                f"($1, $2, $3, $4, $5, {output_sql}, $6)"
            )
            arguments = (
                constraint_dispatch.task_attempt_id,
                constraint_claim.claim.generation,
                constraint_dispatch.dispatch_id,
                result_kind,
                failure_kind,
                f"{index:064x}",
            )
            if should_succeed:
                await setup.execute(insert, *arguments)
            else:
                with pytest.raises(asyncpg.CheckViolationError) as invalid_output:
                    await setup.execute(insert, *arguments)
                assert (
                    invalid_output.value.constraint_name
                    == "ck_task_attempt_results_output_presence_valid"
                )

        outcomes = (
            (
                TaskExecutionResult.retryable_handler_reported(),
                "retry_pending",
                "handler_reported",
            ),
            (
                TaskExecutionResult.retryable_handler_exception(),
                "retry_pending",
                "handler_exception",
            ),
            (
                TaskExecutionResult.retryable_execution_timeout(),
                "retry_pending",
                "execution_timeout",
            ),
            (TaskExecutionResult.permanent_failure(), "failed", "handler_reported"),
            (TaskExecutionResult.cancellation(), "cancelled", None),
        )
        for normalized, expected_status, expected_failure in outcomes:
            (
                outcome_worker,
                outcome_dispatch,
                outcome_claim,
            ) = await claimed_running_task(setup, claim_service)
            receipt = await result_service.submit_result(
                outcome_worker.authenticated,
                outcome_worker.session_id,
                submission(outcome_dispatch, outcome_claim, normalized),
            )
            assert receipt.outcome is TaskResultSubmissionOutcome.ACCEPTED
            persisted = await setup.fetchrow(
                "SELECT tr.status::text, tar.failure_kind, wr.status::text "
                "FROM task_runs tr JOIN workflow_runs wr ON wr.id = "
                "tr.workflow_run_id "
                "JOIN task_attempts ta ON ta.task_run_id = tr.id "
                "JOIN task_attempt_results tar ON tar.task_attempt_id = ta.id "
                "WHERE ta.id = $1",
                outcome_dispatch.task_attempt_id,
            )
            assert tuple(persisted) == (
                expected_status,
                expected_failure,
                "running",
            )
            dead_letter = await setup.fetchrow(
                "SELECT d.task_run_id, d.source_task_attempt_id, d.reason, "
                "s.status FROM dead_letter_items d JOIN dead_letter_status s "
                "ON s.dead_letter_item_id = d.id "
                "WHERE d.source_task_attempt_id = $1",
                outcome_dispatch.task_attempt_id,
            )
            if normalized.kind is TaskExecutionResultKind.PERMANENT_FAILURE:
                assert tuple(dead_letter) == (
                    outcome_dispatch.task_run_id,
                    outcome_dispatch.task_attempt_id,
                    "permanent_failure",
                    "open",
                )
                replayed = await result_service.submit_result(
                    outcome_worker.authenticated,
                    outcome_worker.session_id,
                    submission(outcome_dispatch, outcome_claim, normalized),
                )
                assert (
                    replayed.outcome is TaskResultSubmissionOutcome.REPLAYED_IDENTICAL
                )
                assert (
                    await setup.fetchval(
                        "SELECT count(*) FROM dead_letter_items "
                        "WHERE source_task_attempt_id = $1",
                        outcome_dispatch.task_attempt_id,
                    )
                    == 1
                )
            else:
                assert dead_letter is None

        stale_worker, stale_dispatch, stale_claim = await claimed_running_task(
            setup, claim_service
        )
        await setup.execute(
            "UPDATE task_attempt_claims SET lease_expires_at = acquired_at + "
            "interval '1 microsecond' WHERE task_attempt_id = $1",
            stale_dispatch.task_attempt_id,
        )
        with pytest.raises(TaskResultStale):
            await result_service.submit_result(
                stale_worker.authenticated,
                stale_worker.session_id,
                submission(
                    stale_dispatch, stale_claim, TaskExecutionResult.success(None)
                ),
            )
        assert (
            await setup.fetchval(
                "SELECT event_type FROM task_result_events WHERE task_attempt_id = $1",
                stale_dispatch.task_attempt_id,
            )
            == "result_stale_rejected"
        )
        assert not await setup.fetchval(
            "SELECT EXISTS (SELECT FROM task_attempt_results WHERE task_attempt_id = $1)",
            stale_dispatch.task_attempt_id,
        )

        superseded_worker = await add_worker(setup)
        superseded_dispatch = await add_dispatched_task(setup)
        await setup.execute(
            "INSERT INTO task_attempt_claims "
            "(task_attempt_id, generation, worker_session_id, lease_expires_at, "
            "terminated_at) VALUES ($1, 1, $2, statement_timestamp() + "
            "interval '1 minute', statement_timestamp())",
            superseded_dispatch.task_attempt_id,
            superseded_worker.session_id,
        )
        current_claim = await claim_service.claim_task(
            superseded_worker.authenticated,
            superseded_worker.session_id,
            superseded_dispatch,
        )
        assert current_claim.claim.generation == 2
        await setup.execute(
            "UPDATE task_runs SET status = 'running' WHERE id = $1",
            superseded_dispatch.task_run_id,
        )
        historical_authority = issuer.issue(
            worker_identity_id=superseded_worker.authenticated.worker_identity_id,
            worker_session_id=superseded_worker.session_id,
            task_attempt_id=superseded_dispatch.task_attempt_id,
            generation=1,
        )
        with pytest.raises(TaskResultStale):
            await result_service.submit_result(
                superseded_worker.authenticated,
                superseded_worker.session_id,
                TaskResultSubmissionRequest(
                    superseded_dispatch.dispatch_id,
                    superseded_dispatch.task_run_id,
                    superseded_dispatch.task_attempt_id,
                    1,
                    historical_authority,
                    TaskExecutionResult.success(None),
                ),
            )
        assert (
            await setup.fetchval(
                "SELECT event_type FROM task_result_events WHERE task_attempt_id = $1",
                superseded_dispatch.task_attempt_id,
            )
            == "result_stale_rejected"
        )

        race_worker, race_dispatch, race_claim = await claimed_running_task(
            setup, claim_service
        )
        race_requests = (
            submission(race_dispatch, race_claim, TaskExecutionResult.success("first")),
            submission(
                race_dispatch, race_claim, TaskExecutionResult.success("second")
            ),
        )
        race = await asyncio.gather(
            *(
                result_service.submit_result(
                    race_worker.authenticated, race_worker.session_id, item
                )
                for item in race_requests
            ),
            return_exceptions=True,
        )
        assert (
            sum(
                getattr(item, "outcome", None) is TaskResultSubmissionOutcome.ACCEPTED
                for item in race
            )
            == 1
        )
        assert sum(isinstance(item, TaskResultConflict) for item in race) == 1
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM task_attempt_results WHERE task_attempt_id = $1",
                race_dispatch.task_attempt_id,
            )
            == 1
        )

        rollback_worker, rollback_dispatch, rollback_claim = await claimed_running_task(
            setup, claim_service
        )
        await setup.execute(
            "CREATE FUNCTION reject_result_event() RETURNS trigger LANGUAGE plpgsql "
            "AS $$ BEGIN RAISE EXCEPTION 'forced result rollback'; END $$"
        )
        await setup.execute(
            "CREATE TRIGGER reject_result_event_trigger BEFORE INSERT ON "
            "workflow_run_execution_events FOR EACH ROW WHEN "
            f"(NEW.task_run_id = '{rollback_dispatch.task_run_id}'::uuid) "
            "EXECUTE FUNCTION reject_result_event()"
        )
        with pytest.raises(TaskResultServiceUnavailable):
            await result_service.submit_result(
                rollback_worker.authenticated,
                rollback_worker.session_id,
                submission(
                    rollback_dispatch, rollback_claim, TaskExecutionResult.success(None)
                ),
            )
        rollback_state = await setup.fetchrow(
            "SELECT tr.status::text, tac.terminated_at, "
            "EXISTS (SELECT FROM task_attempt_results tar WHERE "
            "tar.task_attempt_id = ta.id) FROM task_runs tr "
            "JOIN task_attempts ta ON ta.task_run_id = tr.id "
            "JOIN task_attempt_claims tac ON tac.task_attempt_id = ta.id "
            "WHERE ta.id = $1",
            rollback_dispatch.task_attempt_id,
        )
        assert tuple(rollback_state) == ("running", None, False)
        await assert_status_execution_events(
            setup,
            rollback_dispatch.workflow_run_id,
            (
                ExpectedStatusExecutionEvent(
                    rollback_dispatch.task_run_id, "dispatched", "claimed"
                ),
            ),
        )
        assert not await setup.fetchval(
            "SELECT EXISTS (SELECT FROM task_result_events WHERE task_attempt_id = $1)",
            rollback_dispatch.task_attempt_id,
        )
        await setup.execute(
            "DROP TRIGGER reject_result_event_trigger ON workflow_run_execution_events"
        )
        await setup.execute("DROP FUNCTION reject_result_event()")

        (
            permanent_worker,
            permanent_dispatch,
            permanent_claim,
        ) = await claimed_running_task(setup, claim_service)
        await setup.execute(
            "CREATE FUNCTION reject_dead_letter_status() RETURNS trigger "
            "LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION "
            "'forced dead-letter rollback'; END $$"
        )
        await setup.execute(
            "CREATE TRIGGER reject_dead_letter_status_trigger BEFORE INSERT ON "
            "dead_letter_status FOR EACH ROW EXECUTE FUNCTION "
            "reject_dead_letter_status()"
        )
        with pytest.raises(TaskResultServiceUnavailable):
            await result_service.submit_result(
                permanent_worker.authenticated,
                permanent_worker.session_id,
                submission(
                    permanent_dispatch,
                    permanent_claim,
                    TaskExecutionResult.permanent_failure(),
                ),
            )
        assert tuple(
            await setup.fetchrow(
                "SELECT tr.status::text, c.terminated_at, "
                "EXISTS (SELECT FROM task_attempt_results r WHERE "
                "r.task_attempt_id = $1), EXISTS (SELECT FROM dead_letter_items d "
                "WHERE d.source_task_attempt_id = $1) FROM task_runs tr "
                "JOIN task_attempts a ON a.task_run_id = tr.id "
                "JOIN task_attempt_claims c ON c.task_attempt_id = a.id "
                "WHERE a.id = $1",
                permanent_dispatch.task_attempt_id,
            )
        ) == ("running", None, False, False)
    finally:
        await setup.close()
        await engine.dispose()


def test_real_postgresql_authoritative_results() -> None:
    with temporary_database(
        "TASKFORGE_CLAIM_TEST_DATABASE_URL", "taskforge_task_results"
    ) as database_url:
        configuration = Config("alembic.ini")
        with migration_database_url(database_url.render_as_string(hide_password=False)):
            command.upgrade(configuration, "head")
        asyncio.run(exercise_results(database_url))
