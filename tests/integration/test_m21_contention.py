"""Opt-in real-PostgreSQL M21 Task 3 contention scenarios."""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from taskforge.claims.authority import TaskClaimResultAuthorityIssuer
from taskforge.claims.domain import TaskClaimOutcome, TaskClaimRejected
from taskforge.claims.service import TaskClaimService
from taskforge.dispatch.envelope import (
    create_dispatch_envelope,
    dispatch_envelope_to_mapping,
)
from taskforge.identity.authorization import OwnerFilter
from taskforge.persistence.claims import SQLAlchemyTaskClaimRepository
from taskforge.persistence.database import build_session_factory
from taskforge.persistence.retries import SQLAlchemyRetryTransitionRepository
from taskforge.persistence.runs import SQLAlchemyWorkflowRunRepository
from taskforge.persistence.task_results import SQLAlchemyTaskResultRepository
from taskforge.rate_limits import AllowAllRateLimiter
from taskforge.retries.scanner import DueRetryScanner
from taskforge.runs.domain import (
    ExplicitWorkflowVersion,
    LatestWorkflowVersion,
    TaskRunStatus,
    WorkflowRunCancellationOutcome,
    WorkflowRunStatus,
    create_workflow_run_input,
)
from taskforge.runs.schema import workflow_runs
from taskforge.runs.service import WorkflowRunService
from taskforge.worker.result_submission import (
    TaskResultSubmissionOutcome,
    TaskResultSubmissionService,
    prepare_task_result,
)
from taskforge.worker.results import TaskExecutionResult
from taskforge.workflows.schema import workflow_definitions
from tests.integration.contention import (
    ContenderSessionFactory,
    LockStatementIdentity,
    PostLockPause,
    observe_blocked_followers,
)
from tests.integration.postgresql import (
    asyncpg_dsn,
    migration_database_url,
    temporary_database,
)
from tests.integration.test_retry_scanner import (
    PausingRepository,
    add_scheduled_workflow,
    assert_attempt_counts,
    registry,
)
from tests.integration.test_task_claim_acquisition import (
    add_dispatched_task,
    add_worker,
)
from tests.integration.test_task_result_submission import (
    claimed_running_task,
    submission,
)
from tests.integration.test_workflow_run_creation import seed_workflow
from tests.integration.test_workflow_run_dependency_failure_propagation import (
    seed_failure_graph,
)
from tests.integration.test_workflow_run_state_evaluation import (
    run_projection,
    set_all_tasks,
    set_run_status,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.workload,
    pytest.mark.contention,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_M21_CONTENTION") != "1",
        reason="set TASKFORGE_RUN_M21_CONTENTION=1 explicitly",
    ),
]

ScenarioBody = Callable[[URL], Awaitable[dict[str, Any] | None]]
_DATABASE_LABELS = {
    "run_creation_materialization": "taskforge_m21_contention_rc",
    "dependency_joins": "taskforge_m21_contention_dj",
    "claims": "taskforge_m21_contention_cl",
    "retry_scanners": "taskforge_m21_contention_rs",
    "cancellation": "taskforge_m21_contention_ca",
    "terminal_state_updates": "taskforge_m21_contention_ts",
}


def _run_scenario(name: str, body: ScenarioBody) -> None:
    with temporary_database(
        "TASKFORGE_M21_CONTENTION_DATABASE_URL", _DATABASE_LABELS[name]
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        with migration_database_url(alembic_url):
            command.upgrade(Config("alembic.ini"), "head")
        asyncio.run(asyncio.wait_for(body(database_url), timeout=20))


SCENARIO_POOL_SIZE = 12


def _scenario_engine(database_url: URL) -> Any:
    return create_async_engine(
        database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        ),
        pool_size=SCENARIO_POOL_SIZE,
        max_overflow=0,
        pool_timeout=2,
    )


def _contender_sessions(
    sessions: Any, application: str, pause: PostLockPause | None = None
) -> Any:
    return ContenderSessionFactory(sessions, application, pause)


async def _run_creation_scenario(database_url: URL) -> dict[str, Any]:
    setup_engine = _scenario_engine(database_url)
    setup_sessions = build_session_factory(setup_engine)
    setup = await asyncpg.connect(asyncpg_dsn(database_url))
    observer = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        owner_id, _, workflow_id, _, _ = await seed_workflow(setup_sessions)

        async def contend(keys: list[str], label: str) -> tuple[list[Any], list[Any]]:
            pause = PostLockPause(
                LockStatementIdentity(workflow_definitions, workflow_id)
            )
            services: list[WorkflowRunService] = []
            for index in range(len(keys)):
                application = f"m21_rc_{label}_{index}"
                sessions = _contender_sessions(
                    setup_sessions, application, pause if index == 0 else None
                )
                services.append(
                    WorkflowRunService(SQLAlchemyWorkflowRunRepository(sessions))
                )

            async def create(index: int) -> Any:
                return await services[index].create_idempotent_run(
                    workflow_id,
                    owner_filter=OwnerFilter.only(owner_id),
                    requested_by_principal_id=owner_id,
                    selection=LatestWorkflowVersion(),
                    input_snapshot=create_workflow_run_input({}, {}),
                    idempotency_key=keys[index],
                )

            owner = asyncio.create_task(create(0))
            acquired = asyncio.create_task(pause.acquired.wait())
            completed, _ = await asyncio.wait(
                {owner, acquired}, timeout=2, return_when=asyncio.FIRST_COMPLETED
            )
            if owner in completed:
                acquired.cancel()
                await owner
                raise AssertionError("creator completed before its lock pause")
            if acquired not in completed:
                acquired.cancel()
                raise TimeoutError("creator did not reach its production lock pause")
            blocker_evidence: list[dict[str, Any]] = []
            for index in range(1, len(keys)):
                probe = asyncio.create_task(create(index))
                blocker_evidence.extend(
                    await observe_blocked_followers(
                        observer,
                        owner_application=f"m21_rc_{label}_0",
                        follower_applications=[f"m21_rc_{label}_{index}"],
                    )
                )
                probe.cancel()
                with suppress(asyncio.CancelledError):
                    await probe
            followers = [
                asyncio.create_task(create(index)) for index in range(1, len(keys))
            ]
            pause.release.set()
            return list(await asyncio.gather(owner, *followers)), blocker_evidence

        identical, first_blockers = await contend(["same-contention-key"] * 8, "same")
        assert len({item.id for item in identical}) == 1
        distinct, second_blockers = await contend(
            [f"distinct-contention-{index}" for index in range(8)], "distinct"
        )
        assert len({item.id for item in distinct}) == 8
        canonical_run_id = identical[0].id
        run_ids = [canonical_run_id, *(item.id for item in distinct)]
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM workflow_run_idempotency "
                "WHERE principal_id=$1 AND workflow_definition_id=$2 "
                "AND workflow_run_id=$3",
                owner_id,
                workflow_id,
                canonical_run_id,
            )
            == 1
        )
        for run_id in run_ids:
            input_rows = await setup.fetch(
                "SELECT payload::text, input_references::text "
                "FROM workflow_run_inputs WHERE workflow_run_id=$1",
                run_id,
            )
            assert len(input_rows) == 1
            assert json.loads(input_rows[0]["payload"]) == {}
            assert json.loads(input_rows[0]["input_references"]) == {}
            assert (
                await setup.fetchval(
                    "SELECT count(*) FROM workflow_run_idempotency "
                    "WHERE principal_id=$1 AND workflow_definition_id=$2 "
                    "AND workflow_run_id=$3",
                    owner_id,
                    workflow_id,
                    run_id,
                )
                == 1
            )
            graph = await setup.fetch(
                "SELECT step_identifier, status::text FROM task_runs "
                "WHERE workflow_run_id=$1 ORDER BY step_identifier",
                run_id,
            )
            assert [(row["step_identifier"], row["status"]) for row in graph] == [
                ("join", "blocked"),
                ("left", "runnable"),
                ("right", "runnable"),
            ]
            assert (
                await setup.fetchval(
                    "SELECT count(DISTINCT step_identifier) FROM task_runs "
                    "WHERE workflow_run_id=$1",
                    run_id,
                )
                == 3
            )
            event_counts = await setup.fetchrow(
                "SELECT count(*) FILTER (WHERE event_type='workflow_run.created') created, "
                "count(*) FILTER (WHERE event_type='task_run.status_changed') initial_transitions, "
                "count(*) total FROM workflow_run_execution_events "
                "WHERE workflow_run_id=$1",
                run_id,
            )
            assert event_counts is not None
            # Initial task states are materialized facts, not transitions.
            assert tuple(event_counts) == (1, 0, 1)
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM workflow_runs WHERE workflow_definition_id=$1",
                workflow_id,
            )
            == 9
        )
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM workflow_run_idempotency "
                "WHERE principal_id=$1 AND workflow_definition_id=$2",
                owner_id,
                workflow_id,
            )
            == 9
        )
        return {
            "lock_owner": "creator_0",
            "blocked_followers": len(first_blockers) + len(second_blockers),
            "blocking_relationships_proven": all(
                item["relationship_proven"]
                for item in (*first_blockers, *second_blockers)
            ),
            "canonical_run_count": 1,
            "distinct_complete_run_count": 8,
        }
    finally:
        await setup.close()
        await observer.close()
        await setup_engine.dispose()


def test_m21_run_creation_materialization_contention() -> None:
    _run_scenario("run_creation_materialization", _run_creation_scenario)


async def _dependency_join_scenario(database_url: URL) -> dict[str, Any]:
    setup_engine = _scenario_engine(database_url)
    setup_sessions = build_session_factory(setup_engine)
    setup = await asyncpg.connect(asyncpg_dsn(database_url))
    observer = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        owner_id, _, workflow_id, _, _ = await seed_workflow(setup_sessions)
        setup_service = WorkflowRunService(
            SQLAlchemyWorkflowRunRepository(setup_sessions)
        )
        run = await setup_service.create_run(
            workflow_id,
            owner_filter=OwnerFilter.only(owner_id),
            requested_by_principal_id=owner_id,
            selection=ExplicitWorkflowVersion(2),
            input_snapshot=create_workflow_run_input({}, {}),
        )
        rows = await setup.fetch(
            "SELECT id, step_identifier FROM task_runs WHERE workflow_run_id = $1",
            run.id,
        )
        task_ids = {row["step_identifier"]: row["id"] for row in rows}
        issuer = TaskClaimResultAuthorityIssuer(
            b"m21-dependency-result-authority-secret"
        )

        async def prepare(step: str) -> tuple[Any, Any, Any]:
            task_id = task_ids[step]
            attempt_id, dispatch_id = uuid4(), uuid4()
            envelope = create_dispatch_envelope(
                dispatch_id=dispatch_id,
                task_attempt_id=attempt_id,
                task_run_id=task_id,
                workflow_run_id=run.id,
                attempt_number=1,
                task_type="test.task",
                required_capability="test-capability",
                task_payload={},
                references={},
            )
            await setup.execute(
                "UPDATE task_runs SET status = 'dispatched' WHERE id = $1",
                task_id,
            )
            await setup.execute(
                "INSERT INTO task_attempts (id, task_run_id, attempt_number) VALUES ($1,$2,1)",
                attempt_id,
                task_id,
            )
            await setup.execute(
                "INSERT INTO task_dispatch_outbox (id, task_attempt_id, route, payload) "
                "VALUES ($1,$2,$3,$4::jsonb)",
                dispatch_id,
                attempt_id,
                envelope.route,
                json.dumps(dispatch_envelope_to_mapping(envelope)),
            )
            worker = await add_worker(setup)
            claim_service = TaskClaimService(
                SQLAlchemyTaskClaimRepository(
                    setup_sessions, worker_stale_after_seconds=30
                ),
                issuer,
                lease_seconds=60,
            )
            claim = await claim_service.claim_task(
                worker.authenticated, worker.session_id, envelope
            )
            await setup.execute(
                "UPDATE task_runs SET status = 'running' WHERE id = $1", task_id
            )
            return worker, envelope, claim

        left = await prepare("left")
        right = await prepare("right")

        def result_actor(label: str, facts: tuple[Any, Any, Any]) -> tuple[Any, Any]:
            worker, dispatch, claim = facts
            pause = PostLockPause(
                LockStatementIdentity(workflow_runs, dispatch.task_attempt_id)
            )
            sessions = _contender_sessions(setup_sessions, label, pause)
            service = TaskResultSubmissionService(
                SQLAlchemyTaskResultRepository(sessions),
                issuer,
                rate_limiter=AllowAllRateLimiter(),
            )
            call = service.submit_result(
                worker.authenticated,
                worker.session_id,
                submission(dispatch, claim, TaskExecutionResult.success(label)),
            )
            return pause, call

        left_pause, left_call = result_actor("m21_join_left", left)
        right_pause, right_call = result_actor("m21_join_right", right)
        left_task = asyncio.create_task(left_call)
        await asyncio.wait_for(left_pause.acquired.wait(), 2)
        right_task = asyncio.create_task(right_call)
        left_block = await observe_blocked_followers(
            observer,
            owner_application="m21_join_left",
            follower_applications=["m21_join_right"],
        )
        left_pause.release.set()
        await left_task
        # The second pause is intentional. It creates the only deterministic,
        # committed midpoint where left succeeded while right still owns the run
        # lock, so the blocked join and the reconciler's wait can both be proved.
        # A single pre-commit pause cannot expose that intermediate state.
        await asyncio.wait_for(right_pause.acquired.wait(), 2)

        snapshot = await setup.fetchrow(
            "SELECT "
            "(SELECT status::text FROM task_runs WHERE id=$1) left_status, "
            "(SELECT count(*) FROM task_attempt_results r JOIN task_attempts a ON a.id=r.task_attempt_id WHERE a.task_run_id=$1) left_results, "
            "(SELECT status::text FROM task_runs WHERE id=$2) right_status, "
            "(SELECT count(*) FROM task_attempt_results r JOIN task_attempts a ON a.id=r.task_attempt_id WHERE a.task_run_id=$2) right_results, "
            "(SELECT status::text FROM task_runs WHERE id=$3) join_status, "
            "(SELECT count(*) FROM workflow_run_execution_events WHERE workflow_run_id=$4 AND task_run_id=$3 AND payload->>'status'='runnable') join_events",
            task_ids["left"],
            task_ids["right"],
            task_ids["join"],
            run.id,
        )
        assert snapshot is not None
        assert tuple(snapshot) == ("succeeded", 1, "running", 0, "blocked", 0)

        reconcile_service = WorkflowRunService(
            SQLAlchemyWorkflowRunRepository(
                _contender_sessions(setup_sessions, "m21_join_reconciler")
            )
        )
        reconciler = asyncio.create_task(
            reconcile_service.reconcile_workflow_run(run.id)
        )
        right_block = await observe_blocked_followers(
            observer,
            owner_application="m21_join_right",
            follower_applications=["m21_join_reconciler"],
        )
        right_pause.release.set()
        await right_task
        contender = asyncio.create_task(setup_service.reconcile_workflow_run(run.id))
        reconciled, competed = await asyncio.gather(reconciler, contender)
        assert (
            reconciled.runnable_transition_count + competed.runnable_transition_count
            == 1
        )
        final_status = await setup.fetchval(
            "SELECT status::text FROM task_runs WHERE id=$1", task_ids["join"]
        )
        event_count = await setup.fetchval(
            "SELECT count(*) FROM workflow_run_execution_events WHERE workflow_run_id=$1 AND task_run_id=$2 AND event_type='task_run.status_changed' AND payload=$3::jsonb",
            run.id,
            task_ids["join"],
            json.dumps({"previous_status": "blocked", "status": "runnable"}),
        )
        assert (final_status, event_count) == ("runnable", 1)
        return {
            "left_blocks_right": left_block[0]["relationship_proven"],
            "right_blocks_reconciler": right_block[0]["relationship_proven"],
            "intermediate": {
                "left_status": "succeeded",
                "left_results": 1,
                "right_status": "running",
                "right_results": 0,
                "join_status": "blocked",
                "join_runnable_events": 0,
            },
            "join_promotions": 1,
        }
    finally:
        await observer.close()
        await setup.close()
        await setup_engine.dispose()


def test_m21_dependency_join_contention() -> None:
    _run_scenario("dependency_joins", _dependency_join_scenario)


async def _claim_scenario(database_url: URL) -> dict[str, Any]:
    engine = _scenario_engine(database_url)
    sessions = build_session_factory(engine)
    setup = await asyncpg.connect(asyncpg_dsn(database_url))
    observer = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        dispatch = await add_dispatched_task(setup)
        workers = [await add_worker(setup) for _ in range(8)]
        pause = PostLockPause(
            LockStatementIdentity(workflow_runs, dispatch.workflow_run_id)
        )
        services: list[TaskClaimService] = []
        for index in range(8):
            contender_sessions = _contender_sessions(
                sessions,
                f"m21_claim_{index}",
                pause if index == 0 else None,
            )
            services.append(
                TaskClaimService(
                    SQLAlchemyTaskClaimRepository(
                        contender_sessions, worker_stale_after_seconds=30
                    ),
                    TaskClaimResultAuthorityIssuer(
                        b"m21-contention-claim-authority-secret"
                    ),
                    lease_seconds=60,
                )
            )

        async def claim(index: int) -> Any:
            worker = workers[index]
            return await services[index].claim_task(
                worker.authenticated, worker.session_id, dispatch
            )

        owner = asyncio.create_task(claim(0))
        await asyncio.wait_for(pause.acquired.wait(), 2)
        first_follower = asyncio.create_task(claim(1))
        blocking = await observe_blocked_followers(
            observer,
            owner_application="m21_claim_0",
            follower_applications=["m21_claim_1"],
        )
        followers = [first_follower]
        followers.extend(asyncio.create_task(claim(index)) for index in range(2, 8))
        pause.release.set()
        outcomes = await asyncio.gather(owner, *followers, return_exceptions=True)
        assert (
            sum(
                getattr(outcome, "outcome", None) is TaskClaimOutcome.ACQUIRED_ACTIVE
                for outcome in outcomes
            )
            == 1
        )
        assert sum(isinstance(outcome, TaskClaimRejected) for outcome in outcomes) == 7
        claim_count = await setup.fetchval(
            "SELECT count(*) FROM task_attempt_claims WHERE task_attempt_id = $1",
            dispatch.task_attempt_id,
        )
        current_count = await setup.fetchval(
            "SELECT count(*) FROM task_attempt_claims "
            "WHERE task_attempt_id = $1 AND terminated_at IS NULL",
            dispatch.task_attempt_id,
        )
        event_count = await setup.fetchval(
            "SELECT count(*) FROM task_claim_events "
            "WHERE task_attempt_id = $1 AND event_type = 'claim_acquired'",
            dispatch.task_attempt_id,
        )
        assert (claim_count, current_count, event_count) == (1, 1, 1)
        task_status = await setup.fetchval(
            "SELECT status::text FROM task_runs WHERE id=$1",
            dispatch.task_run_id,
        )
        assert task_status == "claimed"
        return {
            "lock_owner": "claimant_0",
            "blocked_follower": "claimant_1",
            "blocking_relationship_proven": blocking[0]["relationship_proven"],
            "authoritative_claim_count": claim_count,
            "non_mutating_follower_count": 7,
        }
    finally:
        await observer.close()
        await setup.close()
        await engine.dispose()


def test_m21_task_claim_contention() -> None:
    _run_scenario("claims", _claim_scenario)


async def _retry_scanner_scenario(database_url: URL) -> dict[str, Any]:
    engine = _scenario_engine(database_url)
    sessions = build_session_factory(engine)
    setup = await asyncpg.connect(asyncpg_dsn(database_url))
    locked, release = asyncio.Event(), asyncio.Event()
    pending_a: asyncio.Task[Any] | None = None
    try:
        now = await setup.fetchval("SELECT statement_timestamp()")
        first_run_attempt_ids = (UUID(int=2101), UUID(int=2102))
        second_run_attempt_id = UUID(int=2103)
        first_run = await add_scheduled_workflow(
            setup,
            eligible_at=(now - timedelta(seconds=30), now - timedelta(seconds=20)),
            scheduled_attempt_ids=first_run_attempt_ids,
        )
        second_run = await add_scheduled_workflow(
            setup,
            eligible_at=(now - timedelta(seconds=10),),
            scheduled_attempt_ids=(second_run_attempt_id,),
        )
        repository_a = SQLAlchemyRetryTransitionRepository(
            _contender_sessions(sessions, "m21_retry_scanner_a")
        )
        repository_b = SQLAlchemyRetryTransitionRepository(
            _contender_sessions(sessions, "m21_retry_scanner_b")
        )
        scanner_a = DueRetryScanner(
            PausingRepository(repository_a, locked, release), registry()
        )
        scanner_b = DueRetryScanner(repository_b, registry())

        pending_a = asyncio.create_task(scanner_a.scan_due_retries(batch_size=1))
        await asyncio.wait_for(locked.wait(), 2)
        result_b = await asyncio.wait_for(
            scanner_b.scan_due_retries(batch_size=1), timeout=2
        )
        scanner_b_progressed = result_b.dispatched_attempt_ids == (
            second_run_attempt_id,
        )
        assert scanner_b_progressed
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM task_dispatch_outbox "
                "WHERE task_attempt_id = ANY($1)",
                list(first_run_attempt_ids),
            )
            == 0
        )

        release.set()
        result_a = await pending_a
        assert result_a.dispatched_attempt_ids == (first_run_attempt_ids[0],)
        remaining = await scanner_b.scan_due_retries(batch_size=1)
        assert remaining.dispatched_attempt_ids == (first_run_attempt_ids[1],)
        drained = await scanner_b.scan_due_retries(batch_size=10)
        assert (drained.examined, drained.dispatched, drained.skipped) == (0, 0, 0)

        await assert_attempt_counts(setup, first_run)
        await assert_attempt_counts(setup, second_run)
        all_tasks = (*first_run.tasks, *second_run.tasks)
        for task in all_tasks:
            durable = await setup.fetchrow(
                "SELECT tr.status::text, "
                "(SELECT count(*) FROM task_dispatch_outbox o "
                "WHERE o.task_attempt_id=$2) outbox_count, "
                "(SELECT count(*) FROM task_retry_events e "
                "WHERE e.task_run_id=tr.id AND e.event_type='retry_dispatched') "
                "event_count FROM task_runs tr WHERE tr.id=$1",
                task.task_run_id,
                task.scheduled_attempt_id,
            )
            assert durable is not None
            assert tuple(durable) == ("dispatched", 1, 1)
        authoritative_dispatch_count = await setup.fetchval(
            "SELECT count(*) FROM task_dispatch_outbox WHERE task_attempt_id = ANY($1)",
            [task.scheduled_attempt_id for task in all_tasks],
        )
        retry_dispatch_event_count = await setup.fetchval(
            "SELECT count(*) FROM task_retry_events "
            "WHERE task_run_id = ANY($1) AND event_type='retry_dispatched'",
            [task.task_run_id for task in all_tasks],
        )
        assert authoritative_dispatch_count == len(all_tasks)
        assert retry_dispatch_event_count == len(all_tasks)
        return {
            "scanner_b_progressed_while_scanner_a_held_lock": scanner_b_progressed,
            "eventual_drain_count": len(all_tasks),
            "authoritative_dispatch_count": authoritative_dispatch_count,
            "retry_dispatch_event_count": retry_dispatch_event_count,
        }
    finally:
        release.set()
        if pending_a is not None and not pending_a.done():
            pending_a.cancel()
            with suppress(asyncio.CancelledError):
                await pending_a
        await setup.close()
        await engine.dispose()


def test_m21_retry_scanner_contention() -> None:
    _run_scenario("retry_scanners", _retry_scanner_scenario)


async def _cancellation_scenario(database_url: URL) -> dict[str, Any]:
    engine = _scenario_engine(database_url)
    sessions = build_session_factory(engine)
    setup = await asyncpg.connect(asyncpg_dsn(database_url))
    observer = await asyncpg.connect(asyncpg_dsn(database_url))
    issuer = TaskClaimResultAuthorityIssuer(b"m21-cancellation-claim-authority-secret")

    async def owner_for(run_id: Any) -> Any:
        return await setup.fetchval(
            "SELECT d.owner_principal_id FROM workflow_runs r "
            "JOIN workflow_definitions d ON d.id=r.workflow_definition_id "
            "WHERE r.id=$1",
            run_id,
        )

    def run_service(application: str, pause: PostLockPause | None = None) -> Any:
        contender_sessions = _contender_sessions(sessions, application, pause)
        return WorkflowRunService(SQLAlchemyWorkflowRunRepository(contender_sessions))

    def claim_service(application: str, pause: PostLockPause | None = None) -> Any:
        contender_sessions = _contender_sessions(sessions, application, pause)
        return TaskClaimService(
            SQLAlchemyTaskClaimRepository(
                contender_sessions, worker_stale_after_seconds=30
            ),
            issuer,
            lease_seconds=60,
        )

    try:
        # Cancellation owns and commits the common run lock before claim resumes.
        first_dispatch = await add_dispatched_task(setup)
        first_worker = await add_worker(setup)
        first_owner_id = await owner_for(first_dispatch.workflow_run_id)
        cancel_pause = PostLockPause(
            LockStatementIdentity(workflow_runs, first_dispatch.workflow_run_id)
        )
        cancellation_owner = run_service("m21_cancel_first", cancel_pause)
        claim_follower_service = claim_service("m21_claim_after_cancel")
        cancellation = asyncio.create_task(
            cancellation_owner.cancel_run(
                first_dispatch.workflow_run_id,
                OwnerFilter.only(first_owner_id),
                requested_by_principal_id=first_owner_id,
                idempotency_key="cancel-before-claim-key",
                reason=None,
            )
        )
        await asyncio.wait_for(cancel_pause.acquired.wait(), 2)
        claim_follower = asyncio.create_task(
            claim_follower_service.claim_task(
                first_worker.authenticated, first_worker.session_id, first_dispatch
            )
        )
        cancel_blocks_claim = await observe_blocked_followers(
            observer,
            owner_application="m21_cancel_first",
            follower_applications=["m21_claim_after_cancel"],
        )
        cancel_pause.release.set()
        accepted = await cancellation
        assert accepted.outcome is WorkflowRunCancellationOutcome.NEWLY_ACCEPTED
        claim_outcome = (await asyncio.gather(claim_follower, return_exceptions=True))[
            0
        ]
        assert isinstance(claim_outcome, TaskClaimRejected)

        before_replay = await setup.fetchrow(
            "SELECT "
            "(SELECT count(*) FROM workflow_run_cancellation_requests "
            "WHERE workflow_run_id=$1) requests, "
            "count(*) FILTER (WHERE event_type='workflow_run.status_changed' "
            "AND payload->>'status'='cancelling') run_transitions, "
            "count(*) FILTER (WHERE event_type='task_run.status_changed' "
            "AND payload->>'status'='cancelled') task_transitions, "
            "count(*) total_events FROM workflow_run_execution_events "
            "WHERE workflow_run_id=$1",
            first_dispatch.workflow_run_id,
        )
        assert before_replay is not None
        assert tuple(before_replay) == (1, 1, 1, 2)
        replayed = await run_service("m21_cancel_replay").cancel_run(
            first_dispatch.workflow_run_id,
            OwnerFilter.only(first_owner_id),
            requested_by_principal_id=first_owner_id,
            idempotency_key="cancel-before-claim-key",
            reason=None,
        )
        assert replayed.outcome is WorkflowRunCancellationOutcome.EXACT_RETRY
        assert replayed.accepted_request == accepted.accepted_request
        after_replay = await setup.fetchrow(
            "SELECT "
            "(SELECT count(*) FROM workflow_run_cancellation_requests "
            "WHERE workflow_run_id=$1) requests, "
            "count(*) FILTER (WHERE event_type='workflow_run.status_changed' "
            "AND payload->>'status'='cancelling') run_transitions, "
            "count(*) FILTER (WHERE event_type='task_run.status_changed' "
            "AND payload->>'status'='cancelled') task_transitions, "
            "count(*) total_events FROM workflow_run_execution_events "
            "WHERE workflow_run_id=$1",
            first_dispatch.workflow_run_id,
        )
        assert after_replay is not None
        assert tuple(after_replay) == tuple(before_replay)

        # Claim owns and commits the same production lock before cancellation.
        second_dispatch = await add_dispatched_task(setup)
        second_worker = await add_worker(setup)
        second_owner_id = await owner_for(second_dispatch.workflow_run_id)
        claim_pause = PostLockPause(
            LockStatementIdentity(workflow_runs, second_dispatch.workflow_run_id)
        )
        claim_owner_service = claim_service("m21_claim_first", claim_pause)
        cancellation_follower_service = run_service("m21_cancel_after_claim")
        claim_owner = asyncio.create_task(
            claim_owner_service.claim_task(
                second_worker.authenticated,
                second_worker.session_id,
                second_dispatch,
            )
        )
        await asyncio.wait_for(claim_pause.acquired.wait(), 2)
        cancellation_follower = asyncio.create_task(
            cancellation_follower_service.cancel_run(
                second_dispatch.workflow_run_id,
                OwnerFilter.only(second_owner_id),
                requested_by_principal_id=second_owner_id,
                idempotency_key="claim-before-cancel-key",
                reason=None,
            )
        )
        claim_blocks_cancel = await observe_blocked_followers(
            observer,
            owner_application="m21_claim_first",
            follower_applications=["m21_cancel_after_claim"],
        )
        claim_pause.release.set()
        claimed = await claim_owner
        cancelled = await cancellation_follower
        assert claimed.outcome is TaskClaimOutcome.ACQUIRED_ACTIVE
        assert cancelled.outcome is WorkflowRunCancellationOutcome.NEWLY_ACCEPTED

        states = []
        for dispatch in (first_dispatch, second_dispatch):
            state = await setup.fetchrow(
                "SELECT r.status::text run_status, t.status::text task_status, "
                "(SELECT count(*) FROM workflow_run_cancellation_requests WHERE workflow_run_id=r.id) cancellations, "
                "(SELECT count(*) FROM task_attempt_claims c JOIN task_attempts a ON a.id=c.task_attempt_id WHERE a.task_run_id=t.id) claims, "
                "(SELECT count(*) FROM task_attempt_results x JOIN task_attempts a ON a.id=x.task_attempt_id WHERE a.task_run_id=t.id) results, "
                "(SELECT count(*) FROM task_dispatch_outbox o JOIN task_attempts a ON a.id=o.task_attempt_id WHERE a.task_run_id=t.id) outbox "
                "FROM workflow_runs r JOIN task_runs t ON t.workflow_run_id=r.id "
                "WHERE r.id=$1",
                dispatch.workflow_run_id,
            )
            assert state is not None
            states.append(dict(state))
        assert states[0] == {
            "run_status": "cancelling",
            "task_status": "cancelled",
            "cancellations": 1,
            "claims": 0,
            "results": 0,
            "outbox": 1,
        }
        assert states[1] == {
            "run_status": "cancelling",
            "task_status": "claimed",
            "cancellations": 1,
            "claims": 1,
            "results": 0,
            "outbox": 1,
        }
        return {
            "cancel_then_claim": {
                "owner_blocks_follower": cancel_blocks_claim[0]["relationship_proven"],
                "follower_outcome": "claim_rejected",
                "identical_replay_outcome": replayed.outcome.value,
                "durable_state": states[0],
            },
            "claim_then_cancel": {
                "owner_blocks_follower": claim_blocks_cancel[0]["relationship_proven"],
                "follower_outcome": "cancellation_accepted",
                "durable_state": states[1],
            },
        }
    finally:
        await observer.close()
        await setup.close()
        await engine.dispose()


def test_m21_cancellation_contention() -> None:
    _run_scenario("cancellation", _cancellation_scenario)


async def _terminal_scenario(database_url: URL) -> dict[str, Any]:
    setup_engine = _scenario_engine(database_url)
    setup_sessions = build_session_factory(setup_engine)
    setup = await asyncpg.connect(asyncpg_dsn(database_url))
    observer = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        issuer = TaskClaimResultAuthorityIssuer(b"m21-terminal-result-authority-secret")
        claim_service = TaskClaimService(
            SQLAlchemyTaskClaimRepository(
                setup_sessions, worker_stale_after_seconds=30
            ),
            issuer,
            lease_seconds=60,
        )
        worker, dispatch, claim = await claimed_running_task(setup, claim_service)
        result_pause = PostLockPause(
            LockStatementIdentity(workflow_runs, dispatch.task_attempt_id)
        )
        result_services: list[TaskResultSubmissionService] = []
        for index in range(2):
            sessions = _contender_sessions(
                setup_sessions,
                f"m21_terminal_result_{index}",
                result_pause if index == 0 else None,
            )
            result_services.append(
                TaskResultSubmissionService(
                    SQLAlchemyTaskResultRepository(sessions),
                    issuer,
                    rate_limiter=AllowAllRateLimiter(),
                )
            )
        request = submission(dispatch, claim, TaskExecutionResult.success("done"))
        prepared = prepare_task_result(request)
        result_owner = asyncio.create_task(
            result_services[0].submit_result(
                worker.authenticated, worker.session_id, request
            )
        )
        await asyncio.wait_for(result_pause.acquired.wait(), 2)
        result_follower = asyncio.create_task(
            result_services[1].submit_result(
                worker.authenticated, worker.session_id, request
            )
        )
        result_block = await observe_blocked_followers(
            observer,
            owner_application="m21_terminal_result_0",
            follower_applications=["m21_terminal_result_1"],
        )
        result_pause.release.set()
        result_receipts = await asyncio.gather(result_owner, result_follower)
        assert (
            sum(
                receipt.outcome is TaskResultSubmissionOutcome.ACCEPTED
                for receipt in result_receipts
            )
            == 1
        )
        assert (
            sum(
                receipt.outcome is TaskResultSubmissionOutcome.REPLAYED_IDENTICAL
                for receipt in result_receipts
            )
            == 1
        )
        result_state = await setup.fetchrow(
            "SELECT "
            "(SELECT count(*) FROM task_attempt_results r "
            "WHERE r.task_attempt_id=$1) result_count, "
            "(SELECT result_kind FROM task_attempt_results r "
            "WHERE r.task_attempt_id=$1) result_kind, "
            "(SELECT output::text FROM task_attempt_results r "
            "WHERE r.task_attempt_id=$1) output, "
            "(SELECT result_fingerprint FROM task_attempt_results r "
            "WHERE r.task_attempt_id=$1) result_fingerprint, "
            "(SELECT count(*) FROM task_attempt_claims c "
            "WHERE c.task_attempt_id=$1) claim_count, "
            "(SELECT count(*) FROM task_attempt_claims c "
            "WHERE c.task_attempt_id=$1 AND c.terminated_at IS NOT NULL) "
            "terminated_claim_count, "
            "(SELECT status::text FROM task_runs t WHERE t.id=$2) task_status, "
            "(SELECT count(*) FROM task_result_events e "
            "WHERE e.task_attempt_id=$1 AND e.event_type='result_accepted') "
            "accepted_events, "
            "(SELECT count(*) FROM task_result_events e "
            "WHERE e.task_attempt_id=$1 AND e.event_type='result_replayed') "
            "replayed_events, "
            "(SELECT count(*) FROM workflow_run_execution_events e "
            "WHERE e.workflow_run_id=$3 AND e.task_run_id=$2 "
            "AND e.event_type='task_run.status_changed' "
            "AND e.payload->>'status'='succeeded') terminal_task_events",
            dispatch.task_attempt_id,
            dispatch.task_run_id,
            dispatch.workflow_run_id,
        )
        assert result_state is not None
        assert result_state["result_count"] == 1
        assert result_state["result_kind"] == "success"
        assert json.loads(result_state["output"]) == prepared.output
        assert result_state["result_fingerprint"] == prepared.result_fingerprint
        assert result_state["claim_count"] == 1
        assert result_state["terminated_claim_count"] == 1
        assert result_state["task_status"] == "succeeded"
        assert result_state["accepted_events"] == 1
        assert result_state["replayed_events"] == 1
        assert result_state["terminal_task_events"] == 1

        owner_id, workflow_id, _ = await seed_failure_graph(setup_sessions)
        setup_service = WorkflowRunService(
            SQLAlchemyWorkflowRunRepository(setup_sessions)
        )
        created = await setup_service.create_run(
            workflow_id,
            owner_filter=OwnerFilter.only(owner_id),
            requested_by_principal_id=owner_id,
            selection=LatestWorkflowVersion(),
            input_snapshot=create_workflow_run_input({}, {}),
        )
        await set_run_status(setup_sessions, created.id, WorkflowRunStatus.PENDING)
        await set_all_tasks(setup_sessions, created.id, TaskRunStatus.SUCCEEDED)
        reconcile_pause = PostLockPause(
            LockStatementIdentity(workflow_runs, created.id)
        )
        services: list[WorkflowRunService] = []
        for index in range(2):
            sessions = _contender_sessions(
                setup_sessions,
                f"m21_terminal_reconcile_{index}",
                reconcile_pause if index == 0 else None,
            )
            services.append(
                WorkflowRunService(SQLAlchemyWorkflowRunRepository(sessions))
            )
        owner = asyncio.create_task(services[0].reconcile_workflow_run(created.id))
        await asyncio.wait_for(reconcile_pause.acquired.wait(), 2)
        follower = asyncio.create_task(services[1].reconcile_workflow_run(created.id))
        reconcile_block = await observe_blocked_followers(
            observer,
            owner_application="m21_terminal_reconcile_0",
            follower_applications=["m21_terminal_reconcile_1"],
        )
        reconcile_pause.release.set()
        results = await asyncio.gather(owner, follower)
        assert all(result.quiescent for result in results)
        assert sum(result.workflow_transition_count for result in results) == 2
        assert (await run_projection(setup_sessions, created.id))[0] == "succeeded"
        final = await setup_service.reconcile_workflow_run(created.id)
        assert final.final_status is WorkflowRunStatus.SUCCEEDED
        assert final.workflow_transition_count == 0
        terminal_events = await setup.fetchval(
            "SELECT count(*) FROM workflow_run_execution_events "
            "WHERE workflow_run_id=$1 AND event_type='workflow_run.status_changed' "
            "AND payload->>'status'='succeeded'",
            created.id,
        )
        assert terminal_events == 1
        return {
            "result_owner_blocks_follower": result_block[0]["relationship_proven"],
            "reconciler_owner_blocks_follower": reconcile_block[0][
                "relationship_proven"
            ],
            "authoritative_result_count": result_state["result_count"],
            "accepted_result_events": result_state["accepted_events"],
            "identical_replay_events": result_state["replayed_events"],
            "terminated_claim_count": result_state["terminated_claim_count"],
            "terminal_task_transition_events": result_state["terminal_task_events"],
            "terminal_transition_events": terminal_events,
            "no_resurrection": True,
        }
    finally:
        await observer.close()
        await setup.close()
        await setup_engine.dispose()


def test_m21_terminal_state_update_contention() -> None:
    _run_scenario("terminal_state_updates", _terminal_scenario)
