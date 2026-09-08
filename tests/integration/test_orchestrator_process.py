"""Two independent production orchestrator processes against shared dependencies."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse
from uuid import UUID, uuid4

import aio_pika
import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import select, text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.broker.topology import (
    RabbitMQTopologyConfiguration,
    declare_dispatch_topology,
)
from taskforge.claims.authority import TaskClaimResultAuthorityIssuer
from taskforge.claims.domain import TaskClaimResultAuthority
from taskforge.claims.service import TaskClaimService
from taskforge.persistence.claims import SQLAlchemyTaskClaimRepository
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.task_results import SQLAlchemyTaskResultRepository
from taskforge.persistence.task_start import SQLAlchemyTaskStartRepository
from taskforge.rate_limits import AllowAllRateLimiter
from taskforge.runs.schema import task_attempts, task_dispatch_outbox
from taskforge.worker.result_submission import (
    TaskResultSubmissionRequest,
    TaskResultSubmissionService,
)
from taskforge.worker.results import TaskExecutionResult
from taskforge.worker.start import TaskStartRequest, TaskStartService
from taskforge.workflows.task_types import TaskTypeDefinition, TaskTypeRegistry
from tests.integration.postgresql import (
    asyncpg_dsn,
    migration_database_url,
    temporary_database,
)
from tests.integration.test_authentication_persistence import settings_for
from tests.integration.test_recovery_transition import recoverable_candidate
from tests.integration.test_task_claim_acquisition import (
    add_dispatched_task,
    add_worker,
    wait_for_lock_waiter,
)
from tests.integration.test_task_dispatch_creation import (
    AcceptParameters,
    seed_runnable_task,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_ORCHESTRATOR_PROCESS_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_ORCHESTRATOR_PROCESS_INTEGRATION=1 explicitly",
    ),
]

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOT = PROJECT_ROOT / "src"


def install_test_catalog(root: Path) -> None:
    module = root / "taskforge_test_catalog.py"
    module.write_text(
        "from taskforge.workflows.task_types import TaskTypeDefinition\n"
        "class Validator:\n"
        "    def validate(self, parameters):\n"
        "        return ()\n"
        "def catalog():\n"
        "    return (TaskTypeDefinition('document.extract', "
        "'document-workers', Validator()), "
        "TaskTypeDefinition('test.task', 'test-capability', Validator()))\n",
        encoding="utf-8",
    )
    distribution = root / "taskforge_test_catalog-1.0.dist-info"
    distribution.mkdir()
    (distribution / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: taskforge-test-catalog\nVersion: 1.0\n",
        encoding="utf-8",
    )
    (distribution / "entry_points.txt").write_text(
        "[taskforge.task_catalog]\ntest = taskforge_test_catalog:catalog\n",
        encoding="utf-8",
    )


def process_environment(
    database_url: URL,
    amqp_url: str,
    provider_root: Path,
    *,
    suffix: str,
) -> dict[str, str]:
    parsed = urlparse(amqp_url)
    assert parsed.hostname and parsed.port and parsed.username and parsed.password
    assert database_url.host and database_url.port and database_url.database
    assert database_url.username and database_url.password
    environment = os.environ.copy()
    environment.update(
        {
            "PYTHONPATH": f"{provider_root}{os.pathsep}{SOURCE_ROOT}",
            "POSTGRES_HOST": database_url.host,
            "POSTGRES_PORT": str(database_url.port),
            "POSTGRES_DB": database_url.database,
            "POSTGRES_USER": database_url.username,
            "POSTGRES_PASSWORD": database_url.password,
            "RABBITMQ_HOST": parsed.hostname,
            "RABBITMQ_AMQP_PORT": str(parsed.port),
            "RABBITMQ_DEFAULT_USER": unquote(parsed.username),
            "RABBITMQ_DEFAULT_PASS": unquote(parsed.password),
            "RABBITMQ_DEFAULT_VHOST": unquote(parsed.path.lstrip("/")) or "/",
            "TASKFORGE_RABBITMQ_DISPATCH_EXCHANGE_NAME": (
                f"taskforge.dispatch.process.{suffix}"
            ),
            "TASKFORGE_RABBITMQ_MALFORMED_EXCHANGE_NAME": (
                f"taskforge.dispatch.process.malformed.{suffix}"
            ),
            "TASKFORGE_ORCHESTRATOR_BATCH_SIZE": "10",
            "TASKFORGE_ORCHESTRATOR_POLL_INTERVAL_SECONDS": "0.05",
            "TASKFORGE_LOG_LEVEL": "WARNING",
        }
    )
    return environment


@dataclass(frozen=True)
class ProcessCandidates:
    runnable_task_id: UUID
    retry_task_id: UUID
    retry_failed_attempt_id: UUID
    recovery_task_id: UUID
    recovery_attempt_id: UUID
    recovery_generation: int
    stale_session_id: UUID
    stale_session_claim_id: UUID
    cancelling_run_id: UUID
    cancellation_task_id: UUID
    cancellation_attempt_id: UUID
    normal_terminal_run_id: UUID


@dataclass(frozen=True)
class PublicationCrashCandidate:
    task_run_id: UUID
    task_attempt_id: UUID
    dispatch_id: UUID


async def seed_genuine_retry_pending(
    connection: asyncpg.Connection[asyncpg.Record],
    sessions: async_sessionmaker[AsyncSession],
) -> tuple[UUID, UUID]:
    """Reach retry_pending through claim, start, and authoritative result services."""
    issuer = TaskClaimResultAuthorityIssuer(b"orchestrator-process-retry-secret")
    worker = await add_worker(connection)
    dispatch = await add_dispatched_task(
        connection,
        workflow_policy=json.dumps(
            {
                "retry_policy": {
                    "maximum_attempts": 3,
                    "initial_delay_seconds": 0,
                    "multiplier": 1,
                    "maximum_delay_seconds": 0,
                }
            }
        ),
    )
    claim = await TaskClaimService(
        SQLAlchemyTaskClaimRepository(sessions, worker_stale_after_seconds=30),
        issuer,
        lease_seconds=60,
    ).claim_task(worker.authenticated, worker.session_id, dispatch)
    await TaskStartService(SQLAlchemyTaskStartRepository(sessions)).start_task(
        worker.authenticated,
        worker.session_id,
        TaskStartRequest(
            dispatch.task_run_id,
            dispatch.task_attempt_id,
            claim.claim.generation,
        ),
    )
    authority = claim.result_authority
    assert isinstance(authority, TaskClaimResultAuthority)
    await TaskResultSubmissionService(
        SQLAlchemyTaskResultRepository(sessions),
        issuer,
        rate_limiter=AllowAllRateLimiter(),
    ).submit_result(
        worker.authenticated,
        worker.session_id,
        TaskResultSubmissionRequest(
            dispatch.dispatch_id,
            dispatch.task_run_id,
            dispatch.task_attempt_id,
            claim.claim.generation,
            authority,
            TaskExecutionResult.retryable_handler_reported(),
        ),
    )
    state = await connection.fetchval(
        "SELECT status::text FROM task_runs WHERE id=$1", dispatch.task_run_id
    )
    assert state == "retry_pending"
    return dispatch.task_run_id, dispatch.task_attempt_id


async def wait_for_convergence(
    database_url: URL, candidates: ProcessCandidates
) -> dict[str, str]:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    try:
        for _ in range(200):
            async with sessions() as session:
                converged = await session.execute(
                    text(
                        "SELECT "
                        "(SELECT status::text FROM task_runs WHERE id=:runnable) "
                        "= 'dispatched' AS runnable_done, "
                        "(SELECT status::text FROM task_runs WHERE id=:retry) "
                        "= 'dispatched' AS retry_done, "
                        "(SELECT status::text FROM task_runs WHERE id=:recovery) "
                        "= 'dispatched' AS recovery_done, "
                        "(SELECT ended_at IS NOT NULL FROM worker_sessions WHERE id=:session) "
                        "AS stale_done, "
                        "(SELECT status::text FROM workflow_runs WHERE id=:cancelling) "
                        "= 'cancelled' AS cancellation_done, "
                        "(SELECT status::text FROM workflow_runs WHERE id=:terminal) "
                        "= 'succeeded' AS terminal_done, "
                        "NOT EXISTS (SELECT FROM task_dispatch_outbox "
                        "WHERE published_at IS NULL) AS publication_done"
                    ),
                    {
                        "runnable": candidates.runnable_task_id,
                        "retry": candidates.retry_task_id,
                        "recovery": candidates.recovery_task_id,
                        "session": candidates.stale_session_id,
                        "cancelling": candidates.cancelling_run_id,
                        "terminal": candidates.normal_terminal_run_id,
                    },
                )
                complete = converged.one()
                dispatches = (
                    await session.execute(
                        select(task_dispatch_outbox.c.id, task_dispatch_outbox.c.route)
                    )
                ).all()
                attempt_count = await session.scalar(select(task_attempts.c.id))
            if all(complete) and attempt_count and dispatches:
                return {str(row.id): row.route for row in dispatches}
            await asyncio.sleep(0.05)
    finally:
        await engine.dispose()
    raise AssertionError("orchestrators did not publish durable dispatch in time")


async def assert_singular_authoritative_facts(
    database_url: URL, candidates: ProcessCandidates
) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        runnable = await connection.fetchrow(
            "SELECT tr.status::text, count(DISTINCT ta.id) AS attempts, "
            "array_agg(DISTINCT ta.attempt_number ORDER BY ta.attempt_number) "
            "AS numbers, count(DISTINCT o.id) AS outboxes "
            "FROM task_runs tr JOIN task_attempts ta ON ta.task_run_id=tr.id "
            "JOIN task_dispatch_outbox o ON o.task_attempt_id=ta.id "
            "WHERE tr.id=$1 GROUP BY tr.status",
            candidates.runnable_task_id,
        )
        assert runnable is not None
        assert tuple(runnable) == ("dispatched", 1, [1], 1)

        retry = await connection.fetchrow(
            "SELECT tr.status::text, count(DISTINCT ta.id) AS attempts, "
            "array_agg(DISTINCT ta.attempt_number ORDER BY ta.attempt_number) "
            "AS numbers, count(DISTINCT o.id) FILTER (WHERE ta.attempt_number=2) "
            "AS replacement_outboxes, "
            "(SELECT count(*) FROM task_retry_events e WHERE e.task_run_id=tr.id "
            "AND e.event_type='retry_scheduled' AND e.failed_attempt_number=1 "
            "AND e.retry_attempt_number=2) AS scheduling_events "
            "FROM task_runs tr JOIN task_attempts ta ON ta.task_run_id=tr.id "
            "LEFT JOIN task_dispatch_outbox o ON o.task_attempt_id=ta.id "
            "WHERE tr.id=$1 GROUP BY tr.id,tr.status",
            candidates.retry_task_id,
        )
        assert retry is not None
        assert tuple(retry) == ("dispatched", 2, [1, 2], 1, 1)

        recovery = await connection.fetchrow(
            "SELECT tr.status::text, count(DISTINCT ta.id) AS attempts, "
            "array_agg(DISTINCT ta.attempt_number ORDER BY ta.attempt_number) "
            "AS numbers, count(DISTINCT o.id) FILTER (WHERE ta.attempt_number=2) "
            "AS replacement_outboxes, "
            "(SELECT count(*) FROM task_attempt_results r WHERE "
            "r.task_attempt_id=$2 AND r.claim_generation=$3 "
            "AND r.result_kind='retryable_failure' "
            "AND r.failure_kind='claim_expired') AS recovered_results, "
            "(SELECT count(*) FROM task_result_events e WHERE "
            "e.task_attempt_id=$2 AND e.claim_generation=$3 "
            "AND e.event_type='result_recovered' "
            "AND e.actor_component='expired_claim_recovery') AS recovery_events, "
            "(SELECT count(*) FROM task_attempt_results newer "
            "JOIN task_attempts nta ON nta.id=newer.task_attempt_id "
            "WHERE nta.task_run_id=tr.id AND nta.attempt_number=2) "
            "AS newer_results "
            "FROM task_runs tr JOIN task_attempts ta ON ta.task_run_id=tr.id "
            "LEFT JOIN task_dispatch_outbox o ON o.task_attempt_id=ta.id "
            "WHERE tr.id=$1 GROUP BY tr.id,tr.status",
            candidates.recovery_task_id,
            candidates.recovery_attempt_id,
            candidates.recovery_generation,
        )
        assert recovery is not None
        assert tuple(recovery) == ("dispatched", 2, [1, 2], 1, 1, 1, 0)
        claim = await connection.fetchrow(
            "SELECT count(*) AS rows, count(*) FILTER (WHERE terminated_at IS NOT NULL) "
            "AS terminated FROM task_attempt_claims WHERE task_attempt_id=$1 "
            "AND generation=$2",
            candidates.recovery_attempt_id,
            candidates.recovery_generation,
        )
        assert claim is not None and tuple(claim) == (1, 1)

        stale = await connection.fetchrow(
            "SELECT s.ended_at IS NOT NULL, "
            "(SELECT count(*) FROM audit_records a WHERE "
            "a.action='worker_session.ended_stale' "
            "AND a.resource_id=s.id) AS audit_count, "
            "(SELECT count(*) FROM task_attempt_claims c WHERE "
            "c.task_attempt_id=$2 AND c.worker_session_id=s.id "
            "AND c.terminated_at IS NULL) AS active_claims "
            "FROM worker_sessions s WHERE s.id=$1",
            candidates.stale_session_id,
            candidates.stale_session_claim_id,
        )
        assert stale is not None and tuple(stale) == (True, 1, 1)

        cancellation = await connection.fetchrow(
            "SELECT wr.status::text, tr.status::text, "
            "(SELECT count(*) FROM workflow_run_cancellation_requests c "
            "WHERE c.workflow_run_id=wr.id) AS intents, "
            "(SELECT count(*) FROM task_attempt_results r WHERE "
            "r.task_attempt_id=$3 AND r.result_kind='cancellation') AS results, "
            "(SELECT count(*) FROM task_result_events e WHERE "
            "e.task_attempt_id=$3 AND e.event_type='result_recovered' "
            "AND e.actor_component='cancellation_recovery') AS recovery_events, "
            "(SELECT count(*) FROM workflow_run_execution_events e WHERE "
            "e.workflow_run_id=wr.id AND e.event_type='workflow_run.status_changed' "
            "AND e.payload->>'status'='cancelled') AS terminal_events "
            "FROM workflow_runs wr JOIN task_runs tr ON tr.workflow_run_id=wr.id "
            "WHERE wr.id=$1 AND tr.id=$2",
            candidates.cancelling_run_id,
            candidates.cancellation_task_id,
            candidates.cancellation_attempt_id,
        )
        assert cancellation is not None
        assert tuple(cancellation) == ("cancelled", "cancelled", 1, 1, 1, 1)

        terminal = await connection.fetchrow(
            "SELECT wr.status::text, "
            "(SELECT count(*) FROM workflow_run_execution_events e WHERE "
            "e.workflow_run_id=wr.id AND e.event_type='workflow_run.status_changed' "
            "AND e.payload->>'status'='succeeded') AS terminal_events "
            "FROM workflow_runs wr WHERE wr.id=$1",
            candidates.normal_terminal_run_id,
        )
        assert terminal is not None and tuple(terminal) == ("succeeded", 1)
    finally:
        await connection.close()


async def seed_process_candidates(database_url: URL) -> ProcessCandidates:
    engine = build_async_engine(settings_for(database_url))
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        sessions = build_session_factory(engine)
        _run_id, runnable_task_id, _dispatch_id = await seed_runnable_task(sessions)

        retry_task_id, retry_failed_attempt_id = await seed_genuine_retry_pending(
            connection, sessions
        )

        recovery_candidate = await recoverable_candidate(
            connection, maximum_attempts=3, initial_delay_seconds=0
        )
        cancellation_candidate = await recoverable_candidate(
            connection,
            maximum_attempts=3,
            run_status="cancelling",
            initial_delay_seconds=0,
        )
        assert (
            await connection.fetchval(
                "SELECT status::text FROM workflow_runs WHERE id=$1",
                cancellation_candidate.workflow_run_id,
            )
            == "cancelling"
        )
        stale_worker = await add_worker(connection)
        stale_dispatch = await add_dispatched_task(connection)
        stale_claim = await TaskClaimService(
            SQLAlchemyTaskClaimRepository(sessions, worker_stale_after_seconds=30),
            TaskClaimResultAuthorityIssuer(b"orchestrator-process-stale-secret"),
            lease_seconds=300,
        ).claim_task(
            stale_worker.authenticated,
            stale_worker.session_id,
            stale_dispatch,
        )
        assert stale_claim.claim.worker_session_id == stale_worker.session_id
        last_seen_at = await connection.fetchval(
            "UPDATE worker_session_health SET last_sequence=1, "
            "last_seen_at=statement_timestamp()-interval '45 seconds', "
            "accepting_work=true, availability_changed_at="
            "statement_timestamp()-interval '45 seconds' "
            "WHERE worker_session_id=$1 RETURNING last_seen_at",
            stale_worker.session_id,
        )
        await connection.execute(
            "INSERT INTO worker_heartbeats "
            "(worker_session_id, sequence, received_at, accepting_work, "
            "worker_identity_id, correlation_id) VALUES ($1, 1, $2, true, $3, $4)",
            stale_worker.session_id,
            last_seen_at,
            stale_worker.authenticated.worker_identity_id,
            f"process-stale-{stale_worker.session_id}",
        )

        normal_terminal_run_id, normal_terminal_task_id, _ = await seed_runnable_task(
            sessions
        )
        await connection.execute(
            "UPDATE task_runs SET status='succeeded' WHERE id=$1",
            normal_terminal_task_id,
        )
        return ProcessCandidates(
            runnable_task_id,
            retry_task_id,
            retry_failed_attempt_id,
            recovery_candidate.task_run_id,
            recovery_candidate.task_attempt_id,
            recovery_candidate.generation,
            stale_worker.session_id,
            stale_dispatch.task_attempt_id,
            cancellation_candidate.workflow_run_id,
            cancellation_candidate.task_run_id,
            cancellation_candidate.task_attempt_id,
            normal_terminal_run_id,
        )
    finally:
        await connection.close()
        await engine.dispose()


async def verify_broker_message(
    amqp_url: str,
    exchange_name: str,
    malformed_name: str,
    expected_dispatches: dict[str, str],
) -> int:
    connection = await aio_pika.connect(amqp_url)
    try:
        channel = await connection.channel()
        message_ids: list[str] = []
        queue_names = (
            f"{exchange_name}.capability.document-workers",
            f"{exchange_name}.capability.test-capability",
        )
        for queue_name in queue_names:
            queue = await channel.declare_queue(queue_name, passive=True, timeout=3)
            while True:
                message = await queue.get(fail=False, timeout=0.25)
                if message is None:
                    break
                assert message.message_id is not None
                message_ids.append(message.message_id)
                await message.ack()
        assert message_ids
        assert set(message_ids) == set(expected_dispatches)
        assert len(message_ids) >= len(expected_dispatches)
        for queue_name in queue_names:
            await channel.queue_delete(queue_name, timeout=3)
        await channel.queue_delete(f"{malformed_name}.quarantine", timeout=3)
        await channel.exchange_delete(exchange_name, timeout=3)
        await channel.exchange_delete(malformed_name, timeout=3)
        return len(message_ids)
    finally:
        await connection.close()


async def prepare_process_topology(amqp_url: str, suffix: str) -> None:
    connection = await aio_pika.connect(amqp_url)
    try:
        channel = await connection.channel(
            publisher_confirms=True, on_return_raises=True
        )
        await declare_dispatch_topology(
            channel,
            TaskTypeRegistry(
                (
                    TaskTypeDefinition(
                        "document.extract", "document-workers", AcceptParameters()
                    ),
                    TaskTypeDefinition(
                        "test.task", "test-capability", AcceptParameters()
                    ),
                )
            ),
            RabbitMQTopologyConfiguration(
                f"taskforge.dispatch.process.{suffix}",
                f"taskforge.dispatch.process.malformed.{suffix}",
                3,
            ),
        )
    finally:
        await connection.close()


async def consume_one(amqp_url: str, queue_name: str, *, timeout: float = 10) -> str:
    connection = await aio_pika.connect(amqp_url)
    try:
        channel = await connection.channel()
        queue = await channel.declare_queue(queue_name, passive=True, timeout=3)
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            message = await queue.get(fail=False, timeout=3)
            if message is not None:
                assert message.message_id is not None
                message_id = message.message_id
                await message.ack()
                return message_id
            await asyncio.sleep(0.05)
        raise AssertionError(f"no message arrived on {queue_name}")
    finally:
        await connection.close()


async def cleanup_process_topology(amqp_url: str, suffix: str) -> None:
    connection = await aio_pika.connect(amqp_url)
    try:
        channel = await connection.channel()
        exchange_name = f"taskforge.dispatch.process.{suffix}"
        malformed_name = f"taskforge.dispatch.process.malformed.{suffix}"
        for capability in ("document-workers", "test-capability"):
            await channel.queue_delete(
                f"{exchange_name}.capability.{capability}", timeout=3
            )
        await channel.queue_delete(f"{malformed_name}.quarantine", timeout=3)
        await channel.exchange_delete(exchange_name, timeout=3)
        await channel.exchange_delete(malformed_name, timeout=3)
    finally:
        await connection.close()


async def seed_publication_crash_candidate(
    database_url: URL,
) -> PublicationCrashCandidate:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        dispatch = await add_dispatched_task(connection)
        published_at = await connection.fetchval(
            "SELECT published_at FROM task_dispatch_outbox WHERE id=$1",
            dispatch.dispatch_id,
        )
        assert published_at is None
        return PublicationCrashCandidate(
            dispatch.task_run_id,
            dispatch.task_attempt_id,
            dispatch.dispatch_id,
        )
    finally:
        await connection.close()


def stop_process(process: subprocess.Popen[str]) -> tuple[int, str]:
    process.send_signal(signal.SIGTERM)
    try:
        _stdout, stderr = process.communicate(timeout=10)
    except subprocess.TimeoutExpired as exc:
        process.kill()
        _stdout, stderr = process.communicate(timeout=5)
        raise AssertionError("orchestrator did not stop after SIGTERM") from exc
    return process.returncode, stderr


def test_two_production_orchestrator_processes_converge_and_stop(
    tmp_path: Path,
) -> None:
    amqp_url = os.getenv("TASKFORGE_BROKER_TEST_AMQP_URL")
    if not amqp_url:
        pytest.fail("TASKFORGE_BROKER_TEST_AMQP_URL is required")
    install_test_catalog(tmp_path)
    suffix = uuid4().hex
    exchange_name = f"taskforge.dispatch.process.{suffix}"
    malformed_name = f"taskforge.dispatch.process.malformed.{suffix}"
    with temporary_database(
        "TASKFORGE_ORCHESTRATOR_TEST_DATABASE_URL", "taskforge_m21_workload"
    ) as database_url:
        config = Config("alembic.ini")
        with migration_database_url(database_url.render_as_string(hide_password=False)):
            command.upgrade(config, "head")
        candidates = asyncio.run(seed_process_candidates(database_url))

        environment = process_environment(
            database_url, amqp_url, tmp_path, suffix=suffix
        )
        processes = tuple(
            subprocess.Popen(
                [sys.executable, "-m", "taskforge.orchestrator"],
                cwd=PROJECT_ROOT,
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            for _ in range(2)
        )
        try:
            dispatches = asyncio.run(wait_for_convergence(database_url, candidates))
            assert set(dispatches.values()) == {
                "capability.document-workers",
                "capability.test-capability",
            }
            asyncio.run(assert_singular_authoritative_facts(database_url, candidates))
            assert all(process.poll() is None for process in processes)
            results = tuple(stop_process(process) for process in processes)
            assert [code for code, _stderr in results] == [0, 0]
            asyncio.run(assert_singular_authoritative_facts(database_url, candidates))
            assert (
                asyncio.run(
                    verify_broker_message(
                        amqp_url,
                        exchange_name,
                        malformed_name,
                        dispatches,
                    )
                )
                >= 1
            )
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)


async def verify_process_publication_crash_boundary(
    database_url: URL,
    amqp_url: str,
    provider_root: Path,
    *,
    suffix: str,
) -> None:
    candidate = await seed_publication_crash_candidate(database_url)
    await prepare_process_topology(amqp_url, suffix)
    queue_name = f"taskforge.dispatch.process.{suffix}.capability.test-capability"
    environment = process_environment(
        database_url, amqp_url, provider_root, suffix=suffix
    )
    lock_connection = await asyncpg.connect(asyncpg_dsn(database_url))
    transaction = lock_connection.transaction()
    await transaction.start()
    await lock_connection.fetchval(
        "SELECT id FROM task_dispatch_outbox WHERE id=$1 FOR UPDATE",
        candidate.dispatch_id,
    )
    victim = subprocess.Popen(
        [sys.executable, "-m", "taskforge.orchestrator"],
        cwd=PROJECT_ROOT,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    recovery: subprocess.Popen[str] | None = None
    try:
        first_message_id = await consume_one(amqp_url, queue_name)
        await wait_for_lock_waiter(lock_connection)
        assert (
            await lock_connection.fetchval(
                "SELECT published_at FROM task_dispatch_outbox WHERE id=$1",
                candidate.dispatch_id,
            )
            is None
        )
        assert victim.poll() is None
        victim.kill()
        await asyncio.to_thread(victim.communicate, timeout=5)
        assert victim.returncode is not None and victim.returncode < 0

        await transaction.rollback()
        await lock_connection.close()

        recovery = subprocess.Popen(
            [sys.executable, "-m", "taskforge.orchestrator"],
            cwd=PROJECT_ROOT,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(200):
            observer = await asyncpg.connect(asyncpg_dsn(database_url))
            try:
                published_at = await observer.fetchval(
                    "SELECT published_at FROM task_dispatch_outbox WHERE id=$1",
                    candidate.dispatch_id,
                )
            finally:
                await observer.close()
            if published_at is not None:
                break
            await asyncio.sleep(0.05)
        else:
            raise AssertionError(
                "restarted orchestrator did not acknowledge publication"
            )

        second_message_id = await consume_one(amqp_url, queue_name)
        recovery_code, _stderr = await asyncio.to_thread(stop_process, recovery)
        assert recovery_code == 0
        assert first_message_id == second_message_id == str(candidate.dispatch_id)

        observer = await asyncpg.connect(asyncpg_dsn(database_url))
        try:
            authority = await observer.fetchrow(
                "SELECT tr.status::text, count(DISTINCT ta.id) AS attempts, "
                "count(DISTINCT o.id) AS outboxes, "
                "count(DISTINCT r.task_attempt_id) AS results, "
                "bool_and(o.published_at IS NOT NULL) AS published "
                "FROM task_runs tr JOIN task_attempts ta ON ta.task_run_id=tr.id "
                "JOIN task_dispatch_outbox o ON o.task_attempt_id=ta.id "
                "LEFT JOIN task_attempt_results r ON r.task_attempt_id=ta.id "
                "WHERE tr.id=$1 GROUP BY tr.status",
                candidate.task_run_id,
            )
            assert authority is not None
            assert tuple(authority) == ("dispatched", 1, 1, 0, True)
        finally:
            await observer.close()
    finally:
        if victim.poll() is None:
            victim.kill()
            await asyncio.to_thread(victim.communicate, timeout=5)
        if recovery is not None and recovery.poll() is None:
            recovery.kill()
            await asyncio.to_thread(recovery.communicate, timeout=5)
        if not lock_connection.is_closed():
            with suppress(Exception):
                await transaction.rollback()
            await lock_connection.close()
        with suppress(Exception):
            await cleanup_process_topology(amqp_url, suffix)


def test_orchestrator_process_crash_after_publish_before_db_ack_republishes(
    tmp_path: Path,
) -> None:
    amqp_url = os.getenv("TASKFORGE_BROKER_TEST_AMQP_URL")
    if not amqp_url:
        pytest.fail("TASKFORGE_BROKER_TEST_AMQP_URL is required")
    install_test_catalog(tmp_path)
    suffix = uuid4().hex
    with temporary_database(
        "TASKFORGE_ORCHESTRATOR_TEST_DATABASE_URL", "taskforge_m21_workload"
    ) as database_url:
        config = Config("alembic.ini")
        with migration_database_url(database_url.render_as_string(hide_password=False)):
            command.upgrade(config, "head")
        asyncio.run(
            verify_process_publication_crash_boundary(
                database_url,
                amqp_url,
                tmp_path,
                suffix=suffix,
            )
        )
