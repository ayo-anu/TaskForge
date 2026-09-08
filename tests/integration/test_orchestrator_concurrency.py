"""Real PostgreSQL pass-layer multi-orchestrator safety evidence."""

from __future__ import annotations

import asyncio
import os

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import func, select
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.dispatch.service import TaskDispatchService
from taskforge.orchestrator.workloads import (
    ProgressionDispatchWorkload,
    RecoveryWorkload,
    RetryWorkload,
)
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.dispatch import SQLAlchemyTaskDispatchRepository
from taskforge.persistence.orchestrator import SQLAlchemyOrchestratorCandidateRepository
from taskforge.persistence.recovery import (
    SQLAlchemyExpiredClaimRecoveryRepository,
    SQLAlchemyRecoveryCandidateRepository,
    SQLAlchemyStaleWorkerSessionRecoveryRepository,
)
from taskforge.persistence.retries import SQLAlchemyRetryTransitionRepository
from taskforge.persistence.runs import SQLAlchemyWorkflowRunRepository
from taskforge.recovery.progression import ExpiredClaimRecoveryProgressionService
from taskforge.recovery.scanner import RecoveryCandidateScanner
from taskforge.recovery.service import (
    ExpiredClaimRecoveryService,
    StaleWorkerSessionRecoveryService,
)
from taskforge.retries.scanner import DueRetryScanner
from taskforge.retries.service import RetryTransitionService
from taskforge.runs.schema import task_attempts, task_dispatch_outbox, task_runs
from taskforge.runs.service import WorkflowRunService
from taskforge.workflows.task_types import TaskTypeDefinition, TaskTypeRegistry
from tests.integration.postgresql import migration_database_url, temporary_database
from tests.integration.test_authentication_persistence import settings_for
from tests.integration.test_orchestrator_process import (
    assert_singular_authoritative_facts,
    seed_process_candidates,
)
from tests.integration.test_task_dispatch_creation import (
    AcceptParameters,
    seed_runnable_task,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_ORCHESTRATOR_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_ORCHESTRATOR_INTEGRATION=1 explicitly",
    ),
]


async def verify_two_passes_converge(database_url: URL) -> None:
    engine_a = build_async_engine(settings_for(database_url))
    engine_b = build_async_engine(settings_for(database_url))
    sessions_a = build_session_factory(engine_a)
    sessions_b = build_session_factory(engine_b)
    registry = TaskTypeRegistry(
        (
            TaskTypeDefinition(
                "document.extract", "document-workers", AcceptParameters()
            ),
            TaskTypeDefinition("test.task", "test-capability", AcceptParameters()),
        )
    )
    try:
        workflow_run_id, task_run_id, _version_id = await seed_runnable_task(sessions_a)
        workload_a = ProgressionDispatchWorkload(
            SQLAlchemyOrchestratorCandidateRepository(sessions_a),
            WorkflowRunService(SQLAlchemyWorkflowRunRepository(sessions_a)),
            TaskDispatchService(SQLAlchemyTaskDispatchRepository(sessions_a), registry),
            batch_size=10,
        )
        workload_b = ProgressionDispatchWorkload(
            SQLAlchemyOrchestratorCandidateRepository(sessions_b),
            WorkflowRunService(SQLAlchemyWorkflowRunRepository(sessions_b)),
            TaskDispatchService(SQLAlchemyTaskDispatchRepository(sessions_b), registry),
            batch_size=10,
        )

        results = await asyncio.gather(workload_a.run_once(), workload_b.run_once())
        assert sum(result.transitions for result in results) >= 2

        async with sessions_a() as session:
            attempt_count = await session.scalar(
                select(func.count()).select_from(task_attempts)
            )
            outbox_count = await session.scalar(
                select(func.count()).select_from(task_dispatch_outbox)
            )
            status = await session.scalar(
                select(task_runs.c.status).where(task_runs.c.id == task_run_id)
            )
        assert attempt_count == 1
        assert outbox_count == 1
        assert status == "dispatched"

        # A restarted process-local sweep sees no dispatch candidate and creates no fact.
        restarted = ProgressionDispatchWorkload(
            SQLAlchemyOrchestratorCandidateRepository(sessions_b),
            WorkflowRunService(SQLAlchemyWorkflowRunRepository(sessions_b)),
            TaskDispatchService(SQLAlchemyTaskDispatchRepository(sessions_b), registry),
            batch_size=10,
        )
        await restarted.run_once()
        async with sessions_b() as session:
            assert (
                await session.scalar(select(func.count()).select_from(task_attempts))
                == 1
            )
        assert workflow_run_id is not None
    finally:
        await engine_a.dispose()
        await engine_b.dispose()


def progression_workload(
    sessions: async_sessionmaker[AsyncSession], registry: TaskTypeRegistry
) -> ProgressionDispatchWorkload:
    return ProgressionDispatchWorkload(
        SQLAlchemyOrchestratorCandidateRepository(sessions),
        WorkflowRunService(SQLAlchemyWorkflowRunRepository(sessions)),
        TaskDispatchService(SQLAlchemyTaskDispatchRepository(sessions), registry),
        batch_size=100,
    )


def retry_workload(
    sessions: async_sessionmaker[AsyncSession], registry: TaskTypeRegistry
) -> RetryWorkload:
    repository = SQLAlchemyRetryTransitionRepository(sessions)
    return RetryWorkload(
        SQLAlchemyOrchestratorCandidateRepository(sessions),
        RetryTransitionService(repository),
        DueRetryScanner(repository, registry),
        batch_size=100,
    )


def recovery_workload(
    sessions: async_sessionmaker[AsyncSession],
) -> RecoveryWorkload:
    runs = WorkflowRunService(SQLAlchemyWorkflowRunRepository(sessions))
    return RecoveryWorkload(
        RecoveryCandidateScanner(
            SQLAlchemyRecoveryCandidateRepository(sessions),
            worker_stale_after_seconds=30,
        ),
        ExpiredClaimRecoveryProgressionService(
            ExpiredClaimRecoveryService(
                SQLAlchemyExpiredClaimRecoveryRepository(sessions)
            ),
            runs,
        ),
        StaleWorkerSessionRecoveryService(
            SQLAlchemyStaleWorkerSessionRecoveryRepository(sessions)
        ),
        batch_size=100,
        stale_after_seconds=30,
    )


async def verify_all_passes_converge(database_url: URL) -> None:
    engine_a = build_async_engine(settings_for(database_url))
    engine_b = build_async_engine(settings_for(database_url))
    sessions_a = build_session_factory(engine_a)
    sessions_b = build_session_factory(engine_b)
    registry = TaskTypeRegistry(
        (
            TaskTypeDefinition(
                "document.extract", "document-workers", AcceptParameters()
            ),
            TaskTypeDefinition("test.task", "test-capability", AcceptParameters()),
        )
    )
    try:
        candidates = await seed_process_candidates(database_url)

        # Each pair owns independent cursor and service instances. Both observe the
        # same durable candidates and rely only on existing service serialization.
        await asyncio.gather(
            progression_workload(sessions_a, registry).run_once(),
            progression_workload(sessions_b, registry).run_once(),
        )
        await asyncio.gather(
            retry_workload(sessions_a, registry).run_once(),
            retry_workload(sessions_b, registry).run_once(),
        )
        await asyncio.gather(
            recovery_workload(sessions_a).run_once(),
            recovery_workload(sessions_b).run_once(),
        )
        # Recovery creates a genuine retry_pending successor for the expired claim;
        # another concurrent retry pass schedules and dispatches it exactly once.
        await asyncio.gather(
            retry_workload(sessions_a, registry).run_once(),
            retry_workload(sessions_b, registry).run_once(),
        )
        # Cancellation recovery makes final cancellation derivable; normal terminal
        # reconciliation is replayed concurrently in the same pass layer.
        await asyncio.gather(
            progression_workload(sessions_a, registry).run_once(),
            progression_workload(sessions_b, registry).run_once(),
        )

        await assert_singular_authoritative_facts(database_url, candidates)
    finally:
        await engine_a.dispose()
        await engine_b.dispose()


def test_two_progression_dispatch_passes_share_one_authoritative_result() -> None:
    with temporary_database(
        "TASKFORGE_ORCHESTRATOR_TEST_DATABASE_URL", "taskforge_m21_workload"
    ) as database_url:
        config = Config("alembic.ini")
        with migration_database_url(database_url.render_as_string(hide_password=False)):
            command.upgrade(config, "head")
        asyncio.run(verify_two_passes_converge(database_url))


def test_two_instances_of_every_orchestrator_pass_preserve_authority() -> None:
    with temporary_database(
        "TASKFORGE_ORCHESTRATOR_TEST_DATABASE_URL", "taskforge_m21_workload"
    ) as database_url:
        config = Config("alembic.ini")
        with migration_database_url(database_url.render_as_string(hide_password=False)):
            command.upgrade(config, "head")
        asyncio.run(verify_all_passes_converge(database_url))
