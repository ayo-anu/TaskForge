"""Opt-in atomic workflow-run cancellation verification against PostgreSQL."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import delete, insert, select, text, update
from sqlalchemy.engine import URL, RowMapping
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.identity.authorization import OwnerFilter
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.runs import SQLAlchemyWorkflowRunRepository
from taskforge.runs.domain import (
    WorkflowRunCancellationIdempotencyConflict,
    WorkflowRunCancellationOutcome,
    WorkflowRunStatus,
    create_workflow_run_cancellation_command,
)
from taskforge.runs.schema import (
    workflow_run_cancellation_requests,
    workflow_runs,
)
from taskforge.runs.service import (
    WorkflowRunCancellationInvariantError,
    WorkflowRunNotFound,
    WorkflowRunService,
    WorkflowRunServiceUnavailable,
)
from tests.integration.postgresql import migration_database_url, temporary_database
from tests.integration.test_authentication_persistence import settings_for
from tests.integration.test_workflow_run_creation import seed_workflow

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION=1 explicitly",
    ),
]


async def _seed_run(
    sessions: async_sessionmaker[AsyncSession],
    *,
    workflow_id: UUID,
    version_id: UUID,
    requester_id: UUID,
    status: WorkflowRunStatus,
) -> UUID:
    run_id = uuid4()
    async with sessions.begin() as session:
        await session.execute(
            insert(workflow_runs).values(
                id=run_id,
                workflow_definition_id=workflow_id,
                workflow_version_id=version_id,
                requested_by_principal_id=requester_id,
                status=status.value,
            )
        )
    return run_id


async def _stored(
    sessions: async_sessionmaker[AsyncSession], run_id: UUID
) -> tuple[str, RowMapping | None]:
    async with sessions() as session:
        status_value = await session.scalar(
            select(workflow_runs.c.status).where(workflow_runs.c.id == run_id)
        )
        request = (
            (
                await session.execute(
                    select(workflow_run_cancellation_requests).where(
                        workflow_run_cancellation_requests.c.workflow_run_id == run_id
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
    assert status_value is not None
    return str(status_value), request


async def _assert_raises(
    exception: type[BaseException], operation: Callable[[], Awaitable[object]]
) -> None:
    with pytest.raises(exception):
        await operation()


async def _verify_cancellation(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    service = WorkflowRunService(SQLAlchemyWorkflowRunRepository(sessions))
    try:
        owner_id, other_id, workflow_id, _, version_id = await seed_workflow(sessions)
        owner_filter = OwnerFilter.only(owner_id)
        other_filter = OwnerFilter.only(other_id)

        for initial in (WorkflowRunStatus.PENDING, WorkflowRunStatus.RUNNING):
            run_id = await _seed_run(
                sessions,
                workflow_id=workflow_id,
                version_id=version_id,
                requester_id=owner_id,
                status=initial,
            )
            result = await service.cancel_run(
                run_id,
                owner_filter,
                requested_by_principal_id=owner_id,
                idempotency_key=f"accepted-{initial.value}-key",
                reason="  planned maintenance  ",
            )
            assert result.outcome is WorkflowRunCancellationOutcome.NEWLY_ACCEPTED
            assert result.status is WorkflowRunStatus.CANCELLING
            assert result.accepted_request is not None
            assert result.accepted_request.reason == "planned maintenance"
            stored_status, stored_request = await _stored(sessions, run_id)
            assert stored_status == WorkflowRunStatus.CANCELLING.value
            assert stored_request is not None
            assert (
                stored_request["requested_at"] == result.accepted_request.requested_at
            )
            assert stored_request["requested_by_principal_id"] == owner_id
            assert stored_request["reason"] == "planned maintenance"

        retry_run = await _seed_run(
            sessions,
            workflow_id=workflow_id,
            version_id=version_id,
            requester_id=owner_id,
            status=WorkflowRunStatus.RUNNING,
        )
        first = await service.cancel_run(
            retry_run,
            owner_filter,
            requested_by_principal_id=owner_id,
            idempotency_key="canonical-cancel-key",
            reason=" reason ",
        )
        exact = await service.cancel_run(
            retry_run,
            owner_filter,
            requested_by_principal_id=owner_id,
            idempotency_key="canonical-cancel-key",
            reason="reason",
        )
        assert exact.outcome is WorkflowRunCancellationOutcome.EXACT_RETRY
        assert exact.accepted_request == first.accepted_request
        before_status, before_request = await _stored(sessions, retry_run)
        with pytest.raises(WorkflowRunCancellationIdempotencyConflict):
            await service.cancel_run(
                retry_run,
                owner_filter,
                requested_by_principal_id=owner_id,
                idempotency_key="canonical-cancel-key",
                reason="changed",
            )
        different_key = await service.cancel_run(
            retry_run,
            owner_filter,
            requested_by_principal_id=owner_id,
            idempotency_key="another-cancel-key",
            reason="reason",
        )
        different_requester = await service.cancel_run(
            retry_run,
            owner_filter,
            requested_by_principal_id=other_id,
            idempotency_key="requester-cancel-key",
            reason="reason",
        )
        same_key_different_requester = await service.cancel_run(
            retry_run,
            owner_filter,
            requested_by_principal_id=other_id,
            idempotency_key="canonical-cancel-key",
            reason="conflicting in another principal scope",
        )
        assert (
            different_key.outcome is WorkflowRunCancellationOutcome.ALREADY_CANCELLING
        )
        assert (
            different_requester.outcome
            is WorkflowRunCancellationOutcome.ALREADY_CANCELLING
        )
        assert (
            same_key_different_requester.outcome
            is WorkflowRunCancellationOutcome.ALREADY_CANCELLING
        )
        assert (
            different_key.accepted_request
            is different_requester.accepted_request
            is same_key_different_requester.accepted_request
            is None
        )
        assert await _stored(sessions, retry_run) == (before_status, before_request)

        hidden_run = await _seed_run(
            sessions,
            workflow_id=workflow_id,
            version_id=version_id,
            requester_id=owner_id,
            status=WorkflowRunStatus.PENDING,
        )
        await _assert_raises(
            WorkflowRunNotFound,
            lambda: service.cancel_run(
                hidden_run,
                other_filter,
                requested_by_principal_id=other_id,
                idempotency_key="hidden-cancel-key",
                reason=None,
            ),
        )
        await _assert_raises(
            WorkflowRunNotFound,
            lambda: service.cancel_run(
                uuid4(),
                owner_filter,
                requested_by_principal_id=owner_id,
                idempotency_key="missing-cancel-key",
                reason=None,
            ),
        )
        assert await _stored(sessions, hidden_run) == (
            WorkflowRunStatus.PENDING.value,
            None,
        )

        for terminal in (WorkflowRunStatus.SUCCEEDED, WorkflowRunStatus.FAILED):
            run_id = await _seed_run(
                sessions,
                workflow_id=workflow_id,
                version_id=version_id,
                requester_id=owner_id,
                status=terminal,
            )
            result = await service.cancel_run(
                run_id,
                owner_filter,
                requested_by_principal_id=owner_id,
                idempotency_key=f"terminal-{terminal.value}-key",
                reason=None,
            )
            assert result.outcome is WorkflowRunCancellationOutcome.TERMINAL_STATE_WON
            assert result.status is terminal
            assert await _stored(sessions, run_id) == (terminal.value, None)

        cancelled_run = await _seed_run(
            sessions,
            workflow_id=workflow_id,
            version_id=version_id,
            requester_id=owner_id,
            status=WorkflowRunStatus.RUNNING,
        )
        await service.cancel_run(
            cancelled_run,
            owner_filter,
            requested_by_principal_id=owner_id,
            idempotency_key="eventually-cancelled",
            reason=None,
        )
        async with sessions.begin() as session:
            await session.execute(
                update(workflow_runs)
                .where(workflow_runs.c.id == cancelled_run)
                .values(status=WorkflowRunStatus.CANCELLED.value)
            )
        cancelled = await service.cancel_run(
            cancelled_run,
            owner_filter,
            requested_by_principal_id=other_id,
            idempotency_key="different-after-cancelled",
            reason=None,
        )
        assert cancelled.outcome is WorkflowRunCancellationOutcome.ALREADY_CANCELLED
        assert cancelled.accepted_request is None

        for impossible in (WorkflowRunStatus.CANCELLING, WorkflowRunStatus.CANCELLED):
            run_id = await _seed_run(
                sessions,
                workflow_id=workflow_id,
                version_id=version_id,
                requester_id=owner_id,
                status=impossible,
            )

            async def cancel_impossible(target_run_id: UUID = run_id) -> object:
                return await service.cancel_run(
                    target_run_id,
                    owner_filter,
                    requested_by_principal_id=owner_id,
                    idempotency_key="impossible-state-key",
                    reason=None,
                )

            await _assert_raises(
                WorkflowRunCancellationInvariantError,
                cancel_impossible,
            )

        active_with_request = await _seed_run(
            sessions,
            workflow_id=workflow_id,
            version_id=version_id,
            requester_id=owner_id,
            status=WorkflowRunStatus.PENDING,
        )
        impossible_command = create_workflow_run_cancellation_command(
            active_with_request,
            owner_id,
            idempotency_key="active-invariant-key",
            reason=None,
        )
        async with sessions.begin() as session:
            await session.execute(
                insert(workflow_run_cancellation_requests).values(
                    workflow_run_id=active_with_request,
                    requested_by_principal_id=owner_id,
                    idempotency_key_digest=impossible_command.idempotency.key_digest,
                    request_fingerprint=(
                        impossible_command.idempotency.request_fingerprint
                    ),
                )
            )
        await _assert_raises(
            WorkflowRunCancellationInvariantError,
            lambda: service.cancel_run(
                active_with_request,
                owner_filter,
                requested_by_principal_id=owner_id,
                idempotency_key="active-invariant-key",
                reason=None,
            ),
        )

        same_key_run = await _seed_run(
            sessions,
            workflow_id=workflow_id,
            version_id=version_id,
            requester_id=owner_id,
            status=WorkflowRunStatus.RUNNING,
        )
        same_key = await asyncio.gather(
            *(
                service.cancel_run(
                    same_key_run,
                    owner_filter,
                    requested_by_principal_id=owner_id,
                    idempotency_key="concurrent-same-key",
                    reason="same",
                )
                for _ in range(2)
            )
        )
        assert {value.outcome for value in same_key} == {
            WorkflowRunCancellationOutcome.NEWLY_ACCEPTED,
            WorkflowRunCancellationOutcome.EXACT_RETRY,
        }
        assert same_key[0].accepted_request == same_key[1].accepted_request

        different_key_run = await _seed_run(
            sessions,
            workflow_id=workflow_id,
            version_id=version_id,
            requester_id=owner_id,
            status=WorkflowRunStatus.RUNNING,
        )
        different_keys = await asyncio.gather(
            service.cancel_run(
                different_key_run,
                owner_filter,
                requested_by_principal_id=owner_id,
                idempotency_key="concurrent-first-key",
                reason="first",
            ),
            service.cancel_run(
                different_key_run,
                owner_filter,
                requested_by_principal_id=other_id,
                idempotency_key="concurrent-second-key",
                reason="second",
            ),
        )
        assert {value.outcome for value in different_keys} == {
            WorkflowRunCancellationOutcome.NEWLY_ACCEPTED,
            WorkflowRunCancellationOutcome.ALREADY_CANCELLING,
        }
        assert sum(value.accepted_request is not None for value in different_keys) == 1

        race_run = await _seed_run(
            sessions,
            workflow_id=workflow_id,
            version_id=version_id,
            requester_id=owner_id,
            status=WorkflowRunStatus.RUNNING,
        )

        async def terminalize() -> None:
            async with sessions.begin() as session:
                await session.execute(
                    select(workflow_runs.c.id)
                    .where(workflow_runs.c.id == race_run)
                    .with_for_update(of=workflow_runs)
                )
                await session.execute(
                    update(workflow_runs)
                    .where(
                        workflow_runs.c.id == race_run,
                        workflow_runs.c.status == WorkflowRunStatus.RUNNING.value,
                    )
                    .values(status=WorkflowRunStatus.SUCCEEDED.value)
                )

        race_result, _ = await asyncio.gather(
            service.cancel_run(
                race_run,
                owner_filter,
                requested_by_principal_id=owner_id,
                idempotency_key="terminal-race-key",
                reason=None,
            ),
            terminalize(),
        )
        race_status, race_request = await _stored(sessions, race_run)
        if race_result.outcome is WorkflowRunCancellationOutcome.NEWLY_ACCEPTED:
            assert (race_status, race_request is not None) == ("cancelling", True)
        else:
            assert (
                race_result.outcome is WorkflowRunCancellationOutcome.TERMINAL_STATE_WON
            )
            assert (race_status, race_request) == ("succeeded", None)

        rollback_run = await _seed_run(
            sessions,
            workflow_id=workflow_id,
            version_id=version_id,
            requester_id=owner_id,
            status=WorkflowRunStatus.RUNNING,
        )
        async with sessions.begin() as session:
            await session.execute(
                text(
                    "CREATE FUNCTION reject_test_cancellation() RETURNS trigger "
                    "LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected'; END $$"
                )
            )
            await session.execute(
                text(
                    "CREATE TRIGGER reject_test_cancellation BEFORE UPDATE ON workflow_runs "
                    "FOR EACH ROW WHEN (NEW.id = '"
                    f"{rollback_run}'::uuid AND NEW.status = 'cancelling') "
                    "EXECUTE FUNCTION reject_test_cancellation()"
                )
            )
        await _assert_raises(
            WorkflowRunServiceUnavailable,
            lambda: service.cancel_run(
                rollback_run,
                owner_filter,
                requested_by_principal_id=owner_id,
                idempotency_key="rollback-cancel-key",
                reason=None,
            ),
        )
        assert await _stored(sessions, rollback_run) == ("running", None)
        async with sessions.begin() as session:
            await session.execute(
                text("DROP TRIGGER reject_test_cancellation ON workflow_runs")
            )
            await session.execute(text("DROP FUNCTION reject_test_cancellation()"))

        immutable_run = retry_run
        async with sessions.begin() as session:
            with pytest.raises(DBAPIError):
                await session.execute(
                    delete(workflow_run_cancellation_requests).where(
                        workflow_run_cancellation_requests.c.workflow_run_id
                        == immutable_run
                    )
                )
        assert await _stored(sessions, immutable_run) == (before_status, before_request)
    finally:
        await engine.dispose()


def test_workflow_run_cancellation_is_atomic_scoped_and_concurrent() -> None:
    with temporary_database(
        "TASKFORGE_WORKFLOW_PERSISTENCE_TEST_DATABASE_URL",
        "taskforge_run_cancellation",
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        configuration = Config("alembic.ini")
        with migration_database_url(alembic_url):
            command.upgrade(configuration, "head")
        asyncio.run(_verify_cancellation(database_url))
