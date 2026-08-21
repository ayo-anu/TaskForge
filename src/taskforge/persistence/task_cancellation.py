"""Read-only PostgreSQL observation of authoritative task cancellation."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import func, or_, select
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.identity.schema import worker_credentials
from taskforge.runs.domain import TaskRunStatus, WorkflowRunStatus
from taskforge.runs.schema import (
    task_attempt_claims,
    task_attempts,
    task_runs,
    workflow_run_cancellation_requests,
    workflow_runs,
)
from taskforge.worker.cancellation import (
    TaskCancellationObservation,
    TaskCancellationObservationInvariantError,
    TaskCancellationObservationOutcome,
    TaskCancellationObservationUnavailable,
)
from taskforge.worker.schema import worker_sessions


class SQLAlchemyTaskCancellationObserver:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def observe_cancellation(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        workflow_run_id: UUID,
        task_run_id: UUID,
        task_attempt_id: UUID,
        claim_generation: int,
    ) -> TaskCancellationObservation:
        try:
            async with self._sessions() as session:
                row = (
                    await session.execute(
                        select(
                            workflow_runs.c.status.label("workflow_status"),
                            task_runs.c.status.label("task_status"),
                            workflow_run_cancellation_requests.c.requested_at,
                        )
                        .select_from(
                            task_attempt_claims.join(
                                task_attempts,
                                task_attempts.c.id
                                == task_attempt_claims.c.task_attempt_id,
                            )
                            .join(
                                task_runs,
                                task_runs.c.id == task_attempts.c.task_run_id,
                            )
                            .join(
                                workflow_runs,
                                workflow_runs.c.id == task_runs.c.workflow_run_id,
                            )
                            .join(
                                worker_sessions,
                                worker_sessions.c.id
                                == task_attempt_claims.c.worker_session_id,
                            )
                            .join(
                                worker_credentials,
                                worker_credentials.c.worker_identity_id
                                == worker_sessions.c.worker_identity_id,
                            )
                            .outerjoin(
                                workflow_run_cancellation_requests,
                                workflow_run_cancellation_requests.c.workflow_run_id
                                == workflow_runs.c.id,
                            )
                        )
                        .where(
                            workflow_runs.c.id == workflow_run_id,
                            task_runs.c.id == task_run_id,
                            task_attempts.c.id == task_attempt_id,
                            task_attempt_claims.c.generation == claim_generation,
                            task_attempt_claims.c.worker_session_id
                            == worker_session_id,
                            task_attempt_claims.c.terminated_at.is_(None),
                            worker_sessions.c.worker_identity_id
                            == authenticated_worker.worker_identity_id,
                            worker_sessions.c.ended_at.is_(None),
                            worker_credentials.c.id
                            == authenticated_worker.credential_id,
                            worker_credentials.c.revoked_at.is_(None),
                            or_(
                                worker_credentials.c.expires_at.is_(None),
                                worker_credentials.c.expires_at
                                > func.statement_timestamp(),
                            ),
                        )
                    )
                ).one_or_none()
        except DBAPIError as error:
            raise TaskCancellationObservationUnavailable from error
        if row is None:
            return TaskCancellationObservation(
                TaskCancellationObservationOutcome.NO_LONGER_AUTHORITATIVE
            )
        try:
            workflow_status = WorkflowRunStatus(row.workflow_status)
            task_status = TaskRunStatus(row.task_status)
        except ValueError as error:
            raise TaskCancellationObservationInvariantError from error
        if workflow_status is WorkflowRunStatus.CANCELLING:
            if row.requested_at is None:
                raise TaskCancellationObservationInvariantError
            return TaskCancellationObservation(
                TaskCancellationObservationOutcome.CANCELLATION_REQUESTED,
                row.requested_at,
            )
        if workflow_status in (WorkflowRunStatus.PENDING, WorkflowRunStatus.RUNNING):
            if task_status is not TaskRunStatus.RUNNING:
                return TaskCancellationObservation(
                    TaskCancellationObservationOutcome.NO_LONGER_AUTHORITATIVE
                )
            return TaskCancellationObservation(
                TaskCancellationObservationOutcome.ACTIVE
            )
        return TaskCancellationObservation(
            TaskCancellationObservationOutcome.NO_LONGER_AUTHORITATIVE
        )
