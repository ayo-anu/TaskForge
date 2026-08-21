"""Atomic PostgreSQL task start acknowledgement."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import func, or_, select, update
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.identity.schema import worker_credentials, worker_identities
from taskforge.persistence.execution_events import append_status_changed_execution_event
from taskforge.runs.domain import TaskRunStatus, WorkflowRunStatus
from taskforge.runs.persistence_ports import (
    WorkflowRunExecutionEventInvariantViolation,
    WorkflowRunExecutionEventPersistenceUnavailable,
)
from taskforge.runs.schema import (
    task_attempt_claims,
    task_attempts,
    task_runs,
    workflow_runs,
)
from taskforge.worker.schema import worker_sessions
from taskforge.worker.start_persistence_ports import (
    PersistedTaskStart,
    TaskStartAuthorityRejected,
    TaskStartClaimStale,
    TaskStartInvariantViolation,
    TaskStartPersistenceUnavailable,
    TaskStartSessionRejected,
)

_START_REJECTIONS = (
    TaskStartAuthorityRejected,
    TaskStartSessionRejected,
    TaskStartClaimStale,
    TaskStartInvariantViolation,
)


class SQLAlchemyTaskStartRepository:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def start_task(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        task_run_id: UUID,
        task_attempt_id: UUID,
        claim_generation: int,
    ) -> PersistedTaskStart:
        if claim_generation <= 0:
            raise ValueError("claim generation must be positive")
        try:
            async with self._sessions.begin() as session:
                await _lock_authority(session, authenticated_worker)
                await _lock_session(
                    session, authenticated_worker.worker_identity_id, worker_session_id
                )
                workflow = (
                    await session.execute(
                        select(workflow_runs.c.id, workflow_runs.c.status)
                        .select_from(
                            workflow_runs.join(
                                task_runs,
                                task_runs.c.workflow_run_id == workflow_runs.c.id,
                            ).join(
                                task_attempts,
                                task_attempts.c.task_run_id == task_runs.c.id,
                            )
                        )
                        .where(
                            task_runs.c.id == task_run_id,
                            task_attempts.c.id == task_attempt_id,
                        )
                        .with_for_update(of=workflow_runs)
                    )
                ).one_or_none()
                if workflow is None:
                    raise TaskStartInvariantViolation
                task = (
                    await session.execute(
                        select(
                            task_runs.c.id,
                            task_runs.c.status,
                            task_attempts.c.attempt_number,
                        )
                        .select_from(
                            task_attempts.join(
                                task_runs,
                                task_runs.c.id == task_attempts.c.task_run_id,
                            )
                        )
                        .where(
                            task_runs.c.id == task_run_id,
                            task_attempts.c.id == task_attempt_id,
                        )
                        .with_for_update(of=task_runs)
                    )
                ).one_or_none()
                if task is None:
                    raise TaskStartInvariantViolation
                workflow_status = WorkflowRunStatus(workflow.status)
                if workflow_status in (
                    WorkflowRunStatus.SUCCEEDED,
                    WorkflowRunStatus.FAILED,
                    WorkflowRunStatus.CANCELLED,
                ):
                    raise TaskStartInvariantViolation

                latest_attempt = await session.scalar(
                    select(func.max(task_attempts.c.attempt_number)).where(
                        task_attempts.c.task_run_id == task_run_id
                    )
                )
                if latest_attempt != task.attempt_number:
                    raise TaskStartClaimStale

                claim = (
                    await session.execute(
                        select(
                            task_attempt_claims.c.task_attempt_id,
                            task_attempt_claims.c.generation,
                            task_attempt_claims.c.worker_session_id,
                            task_attempt_claims.c.lease_expires_at,
                        )
                        .where(
                            task_attempt_claims.c.task_attempt_id == task_attempt_id,
                            task_attempt_claims.c.terminated_at.is_(None),
                        )
                        .with_for_update()
                    )
                ).one_or_none()
                if claim is None:
                    raise TaskStartInvariantViolation
                if (
                    claim.generation != claim_generation
                    or claim.worker_session_id != worker_session_id
                ):
                    raise TaskStartClaimStale
                expired = await session.scalar(
                    select(claim.lease_expires_at <= func.statement_timestamp())
                )
                if expired:
                    raise TaskStartClaimStale

                if task.status == TaskRunStatus.RUNNING.value:
                    return PersistedTaskStart(False, workflow_status)
                if task.status != TaskRunStatus.CLAIMED.value:
                    raise TaskStartInvariantViolation
                if workflow_status is WorkflowRunStatus.CANCELLING:
                    raise TaskStartClaimStale
                transitioned = (
                    await session.execute(
                        update(task_runs)
                        .where(
                            task_runs.c.id == task_run_id,
                            task_runs.c.status == TaskRunStatus.CLAIMED.value,
                        )
                        .values(
                            status=TaskRunStatus.RUNNING.value,
                            updated_at=func.current_timestamp(),
                        )
                        .returning(task_runs.c.id)
                    )
                ).one_or_none()
                if transitioned is None:
                    raise TaskStartInvariantViolation
                await append_status_changed_execution_event(
                    session,
                    workflow_run_id=workflow.id,
                    task_run_id=task_run_id,
                    previous_status=TaskRunStatus.CLAIMED,
                    status=TaskRunStatus.RUNNING,
                )
                return PersistedTaskStart(True, workflow_status)
        except _START_REJECTIONS:
            raise
        except WorkflowRunExecutionEventInvariantViolation as error:
            raise TaskStartInvariantViolation from error
        except WorkflowRunExecutionEventPersistenceUnavailable as error:
            raise TaskStartPersistenceUnavailable from error
        except IntegrityError as error:
            raise TaskStartInvariantViolation from error
        except DBAPIError as error:
            raise TaskStartPersistenceUnavailable from error


async def _lock_authority(
    session: AsyncSession, authenticated_worker: AuthenticatedWorker
) -> None:
    identity = (
        await session.execute(
            select(worker_identities.c.id)
            .where(
                worker_identities.c.id == authenticated_worker.worker_identity_id,
                worker_identities.c.disabled_at.is_(None),
            )
            .with_for_update(read=True)
        )
    ).one_or_none()
    if identity is None:
        raise TaskStartAuthorityRejected
    credential = (
        await session.execute(
            select(worker_credentials.c.id)
            .where(
                worker_credentials.c.id == authenticated_worker.credential_id,
                worker_credentials.c.worker_identity_id
                == authenticated_worker.worker_identity_id,
                worker_credentials.c.revoked_at.is_(None),
                or_(
                    worker_credentials.c.expires_at.is_(None),
                    worker_credentials.c.expires_at > func.statement_timestamp(),
                ),
            )
            .with_for_update(read=True)
        )
    ).one_or_none()
    if credential is None:
        raise TaskStartAuthorityRejected


async def _lock_session(
    session: AsyncSession, worker_identity_id: UUID, worker_session_id: UUID
) -> None:
    worker_session = (
        await session.execute(
            select(worker_sessions.c.ended_at)
            .where(
                worker_sessions.c.id == worker_session_id,
                worker_sessions.c.worker_identity_id == worker_identity_id,
            )
            .with_for_update(read=True)
        )
    ).one_or_none()
    if worker_session is None or worker_session.ended_at is not None:
        raise TaskStartSessionRejected
