"""Reason-preserving read-only observation of task execution authority."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import exists, func, select
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.identity.schema import worker_credentials, worker_identities
from taskforge.runs.domain import TaskRunStatus, WorkflowRunStatus
from taskforge.runs.schema import (
    task_attempt_claims,
    task_attempt_results,
    task_attempts,
    task_result_events,
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
    """Observe cancellation and exact authority reasons in one statement snapshot."""

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
        if claim_generation <= 0:
            raise ValueError("claim generation must be positive")
        try:
            async with self._sessions() as session:
                row = (
                    await session.execute(
                        _observation_statement(
                            authenticated_worker,
                            worker_session_id,
                            workflow_run_id,
                            task_run_id,
                            task_attempt_id,
                            claim_generation,
                        )
                    )
                ).one()
        except DBAPIError as error:
            raise TaskCancellationObservationUnavailable from error
        return _classify(
            row._mapping,
            authenticated_worker,
            worker_session_id,
            workflow_run_id,
            task_run_id,
            claim_generation,
        )


def _scalar(statement: Any) -> Any:
    return statement.scalar_subquery()


def _observation_statement(
    worker: AuthenticatedWorker,
    worker_session_id: UUID,
    workflow_run_id: UUID,
    task_run_id: UUID,
    task_attempt_id: UUID,
    claim_generation: int,
) -> Any:
    return select(
        exists(
            select(worker_identities.c.id).where(
                worker_identities.c.id == worker.worker_identity_id
            )
        ).label("identity_exists"),
        _scalar(
            select(worker_identities.c.disabled_at).where(
                worker_identities.c.id == worker.worker_identity_id
            )
        ).label("identity_disabled_at"),
        exists(
            select(worker_credentials.c.id).where(
                worker_credentials.c.id == worker.credential_id
            )
        ).label("credential_exists"),
        _scalar(
            select(worker_credentials.c.worker_identity_id).where(
                worker_credentials.c.id == worker.credential_id
            )
        ).label("credential_identity_id"),
        _scalar(
            select(worker_credentials.c.revoked_at).where(
                worker_credentials.c.id == worker.credential_id
            )
        ).label("credential_revoked_at"),
        _scalar(
            select(worker_credentials.c.expires_at).where(
                worker_credentials.c.id == worker.credential_id
            )
        ).label("credential_expires_at"),
        exists(
            select(worker_sessions.c.id).where(
                worker_sessions.c.id == worker_session_id
            )
        ).label("session_exists"),
        _scalar(
            select(worker_sessions.c.worker_identity_id).where(
                worker_sessions.c.id == worker_session_id
            )
        ).label("session_identity_id"),
        _scalar(
            select(worker_sessions.c.ended_at).where(
                worker_sessions.c.id == worker_session_id
            )
        ).label("session_ended_at"),
        _scalar(
            select(workflow_runs.c.status).where(workflow_runs.c.id == workflow_run_id)
        ).label("workflow_status"),
        _scalar(
            select(task_runs.c.workflow_run_id).where(task_runs.c.id == task_run_id)
        ).label("task_workflow_run_id"),
        _scalar(select(task_runs.c.status).where(task_runs.c.id == task_run_id)).label(
            "task_status"
        ),
        _scalar(
            select(task_attempts.c.task_run_id).where(
                task_attempts.c.id == task_attempt_id
            )
        ).label("attempt_task_run_id"),
        _scalar(
            select(task_attempts.c.attempt_number).where(
                task_attempts.c.id == task_attempt_id
            )
        ).label("attempt_number"),
        _scalar(
            select(func.max(task_attempts.c.attempt_number)).where(
                task_attempts.c.task_run_id == task_run_id
            )
        ).label("latest_attempt_number"),
        exists(
            select(task_attempt_claims.c.task_attempt_id).where(
                task_attempt_claims.c.task_attempt_id == task_attempt_id,
                task_attempt_claims.c.generation == claim_generation,
            )
        ).label("claim_exists"),
        _scalar(
            select(task_attempt_claims.c.worker_session_id).where(
                task_attempt_claims.c.task_attempt_id == task_attempt_id,
                task_attempt_claims.c.generation == claim_generation,
            )
        ).label("claim_session_id"),
        _scalar(
            select(task_attempt_claims.c.lease_expires_at).where(
                task_attempt_claims.c.task_attempt_id == task_attempt_id,
                task_attempt_claims.c.generation == claim_generation,
            )
        ).label("lease_expires_at"),
        _scalar(
            select(task_attempt_claims.c.terminated_at).where(
                task_attempt_claims.c.task_attempt_id == task_attempt_id,
                task_attempt_claims.c.generation == claim_generation,
            )
        ).label("claim_terminated_at"),
        _scalar(
            select(func.max(task_attempt_claims.c.generation)).where(
                task_attempt_claims.c.task_attempt_id == task_attempt_id,
                task_attempt_claims.c.terminated_at.is_(None),
            )
        ).label("open_claim_generation"),
        _scalar(
            select(task_attempt_results.c.claim_generation).where(
                task_attempt_results.c.task_attempt_id == task_attempt_id
            )
        ).label("result_generation"),
        exists(
            select(task_result_events.c.id).where(
                task_result_events.c.task_attempt_id == task_attempt_id,
                task_result_events.c.claim_generation == claim_generation,
                task_result_events.c.event_type == "result_recovered",
            )
        ).label("recovered"),
        _scalar(
            select(workflow_run_cancellation_requests.c.requested_at).where(
                workflow_run_cancellation_requests.c.workflow_run_id == workflow_run_id
            )
        ).label("cancellation_requested_at"),
        func.statement_timestamp().label("observed_at"),
    )


def _classify(
    values: Any,
    worker: AuthenticatedWorker,
    worker_session_id: UUID,
    workflow_run_id: UUID,
    task_run_id: UUID,
    claim_generation: int,
) -> TaskCancellationObservation:
    observed_at = values["observed_at"]
    if (
        not values["identity_exists"]
        or values["identity_disabled_at"] is not None
        or not values["credential_exists"]
        or values["credential_identity_id"] != worker.worker_identity_id
        or values["credential_revoked_at"] is not None
        or (
            values["credential_expires_at"] is not None
            and values["credential_expires_at"] <= observed_at
        )
    ):
        return TaskCancellationObservation(
            TaskCancellationObservationOutcome.WORKER_AUTHORITY_REJECTED
        )
    if (
        not values["session_exists"]
        or values["session_identity_id"] != worker.worker_identity_id
        or values["session_ended_at"] is not None
    ):
        return TaskCancellationObservation(
            TaskCancellationObservationOutcome.WORKER_SESSION_INACTIVE
        )
    if (
        values["workflow_status"] is None
        or values["task_workflow_run_id"] != workflow_run_id
        or values["task_status"] is None
        or values["attempt_task_run_id"] != task_run_id
        or values["attempt_number"] is None
        or values["latest_attempt_number"] is None
        or not values["claim_exists"]
        or values["claim_session_id"] != worker_session_id
        or values["lease_expires_at"] is None
    ):
        raise TaskCancellationObservationInvariantError
    try:
        workflow_status = WorkflowRunStatus(values["workflow_status"])
        task_status = TaskRunStatus(values["task_status"])
    except (TypeError, ValueError) as error:
        raise TaskCancellationObservationInvariantError from error
    terminated = values["claim_terminated_at"] is not None
    result_generation = values["result_generation"]
    if values["recovered"]:
        if not terminated or result_generation != claim_generation:
            raise TaskCancellationObservationInvariantError
        return TaskCancellationObservation(
            TaskCancellationObservationOutcome.CLAIM_RECOVERED
        )
    newer_attempt = values["latest_attempt_number"] > values["attempt_number"]
    newer_generation = (
        values["open_claim_generation"] is not None
        and values["open_claim_generation"] != claim_generation
    )
    if newer_attempt or newer_generation:
        if not terminated:
            raise TaskCancellationObservationInvariantError
        return TaskCancellationObservation(
            TaskCancellationObservationOutcome.ATTEMPT_OR_GENERATION_OBSOLETE
        )
    active_task = task_status in (TaskRunStatus.CLAIMED, TaskRunStatus.RUNNING)
    if not active_task or workflow_status in (
        WorkflowRunStatus.SUCCEEDED,
        WorkflowRunStatus.FAILED,
        WorkflowRunStatus.CANCELLED,
    ):
        if not terminated or result_generation != claim_generation:
            raise TaskCancellationObservationInvariantError
        return TaskCancellationObservation(
            TaskCancellationObservationOutcome.TASK_INACTIVE
        )
    if terminated:
        raise TaskCancellationObservationInvariantError
    if values["lease_expires_at"] <= observed_at:
        return TaskCancellationObservation(
            TaskCancellationObservationOutcome.CLAIM_EXPIRED_AWAITING_RECOVERY
        )
    if workflow_status is WorkflowRunStatus.CANCELLING:
        requested_at = values["cancellation_requested_at"]
        if not isinstance(requested_at, datetime):
            raise TaskCancellationObservationInvariantError
        return TaskCancellationObservation(
            TaskCancellationObservationOutcome.CANCELLATION_REQUESTED,
            requested_at,
        )
    if workflow_status not in (WorkflowRunStatus.PENDING, WorkflowRunStatus.RUNNING):
        raise TaskCancellationObservationInvariantError
    return TaskCancellationObservation(TaskCancellationObservationOutcome.ACTIVE)
