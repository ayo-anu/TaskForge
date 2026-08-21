"""Atomic PostgreSQL persistence for authoritative task results."""

from __future__ import annotations

from uuid import UUID, uuid4

from sqlalchemy import func, insert, null, or_, select, update
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.dead_letters.domain import DeadLetterReason
from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.identity.schema import worker_credentials, worker_identities
from taskforge.persistence.dead_letters import (
    DeadLetterPersistenceInvariantViolation,
    ensure_dead_letter,
)
from taskforge.runs.domain import TaskRunStatus, WorkflowRunStatus
from taskforge.runs.schema import (
    task_attempt_claims,
    task_attempt_results,
    task_attempts,
    task_dispatch_outbox,
    task_result_events,
    task_runs,
    workflow_runs,
)
from taskforge.worker.result_persistence_ports import (
    PersistableTaskResult,
    PersistedTaskResult,
    PersistedTaskResultOutcome,
    TaskResultPersistenceAuthorityRejected,
    TaskResultPersistenceInvalidState,
    TaskResultPersistenceInvariantViolation,
    TaskResultPersistenceNotFound,
    TaskResultPersistenceUnavailable,
)
from taskforge.worker.results import (
    TaskExecutionResultKind,
)
from taskforge.worker.schema import worker_sessions

_EXPECTED_REJECTIONS = (
    TaskResultPersistenceAuthorityRejected,
    TaskResultPersistenceInvalidState,
    TaskResultPersistenceNotFound,
)


class SQLAlchemyTaskResultRepository:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def submit_result(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        result: PersistableTaskResult,
    ) -> PersistedTaskResult:
        try:
            async with self._sessions.begin() as session:
                await _lock_worker_authority(session, authenticated_worker)
                worker_session = (
                    await session.execute(
                        select(worker_sessions.c.ended_at)
                        .where(
                            worker_sessions.c.id == worker_session_id,
                            worker_sessions.c.worker_identity_id
                            == authenticated_worker.worker_identity_id,
                        )
                        .with_for_update(read=True)
                    )
                ).one_or_none()
                if worker_session is None:
                    raise TaskResultPersistenceAuthorityRejected

                workflow = (
                    await session.execute(
                        select(
                            workflow_runs.c.id,
                            workflow_runs.c.status,
                        )
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
                            task_attempts.c.id == result.task_attempt_id,
                            task_runs.c.id == result.task_run_id,
                        )
                        .with_for_update(of=workflow_runs)
                    )
                ).one_or_none()
                if workflow is None:
                    raise TaskResultPersistenceNotFound
                durable = (
                    await session.execute(
                        select(
                            task_attempts.c.task_run_id,
                            task_attempts.c.attempt_number,
                            task_runs.c.workflow_run_id,
                            task_runs.c.status,
                        )
                        .select_from(
                            task_attempts.join(
                                task_runs,
                                task_runs.c.id == task_attempts.c.task_run_id,
                            )
                        )
                        .where(
                            task_attempts.c.id == result.task_attempt_id,
                            task_runs.c.id == result.task_run_id,
                        )
                        .with_for_update(of=task_runs)
                    )
                ).one_or_none()
                if durable is None:
                    raise TaskResultPersistenceNotFound
                if durable.workflow_run_id != workflow.id:
                    raise TaskResultPersistenceInvariantViolation
                dispatch_attempt_id = await session.scalar(
                    select(task_dispatch_outbox.c.task_attempt_id).where(
                        task_dispatch_outbox.c.id == result.dispatch_id
                    )
                )
                if dispatch_attempt_id != result.task_attempt_id:
                    raise TaskResultPersistenceNotFound

                claim = (
                    await session.execute(
                        select(
                            task_attempt_claims.c.generation,
                            task_attempt_claims.c.worker_session_id,
                            task_attempt_claims.c.lease_expires_at,
                            task_attempt_claims.c.terminated_at,
                        )
                        .where(
                            task_attempt_claims.c.task_attempt_id
                            == result.task_attempt_id,
                            task_attempt_claims.c.generation == result.claim_generation,
                        )
                        .with_for_update()
                    )
                ).one_or_none()
                if claim is None:
                    raise TaskResultPersistenceNotFound
                if claim.worker_session_id != worker_session_id:
                    raise TaskResultPersistenceAuthorityRejected

                accepted = (
                    await session.execute(
                        select(
                            task_attempt_results.c.claim_generation,
                            task_attempt_results.c.dispatch_id,
                            task_attempt_results.c.result_kind,
                            task_attempt_results.c.failure_kind,
                            task_attempt_results.c.result_fingerprint,
                        ).where(
                            task_attempt_results.c.task_attempt_id
                            == result.task_attempt_id
                        )
                    )
                ).one_or_none()
                if accepted is not None:
                    recovered_generation = await session.scalar(
                        select(task_result_events.c.id).where(
                            task_result_events.c.task_attempt_id
                            == result.task_attempt_id,
                            task_result_events.c.claim_generation
                            == result.claim_generation,
                            task_result_events.c.event_type == "result_recovered",
                        )
                    )
                    if recovered_generation is not None:
                        if claim.terminated_at is None:
                            raise TaskResultPersistenceInvariantViolation
                        await _append_event(
                            session,
                            worker_session_id,
                            result,
                            "result_stale_rejected",
                        )
                        return PersistedTaskResult(
                            PersistedTaskResultOutcome.STALE_REJECTED,
                            result.task_attempt_id,
                        )
                    if accepted.claim_generation != result.claim_generation:
                        await _append_event(
                            session,
                            worker_session_id,
                            result,
                            "result_stale_rejected",
                        )
                        return PersistedTaskResult(
                            PersistedTaskResultOutcome.STALE_REJECTED,
                            result.task_attempt_id,
                        )
                    identical = (
                        accepted.dispatch_id == result.dispatch_id
                        and accepted.result_fingerprint == result.result_fingerprint
                    )
                    await _append_event(
                        session,
                        worker_session_id,
                        result,
                        (
                            "result_replayed"
                            if identical
                            else "result_conflict_rejected"
                        ),
                    )
                    return PersistedTaskResult(
                        (
                            PersistedTaskResultOutcome.REPLAYED_IDENTICAL
                            if identical
                            else PersistedTaskResultOutcome.CONFLICT_REJECTED
                        ),
                        result.task_attempt_id,
                    )

                latest_attempt = await session.scalar(
                    select(func.max(task_attempts.c.attempt_number)).where(
                        task_attempts.c.task_run_id == result.task_run_id
                    )
                )
                current_generation = await session.scalar(
                    select(task_attempt_claims.c.generation).where(
                        task_attempt_claims.c.task_attempt_id == result.task_attempt_id,
                        task_attempt_claims.c.terminated_at.is_(None),
                    )
                )
                expired = await session.scalar(
                    select(claim.lease_expires_at <= func.statement_timestamp())
                )
                stale = (
                    latest_attempt != durable.attempt_number
                    or current_generation != result.claim_generation
                    or claim.terminated_at is not None
                    or bool(expired)
                )
                if stale:
                    await _append_event(
                        session,
                        worker_session_id,
                        result,
                        "result_stale_rejected",
                    )
                    return PersistedTaskResult(
                        PersistedTaskResultOutcome.STALE_REJECTED,
                        result.task_attempt_id,
                    )
                if worker_session.ended_at is not None:
                    raise TaskResultPersistenceAuthorityRejected
                if durable.status != TaskRunStatus.RUNNING.value:
                    raise TaskResultPersistenceInvalidState
                if workflow.status not in (
                    WorkflowRunStatus.PENDING.value,
                    WorkflowRunStatus.RUNNING.value,
                    WorkflowRunStatus.CANCELLING.value,
                ):
                    raise TaskResultPersistenceInvalidState

                target = _target_task_status(
                    result.result_kind,
                    workflow_status=WorkflowRunStatus(workflow.status),
                )
                await session.execute(
                    insert(task_attempt_results).values(
                        task_attempt_id=result.task_attempt_id,
                        claim_generation=result.claim_generation,
                        dispatch_id=result.dispatch_id,
                        result_kind=result.result_kind.value,
                        failure_kind=(
                            result.failure_kind.value
                            if result.failure_kind is not None
                            else None
                        ),
                        output=(
                            result.output
                            if result.result_kind is TaskExecutionResultKind.SUCCESS
                            else null()
                        ),
                        result_fingerprint=result.result_fingerprint,
                    )
                )
                transitioned = (
                    await session.execute(
                        update(task_runs)
                        .where(
                            task_runs.c.id == result.task_run_id,
                            task_runs.c.status == TaskRunStatus.RUNNING.value,
                        )
                        .values(
                            status=target.value, updated_at=func.current_timestamp()
                        )
                        .returning(task_runs.c.id)
                    )
                ).one_or_none()
                if transitioned is None:
                    raise TaskResultPersistenceInvariantViolation
                terminated = (
                    await session.execute(
                        update(task_attempt_claims)
                        .where(
                            task_attempt_claims.c.task_attempt_id
                            == result.task_attempt_id,
                            task_attempt_claims.c.generation == result.claim_generation,
                            task_attempt_claims.c.worker_session_id
                            == worker_session_id,
                            task_attempt_claims.c.terminated_at.is_(None),
                            task_attempt_claims.c.lease_expires_at
                            > func.statement_timestamp(),
                        )
                        .values(terminated_at=func.statement_timestamp())
                        .returning(task_attempt_claims.c.task_attempt_id)
                    )
                ).one_or_none()
                if terminated is None:
                    raise TaskResultPersistenceInvariantViolation
                await _append_event(
                    session, worker_session_id, result, "result_accepted"
                )
                if result.result_kind is TaskExecutionResultKind.PERMANENT_FAILURE:
                    try:
                        await ensure_dead_letter(
                            session,
                            item_id=uuid4(),
                            task_run_id=result.task_run_id,
                            source_task_attempt_id=result.task_attempt_id,
                            reason=DeadLetterReason.PERMANENT_FAILURE,
                        )
                    except DeadLetterPersistenceInvariantViolation as error:
                        raise TaskResultPersistenceInvariantViolation from error
                return PersistedTaskResult(
                    PersistedTaskResultOutcome.ACCEPTED, result.task_attempt_id
                )
        except _EXPECTED_REJECTIONS:
            raise
        except TaskResultPersistenceInvariantViolation:
            raise
        except IntegrityError as error:
            raise TaskResultPersistenceInvariantViolation from error
        except DBAPIError as error:
            raise TaskResultPersistenceUnavailable from error


async def _lock_worker_authority(
    session: AsyncSession, authenticated_worker: AuthenticatedWorker
) -> None:
    identity = await session.scalar(
        select(worker_identities.c.id)
        .where(
            worker_identities.c.id == authenticated_worker.worker_identity_id,
            worker_identities.c.disabled_at.is_(None),
        )
        .with_for_update(read=True)
    )
    if identity is None:
        raise TaskResultPersistenceAuthorityRejected
    credential = await session.scalar(
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
    if credential is None:
        raise TaskResultPersistenceAuthorityRejected


def _target_task_status(
    result_kind: TaskExecutionResultKind,
    *,
    workflow_status: WorkflowRunStatus,
) -> TaskRunStatus:
    if (
        result_kind is TaskExecutionResultKind.RETRYABLE_FAILURE
        and workflow_status is WorkflowRunStatus.CANCELLING
    ):
        return TaskRunStatus.CANCELLED
    return {
        TaskExecutionResultKind.SUCCESS: TaskRunStatus.SUCCEEDED,
        TaskExecutionResultKind.RETRYABLE_FAILURE: TaskRunStatus.RETRY_PENDING,
        TaskExecutionResultKind.PERMANENT_FAILURE: TaskRunStatus.FAILED,
        TaskExecutionResultKind.CANCELLATION: TaskRunStatus.CANCELLED,
    }[result_kind]


async def _append_event(
    session: AsyncSession,
    worker_session_id: UUID,
    result: PersistableTaskResult,
    event_type: str,
) -> None:
    await session.execute(
        insert(task_result_events).values(
            id=uuid4(),
            task_attempt_id=result.task_attempt_id,
            claim_generation=result.claim_generation,
            worker_session_id=worker_session_id,
            dispatch_id=result.dispatch_id,
            event_type=event_type,
            result_kind=result.result_kind.value,
            failure_kind=(
                result.failure_kind.value if result.failure_kind is not None else None
            ),
            result_fingerprint=result.result_fingerprint,
        )
    )
