"""Atomic PostgreSQL task-attempt claim acquisition."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy import exists, func, insert, or_, select, update
from sqlalchemy.engine import Row
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.claims.domain import (
    TaskClaimLease,
    TaskClaimOutcome,
    TaskClaimRenewalOutcome,
    TaskClaimRenewalRequest,
    TaskClaimRenewalResult,
    TaskClaimResult,
)
from taskforge.claims.persistence_ports import (
    TaskClaimAlreadyOwned,
    TaskClaimAttemptStale,
    TaskClaimAuthorityRejected,
    TaskClaimCapabilityMismatch,
    TaskClaimDispatchRejected,
    TaskClaimInvariantViolation,
    TaskClaimNotEligible,
    TaskClaimPersistenceUnavailable,
    TaskClaimRenewalExpired,
    TaskClaimRenewalStale,
    TaskClaimRenewalTaskInactive,
    TaskClaimSessionInactive,
    TaskClaimSessionUnavailable,
    TaskClaimWorkerUnavailable,
)
from taskforge.dispatch.envelope import (
    DispatchEnvelope,
    dispatch_envelope_to_mapping,
)
from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.identity.schema import worker_credentials, worker_identities
from taskforge.runs.domain import TaskRunStatus
from taskforge.runs.schema import (
    task_attempt_claims,
    task_attempts,
    task_dispatch_outbox,
    task_runs,
)
from taskforge.worker.health import worker_session_is_healthy
from taskforge.worker.schema import (
    worker_session_capabilities,
    worker_session_health,
    worker_sessions,
)
from taskforge.workflows.schema import workflow_version_steps

_CLAIM_REJECTIONS = (
    TaskClaimAlreadyOwned,
    TaskClaimAttemptStale,
    TaskClaimAuthorityRejected,
    TaskClaimCapabilityMismatch,
    TaskClaimDispatchRejected,
    TaskClaimInvariantViolation,
    TaskClaimNotEligible,
    TaskClaimSessionInactive,
    TaskClaimSessionUnavailable,
    TaskClaimWorkerUnavailable,
    TaskClaimRenewalExpired,
    TaskClaimRenewalStale,
    TaskClaimRenewalTaskInactive,
)


class SQLAlchemyTaskClaimRepository:
    """Validate and acquire one authoritative claim in one transaction."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        worker_stale_after_seconds: int,
    ) -> None:
        if worker_stale_after_seconds <= 0:
            raise ValueError("worker stale threshold must be positive")
        self._sessions = sessions
        self._worker_stale_after_seconds = worker_stale_after_seconds

    async def acquire_claim(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        dispatch: DispatchEnvelope,
        *,
        lease_seconds: int,
    ) -> TaskClaimResult:
        if lease_seconds <= 0:
            raise ValueError("claim lease duration must be positive")
        try:
            async with self._sessions.begin() as session:
                await _lock_authority(session, authenticated_worker)
                await _lock_active_session(
                    session, authenticated_worker.worker_identity_id, worker_session_id
                )
                task = (
                    await session.execute(
                        select(
                            task_runs.c.id,
                            task_runs.c.workflow_run_id,
                            task_runs.c.workflow_version_id,
                            task_runs.c.step_identifier,
                            task_runs.c.status,
                        )
                        .where(
                            task_runs.c.id == dispatch.task_run_id,
                            task_runs.c.workflow_run_id == dispatch.workflow_run_id,
                        )
                        .with_for_update()
                    )
                ).one_or_none()
                if task is None:
                    raise TaskClaimDispatchRejected

                durable = (
                    await session.execute(
                        select(
                            task_dispatch_outbox.c.route,
                            task_dispatch_outbox.c.payload,
                            task_attempts.c.attempt_number,
                            workflow_version_steps.c.task_type,
                        )
                        .select_from(
                            task_dispatch_outbox.join(
                                task_attempts,
                                task_attempts.c.id
                                == task_dispatch_outbox.c.task_attempt_id,
                            )
                            .join(
                                task_runs,
                                task_runs.c.id == task_attempts.c.task_run_id,
                            )
                            .join(
                                workflow_version_steps,
                                (
                                    workflow_version_steps.c.workflow_version_id
                                    == task_runs.c.workflow_version_id
                                )
                                & (
                                    workflow_version_steps.c.step_identifier
                                    == task_runs.c.step_identifier
                                ),
                            )
                        )
                        .where(
                            task_dispatch_outbox.c.id == dispatch.dispatch_id,
                            task_dispatch_outbox.c.task_attempt_id
                            == dispatch.task_attempt_id,
                            task_attempts.c.task_run_id == dispatch.task_run_id,
                        )
                    )
                ).one_or_none()
                if durable is None or not _dispatch_matches(dispatch, durable):
                    raise TaskClaimDispatchRejected

                latest_attempt = await session.scalar(
                    select(func.max(task_attempts.c.attempt_number)).where(
                        task_attempts.c.task_run_id == dispatch.task_run_id
                    )
                )
                if latest_attempt != dispatch.attempt_number:
                    raise TaskClaimAttemptStale

                current = (
                    await session.execute(
                        select(
                            task_attempt_claims.c.task_attempt_id,
                            task_attempt_claims.c.generation,
                            task_attempt_claims.c.worker_session_id,
                            task_attempt_claims.c.acquired_at,
                            task_attempt_claims.c.lease_expires_at,
                        ).where(
                            task_attempt_claims.c.task_attempt_id
                            == dispatch.task_attempt_id,
                            task_attempt_claims.c.terminated_at.is_(None),
                        )
                    )
                ).one_or_none()
                if current is not None:
                    if current.worker_session_id != worker_session_id:
                        raise TaskClaimAlreadyOwned
                    if task.status != TaskRunStatus.CLAIMED.value:
                        raise TaskClaimInvariantViolation
                    expired = await session.scalar(
                        select(current.lease_expires_at <= func.statement_timestamp())
                    )
                    return _claim_result(
                        current,
                        TaskClaimOutcome.REPLAYED_EXPIRED
                        if expired
                        else TaskClaimOutcome.REPLAYED_ACTIVE,
                    )
                if task.status == TaskRunStatus.CLAIMED.value:
                    raise TaskClaimInvariantViolation
                if task.status != TaskRunStatus.DISPATCHED.value:
                    raise TaskClaimNotEligible

                health = (
                    await session.execute(
                        select(
                            worker_session_health.c.accepting_work,
                            worker_session_is_healthy(
                                worker_session_health.c.last_seen_at,
                                stale_after_seconds=self._worker_stale_after_seconds,
                            ).label("healthy"),
                        )
                        .where(
                            worker_session_health.c.worker_session_id
                            == worker_session_id
                        )
                        .with_for_update(read=True)
                    )
                ).one_or_none()
                if health is None or not health.accepting_work or not health.healthy:
                    raise TaskClaimWorkerUnavailable

                required_capability = durable.payload["required_capability"]
                capability = await session.scalar(
                    select(worker_session_capabilities.c.capability).where(
                        worker_session_capabilities.c.worker_session_id
                        == worker_session_id,
                        worker_session_capabilities.c.capability == required_capability,
                    )
                )
                if capability is None:
                    raise TaskClaimCapabilityMismatch

                generation = await session.scalar(
                    select(
                        func.coalesce(func.max(task_attempt_claims.c.generation), 0) + 1
                    ).where(
                        task_attempt_claims.c.task_attempt_id
                        == dispatch.task_attempt_id
                    )
                )
                if not isinstance(generation, int) or generation <= 0:
                    raise TaskClaimInvariantViolation
                inserted = (
                    await session.execute(
                        insert(task_attempt_claims)
                        .values(
                            task_attempt_id=dispatch.task_attempt_id,
                            generation=generation,
                            worker_session_id=worker_session_id,
                            lease_expires_at=func.statement_timestamp()
                            + func.make_interval(0, 0, 0, 0, 0, 0, lease_seconds),
                        )
                        .returning(
                            task_attempt_claims.c.task_attempt_id,
                            task_attempt_claims.c.generation,
                            task_attempt_claims.c.worker_session_id,
                            task_attempt_claims.c.acquired_at,
                            task_attempt_claims.c.lease_expires_at,
                        )
                    )
                ).one()
                transitioned = (
                    await session.execute(
                        update(task_runs)
                        .where(
                            task_runs.c.id == dispatch.task_run_id,
                            task_runs.c.status == TaskRunStatus.DISPATCHED.value,
                        )
                        .values(
                            status=TaskRunStatus.CLAIMED.value,
                            updated_at=func.current_timestamp(),
                        )
                        .returning(task_runs.c.id)
                    )
                ).one_or_none()
                if transitioned is None:
                    raise TaskClaimInvariantViolation
                return _claim_result(inserted, TaskClaimOutcome.ACQUIRED_ACTIVE)
        except _CLAIM_REJECTIONS:
            raise
        except IntegrityError as error:
            raise TaskClaimInvariantViolation from error
        except DBAPIError as error:
            raise TaskClaimPersistenceUnavailable from error

    async def renew_claim(
        self,
        authenticated_worker: AuthenticatedWorker,
        request: TaskClaimRenewalRequest,
        *,
        lease_seconds: int,
    ) -> TaskClaimRenewalResult:
        if lease_seconds <= 0:
            raise ValueError("claim lease duration must be positive")
        try:
            async with self._sessions.begin() as session:
                await _lock_authority(session, authenticated_worker)
                await _lock_active_session(
                    session,
                    authenticated_worker.worker_identity_id,
                    request.worker_session_id,
                )
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
                        .where(task_attempts.c.id == request.task_attempt_id)
                        .with_for_update(of=task_runs)
                    )
                ).one_or_none()
                if task is None:
                    raise TaskClaimRenewalStale
                if task.status not in (
                    TaskRunStatus.CLAIMED.value,
                    TaskRunStatus.RUNNING.value,
                ):
                    raise TaskClaimRenewalTaskInactive

                latest_attempt = await session.scalar(
                    select(func.max(task_attempts.c.attempt_number)).where(
                        task_attempts.c.task_run_id == task.id
                    )
                )
                if latest_attempt != task.attempt_number:
                    raise TaskClaimRenewalStale

                current = (
                    await session.execute(
                        select(
                            task_attempt_claims.c.task_attempt_id,
                            task_attempt_claims.c.generation,
                            task_attempt_claims.c.worker_session_id,
                            task_attempt_claims.c.acquired_at,
                            task_attempt_claims.c.lease_expires_at,
                        )
                        .where(
                            task_attempt_claims.c.task_attempt_id
                            == request.task_attempt_id,
                            task_attempt_claims.c.terminated_at.is_(None),
                        )
                        .with_for_update()
                    )
                ).one_or_none()
                if (
                    current is None
                    or current.generation != request.generation
                    or current.worker_session_id != request.worker_session_id
                ):
                    raise TaskClaimRenewalStale

                timing = (
                    await session.execute(
                        select(
                            func.statement_timestamp().label("reference_time"),
                            (
                                func.statement_timestamp()
                                + func.make_interval(0, 0, 0, 0, 0, 0, lease_seconds)
                            ).label("candidate_expiry"),
                        )
                    )
                ).one()
                if current.lease_expires_at <= timing.reference_time:
                    raise TaskClaimRenewalExpired

                expected = request.expected_lease_expires_at
                if expected < current.lease_expires_at:
                    return _renewal_result(current, TaskClaimRenewalOutcome.REPLAYED)
                if expected > current.lease_expires_at:
                    raise TaskClaimRenewalStale
                if current.lease_expires_at >= timing.candidate_expiry:
                    return _renewal_result(
                        current, TaskClaimRenewalOutcome.ACTIVE_UNCHANGED
                    )

                active_task_exists = exists(
                    select(task_attempts.c.id)
                    .select_from(
                        task_attempts.join(
                            task_runs,
                            task_runs.c.id == task_attempts.c.task_run_id,
                        )
                    )
                    .where(
                        task_attempts.c.id == request.task_attempt_id,
                        task_runs.c.status.in_(
                            (
                                TaskRunStatus.CLAIMED.value,
                                TaskRunStatus.RUNNING.value,
                            )
                        ),
                    )
                )
                renewed = (
                    await session.execute(
                        update(task_attempt_claims)
                        .where(
                            task_attempt_claims.c.task_attempt_id
                            == request.task_attempt_id,
                            task_attempt_claims.c.generation == request.generation,
                            task_attempt_claims.c.worker_session_id
                            == request.worker_session_id,
                            task_attempt_claims.c.terminated_at.is_(None),
                            task_attempt_claims.c.lease_expires_at == expected,
                            task_attempt_claims.c.lease_expires_at
                            > func.statement_timestamp(),
                            active_task_exists,
                        )
                        .values(
                            lease_expires_at=func.greatest(
                                task_attempt_claims.c.lease_expires_at,
                                func.statement_timestamp()
                                + func.make_interval(0, 0, 0, 0, 0, 0, lease_seconds),
                            )
                        )
                        .returning(
                            task_attempt_claims.c.task_attempt_id,
                            task_attempt_claims.c.generation,
                            task_attempt_claims.c.worker_session_id,
                            task_attempt_claims.c.acquired_at,
                            task_attempt_claims.c.lease_expires_at,
                        )
                    )
                ).one_or_none()
                if renewed is None:
                    expired = await session.scalar(
                        select(current.lease_expires_at <= func.statement_timestamp())
                    )
                    if expired:
                        raise TaskClaimRenewalExpired
                    raise TaskClaimInvariantViolation
                return _renewal_result(renewed, TaskClaimRenewalOutcome.RENEWED)
        except _CLAIM_REJECTIONS:
            raise
        except IntegrityError as error:
            raise TaskClaimInvariantViolation from error
        except DBAPIError as error:
            raise TaskClaimPersistenceUnavailable from error


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
        raise TaskClaimAuthorityRejected
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
        raise TaskClaimAuthorityRejected


async def _lock_active_session(
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
    if worker_session is None:
        raise TaskClaimSessionUnavailable
    if worker_session.ended_at is not None:
        raise TaskClaimSessionInactive


def _dispatch_matches(dispatch: DispatchEnvelope, durable: Row[Any]) -> bool:
    return bool(
        durable.attempt_number == dispatch.attempt_number
        and durable.task_type == dispatch.task_type
        and durable.route == dispatch.route
        and durable.payload == dispatch_envelope_to_mapping(dispatch)
    )


def _claim_result(row: Row[Any], outcome: TaskClaimOutcome) -> TaskClaimResult:
    return TaskClaimResult(
        outcome,
        TaskClaimLease(
            row.task_attempt_id,
            row.generation,
            row.worker_session_id,
            row.acquired_at,
            row.lease_expires_at,
        ),
    )


def _renewal_result(
    row: Row[Any], outcome: TaskClaimRenewalOutcome
) -> TaskClaimRenewalResult:
    return TaskClaimRenewalResult(
        outcome,
        TaskClaimLease(
            row.task_attempt_id,
            row.generation,
            row.worker_session_id,
            row.acquired_at,
            row.lease_expires_at,
        ),
    )
