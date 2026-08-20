"""PostgreSQL persistence for worker crash discovery and recovery."""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import AbstractAsyncContextManager
from copy import deepcopy
from datetime import datetime
from types import TracebackType
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Integer,
    and_,
    cast,
    func,
    insert,
    literal,
    null,
    or_,
    select,
    union_all,
    update,
)
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.sql import Select

from taskforge.dead_letters.domain import DeadLetterReason
from taskforge.persistence.dead_letters import (
    DeadLetterPersistenceInvariantViolation,
    ensure_dead_letter,
)
from taskforge.recovery.domain import (
    ExpiredClaimCandidate,
    ExpiredClaimCandidatePage,
    ExpiredClaimScanCursor,
    PreparedExpiredClaimRecovery,
    StaleWorkerSessionCandidate,
    StaleWorkerSessionCandidatePage,
    StaleWorkerSessionScanCursor,
)
from taskforge.recovery.persistence_ports import (
    EndedStaleWorkerSession,
    ExpiredClaimRecoveryNoOp,
    ExpiredClaimRecoveryNoOpReason,
    ExpiredClaimRecoveryPersistenceInvariantViolation,
    ExpiredClaimRecoveryPersistenceUnavailable,
    ExpiredClaimRecoveryPreparation,
    RecoveryScanPersistenceInvariantViolation,
    RecoveryScanPersistenceUnavailable,
    StaleWorkerSessionRecoveryNoOpReason,
    StaleWorkerSessionRecoveryPersistenceInvariantViolation,
    StaleWorkerSessionRecoveryPersistenceUnavailable,
    StaleWorkerSessionRecoveryResult,
)
from taskforge.retries.domain import RetryEventType, RetryNotScheduledReason
from taskforge.retries.persistence_ports import NewScheduledRetryAttempt
from taskforge.runs.domain import TaskRunStatus, WorkflowRunStatus
from taskforge.runs.schema import (
    task_attempt_claims,
    task_attempt_results,
    task_attempts,
    task_dispatch_outbox,
    task_result_events,
    task_retry_events,
    task_runs,
    workflow_runs,
)
from taskforge.worker.results import (
    TaskExecutionFailureKind,
    TaskExecutionResultKind,
    task_result_fingerprint,
)
from taskforge.worker.schema import worker_session_health, worker_sessions
from taskforge.workflows.schema import workflow_version_steps, workflow_versions


class SQLAlchemyRecoveryCandidateRepository:
    """Return bounded advisory observations without locking candidate rows."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def scan_expired_claims(
        self, *, limit: int, cursor: ExpiredClaimScanCursor | None
    ) -> ExpiredClaimCandidatePage:
        try:
            async with self._sessions() as session:
                rows = (
                    await session.execute(_expired_claim_statement(limit, cursor))
                ).all()
                observed_at = _page_observation_time(rows)
                items = tuple(
                    ExpiredClaimCandidate(
                        row.task_attempt_id,
                        row.task_run_id,
                        row.workflow_run_id,
                        row.attempt_number,
                        row.generation,
                        row.worker_session_id,
                        row.lease_expires_at,
                        row.observed_at,
                    )
                    for row in rows
                    if row.task_attempt_id is not None
                )
                next_cursor = None
                if rows[0].window_size == limit:
                    next_cursor = ExpiredClaimScanCursor(
                        observed_at,
                        rows[0].cursor_lease_expires_at,
                        rows[0].cursor_task_attempt_id,
                        rows[0].cursor_generation,
                    )
                return ExpiredClaimCandidatePage(items, observed_at, next_cursor)
        except RecoveryScanPersistenceInvariantViolation:
            raise
        except ValueError as error:
            raise RecoveryScanPersistenceInvariantViolation from error
        except DBAPIError as error:
            raise RecoveryScanPersistenceUnavailable from error

    async def scan_stale_worker_sessions(
        self,
        *,
        stale_after_seconds: int,
        limit: int,
        cursor: StaleWorkerSessionScanCursor | None,
    ) -> StaleWorkerSessionCandidatePage:
        try:
            async with self._sessions() as session:
                rows = (
                    await session.execute(
                        _stale_worker_session_statement(
                            stale_after_seconds, limit, cursor
                        )
                    )
                ).all()
                observed_at = _page_observation_time(rows)
                items = tuple(
                    StaleWorkerSessionCandidate(
                        row.worker_session_id,
                        row.worker_identity_id,
                        row.last_sequence,
                        row.last_seen_at,
                        row.accepting_work,
                        row.observed_at,
                    )
                    for row in rows
                    if row.worker_session_id is not None
                )
                next_cursor = None
                if len(items) == limit:
                    last = items[-1]
                    next_cursor = StaleWorkerSessionScanCursor(
                        observed_at,
                        last.last_seen_at,
                        last.worker_session_id,
                        stale_after_seconds,
                    )
                return StaleWorkerSessionCandidatePage(
                    items, observed_at, stale_after_seconds, next_cursor
                )
        except RecoveryScanPersistenceInvariantViolation:
            raise
        except ValueError as error:
            raise RecoveryScanPersistenceInvariantViolation from error
        except DBAPIError as error:
            raise RecoveryScanPersistenceUnavailable from error


class SQLAlchemyExpiredClaimRecoveryRepository:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    def recovery_transaction(self) -> SQLAlchemyExpiredClaimRecoveryTransaction:
        return SQLAlchemyExpiredClaimRecoveryTransaction(self._sessions)


class SQLAlchemyStaleWorkerSessionRecoveryRepository:
    """End one still-stale session after locking current heartbeat authority."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def end_stale_session(
        self,
        candidate: StaleWorkerSessionCandidate,
        *,
        stale_after_seconds: int,
    ) -> StaleWorkerSessionRecoveryResult:
        try:
            async with self._sessions.begin() as session:
                worker_session = (
                    await session.execute(
                        select(
                            worker_sessions.c.id,
                            worker_sessions.c.worker_identity_id,
                            worker_sessions.c.ended_at,
                        )
                        .where(
                            worker_sessions.c.id == candidate.worker_session_id,
                            worker_sessions.c.worker_identity_id
                            == candidate.worker_identity_id,
                        )
                        .with_for_update()
                    )
                ).one_or_none()
                if worker_session is None:
                    raise StaleWorkerSessionRecoveryPersistenceInvariantViolation
                if worker_session.ended_at is not None:
                    return StaleWorkerSessionRecoveryNoOpReason.SESSION_ALREADY_ENDED

                health = (
                    await session.execute(
                        select(
                            worker_session_health.c.last_sequence,
                            worker_session_health.c.last_seen_at,
                            worker_session_health.c.accepting_work,
                        )
                        .where(
                            worker_session_health.c.worker_session_id
                            == candidate.worker_session_id
                        )
                        .with_for_update()
                    )
                ).one_or_none()
                if health is None:
                    raise StaleWorkerSessionRecoveryPersistenceInvariantViolation
                if (
                    health.last_sequence != candidate.last_sequence
                    or health.last_seen_at != candidate.last_seen_at
                    or health.accepting_work != candidate.accepting_work
                ):
                    return StaleWorkerSessionRecoveryNoOpReason.CANDIDATE_REFRESHED

                ended_at = await session.scalar(select(func.statement_timestamp()))
                if not isinstance(ended_at, datetime):
                    raise StaleWorkerSessionRecoveryPersistenceInvariantViolation
                stale_before = await session.scalar(
                    select(
                        ended_at
                        - func.make_interval(0, 0, 0, 0, 0, 0, stale_after_seconds)
                    )
                )
                if not isinstance(stale_before, datetime):
                    raise StaleWorkerSessionRecoveryPersistenceInvariantViolation
                if health.last_seen_at > stale_before:
                    return StaleWorkerSessionRecoveryNoOpReason.CANDIDATE_REFRESHED

                ended = (
                    await session.execute(
                        update(worker_sessions)
                        .where(
                            worker_sessions.c.id == candidate.worker_session_id,
                            worker_sessions.c.worker_identity_id
                            == candidate.worker_identity_id,
                            worker_sessions.c.ended_at.is_(None),
                        )
                        .values(ended_at=ended_at)
                        .returning(worker_sessions.c.ended_at)
                    )
                ).one_or_none()
                if ended is None or ended.ended_at != ended_at:
                    raise StaleWorkerSessionRecoveryPersistenceInvariantViolation
                return EndedStaleWorkerSession(ended_at)
        except StaleWorkerSessionRecoveryPersistenceInvariantViolation:
            raise
        except IntegrityError as error:
            raise StaleWorkerSessionRecoveryPersistenceInvariantViolation from error
        except DBAPIError as error:
            raise StaleWorkerSessionRecoveryPersistenceUnavailable from error


class SQLAlchemyExpiredClaimRecoveryTransaction:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions
        self._context: AbstractAsyncContextManager[AsyncSession] | None = None
        self._session: AsyncSession | None = None

    async def __aenter__(self) -> SQLAlchemyExpiredClaimRecoveryTransaction:
        context = self._sessions.begin()
        self._context = context
        try:
            self._session = await context.__aenter__()
        except DBAPIError as error:
            self._context = None
            raise ExpiredClaimRecoveryPersistenceUnavailable from error
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        context, self._context = self._required_context(), None
        self._session = None
        try:
            await context.__aexit__(exception_type, exception, traceback)
        except IntegrityError as error:
            raise ExpiredClaimRecoveryPersistenceInvariantViolation from error
        except DBAPIError as error:
            raise ExpiredClaimRecoveryPersistenceUnavailable from error

    async def prepare_recovery(
        self, candidate: ExpiredClaimCandidate
    ) -> ExpiredClaimRecoveryPreparation:
        session = self._required_session()
        try:
            workflow = (
                await session.execute(
                    select(workflow_runs.c.id, workflow_runs.c.status)
                    .where(workflow_runs.c.id == candidate.workflow_run_id)
                    .with_for_update()
                )
            ).one_or_none()
            if workflow is None:
                raise ExpiredClaimRecoveryPersistenceInvariantViolation
            task = (
                await session.execute(
                    select(
                        task_runs.c.id,
                        task_runs.c.workflow_run_id,
                        task_runs.c.workflow_version_id,
                        task_runs.c.step_identifier,
                        task_runs.c.status,
                    )
                    .where(task_runs.c.id == candidate.task_run_id)
                    .with_for_update()
                )
            ).one_or_none()
            if task is None or task.workflow_run_id != workflow.id:
                raise ExpiredClaimRecoveryPersistenceInvariantViolation
            claim = (
                await session.execute(
                    select(
                        task_attempt_claims.c.worker_session_id,
                        task_attempt_claims.c.lease_expires_at,
                        task_attempt_claims.c.terminated_at,
                    )
                    .where(
                        task_attempt_claims.c.task_attempt_id
                        == candidate.task_attempt_id,
                        task_attempt_claims.c.generation == candidate.generation,
                    )
                    .with_for_update()
                )
            ).one_or_none()
            if claim is None:
                raise ExpiredClaimRecoveryPersistenceInvariantViolation

            accepted = (
                await session.execute(
                    select(
                        task_attempt_results.c.result_kind,
                        task_attempt_results.c.failure_kind,
                        task_attempt_results.c.claim_generation,
                        task_dispatch_outbox.c.task_attempt_id.label(
                            "dispatch_attempt_id"
                        ),
                    )
                    .select_from(
                        task_attempt_results.join(
                            task_dispatch_outbox,
                            task_dispatch_outbox.c.id
                            == task_attempt_results.c.dispatch_id,
                        )
                    )
                    .where(
                        task_attempt_results.c.task_attempt_id
                        == candidate.task_attempt_id
                    )
                )
            ).one_or_none()
            if accepted is not None:
                if (
                    accepted.result_kind
                    == TaskExecutionResultKind.RETRYABLE_FAILURE.value
                    and accepted.failure_kind
                    == TaskExecutionFailureKind.CLAIM_EXPIRED.value
                    and accepted.claim_generation == candidate.generation
                    and claim.lease_expires_at == candidate.lease_expires_at
                ):
                    if (
                        claim.terminated_at is None
                        or claim.worker_session_id != candidate.worker_session_id
                        or accepted.dispatch_attempt_id != candidate.task_attempt_id
                    ):
                        raise ExpiredClaimRecoveryPersistenceInvariantViolation
                    return ExpiredClaimRecoveryNoOp(
                        ExpiredClaimRecoveryNoOpReason.ALREADY_RECOVERED
                    )
                return ExpiredClaimRecoveryNoOp(
                    ExpiredClaimRecoveryNoOpReason.RESULT_ALREADY_ACCEPTED
                )
            if claim.terminated_at is not None:
                return ExpiredClaimRecoveryNoOp(
                    ExpiredClaimRecoveryNoOpReason.CLAIM_ALREADY_TERMINATED
                )
            if claim.lease_expires_at != candidate.lease_expires_at:
                return ExpiredClaimRecoveryNoOp(
                    ExpiredClaimRecoveryNoOpReason.CANDIDATE_NO_LONGER_EXPIRED
                )
            if claim.worker_session_id != candidate.worker_session_id:
                raise ExpiredClaimRecoveryPersistenceInvariantViolation

            attempt = (
                await session.execute(
                    select(
                        task_attempts.c.task_run_id,
                        task_attempts.c.attempt_number,
                        task_dispatch_outbox.c.id.label("dispatch_id"),
                    )
                    .select_from(
                        task_attempts.join(
                            task_dispatch_outbox,
                            task_dispatch_outbox.c.task_attempt_id
                            == task_attempts.c.id,
                        )
                    )
                    .where(task_attempts.c.id == candidate.task_attempt_id)
                )
            ).one_or_none()
            if (
                attempt is None
                or attempt.task_run_id != candidate.task_run_id
                or attempt.attempt_number != candidate.attempt_number
            ):
                raise ExpiredClaimRecoveryPersistenceInvariantViolation
            latest = await session.scalar(
                select(func.max(task_attempts.c.attempt_number)).where(
                    task_attempts.c.task_run_id == candidate.task_run_id
                )
            )
            if latest != candidate.attempt_number:
                return ExpiredClaimRecoveryNoOp(
                    ExpiredClaimRecoveryNoOpReason.ATTEMPT_NO_LONGER_LATEST
                )
            if workflow.status not in (
                WorkflowRunStatus.PENDING.value,
                WorkflowRunStatus.RUNNING.value,
            ):
                return ExpiredClaimRecoveryNoOp(
                    ExpiredClaimRecoveryNoOpReason.WORKFLOW_NOT_ELIGIBLE
                )
            if task.status not in (
                TaskRunStatus.CLAIMED.value,
                TaskRunStatus.RUNNING.value,
            ):
                return ExpiredClaimRecoveryNoOp(
                    ExpiredClaimRecoveryNoOpReason.TASK_NOT_ELIGIBLE
                )
            recovered_at = await session.scalar(select(func.statement_timestamp()))
            if not isinstance(recovered_at, datetime):
                raise ExpiredClaimRecoveryPersistenceInvariantViolation
            if claim.lease_expires_at > recovered_at:
                return ExpiredClaimRecoveryNoOp(
                    ExpiredClaimRecoveryNoOpReason.CANDIDATE_NO_LONGER_EXPIRED
                )
            snapshot = (
                await session.execute(
                    select(
                        workflow_versions.c.execution_policy.label("workflow_policy"),
                        workflow_version_steps.c.execution_policy.label("step_policy"),
                    )
                    .select_from(
                        workflow_versions.join(
                            workflow_version_steps,
                            workflow_version_steps.c.workflow_version_id
                            == workflow_versions.c.id,
                        )
                    )
                    .where(
                        workflow_versions.c.id == task.workflow_version_id,
                        workflow_version_steps.c.step_identifier
                        == task.step_identifier,
                    )
                )
            ).one_or_none()
            if snapshot is None:
                raise ExpiredClaimRecoveryPersistenceInvariantViolation
            return PreparedExpiredClaimRecovery(
                candidate.task_attempt_id,
                candidate.task_run_id,
                candidate.workflow_run_id,
                candidate.attempt_number,
                candidate.generation,
                claim.worker_session_id,
                attempt.dispatch_id,
                claim.lease_expires_at,
                recovered_at,
                deepcopy(snapshot.workflow_policy),
                deepcopy(snapshot.step_policy),
            )
        except ExpiredClaimRecoveryPersistenceInvariantViolation:
            raise
        except DBAPIError as error:
            raise ExpiredClaimRecoveryPersistenceUnavailable from error

    async def schedule_retry(
        self,
        prepared: PreparedExpiredClaimRecovery,
        attempt: NewScheduledRetryAttempt,
    ) -> None:
        if attempt.attempt_number != prepared.attempt_number + 1:
            raise ExpiredClaimRecoveryPersistenceInvariantViolation
        await self._persist_orphan_outcome(prepared)
        session = self._required_session()
        try:
            await session.execute(
                insert(task_attempts).values(
                    id=attempt.id,
                    task_run_id=attempt.task_run_id,
                    attempt_number=attempt.attempt_number,
                    next_eligible_at=attempt.next_eligible_at,
                )
            )
            transitioned = (
                await session.execute(
                    update(task_runs)
                    .where(
                        task_runs.c.id == prepared.task_run_id,
                        task_runs.c.status.in_(
                            (TaskRunStatus.CLAIMED.value, TaskRunStatus.RUNNING.value)
                        ),
                    )
                    .values(
                        status=TaskRunStatus.RETRY_SCHEDULED.value,
                        updated_at=prepared.recovered_at,
                    )
                    .returning(task_runs.c.id)
                )
            ).one_or_none()
            if transitioned is None:
                raise ExpiredClaimRecoveryPersistenceInvariantViolation
            await session.execute(
                insert(task_retry_events).values(
                    id=uuid4(),
                    task_run_id=prepared.task_run_id,
                    event_type=RetryEventType.RETRY_SCHEDULED.value,
                    failed_attempt_number=prepared.attempt_number,
                    retry_attempt_number=attempt.attempt_number,
                    next_eligible_at=attempt.next_eligible_at,
                )
            )
        except IntegrityError as error:
            raise ExpiredClaimRecoveryPersistenceInvariantViolation from error
        except DBAPIError as error:
            raise ExpiredClaimRecoveryPersistenceUnavailable from error

    async def exhaust(
        self,
        prepared: PreparedExpiredClaimRecovery,
        reason: RetryNotScheduledReason,
    ) -> None:
        await self._persist_orphan_outcome(prepared)
        session = self._required_session()
        try:
            transitioned = (
                await session.execute(
                    update(task_runs)
                    .where(
                        task_runs.c.id == prepared.task_run_id,
                        task_runs.c.status.in_(
                            (TaskRunStatus.CLAIMED.value, TaskRunStatus.RUNNING.value)
                        ),
                    )
                    .values(
                        status=TaskRunStatus.FAILED.value,
                        updated_at=prepared.recovered_at,
                    )
                    .returning(task_runs.c.id)
                )
            ).one_or_none()
            if transitioned is None:
                raise ExpiredClaimRecoveryPersistenceInvariantViolation
            await session.execute(
                insert(task_retry_events).values(
                    id=uuid4(),
                    task_run_id=prepared.task_run_id,
                    event_type=RetryEventType.RETRY_NOT_SCHEDULED.value,
                    failed_attempt_number=prepared.attempt_number,
                    decision_reason=reason.value,
                )
            )
            await ensure_dead_letter(
                session,
                item_id=uuid4(),
                task_run_id=prepared.task_run_id,
                source_task_attempt_id=prepared.task_attempt_id,
                reason=DeadLetterReason.RETRY_EXHAUSTED,
            )
        except DeadLetterPersistenceInvariantViolation as error:
            raise ExpiredClaimRecoveryPersistenceInvariantViolation from error
        except IntegrityError as error:
            raise ExpiredClaimRecoveryPersistenceInvariantViolation from error
        except DBAPIError as error:
            raise ExpiredClaimRecoveryPersistenceUnavailable from error

    async def _persist_orphan_outcome(
        self, prepared: PreparedExpiredClaimRecovery
    ) -> None:
        fingerprint = task_result_fingerprint(
            result_kind=TaskExecutionResultKind.RETRYABLE_FAILURE,
            failure_kind=TaskExecutionFailureKind.CLAIM_EXPIRED,
            output=None,
        )
        session = self._required_session()
        try:
            # The result FK requires the historical claim to remain present. The
            # result is inserted before terminating that same immutable generation.
            await session.execute(
                insert(task_attempt_results).values(
                    task_attempt_id=prepared.task_attempt_id,
                    claim_generation=prepared.generation,
                    dispatch_id=prepared.dispatch_id,
                    result_kind=TaskExecutionResultKind.RETRYABLE_FAILURE.value,
                    failure_kind=TaskExecutionFailureKind.CLAIM_EXPIRED.value,
                    output=null(),
                    result_fingerprint=fingerprint,
                    completed_at=prepared.recovered_at,
                )
            )
            terminated = (
                await session.execute(
                    update(task_attempt_claims)
                    .where(
                        task_attempt_claims.c.task_attempt_id
                        == prepared.task_attempt_id,
                        task_attempt_claims.c.generation == prepared.generation,
                        task_attempt_claims.c.terminated_at.is_(None),
                        task_attempt_claims.c.lease_expires_at
                        == prepared.lease_expires_at,
                        task_attempt_claims.c.lease_expires_at <= prepared.recovered_at,
                    )
                    .values(terminated_at=prepared.recovered_at)
                    .returning(task_attempt_claims.c.task_attempt_id)
                )
            ).one_or_none()
            if terminated is None:
                raise ExpiredClaimRecoveryPersistenceInvariantViolation
            await session.execute(
                insert(task_result_events).values(
                    id=uuid4(),
                    task_attempt_id=prepared.task_attempt_id,
                    claim_generation=prepared.generation,
                    worker_session_id=prepared.worker_session_id,
                    dispatch_id=prepared.dispatch_id,
                    event_type="result_recovered",
                    result_kind=TaskExecutionResultKind.RETRYABLE_FAILURE.value,
                    failure_kind=TaskExecutionFailureKind.CLAIM_EXPIRED.value,
                    result_fingerprint=fingerprint,
                    occurred_at=prepared.recovered_at,
                )
            )
        except IntegrityError as error:
            raise ExpiredClaimRecoveryPersistenceInvariantViolation from error
        except DBAPIError as error:
            raise ExpiredClaimRecoveryPersistenceUnavailable from error

    def _required_session(self) -> AsyncSession:
        if self._session is None:
            raise RuntimeError("recovery transaction is not active")
        return self._session

    def _required_context(self) -> AbstractAsyncContextManager[AsyncSession]:
        if self._context is None:
            raise RuntimeError("recovery transaction is not active")
        return self._context


def _reference_expression(reference_time: datetime | None) -> Any:
    return (
        func.statement_timestamp()
        if reference_time is None
        else cast(literal(reference_time), DateTime(timezone=True))
    )


def _expired_claim_statement(
    limit: int, cursor: ExpiredClaimScanCursor | None
) -> Select[Any]:
    reference = _reference_expression(
        cursor.observed_at if cursor is not None else None
    )
    window = select(
        reference.label("observed_at"),
        task_attempt_claims.c.task_attempt_id,
        task_attempt_claims.c.generation,
        task_attempt_claims.c.worker_session_id,
        task_attempt_claims.c.lease_expires_at,
    ).where(
        task_attempt_claims.c.terminated_at.is_(None),
        task_attempt_claims.c.lease_expires_at <= reference,
    )
    if cursor is not None:
        window = window.where(
            or_(
                task_attempt_claims.c.lease_expires_at > cursor.lease_expires_at,
                and_(
                    task_attempt_claims.c.lease_expires_at == cursor.lease_expires_at,
                    task_attempt_claims.c.task_attempt_id > cursor.task_attempt_id,
                ),
                and_(
                    task_attempt_claims.c.lease_expires_at == cursor.lease_expires_at,
                    task_attempt_claims.c.task_attempt_id == cursor.task_attempt_id,
                    task_attempt_claims.c.generation > cursor.generation,
                ),
            )
        )
    claim_window = (
        window.order_by(
            task_attempt_claims.c.lease_expires_at,
            task_attempt_claims.c.task_attempt_id,
            task_attempt_claims.c.generation,
        )
        .limit(limit)
        .cte("expired_claim_window")
        .prefix_with("MATERIALIZED")
    )
    window_last = (
        select(
            claim_window.c.lease_expires_at,
            claim_window.c.task_attempt_id,
            claim_window.c.generation,
        )
        .order_by(
            claim_window.c.lease_expires_at.desc(),
            claim_window.c.task_attempt_id.desc(),
            claim_window.c.generation.desc(),
        )
        .limit(1)
        .cte("expired_claim_window_last")
    )
    later_attempt = task_attempts.alias("later_attempt")
    latest_attempt = (
        ~select(literal(1))
        .where(
            later_attempt.c.task_run_id == task_attempts.c.task_run_id,
            later_attempt.c.attempt_number > task_attempts.c.attempt_number,
        )
        .exists()
    )
    candidates = (
        select(
            claim_window.c.observed_at,
            claim_window.c.task_attempt_id,
            task_attempts.c.task_run_id,
            task_runs.c.workflow_run_id,
            task_attempts.c.attempt_number,
            claim_window.c.generation,
            claim_window.c.worker_session_id,
            claim_window.c.lease_expires_at,
        )
        .select_from(
            claim_window.join(
                task_attempts, task_attempts.c.id == claim_window.c.task_attempt_id
            )
            .join(task_runs, task_runs.c.id == task_attempts.c.task_run_id)
            .join(workflow_runs, workflow_runs.c.id == task_runs.c.workflow_run_id)
        )
        .where(
            task_runs.c.status.in_(
                (TaskRunStatus.CLAIMED.value, TaskRunStatus.RUNNING.value)
            ),
            workflow_runs.c.status.in_(
                (
                    WorkflowRunStatus.PENDING.value,
                    WorkflowRunStatus.RUNNING.value,
                    WorkflowRunStatus.CANCELLING.value,
                )
            ),
            latest_attempt,
        )
    )
    bounded = candidates.order_by(
        claim_window.c.lease_expires_at,
        claim_window.c.task_attempt_id,
        claim_window.c.generation,
    ).cte("expired_claim_candidates")
    window_size = select(func.count()).select_from(claim_window).scalar_subquery()
    cursor_lease = select(window_last.c.lease_expires_at).scalar_subquery()
    cursor_attempt = select(window_last.c.task_attempt_id).scalar_subquery()
    cursor_generation = select(window_last.c.generation).scalar_subquery()
    sentinel = select(
        reference.label("observed_at"),
        cast(null(), PostgreSQLUUID(as_uuid=True)).label("task_attempt_id"),
        cast(null(), PostgreSQLUUID(as_uuid=True)).label("task_run_id"),
        cast(null(), PostgreSQLUUID(as_uuid=True)).label("workflow_run_id"),
        cast(null(), Integer()).label("attempt_number"),
        cast(null(), BigInteger()).label("generation"),
        cast(null(), PostgreSQLUUID(as_uuid=True)).label("worker_session_id"),
        cast(null(), DateTime(timezone=True)).label("lease_expires_at"),
    ).where(~select(literal(1)).select_from(bounded).exists())
    page = union_all(select(bounded), sentinel).subquery("expired_claim_page")
    return select(
        page,
        window_size.label("window_size"),
        cursor_lease.label("cursor_lease_expires_at"),
        cursor_attempt.label("cursor_task_attempt_id"),
        cursor_generation.label("cursor_generation"),
    ).order_by(page.c.lease_expires_at, page.c.task_attempt_id, page.c.generation)


def _stale_worker_session_statement(
    stale_after_seconds: int,
    limit: int,
    cursor: StaleWorkerSessionScanCursor | None,
) -> Select[Any]:
    reference = _reference_expression(
        cursor.observed_at if cursor is not None else None
    )
    candidates = (
        select(
            reference.label("observed_at"),
            worker_sessions.c.id.label("worker_session_id"),
            worker_sessions.c.worker_identity_id,
            worker_session_health.c.last_sequence,
            worker_session_health.c.last_seen_at,
            worker_session_health.c.accepting_work,
        )
        .select_from(
            worker_session_health.join(
                worker_sessions,
                worker_sessions.c.id == worker_session_health.c.worker_session_id,
            )
        )
        .where(
            worker_sessions.c.ended_at.is_(None),
            worker_session_health.c.last_seen_at
            <= reference - func.make_interval(0, 0, 0, 0, 0, 0, stale_after_seconds),
        )
    )
    if cursor is not None:
        candidates = candidates.where(
            or_(
                worker_session_health.c.last_seen_at > cursor.last_seen_at,
                and_(
                    worker_session_health.c.last_seen_at == cursor.last_seen_at,
                    worker_sessions.c.id > cursor.worker_session_id,
                ),
            )
        )
    bounded = (
        candidates.order_by(worker_session_health.c.last_seen_at, worker_sessions.c.id)
        .limit(limit)
        .cte("stale_worker_session_candidates")
        .prefix_with("MATERIALIZED")
    )
    sentinel = select(
        reference.label("observed_at"),
        cast(null(), PostgreSQLUUID(as_uuid=True)).label("worker_session_id"),
        cast(null(), PostgreSQLUUID(as_uuid=True)).label("worker_identity_id"),
        cast(null(), BigInteger()).label("last_sequence"),
        cast(null(), DateTime(timezone=True)).label("last_seen_at"),
        cast(null(), Boolean()).label("accepting_work"),
    ).where(~select(literal(1)).select_from(bounded).exists())
    page = union_all(select(bounded), sentinel).subquery("stale_worker_session_page")
    return select(page).order_by(page.c.last_seen_at, page.c.worker_session_id)


def _page_observation_time(rows: Sequence[Any]) -> datetime:
    if not rows:
        raise RecoveryScanPersistenceInvariantViolation
    observed_at = rows[0].observed_at
    if not isinstance(observed_at, datetime):
        raise RecoveryScanPersistenceInvariantViolation
    return observed_at
