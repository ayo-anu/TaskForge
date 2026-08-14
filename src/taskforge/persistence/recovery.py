"""PostgreSQL candidate queries for read-only worker crash recovery."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Integer,
    and_,
    cast,
    func,
    literal,
    null,
    or_,
    select,
    union_all,
)
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.sql import Select

from taskforge.recovery.domain import (
    ExpiredClaimCandidate,
    ExpiredClaimCandidatePage,
    ExpiredClaimScanCursor,
    StaleWorkerSessionCandidate,
    StaleWorkerSessionCandidatePage,
    StaleWorkerSessionScanCursor,
)
from taskforge.recovery.persistence_ports import (
    RecoveryScanPersistenceInvariantViolation,
    RecoveryScanPersistenceUnavailable,
)
from taskforge.runs.domain import TaskRunStatus, WorkflowRunStatus
from taskforge.runs.schema import (
    task_attempt_claims,
    task_attempts,
    task_runs,
    workflow_runs,
)
from taskforge.worker.schema import worker_session_health, worker_sessions


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
