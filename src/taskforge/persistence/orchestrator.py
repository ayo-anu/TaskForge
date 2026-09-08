"""PostgreSQL read-only discovery for bounded orchestrator sweeps."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from sqlalchemy import Select, literal, select, tuple_
from sqlalchemy.engine import Row
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.sql.schema import Table

from taskforge.orchestrator.domain import (
    ActiveWorkflowRunCandidate,
    DiscoveryCursor,
    DiscoveryHighWater,
    DiscoveryPage,
    OrchestratorPersistenceInvariantError,
    OrchestratorPersistenceUnavailable,
    RetryPendingTaskCandidate,
    RunnableTaskCandidate,
)
from taskforge.runs.domain import TaskRunStatus, WorkflowRunStatus
from taskforge.runs.schema import task_runs, workflow_runs

MAX_ORCHESTRATOR_DISCOVERY_PAGE_SIZE = 100


class SQLAlchemyOrchestratorCandidateRepository:
    """Discover advisory candidates without locking or granting authority."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def capture_active_run_high_water(self) -> DiscoveryHighWater | None:
        return await self._capture_high_water(workflow_runs)

    async def list_active_workflow_runs(
        self,
        *,
        high_water: DiscoveryHighWater,
        cursor: DiscoveryCursor | None,
        limit: int,
    ) -> DiscoveryPage[ActiveWorkflowRunCandidate]:
        statement = _active_run_page_statement(high_water, cursor, limit)
        return await self._page(
            statement,
            high_water,
            limit,
            lambda row: ActiveWorkflowRunCandidate(row.id, row.created_at),
        )

    async def capture_runnable_task_high_water(self) -> DiscoveryHighWater | None:
        return await self._capture_high_water(task_runs)

    async def list_runnable_tasks(
        self,
        *,
        high_water: DiscoveryHighWater,
        cursor: DiscoveryCursor | None,
        limit: int,
    ) -> DiscoveryPage[RunnableTaskCandidate]:
        statement = _runnable_task_page_statement(high_water, cursor, limit)
        return await self._page(
            statement,
            high_water,
            limit,
            lambda row: RunnableTaskCandidate(
                row.workflow_run_id, row.id, row.created_at
            ),
        )

    async def capture_retry_pending_high_water(
        self,
    ) -> DiscoveryHighWater | None:
        return await self._capture_high_water(task_runs)

    async def list_retry_pending_tasks(
        self,
        *,
        high_water: DiscoveryHighWater,
        cursor: DiscoveryCursor | None,
        limit: int,
    ) -> DiscoveryPage[RetryPendingTaskCandidate]:
        statement = _retry_pending_page_statement(high_water, cursor, limit)
        return await self._page(
            statement,
            high_water,
            limit,
            lambda row: RetryPendingTaskCandidate(row.id, row.created_at),
        )

    async def _capture_high_water(self, source: Table) -> DiscoveryHighWater | None:
        statement = (
            select(source.c.created_at, source.c.id)
            .order_by(source.c.created_at.desc(), source.c.id.desc())
            .limit(1)
        )
        try:
            async with self._sessions() as session:
                row = (await session.execute(statement)).one_or_none()
        except DBAPIError as error:
            raise OrchestratorPersistenceUnavailable from error
        if row is None:
            return None
        try:
            return DiscoveryHighWater(row.created_at, row.id)
        except (TypeError, ValueError) as error:
            raise OrchestratorPersistenceInvariantError from error

    async def _page[T](
        self,
        statement: Select[Any],
        high_water: DiscoveryHighWater,
        limit: int,
        factory: Callable[[Row[Any]], T],
    ) -> DiscoveryPage[T]:
        _validate_page_request(high_water, limit)
        try:
            async with self._sessions() as session:
                rows = (await session.execute(statement)).all()
            items = tuple(factory(row) for row in rows)
            next_cursor = None
            if len(items) == limit:
                last = rows[-1]
                next_cursor = DiscoveryCursor(
                    high_water.created_at,
                    high_water.entity_id,
                    last.created_at,
                    last.id,
                )
            return DiscoveryPage(items, next_cursor)
        except OrchestratorPersistenceInvariantError:
            raise
        except DBAPIError as error:
            raise OrchestratorPersistenceUnavailable from error
        except (AttributeError, IndexError, TypeError, ValueError) as error:
            raise OrchestratorPersistenceInvariantError from error


def _validate_page_request(high_water: DiscoveryHighWater, limit: int) -> None:
    if not isinstance(high_water, DiscoveryHighWater):
        raise ValueError("discovery high-water is invalid")
    if type(limit) is not int or not 1 <= limit <= MAX_ORCHESTRATOR_DISCOVERY_PAGE_SIZE:
        raise ValueError("orchestrator discovery limit is outside supported bounds")


def _bounded_page(
    statement: Select[Any],
    source: Table,
    high_water: DiscoveryHighWater,
    cursor: DiscoveryCursor | None,
    limit: int,
) -> Select[Any]:
    _validate_page_request(high_water, limit)
    if cursor is not None and cursor.high_water != high_water:
        raise ValueError("discovery cursor high-water does not match sweep")
    source_key = tuple_(source.c.created_at, source.c.id)
    statement = statement.where(
        source_key
        <= tuple_(literal(high_water.created_at), literal(high_water.entity_id))
    )
    if cursor is not None:
        statement = statement.where(
            source_key
            > tuple_(literal(cursor.last_created_at), literal(cursor.last_id))
        )
    return statement.order_by(source.c.created_at, source.c.id).limit(limit)


def _active_run_page_statement(
    high_water: DiscoveryHighWater,
    cursor: DiscoveryCursor | None,
    limit: int,
) -> Select[Any]:
    statement = select(
        workflow_runs.c.id,
        workflow_runs.c.created_at,
    ).where(
        workflow_runs.c.status.in_(
            (
                WorkflowRunStatus.PENDING.value,
                WorkflowRunStatus.RUNNING.value,
                WorkflowRunStatus.CANCELLING.value,
            )
        )
    )
    return _bounded_page(statement, workflow_runs, high_water, cursor, limit)


def _active_task_page_base(status: TaskRunStatus) -> Select[Any]:
    return (
        select(
            task_runs.c.id,
            task_runs.c.workflow_run_id,
            task_runs.c.created_at,
        )
        .select_from(
            task_runs.join(
                workflow_runs,
                (workflow_runs.c.id == task_runs.c.workflow_run_id)
                & (
                    workflow_runs.c.workflow_version_id
                    == task_runs.c.workflow_version_id
                ),
            )
        )
        .where(
            task_runs.c.status == status.value,
            workflow_runs.c.status.in_(
                (WorkflowRunStatus.PENDING.value, WorkflowRunStatus.RUNNING.value)
            ),
        )
    )


def _runnable_task_page_statement(
    high_water: DiscoveryHighWater,
    cursor: DiscoveryCursor | None,
    limit: int,
) -> Select[Any]:
    return _bounded_page(
        _active_task_page_base(TaskRunStatus.RUNNABLE),
        task_runs,
        high_water,
        cursor,
        limit,
    )


def _retry_pending_page_statement(
    high_water: DiscoveryHighWater,
    cursor: DiscoveryCursor | None,
    limit: int,
) -> Select[Any]:
    return _bounded_page(
        _active_task_page_base(TaskRunStatus.RETRY_PENDING),
        task_runs,
        high_water,
        cursor,
        limit,
    )
