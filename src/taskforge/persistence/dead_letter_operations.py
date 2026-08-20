"""PostgreSQL inspection and operator transitions for dead letters."""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Row, and_, func, insert, or_, select, update
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.dead_letters.domain import (
    DeadLetterActionCursor,
    DeadLetterActionPage,
    DeadLetterActionType,
    DeadLetterCursor,
    DeadLetterDetail,
    DeadLetterFilters,
    DeadLetterOperatorAction,
    DeadLetterPage,
    DeadLetterReason,
    DeadLetterStatus,
    DeadLetterSummary,
)
from taskforge.dead_letters.persistence_ports import (
    DeadLetterPersistenceInvariantViolation,
    DeadLetterPersistenceUnavailable,
    DeadLetterTransitionConflict,
)
from taskforge.dead_letters.schema import (
    dead_letter_items,
    dead_letter_operator_actions,
    dead_letter_status,
)
from taskforge.identity.authorization import OwnerFilter
from taskforge.retries.domain import RetryNotScheduledReason
from taskforge.runs.schema import (
    task_attempt_results,
    task_attempts,
    task_retry_events,
    task_runs,
    workflow_runs,
)
from taskforge.worker.results import TaskExecutionFailureKind, TaskExecutionResultKind
from taskforge.workflows.schema import workflow_definitions


class SQLAlchemyDeadLetterRepository:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def list_items(
        self,
        owner_filter: OwnerFilter,
        filters: DeadLetterFilters,
        *,
        limit: int,
        cursor: DeadLetterCursor | None,
    ) -> DeadLetterPage:
        try:
            async with self._sessions() as session, session.begin():
                rows = (
                    await session.execute(
                        _list_statement(owner_filter, filters, limit + 1, cursor)
                    )
                ).all()
        except DBAPIError as error:
            raise DeadLetterPersistenceUnavailable from error
        page_rows = rows[:limit]
        try:
            items = tuple(_summary(row) for row in page_rows)
        except (TypeError, ValueError) as error:
            raise DeadLetterPersistenceInvariantViolation from error
        next_cursor = None
        if len(rows) > limit and items:
            last = items[-1]
            next_cursor = DeadLetterCursor(last.created_at, last.id)
        return DeadLetterPage(items, next_cursor)

    async def get_item(
        self, item_id: UUID, owner_filter: OwnerFilter
    ) -> DeadLetterDetail | None:
        try:
            async with self._sessions() as session, session.begin():
                row = (
                    await session.execute(_detail_statement(item_id, owner_filter))
                ).one_or_none()
        except DBAPIError as error:
            raise DeadLetterPersistenceUnavailable from error
        return _detail(row) if row is not None else None

    async def list_actions(
        self,
        item_id: UUID,
        owner_filter: OwnerFilter,
        *,
        limit: int,
        cursor: DeadLetterActionCursor | None,
    ) -> DeadLetterActionPage | None:
        try:
            async with self._sessions() as session, session.begin():
                if not await session.scalar(_visible_item(item_id, owner_filter)):
                    return None
                rows = (
                    await session.execute(
                        _actions_statement(item_id, limit + 1, cursor)
                    )
                ).all()
        except DBAPIError as error:
            raise DeadLetterPersistenceUnavailable from error
        page_rows = rows[:limit]
        try:
            items = tuple(_action(row) for row in page_rows)
        except (TypeError, ValueError) as error:
            raise DeadLetterPersistenceInvariantViolation from error
        next_cursor = None
        if len(rows) > limit and items:
            last = items[-1]
            next_cursor = DeadLetterActionCursor(last.occurred_at, last.id)
        return DeadLetterActionPage(items, next_cursor)

    async def transition(
        self,
        item_id: UUID,
        owner_filter: OwnerFilter,
        *,
        operator_principal_id: UUID,
        target_status: DeadLetterStatus,
        reason: str | None,
        correlation_id: UUID,
    ) -> DeadLetterDetail | None:
        try:
            async with self._sessions() as session, session.begin():
                locked = (
                    await session.execute(_status_lock_statement(item_id, owner_filter))
                ).one_or_none()
                if locked is None:
                    return None
                previous = DeadLetterStatus(locked.status)
                if not _transition_allowed(previous, target_status):
                    raise DeadLetterTransitionConflict
                occurred_at = locked.occurred_at
                updated = await session.execute(
                    update(dead_letter_status)
                    .where(
                        dead_letter_status.c.dead_letter_item_id == item_id,
                        dead_letter_status.c.status == previous.value,
                    )
                    .values(status=target_status.value, updated_at=occurred_at)
                    .returning(dead_letter_status.c.dead_letter_item_id)
                )
                if updated.one_or_none() is None:
                    raise DeadLetterPersistenceInvariantViolation
                await session.execute(
                    insert(dead_letter_operator_actions).values(
                        id=uuid4(),
                        dead_letter_item_id=item_id,
                        operator_principal_id=operator_principal_id,
                        action_type=target_status.value,
                        previous_status=previous.value,
                        new_status=target_status.value,
                        reason=reason,
                        correlation_id=correlation_id,
                        occurred_at=occurred_at,
                    )
                )
                row = (
                    await session.execute(_detail_statement(item_id, owner_filter))
                ).one()
        except DeadLetterTransitionConflict:
            raise
        except (IntegrityError, ValueError) as error:
            raise DeadLetterPersistenceInvariantViolation from error
        except DBAPIError as error:
            raise DeadLetterPersistenceUnavailable from error
        return _detail(row)


def _base_from() -> Any:
    return (
        dead_letter_items.join(
            dead_letter_status,
            dead_letter_status.c.dead_letter_item_id == dead_letter_items.c.id,
        )
        .join(task_runs, task_runs.c.id == dead_letter_items.c.task_run_id)
        .join(workflow_runs, workflow_runs.c.id == task_runs.c.workflow_run_id)
        .join(
            workflow_definitions,
            workflow_definitions.c.id == workflow_runs.c.workflow_definition_id,
        )
        .join(
            task_attempts,
            task_attempts.c.id == dead_letter_items.c.source_task_attempt_id,
        )
    )


def _owner_predicate(owner_filter: OwnerFilter) -> Any:
    if owner_filter.unrestricted:
        return True
    return workflow_definitions.c.owner_principal_id == owner_filter.principal_id


def _summary_columns() -> tuple[Any, ...]:
    return (
        dead_letter_items.c.id,
        dead_letter_items.c.task_run_id,
        dead_letter_items.c.source_task_attempt_id,
        task_runs.c.workflow_run_id,
        dead_letter_items.c.reason,
        dead_letter_status.c.status,
        dead_letter_items.c.created_at,
        dead_letter_status.c.updated_at.label("status_updated_at"),
        task_attempts.c.attempt_number.label("source_attempt_number"),
    )


def _list_statement(
    owner_filter: OwnerFilter,
    filters: DeadLetterFilters,
    limit: int,
    cursor: DeadLetterCursor | None,
) -> Any:
    conditions: list[Any] = [_owner_predicate(owner_filter)]
    mappings = (
        (dead_letter_status.c.status, filters.status),
        (dead_letter_items.c.reason, filters.reason),
        (dead_letter_items.c.task_run_id, filters.task_run_id),
        (task_runs.c.workflow_run_id, filters.workflow_run_id),
        (dead_letter_items.c.source_task_attempt_id, filters.source_task_attempt_id),
    )
    conditions.extend(
        column == value for column, value in mappings if value is not None
    )
    if filters.created_after is not None:
        conditions.append(dead_letter_items.c.created_at >= filters.created_after)
    if filters.created_before is not None:
        conditions.append(dead_letter_items.c.created_at < filters.created_before)
    if cursor is not None:
        conditions.append(
            or_(
                dead_letter_items.c.created_at < cursor.created_at,
                and_(
                    dead_letter_items.c.created_at == cursor.created_at,
                    dead_letter_items.c.id < cursor.item_id,
                ),
            )
        )
    return (
        select(*_summary_columns())
        .select_from(_base_from())
        .where(*conditions)
        .order_by(dead_letter_items.c.created_at.desc(), dead_letter_items.c.id.desc())
        .limit(limit)
    )


def _detail_statement(item_id: UUID, owner_filter: OwnerFilter) -> Any:
    retry_reason = (
        select(task_retry_events.c.decision_reason)
        .where(
            task_retry_events.c.task_run_id == dead_letter_items.c.task_run_id,
            task_retry_events.c.failed_attempt_number == task_attempts.c.attempt_number,
            task_retry_events.c.event_type == "retry_not_scheduled",
        )
        .order_by(task_retry_events.c.occurred_at.desc(), task_retry_events.c.id.desc())
        .limit(1)
        .scalar_subquery()
    )
    return (
        select(
            *_summary_columns(),
            workflow_runs.c.workflow_definition_id,
            workflow_runs.c.workflow_version_id,
            task_runs.c.step_identifier,
            task_attempt_results.c.result_kind,
            task_attempt_results.c.failure_kind,
            retry_reason.label("retry_decision_reason"),
        )
        .select_from(
            _base_from().join(
                task_attempt_results,
                task_attempt_results.c.task_attempt_id == task_attempts.c.id,
            )
        )
        .where(dead_letter_items.c.id == item_id, _owner_predicate(owner_filter))
    )


def _visible_item(item_id: UUID, owner_filter: OwnerFilter) -> Any:
    return (
        select(dead_letter_items.c.id)
        .select_from(_base_from())
        .where(dead_letter_items.c.id == item_id, _owner_predicate(owner_filter))
    )


def _actions_statement(
    item_id: UUID, limit: int, cursor: DeadLetterActionCursor | None
) -> Any:
    conditions = [dead_letter_operator_actions.c.dead_letter_item_id == item_id]
    if cursor is not None:
        conditions.append(
            or_(
                dead_letter_operator_actions.c.occurred_at < cursor.occurred_at,
                and_(
                    dead_letter_operator_actions.c.occurred_at == cursor.occurred_at,
                    dead_letter_operator_actions.c.id < cursor.action_id,
                ),
            )
        )
    return (
        select(dead_letter_operator_actions)
        .where(*conditions)
        .order_by(
            dead_letter_operator_actions.c.occurred_at.desc(),
            dead_letter_operator_actions.c.id.desc(),
        )
        .limit(limit)
    )


def _status_lock_statement(item_id: UUID, owner_filter: OwnerFilter) -> Any:
    return (
        select(
            dead_letter_status.c.status, func.statement_timestamp().label("occurred_at")
        )
        .select_from(_base_from())
        .where(dead_letter_items.c.id == item_id, _owner_predicate(owner_filter))
        .with_for_update(of=dead_letter_status)
    )


def _transition_allowed(previous: DeadLetterStatus, target: DeadLetterStatus) -> bool:
    return (previous, target) in {
        (DeadLetterStatus.OPEN, DeadLetterStatus.ACKNOWLEDGED),
        (DeadLetterStatus.OPEN, DeadLetterStatus.RESOLVED),
        (DeadLetterStatus.ACKNOWLEDGED, DeadLetterStatus.RESOLVED),
    }


def _summary(row: Row[Any]) -> DeadLetterSummary:
    return DeadLetterSummary(
        id=row.id,
        task_run_id=row.task_run_id,
        source_task_attempt_id=row.source_task_attempt_id,
        workflow_run_id=row.workflow_run_id,
        reason=DeadLetterReason(row.reason),
        status=DeadLetterStatus(row.status),
        created_at=row.created_at,
        status_updated_at=row.status_updated_at,
        source_attempt_number=row.source_attempt_number,
    )


def _detail(row: Row[Any]) -> DeadLetterDetail:
    summary = _summary(row)
    return DeadLetterDetail(
        **summary.__dict__,
        workflow_definition_id=row.workflow_definition_id,
        workflow_version_id=row.workflow_version_id,
        step_identifier=row.step_identifier,
        result_kind=TaskExecutionResultKind(row.result_kind),
        failure_kind=(
            TaskExecutionFailureKind(row.failure_kind)
            if row.failure_kind is not None
            else None
        ),
        retry_decision_reason=(
            RetryNotScheduledReason(row.retry_decision_reason)
            if row.retry_decision_reason is not None
            else None
        ),
    )


def _action(row: Row[Any]) -> DeadLetterOperatorAction:
    return DeadLetterOperatorAction(
        id=row.id,
        dead_letter_item_id=row.dead_letter_item_id,
        operator_principal_id=row.operator_principal_id,
        action_type=DeadLetterActionType(row.action_type),
        previous_status=DeadLetterStatus(row.previous_status),
        new_status=DeadLetterStatus(row.new_status),
        reason=row.reason,
        correlation_id=row.correlation_id,
        occurred_at=row.occurred_at,
    )
