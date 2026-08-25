"""PostgreSQL inspection and operator transitions for dead letters."""

from __future__ import annotations

from copy import deepcopy
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Row, and_, exists, func, insert, or_, select, update
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.dead_letters.domain import (
    CreatedDeadLetterRedrive,
    DeadLetterActionCursor,
    DeadLetterActionPage,
    DeadLetterActionType,
    DeadLetterCursor,
    DeadLetterDetail,
    DeadLetterFilters,
    DeadLetterOperatorAction,
    DeadLetterPage,
    DeadLetterReason,
    DeadLetterRedriveIdempotency,
    DeadLetterRedriveIdempotencyConflict,
    DeadLetterRedriveSummary,
    DeadLetterStatus,
    DeadLetterSummary,
    redrive_fingerprints_match,
)
from taskforge.dead_letters.persistence_ports import (
    DeadLetterPersistenceInvariantViolation,
    DeadLetterPersistenceUnavailable,
    DeadLetterRedriveLimitExceeded,
    DeadLetterRedriveNotEligible,
    DeadLetterTransitionConflict,
)
from taskforge.dead_letters.schema import (
    dead_letter_items,
    dead_letter_operator_actions,
    dead_letter_redrive_requests,
    dead_letter_status,
)
from taskforge.identity.authorization import OwnerFilter
from taskforge.persistence.execution_events import (
    append_workflow_redrive_created_execution_event,
)
from taskforge.persistence.runs import (
    insert_complete_workflow_run,
    load_exact_workflow_version_snapshot,
)
from taskforge.retries.domain import RetryNotScheduledReason
from taskforge.runs.domain import (
    NewTaskRun,
    NewWorkflowRun,
    TaskRunStatus,
    WorkflowRunStatus,
    create_workflow_run_input,
    materialize_initial_tasks,
)
from taskforge.runs.persistence_ports import PreparedWorkflowRunCreation
from taskforge.runs.schema import (
    task_attempt_results,
    task_attempts,
    task_retry_events,
    task_runs,
    workflow_run_inputs,
    workflow_runs,
)
from taskforge.worker.results import TaskExecutionFailureKind, TaskExecutionResultKind
from taskforge.workflows.domain import WorkflowDefinitionStatus
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

    async def redrive(
        self,
        item_id: UUID,
        owner_filter: OwnerFilter,
        *,
        operator_principal_id: UUID,
        idempotency: DeadLetterRedriveIdempotency,
        reason: str | None,
        correlation_id: UUID,
    ) -> CreatedDeadLetterRedrive | None:
        try:
            async with self._sessions.begin() as session:
                source = (
                    await session.execute(_redrive_source_lock(item_id, owner_filter))
                ).one_or_none()
                if source is None:
                    return None
                existing = (
                    await session.execute(
                        _scoped_redrive_statement(
                            item_id,
                            operator_principal_id,
                            idempotency.key_digest,
                        )
                    )
                ).one_or_none()
                if existing is not None:
                    if not redrive_fingerprints_match(
                        existing.request_fingerprint,
                        idempotency.request_fingerprint,
                    ):
                        raise DeadLetterRedriveIdempotencyConflict
                    return _created_redrive(source, existing)

                if source.dead_letter_status not in (
                    DeadLetterStatus.OPEN.value,
                    DeadLetterStatus.ACKNOWLEDGED.value,
                ):
                    raise DeadLetterRedriveNotEligible
                if (
                    source.task_run_status != TaskRunStatus.FAILED.value
                    or source.workflow_run_status != WorkflowRunStatus.FAILED.value
                ):
                    raise DeadLetterRedriveNotEligible
                if await session.scalar(
                    select(
                        exists(
                            select(1).where(
                                dead_letter_redrive_requests.c.dead_letter_item_id
                                == item_id
                            )
                        )
                    )
                ):
                    raise DeadLetterRedriveLimitExceeded

                snapshot = await load_exact_workflow_version_snapshot(
                    session,
                    source.workflow_definition_id,
                    source.workflow_version_id,
                )
                if snapshot is None:
                    raise DeadLetterPersistenceInvariantViolation
                initial_tasks = materialize_initial_tasks(snapshot)
                input_snapshot = create_workflow_run_input(
                    deepcopy(source.payload), deepcopy(source.input_references)
                )
                target_run = NewWorkflowRun(uuid4(), operator_principal_id)
                target_tasks = tuple(
                    NewTaskRun(
                        uuid4(),
                        task.step_identifier,
                        task.status,
                        task.deadline_seconds,
                        task.execution_timeout_seconds,
                    )
                    for task in initial_tasks
                )
                await insert_complete_workflow_run(
                    session,
                    PreparedWorkflowRunCreation(
                        source.workflow_definition_id,
                        WorkflowDefinitionStatus(source.workflow_definition_status),
                        snapshot,
                    ),
                    target_run,
                    input_snapshot,
                    target_tasks,
                )
                request_row = (
                    await session.execute(
                        insert(dead_letter_redrive_requests)
                        .values(
                            id=uuid4(),
                            dead_letter_item_id=item_id,
                            requested_by_principal_id=operator_principal_id,
                            idempotency_key_digest=idempotency.key_digest,
                            request_fingerprint=idempotency.request_fingerprint,
                            target_workflow_run_id=target_run.id,
                            reason=reason,
                            correlation_id=correlation_id,
                        )
                        .returning(*dead_letter_redrive_requests.c)
                    )
                ).one()
                await append_workflow_redrive_created_execution_event(
                    session,
                    workflow_run_id=target_run.id,
                    dead_letter_item_id=item_id,
                    source_workflow_run_id=source.workflow_run_id,
                    source_task_run_id=source.task_run_id,
                    source_task_attempt_id=source.source_task_attempt_id,
                    requested_by_principal_id=operator_principal_id,
                    correlation_id=correlation_id,
                )
                return _created_redrive(source, request_row)
        except (
            DeadLetterPersistenceInvariantViolation,
            DeadLetterRedriveIdempotencyConflict,
            DeadLetterRedriveLimitExceeded,
            DeadLetterRedriveNotEligible,
        ):
            raise
        except IntegrityError as error:
            constraint = _integrity_constraint(error)
            if constraint == "uq_dead_letter_redrive_requests_item_requester_key":
                # Every supported writer holds the item's dead_letter_status row
                # lock before reading or inserting this scope. A scoped-key race is
                # therefore impossible; seeing this constraint means a writer
                # bypassed the repository protocol or durable facts are inconsistent.
                raise DeadLetterPersistenceInvariantViolation from error
            if constraint in {
                "uq_dead_letter_redrive_requests_item",
                "uq_dead_letter_redrive_requests_target_run",
            }:
                raise DeadLetterRedriveLimitExceeded from error
            raise DeadLetterPersistenceInvariantViolation from error
        except (TypeError, ValueError) as error:
            raise DeadLetterPersistenceInvariantViolation from error
        except DBAPIError as error:
            raise DeadLetterPersistenceUnavailable from error


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
    target_run = workflow_runs.alias("redrive_target_workflow_run")
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
            dead_letter_redrive_requests.c.id.label("redrive_id"),
            dead_letter_redrive_requests.c.target_workflow_run_id,
            dead_letter_redrive_requests.c.requested_by_principal_id.label(
                "redrive_requested_by_principal_id"
            ),
            dead_letter_redrive_requests.c.reason.label("redrive_reason"),
            dead_letter_redrive_requests.c.requested_at.label("redrive_requested_at"),
            target_run.c.status.label("redrive_target_workflow_run_status"),
        )
        .select_from(
            _base_from()
            .join(
                task_attempt_results,
                task_attempt_results.c.task_attempt_id == task_attempts.c.id,
            )
            .outerjoin(
                dead_letter_redrive_requests,
                dead_letter_redrive_requests.c.dead_letter_item_id
                == dead_letter_items.c.id,
            )
            .outerjoin(
                target_run,
                target_run.c.id
                == dead_letter_redrive_requests.c.target_workflow_run_id,
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


def _redrive_source_lock(item_id: UUID, owner_filter: OwnerFilter) -> Any:
    return (
        select(
            dead_letter_items.c.id,
            dead_letter_items.c.task_run_id,
            dead_letter_items.c.source_task_attempt_id,
            dead_letter_status.c.status.label("dead_letter_status"),
            task_runs.c.workflow_run_id,
            task_runs.c.status.label("task_run_status"),
            workflow_runs.c.status.label("workflow_run_status"),
            workflow_runs.c.workflow_definition_id,
            workflow_runs.c.workflow_version_id,
            workflow_definitions.c.status.label("workflow_definition_status"),
            workflow_run_inputs.c.payload,
            workflow_run_inputs.c.input_references,
        )
        .select_from(
            _base_from().join(
                workflow_run_inputs,
                workflow_run_inputs.c.workflow_run_id == workflow_runs.c.id,
            )
        )
        .where(dead_letter_items.c.id == item_id, _owner_predicate(owner_filter))
        .with_for_update(of=dead_letter_status)
    )


def _scoped_redrive_statement(
    item_id: UUID, operator_principal_id: UUID, key_digest: str
) -> Any:
    return select(dead_letter_redrive_requests).where(
        dead_letter_redrive_requests.c.dead_letter_item_id == item_id,
        dead_letter_redrive_requests.c.requested_by_principal_id
        == operator_principal_id,
        dead_letter_redrive_requests.c.idempotency_key_digest == key_digest,
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
        redrive=(
            DeadLetterRedriveSummary(
                id=row.redrive_id,
                target_workflow_run_id=row.target_workflow_run_id,
                requested_by_principal_id=row.redrive_requested_by_principal_id,
                reason=row.redrive_reason,
                requested_at=row.redrive_requested_at,
                target_workflow_run_status=row.redrive_target_workflow_run_status,
            )
            if row.redrive_id is not None
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


def _created_redrive(source: Row[Any], request: Row[Any]) -> CreatedDeadLetterRedrive:
    return CreatedDeadLetterRedrive(
        id=request.id,
        dead_letter_item_id=source.id,
        source_workflow_run_id=source.workflow_run_id,
        source_task_run_id=source.task_run_id,
        source_task_attempt_id=source.source_task_attempt_id,
        target_workflow_run_id=request.target_workflow_run_id,
        workflow_definition_id=source.workflow_definition_id,
        workflow_version_id=source.workflow_version_id,
        requested_by_principal_id=request.requested_by_principal_id,
        reason=request.reason,
        requested_at=request.requested_at,
    )


def _integrity_constraint(error: IntegrityError) -> str | None:
    pending: list[BaseException] = [error]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        constraint = getattr(current, "constraint_name", None)
        if isinstance(constraint, str):
            return constraint
        if current.__cause__ is not None:
            pending.append(current.__cause__)
        if current.__context__ is not None:
            pending.append(current.__context__)
        original = getattr(current, "orig", None)
        if isinstance(original, BaseException):
            pending.append(original)
    return None
