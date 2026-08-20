"""Atomic PostgreSQL persistence for retry scheduling transitions."""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from copy import deepcopy
from datetime import datetime
from types import TracebackType
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import exists, func, insert, select, update
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.dead_letters.domain import DeadLetterReason
from taskforge.persistence.dead_letters import (
    DeadLetterPersistenceInvariantViolation,
    ensure_dead_letter,
)
from taskforge.retries.domain import RetryEventType, RetryNotScheduledReason
from taskforge.retries.persistence_ports import (
    DueRetryPersistenceInvariantViolation,
    DueRetryPersistenceUnavailable,
    DueRetryPreparation,
    ExistingScheduledRetry,
    NewScheduledRetryAttempt,
    PreparedDueRetryDispatch,
    PreparedRetryTransition,
    RetryTransitionPersistenceInvariantViolation,
    RetryTransitionPersistenceUnavailable,
    RetryTransitionPreparation,
    SkippedDueRetryCandidate,
)
from taskforge.runs.domain import TaskRunStatus, WorkflowRunStatus
from taskforge.runs.schema import (
    task_attempt_claims,
    task_attempt_results,
    task_attempts,
    task_dispatch_outbox,
    task_retry_events,
    task_runs,
    workflow_runs,
)
from taskforge.worker.results import TaskExecutionResultKind
from taskforge.workflows.schema import workflow_version_steps, workflow_versions


class SQLAlchemyRetryTransitionRepository:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    def transition_transaction(self) -> SQLAlchemyRetryTransitionTransaction:
        return SQLAlchemyRetryTransitionTransaction(self._sessions)

    def due_dispatch_transaction(self) -> SQLAlchemyDueRetryDispatchTransaction:
        return SQLAlchemyDueRetryDispatchTransaction(self._sessions)


class SQLAlchemyRetryTransitionTransaction:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions
        self._context: AbstractAsyncContextManager[AsyncSession] | None = None
        self._session: AsyncSession | None = None

    async def __aenter__(self) -> SQLAlchemyRetryTransitionTransaction:
        context = self._sessions.begin()
        self._context = context
        try:
            self._session = await context.__aenter__()
        except DBAPIError as error:
            self._context = None
            raise RetryTransitionPersistenceUnavailable from error
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
            raise RetryTransitionPersistenceInvariantViolation from error
        except DBAPIError as error:
            raise RetryTransitionPersistenceUnavailable from error

    async def prepare_transition(self, task_run_id: UUID) -> RetryTransitionPreparation:
        session = self._required_session()
        try:
            run = (
                await session.execute(
                    select(
                        workflow_runs.c.id,
                        workflow_runs.c.workflow_version_id,
                        workflow_runs.c.status,
                    )
                    .select_from(
                        workflow_runs.join(
                            task_runs,
                            task_runs.c.workflow_run_id == workflow_runs.c.id,
                        )
                    )
                    .where(task_runs.c.id == task_run_id)
                    .with_for_update(of=workflow_runs)
                )
            ).one_or_none()
            if run is None:
                return None
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
                        task_runs.c.id == task_run_id,
                        task_runs.c.workflow_run_id == run.id,
                    )
                    .with_for_update()
                )
            ).one_or_none()
            if task is None:
                raise RetryTransitionPersistenceInvariantViolation
            if run.status not in (
                WorkflowRunStatus.PENDING.value,
                WorkflowRunStatus.RUNNING.value,
            ):
                return None
            if task.status == TaskRunStatus.RETRY_SCHEDULED.value:
                return await self._existing_scheduled(task_run_id)
            if task.status != TaskRunStatus.RETRY_PENDING.value:
                return None
            if task.workflow_version_id != run.workflow_version_id:
                raise RetryTransitionPersistenceInvariantViolation
            return await self._pending_transition(task)
        except RetryTransitionPersistenceInvariantViolation:
            raise
        except DBAPIError as error:
            raise RetryTransitionPersistenceUnavailable from error

    async def schedule_retry(
        self,
        prepared: PreparedRetryTransition,
        attempt: NewScheduledRetryAttempt,
    ) -> None:
        if (
            attempt.task_run_id != prepared.task_run_id
            or attempt.attempt_number != prepared.failed_attempt_number + 1
        ):
            raise RetryTransitionPersistenceInvariantViolation
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
                        task_runs.c.status == TaskRunStatus.RETRY_PENDING.value,
                    )
                    .values(
                        status=TaskRunStatus.RETRY_SCHEDULED.value,
                        updated_at=func.current_timestamp(),
                    )
                    .returning(task_runs.c.id)
                )
            ).one_or_none()
        except IntegrityError as error:
            raise RetryTransitionPersistenceInvariantViolation from error
        except DBAPIError as error:
            raise RetryTransitionPersistenceUnavailable from error
        if transitioned is None:
            raise RetryTransitionPersistenceInvariantViolation
        await self._append_retry_event(
            prepared.task_run_id,
            RetryEventType.RETRY_SCHEDULED,
            failed_attempt_number=prepared.failed_attempt_number,
            retry_attempt_number=attempt.attempt_number,
            next_eligible_at=attempt.next_eligible_at,
        )

    async def fail_retry(
        self,
        prepared: PreparedRetryTransition,
        reason: RetryNotScheduledReason,
    ) -> None:
        try:
            transitioned = (
                await self._required_session().execute(
                    update(task_runs)
                    .where(
                        task_runs.c.id == prepared.task_run_id,
                        task_runs.c.status == TaskRunStatus.RETRY_PENDING.value,
                    )
                    .values(
                        status=TaskRunStatus.FAILED.value,
                        updated_at=func.current_timestamp(),
                    )
                    .returning(task_runs.c.id)
                )
            ).one_or_none()
        except DBAPIError as error:
            raise RetryTransitionPersistenceUnavailable from error
        if transitioned is None:
            raise RetryTransitionPersistenceInvariantViolation
        await self._append_retry_event(
            prepared.task_run_id,
            RetryEventType.RETRY_NOT_SCHEDULED,
            failed_attempt_number=prepared.failed_attempt_number,
            decision_reason=reason,
        )
        try:
            await ensure_dead_letter(
                self._required_session(),
                item_id=uuid4(),
                task_run_id=prepared.task_run_id,
                source_task_attempt_id=prepared.failed_attempt_id,
                reason=DeadLetterReason.RETRY_EXHAUSTED,
            )
        except DeadLetterPersistenceInvariantViolation as error:
            raise RetryTransitionPersistenceInvariantViolation from error
        except IntegrityError as error:
            raise RetryTransitionPersistenceInvariantViolation from error
        except DBAPIError as error:
            raise RetryTransitionPersistenceUnavailable from error

    async def _append_retry_event(
        self,
        task_run_id: UUID,
        event_type: RetryEventType,
        *,
        failed_attempt_number: int | None = None,
        retry_attempt_number: int | None = None,
        next_eligible_at: datetime | None = None,
        decision_reason: RetryNotScheduledReason | None = None,
    ) -> None:
        try:
            await self._required_session().execute(
                insert(task_retry_events).values(
                    id=uuid4(),
                    task_run_id=task_run_id,
                    event_type=event_type.value,
                    failed_attempt_number=failed_attempt_number,
                    retry_attempt_number=retry_attempt_number,
                    next_eligible_at=next_eligible_at,
                    decision_reason=(
                        decision_reason.value if decision_reason is not None else None
                    ),
                )
            )
        except IntegrityError as error:
            raise RetryTransitionPersistenceInvariantViolation from error
        except DBAPIError as error:
            raise RetryTransitionPersistenceUnavailable from error

    async def _pending_transition(self, task: Any) -> PreparedRetryTransition:
        session = self._required_session()
        latest = (
            await session.execute(
                select(
                    task_attempts.c.id,
                    task_attempts.c.attempt_number,
                    task_attempt_results.c.task_attempt_id.label("result_attempt_id"),
                    task_attempt_results.c.result_kind,
                    task_attempt_results.c.completed_at,
                )
                .select_from(
                    task_attempts.outerjoin(
                        task_attempt_results,
                        task_attempt_results.c.task_attempt_id == task_attempts.c.id,
                    )
                )
                .where(task_attempts.c.task_run_id == task.id)
                .order_by(task_attempts.c.attempt_number.desc())
                .limit(1)
            )
        ).one_or_none()
        if (
            latest is None
            or latest.result_attempt_id != latest.id
            or latest.result_kind != TaskExecutionResultKind.RETRYABLE_FAILURE.value
            or latest.completed_at is None
        ):
            raise RetryTransitionPersistenceInvariantViolation
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
                    workflow_version_steps.c.step_identifier == task.step_identifier,
                )
            )
        ).one_or_none()
        if snapshot is None:
            raise RetryTransitionPersistenceInvariantViolation
        return PreparedRetryTransition(
            task.id,
            latest.id,
            latest.attempt_number,
            latest.completed_at,
            deepcopy(snapshot.workflow_policy),
            deepcopy(snapshot.step_policy),
        )

    async def _existing_scheduled(self, task_run_id: UUID) -> ExistingScheduledRetry:
        session = self._required_session()
        scheduled = (
            await session.execute(
                select(
                    task_attempts.c.id,
                    task_attempts.c.attempt_number,
                    task_attempts.c.next_eligible_at,
                )
                .where(task_attempts.c.task_run_id == task_run_id)
                .order_by(task_attempts.c.attempt_number.desc())
                .limit(1)
            )
        ).one_or_none()
        if (
            scheduled is None
            or scheduled.attempt_number <= 1
            or scheduled.next_eligible_at is None
        ):
            raise RetryTransitionPersistenceInvariantViolation
        has_execution_state = await session.scalar(
            select(
                func.count(task_attempt_results.c.task_attempt_id)
                + func.count(task_attempt_claims.c.task_attempt_id)
                + func.count(task_dispatch_outbox.c.id)
            )
            .select_from(
                task_attempts.outerjoin(
                    task_attempt_results,
                    task_attempt_results.c.task_attempt_id == task_attempts.c.id,
                )
                .outerjoin(
                    task_attempt_claims,
                    task_attempt_claims.c.task_attempt_id == task_attempts.c.id,
                )
                .outerjoin(
                    task_dispatch_outbox,
                    task_dispatch_outbox.c.task_attempt_id == task_attempts.c.id,
                )
            )
            .where(task_attempts.c.id == scheduled.id)
        )
        failed = (
            await session.execute(
                select(
                    task_attempts.c.id,
                    task_attempts.c.attempt_number,
                    task_attempt_results.c.result_kind,
                )
                .select_from(
                    task_attempts.join(
                        task_attempt_results,
                        task_attempt_results.c.task_attempt_id == task_attempts.c.id,
                    )
                )
                .where(
                    task_attempts.c.task_run_id == task_run_id,
                    task_attempts.c.attempt_number == scheduled.attempt_number - 1,
                )
            )
        ).one_or_none()
        if (
            has_execution_state != 0
            or failed is None
            or failed.result_kind != TaskExecutionResultKind.RETRYABLE_FAILURE.value
        ):
            raise RetryTransitionPersistenceInvariantViolation
        return ExistingScheduledRetry(
            task_run_id,
            failed.id,
            failed.attempt_number,
            scheduled.id,
            scheduled.attempt_number,
            scheduled.next_eligible_at,
        )

    def _required_session(self) -> AsyncSession:
        if self._session is None:
            raise RuntimeError("retry transition transaction is not active")
        return self._session

    def _required_context(self) -> AbstractAsyncContextManager[AsyncSession]:
        if self._context is None:
            raise RuntimeError("retry transition transaction is not active")
        return self._context


class SQLAlchemyDueRetryDispatchTransaction:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions
        self._context: AbstractAsyncContextManager[AsyncSession] | None = None
        self._session: AsyncSession | None = None

    async def __aenter__(self) -> SQLAlchemyDueRetryDispatchTransaction:
        context = self._sessions.begin()
        self._context = context
        try:
            self._session = await context.__aenter__()
        except DBAPIError as error:
            self._context = None
            raise DueRetryPersistenceUnavailable from error
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
            raise DueRetryPersistenceInvariantViolation from error
        except DBAPIError as error:
            raise DueRetryPersistenceUnavailable from error

    async def prepare_next_due(self) -> DueRetryPreparation:
        session = self._required_session()
        try:
            candidate = (
                await session.execute(_next_due_retry_workflow_lock_statement())
            ).one_or_none()
            if candidate is None:
                return None
            task = (
                await session.execute(
                    select(
                        task_runs.c.id,
                        task_runs.c.workflow_run_id,
                        task_runs.c.workflow_version_id,
                        task_runs.c.step_identifier,
                        task_runs.c.status,
                        task_runs.c.deadline_at,
                        task_runs.c.execution_timeout_seconds,
                    )
                    .where(
                        task_runs.c.id == candidate.task_run_id,
                        task_runs.c.workflow_run_id == candidate.workflow_run_id,
                    )
                    .with_for_update()
                )
            ).one_or_none()
            if task is None:
                raise DueRetryPersistenceInvariantViolation
            if task.status != TaskRunStatus.RETRY_SCHEDULED.value:
                return SkippedDueRetryCandidate(candidate.task_attempt_id)
            if task.workflow_version_id != candidate.workflow_version_id:
                raise DueRetryPersistenceInvariantViolation

            attempt = (
                await session.execute(
                    select(
                        task_attempts.c.id,
                        task_attempts.c.task_run_id,
                        task_attempts.c.attempt_number,
                        task_attempts.c.next_eligible_at,
                    )
                    .where(
                        task_attempts.c.id == candidate.task_attempt_id,
                        task_attempts.c.task_run_id == task.id,
                    )
                    .with_for_update()
                )
            ).one_or_none()
            if attempt is None:
                raise DueRetryPersistenceInvariantViolation
            latest_attempt_number = await session.scalar(
                select(func.max(task_attempts.c.attempt_number)).where(
                    task_attempts.c.task_run_id == task.id
                )
            )
            if (
                attempt.attempt_number <= 1
                or attempt.next_eligible_at is None
                or latest_attempt_number != attempt.attempt_number
            ):
                raise DueRetryPersistenceInvariantViolation
            due = await session.scalar(
                select(attempt.next_eligible_at <= func.statement_timestamp())
            )
            if not due:
                return SkippedDueRetryCandidate(attempt.id)

            execution_state = (
                await session.execute(
                    select(
                        exists(
                            select(1).where(
                                task_dispatch_outbox.c.task_attempt_id == attempt.id
                            )
                        ).label("has_outbox"),
                        exists(
                            select(1).where(
                                task_attempt_claims.c.task_attempt_id == attempt.id
                            )
                        ).label("has_claim"),
                        exists(
                            select(1).where(
                                task_attempt_results.c.task_attempt_id == attempt.id
                            )
                        ).label("has_result"),
                    )
                )
            ).one()
            if any(execution_state):
                raise DueRetryPersistenceInvariantViolation

            predecessor = (
                await session.execute(
                    select(
                        task_attempts.c.id,
                        task_attempts.c.attempt_number,
                        task_attempt_results.c.result_kind,
                        task_attempt_results.c.dispatch_id,
                        task_dispatch_outbox.c.route,
                        task_dispatch_outbox.c.payload,
                    )
                    .select_from(
                        task_attempts.join(
                            task_attempt_results,
                            task_attempt_results.c.task_attempt_id
                            == task_attempts.c.id,
                        ).join(
                            task_dispatch_outbox,
                            task_dispatch_outbox.c.id
                            == task_attempt_results.c.dispatch_id,
                        )
                    )
                    .where(
                        task_attempts.c.task_run_id == task.id,
                        task_attempts.c.attempt_number == attempt.attempt_number - 1,
                        task_dispatch_outbox.c.task_attempt_id == task_attempts.c.id,
                    )
                )
            ).one_or_none()
            if (
                predecessor is None
                or predecessor.result_kind
                != TaskExecutionResultKind.RETRYABLE_FAILURE.value
            ):
                raise DueRetryPersistenceInvariantViolation

            snapshot = (
                await session.execute(
                    select(
                        workflow_version_steps.c.task_type,
                        workflow_version_steps.c.parameters,
                    ).where(
                        workflow_version_steps.c.workflow_version_id
                        == task.workflow_version_id,
                        workflow_version_steps.c.step_identifier
                        == task.step_identifier,
                    )
                )
            ).one_or_none()
            if snapshot is None:
                raise DueRetryPersistenceInvariantViolation
            return PreparedDueRetryDispatch(
                candidate.workflow_run_id,
                task.id,
                task.workflow_version_id,
                task.step_identifier,
                attempt.id,
                attempt.attempt_number,
                attempt.next_eligible_at,
                snapshot.task_type,
                deepcopy(snapshot.parameters),
                task.deadline_at,
                task.execution_timeout_seconds,
                predecessor.id,
                predecessor.attempt_number,
                predecessor.dispatch_id,
                predecessor.route,
                deepcopy(predecessor.payload),
            )
        except DueRetryPersistenceInvariantViolation:
            raise
        except DBAPIError as error:
            raise DueRetryPersistenceUnavailable from error

    async def persist_dispatch(
        self,
        prepared: PreparedDueRetryDispatch,
        outbox_id: UUID,
        route: str,
        payload: dict[str, object],
    ) -> None:
        session = self._required_session()
        try:
            await session.execute(
                insert(task_dispatch_outbox).values(
                    id=outbox_id,
                    task_attempt_id=prepared.task_attempt_id,
                    route=route,
                    payload=payload,
                )
            )
            transitioned = (
                await session.execute(
                    update(task_runs)
                    .where(
                        task_runs.c.id == prepared.task_run_id,
                        task_runs.c.workflow_run_id == prepared.workflow_run_id,
                        task_runs.c.status == TaskRunStatus.RETRY_SCHEDULED.value,
                    )
                    .values(
                        status=TaskRunStatus.DISPATCHED.value,
                        updated_at=func.current_timestamp(),
                    )
                    .returning(task_runs.c.id)
                )
            ).one_or_none()
        except IntegrityError as error:
            raise DueRetryPersistenceInvariantViolation from error
        except DBAPIError as error:
            raise DueRetryPersistenceUnavailable from error
        if transitioned is None:
            raise DueRetryPersistenceInvariantViolation
        try:
            await session.execute(
                insert(task_retry_events).values(
                    id=uuid4(),
                    task_run_id=prepared.task_run_id,
                    event_type=RetryEventType.RETRY_DISPATCHED.value,
                    retry_attempt_number=prepared.attempt_number,
                )
            )
        except IntegrityError as error:
            raise DueRetryPersistenceInvariantViolation from error
        except DBAPIError as error:
            raise DueRetryPersistenceUnavailable from error

    def _required_session(self) -> AsyncSession:
        if self._session is None:
            raise RuntimeError("due retry dispatch transaction is not active")
        return self._session

    def _required_context(self) -> AbstractAsyncContextManager[AsyncSession]:
        if self._context is None:
            raise RuntimeError("due retry dispatch transaction is not active")
        return self._context


def _next_due_retry_workflow_lock_statement() -> Any:
    later_attempt = task_attempts.alias("later_retry_attempt")
    later_exists = exists(
        select(1).where(
            later_attempt.c.task_run_id == task_attempts.c.task_run_id,
            later_attempt.c.attempt_number > task_attempts.c.attempt_number,
        )
    )
    return (
        select(
            workflow_runs.c.id.label("workflow_run_id"),
            workflow_runs.c.workflow_version_id,
            task_runs.c.id.label("task_run_id"),
            task_attempts.c.id.label("task_attempt_id"),
            task_attempts.c.next_eligible_at,
        )
        .select_from(
            task_attempts.join(
                task_runs, task_runs.c.id == task_attempts.c.task_run_id
            ).join(
                workflow_runs,
                workflow_runs.c.id == task_runs.c.workflow_run_id,
            )
        )
        .where(
            task_runs.c.status == TaskRunStatus.RETRY_SCHEDULED.value,
            workflow_runs.c.status.in_(
                (WorkflowRunStatus.PENDING.value, WorkflowRunStatus.RUNNING.value)
            ),
            task_attempts.c.next_eligible_at.is_not(None),
            task_attempts.c.next_eligible_at <= func.statement_timestamp(),
            ~later_exists,
        )
        .order_by(task_attempts.c.next_eligible_at, task_attempts.c.id)
        .limit(1)
        .with_for_update(of=workflow_runs, skip_locked=True)
    )
