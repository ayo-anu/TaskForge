"""Atomic PostgreSQL persistence for retry scheduling transitions."""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from copy import deepcopy
from types import TracebackType
from typing import Any
from uuid import UUID

from sqlalchemy import func, insert, select, update
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.retries.persistence_ports import (
    ExistingScheduledRetry,
    NewScheduledRetryAttempt,
    PreparedRetryTransition,
    RetryTransitionPersistenceInvariantViolation,
    RetryTransitionPersistenceUnavailable,
    RetryTransitionPreparation,
)
from taskforge.runs.domain import TaskRunStatus, WorkflowRunStatus
from taskforge.runs.schema import (
    task_attempt_claims,
    task_attempt_results,
    task_attempts,
    task_dispatch_outbox,
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

    async def fail_retry(self, prepared: PreparedRetryTransition) -> None:
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
