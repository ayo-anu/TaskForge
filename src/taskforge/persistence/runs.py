"""SQLAlchemy persistence for workflow run target resolution."""

from __future__ import annotations

import hmac
from collections.abc import Sequence
from datetime import datetime, timedelta
from types import TracebackType
from typing import Any
from uuid import UUID

from sqlalchemy import (
    Select,
    and_,
    case,
    cast,
    exists,
    func,
    insert,
    or_,
    select,
    true,
    tuple_,
    update,
)
from sqlalchemy.engine import Row
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.identity.authorization import OwnerFilter
from taskforge.retries.domain import (
    InspectedRetryEvent,
    InspectedRetryEventPage,
    InvalidPersistedRetryPolicy,
    RetryEventCursor,
    RetryEventType,
    RetryNotScheduledReason,
    resolve_persisted_retry_policy,
)
from taskforge.runs.domain import (
    AcceptedWorkflowRunCancellation,
    CancellationFinalizationOutcome,
    CancellationFinalizationResult,
    CancellationPropagationResult,
    CancellationSettlementResult,
    CreatedWorkflowRun,
    DependencyFailurePropagationResult,
    ExplicitWorkflowVersion,
    InspectedTaskRun,
    InspectedWorkflowRun,
    InspectedWorkflowRunCancellation,
    NewTaskRun,
    NewWorkflowRun,
    RunFailureReason,
    RunnableTransitionResult,
    TaskRunStatus,
    WorkflowRunCancellationCaveat,
    WorkflowRunCancellationCommand,
    WorkflowRunEvaluationResult,
    WorkflowRunIdempotency,
    WorkflowRunInput,
    WorkflowRunStatus,
    WorkflowRunVersionDependency,
    WorkflowRunVersionSnapshot,
    WorkflowRunVersionStep,
    WorkflowVersionSelection,
)
from taskforge.runs.persistence_ports import (
    ExistingIdempotentWorkflowRun,
    IdempotentCreationPreparation,
    PersistedCancellationOutcome,
    PersistedWorkflowRunCancellation,
    PreparedWorkflowRunCreation,
    WorkflowRunCancellationPersistenceInvariantViolation,
    WorkflowRunIdempotencyRecordConflict,
    WorkflowRunInspectionInvariantViolation,
    WorkflowRunPersistenceUnavailable,
    WorkflowRunRecordConflict,
    WorkflowRunTimestamps,
    WorkflowVersionResolutionRecord,
)
from taskforge.runs.schema import (
    task_attempt_claims,
    task_attempt_results,
    task_attempts,
    task_result_events,
    task_retry_events,
    task_runs,
    workflow_run_cancellation_requests,
    workflow_run_idempotency,
    workflow_run_inputs,
    workflow_runs,
)
from taskforge.worker.results import (
    TaskExecutionFailureKind,
    TaskExecutionResultKind,
)
from taskforge.workflows.domain import (
    WorkflowDefinitionStatus,
    resolve_deadline_seconds,
    resolve_execution_timeout_seconds,
)
from taskforge.workflows.schema import (
    workflow_definitions,
    workflow_version_dependencies,
    workflow_version_steps,
    workflow_versions,
)

POSTGRES_UNIQUE_VIOLATION = "23505"
IDEMPOTENCY_SCOPE_CONSTRAINT = "pk_workflow_run_idempotency"


class SQLAlchemyWorkflowRunRepository:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    def creation_transaction(self) -> SQLAlchemyWorkflowRunCreationTransaction:
        return SQLAlchemyWorkflowRunCreationTransaction(self._sessions)

    async def cancel_run(
        self,
        workflow_run_id: UUID,
        owner_filter: OwnerFilter,
        command: WorkflowRunCancellationCommand,
    ) -> PersistedWorkflowRunCancellation | None:
        if command.workflow_run_id != workflow_run_id:
            raise ValueError("cancellation command targets a different workflow run")
        try:
            async with self._sessions.begin() as session:
                locked = (
                    await session.execute(
                        _workflow_run_cancellation_lock_statement(
                            workflow_run_id, owner_filter
                        )
                    )
                ).one_or_none()
                if locked is None:
                    return None
                try:
                    status = WorkflowRunStatus(locked.status)
                except ValueError as error:
                    raise WorkflowRunCancellationPersistenceInvariantViolation from error
                existing = (
                    await session.execute(
                        _workflow_run_cancellation_request_statement(workflow_run_id)
                    )
                ).one_or_none()
                if existing is not None:
                    if status not in (
                        WorkflowRunStatus.CANCELLING,
                        WorkflowRunStatus.CANCELLED,
                    ):
                        raise WorkflowRunCancellationPersistenceInvariantViolation
                    canonical = _accepted_cancellation(existing)
                    same_scope_and_key = (
                        existing.requested_by_principal_id
                        == command.requested_by_principal_id
                        and existing.idempotency_key_digest
                        == command.idempotency.key_digest
                    )
                    if same_scope_and_key:
                        outcome = (
                            PersistedCancellationOutcome.EXACT_RETRY
                            if hmac.compare_digest(
                                existing.request_fingerprint,
                                command.idempotency.request_fingerprint,
                            )
                            else PersistedCancellationOutcome.IDEMPOTENCY_CONFLICT
                        )
                    else:
                        outcome = (
                            PersistedCancellationOutcome.ALREADY_CANCELLING
                            if status is WorkflowRunStatus.CANCELLING
                            else PersistedCancellationOutcome.ALREADY_CANCELLED
                        )
                    if (
                        outcome is PersistedCancellationOutcome.EXACT_RETRY
                        and status is WorkflowRunStatus.CANCELLING
                    ):
                        await _suppress_unstarted_task_rows(session, workflow_run_id)
                        await _settle_dispatched_task_rows(session, workflow_run_id)
                    return PersistedWorkflowRunCancellation(
                        workflow_run_id, outcome, status, canonical
                    )

                if status in (
                    WorkflowRunStatus.CANCELLING,
                    WorkflowRunStatus.CANCELLED,
                ):
                    raise WorkflowRunCancellationPersistenceInvariantViolation
                if status in (
                    WorkflowRunStatus.SUCCEEDED,
                    WorkflowRunStatus.FAILED,
                ):
                    return PersistedWorkflowRunCancellation(
                        workflow_run_id,
                        PersistedCancellationOutcome.TERMINAL_STATE_WON,
                        status,
                    )
                if status not in (
                    WorkflowRunStatus.PENDING,
                    WorkflowRunStatus.RUNNING,
                ):
                    raise WorkflowRunCancellationPersistenceInvariantViolation

                inserted = (
                    await session.execute(
                        insert(workflow_run_cancellation_requests)
                        .values(
                            workflow_run_id=workflow_run_id,
                            requested_by_principal_id=(
                                command.requested_by_principal_id
                            ),
                            idempotency_key_digest=command.idempotency.key_digest,
                            request_fingerprint=(
                                command.idempotency.request_fingerprint
                            ),
                            reason=command.reason,
                        )
                        .returning(*workflow_run_cancellation_requests.c)
                    )
                ).one()
                transitioned = (
                    await session.execute(
                        _workflow_run_to_cancelling_statement(
                            workflow_run_id, status, inserted.requested_at
                        )
                    )
                ).one_or_none()
                if transitioned is None:
                    raise WorkflowRunCancellationPersistenceInvariantViolation
                await _suppress_unstarted_task_rows(session, workflow_run_id)
                await _settle_dispatched_task_rows(session, workflow_run_id)
                return PersistedWorkflowRunCancellation(
                    workflow_run_id,
                    PersistedCancellationOutcome.NEWLY_ACCEPTED,
                    WorkflowRunStatus.CANCELLING,
                    _accepted_cancellation(inserted),
                )
        except WorkflowRunCancellationPersistenceInvariantViolation:
            raise
        except IntegrityError as error:
            raise WorkflowRunCancellationPersistenceInvariantViolation from error
        except DBAPIError as error:
            raise WorkflowRunPersistenceUnavailable from error

    async def suppress_unstarted_tasks(
        self,
        workflow_run_id: UUID,
    ) -> CancellationPropagationResult:
        """Cancel pre-dispatch task states under one workflow-run lock."""
        try:
            async with self._sessions.begin() as session:
                locked = (
                    await session.execute(
                        _workflow_run_evaluation_lock_statement(workflow_run_id)
                    )
                ).one_or_none()
                if locked is None:
                    return CancellationPropagationResult(
                        workflow_run_id, False, None, (), ()
                    )
                try:
                    status = WorkflowRunStatus(locked.status)
                except ValueError as error:
                    raise WorkflowRunCancellationPersistenceInvariantViolation from error
                rows = (
                    await _suppress_unstarted_task_rows(session, workflow_run_id)
                    if status is WorkflowRunStatus.CANCELLING
                    else ()
                )
        except WorkflowRunCancellationPersistenceInvariantViolation:
            raise
        except DBAPIError as error:
            raise WorkflowRunPersistenceUnavailable from error

        ordered = sorted(rows, key=lambda row: (row.step_identifier, row.id))
        return CancellationPropagationResult(
            workflow_run_id,
            True,
            status,
            tuple(row.id for row in ordered),
            tuple(row.step_identifier for row in ordered),
        )

    async def settle_dispatched_tasks(
        self,
        workflow_run_id: UUID,
    ) -> CancellationSettlementResult:
        """Cancel unclaimed dispatched work under one workflow-run lock."""
        try:
            async with self._sessions.begin() as session:
                locked = (
                    await session.execute(
                        _workflow_run_evaluation_lock_statement(workflow_run_id)
                    )
                ).one_or_none()
                if locked is None:
                    return CancellationSettlementResult(
                        workflow_run_id, False, None, (), ()
                    )
                try:
                    status = WorkflowRunStatus(locked.status)
                except ValueError as error:
                    raise WorkflowRunCancellationPersistenceInvariantViolation from error
                rows = (
                    await _settle_dispatched_task_rows(session, workflow_run_id)
                    if status is WorkflowRunStatus.CANCELLING
                    else ()
                )
        except WorkflowRunCancellationPersistenceInvariantViolation:
            raise
        except DBAPIError as error:
            raise WorkflowRunPersistenceUnavailable from error

        ordered = sorted(rows, key=lambda row: (row.step_identifier, row.id))
        return CancellationSettlementResult(
            workflow_run_id,
            True,
            status,
            tuple(row.id for row in ordered),
            tuple(row.step_identifier for row in ordered),
        )

    async def finalize_workflow_run_cancellation(
        self,
        workflow_run_id: UUID,
    ) -> CancellationFinalizationResult:
        """Derive cancelling-to-cancelled under the shared workflow-run lock."""
        try:
            async with self._sessions.begin() as session:
                locked = (
                    await session.execute(
                        _workflow_run_evaluation_lock_statement(workflow_run_id)
                    )
                ).one_or_none()
                if locked is None:
                    return CancellationFinalizationResult(
                        workflow_run_id, False, None, None, None
                    )
                try:
                    previous = WorkflowRunStatus(locked.status)
                except ValueError as error:
                    raise WorkflowRunCancellationPersistenceInvariantViolation from error

                request_count = await session.scalar(
                    select(func.count())
                    .select_from(workflow_run_cancellation_requests)
                    .where(
                        workflow_run_cancellation_requests.c.workflow_run_id
                        == workflow_run_id
                    )
                )
                cancellation_owned = previous in (
                    WorkflowRunStatus.CANCELLING,
                    WorkflowRunStatus.CANCELLED,
                )
                if cancellation_owned and request_count != 1:
                    raise WorkflowRunCancellationPersistenceInvariantViolation
                if not cancellation_owned:
                    if request_count:
                        raise WorkflowRunCancellationPersistenceInvariantViolation
                    return CancellationFinalizationResult(
                        workflow_run_id,
                        True,
                        previous,
                        previous,
                        CancellationFinalizationOutcome.NOT_CANCELLING,
                    )

                invalid_claim_result = await session.scalar(
                    _workflow_cancellation_claim_result_violation_statement(
                        workflow_run_id
                    )
                )
                if invalid_claim_result:
                    raise WorkflowRunCancellationPersistenceInvariantViolation
                unsettled = await session.scalar(
                    _workflow_cancellation_unsettled_exists_statement(workflow_run_id)
                )
                if unsettled:
                    if previous is WorkflowRunStatus.CANCELLED:
                        raise WorkflowRunCancellationPersistenceInvariantViolation
                    return CancellationFinalizationResult(
                        workflow_run_id,
                        True,
                        previous,
                        previous,
                        CancellationFinalizationOutcome.AWAITING_TASK_SETTLEMENT,
                    )
                if previous is WorkflowRunStatus.CANCELLED:
                    return CancellationFinalizationResult(
                        workflow_run_id,
                        True,
                        previous,
                        previous,
                        CancellationFinalizationOutcome.ALREADY_CANCELLED,
                    )

                transitioned = (
                    await session.execute(
                        update(workflow_runs)
                        .where(
                            workflow_runs.c.id == workflow_run_id,
                            workflow_runs.c.status
                            == WorkflowRunStatus.CANCELLING.value,
                        )
                        .values(
                            status=WorkflowRunStatus.CANCELLED.value,
                            updated_at=func.statement_timestamp(),
                        )
                        .returning(workflow_runs.c.status)
                    )
                ).one_or_none()
                if transitioned is None:
                    raise WorkflowRunCancellationPersistenceInvariantViolation
                return CancellationFinalizationResult(
                    workflow_run_id,
                    True,
                    previous,
                    WorkflowRunStatus.CANCELLED,
                    CancellationFinalizationOutcome.FINALIZED,
                )
        except WorkflowRunCancellationPersistenceInvariantViolation:
            raise
        except DBAPIError as error:
            raise WorkflowRunPersistenceUnavailable from error

    async def find_idempotent_run(
        self,
        principal_id: UUID,
        workflow_id: UUID,
        key_digest: str,
    ) -> ExistingIdempotentWorkflowRun | None:
        try:
            async with self._sessions() as session, session.begin():
                row = (
                    await session.execute(
                        _idempotent_run_statement(principal_id, workflow_id, key_digest)
                    )
                ).one_or_none()
        except DBAPIError as error:
            raise WorkflowRunPersistenceUnavailable from error
        return _existing_idempotent_run(row) if row is not None else None

    async def get_run(
        self,
        run_id: UUID,
        owner_principal_id: UUID,
    ) -> InspectedWorkflowRun | None:
        try:
            async with self._sessions() as session, session.begin():
                row = (
                    await session.execute(
                        _run_inspection_statement(run_id, owner_principal_id)
                    )
                ).one_or_none()
        except DBAPIError as error:
            raise WorkflowRunPersistenceUnavailable from error
        return _inspected_run(row) if row is not None else None

    async def list_task_runs(
        self,
        run_id: UUID,
        owner_principal_id: UUID,
    ) -> tuple[InspectedTaskRun, ...] | None:
        try:
            async with self._sessions() as session, session.begin():
                exists = await session.scalar(
                    _owner_scoped_run_exists_statement(run_id, owner_principal_id)
                )
                if not exists:
                    return None
                rows = (
                    await session.execute(
                        _task_run_list_statement(run_id, owner_principal_id)
                    )
                ).all()
        except DBAPIError as error:
            raise WorkflowRunPersistenceUnavailable from error
        return tuple(_inspected_task_run(row) for row in rows)

    async def get_task_run(
        self,
        task_run_id: UUID,
        owner_principal_id: UUID,
    ) -> InspectedTaskRun | None:
        try:
            async with self._sessions() as session, session.begin():
                row = (
                    await session.execute(
                        _task_run_inspection_statement(task_run_id, owner_principal_id)
                    )
                ).one_or_none()
        except DBAPIError as error:
            raise WorkflowRunPersistenceUnavailable from error
        return _inspected_task_run(row) if row is not None else None

    async def list_retry_events(
        self,
        task_run_id: UUID,
        owner_principal_id: UUID,
        *,
        limit: int,
        cursor: RetryEventCursor | None,
    ) -> InspectedRetryEventPage | None:
        try:
            async with self._sessions() as session, session.begin():
                if not await session.scalar(
                    _owner_scoped_task_exists_statement(task_run_id, owner_principal_id)
                ):
                    return None
                rows = (
                    await session.execute(
                        _retry_event_history_statement(
                            task_run_id,
                            owner_principal_id,
                            limit=limit,
                            cursor=cursor,
                        )
                    )
                ).all()
        except DBAPIError as error:
            raise WorkflowRunPersistenceUnavailable from error
        page_rows = rows[:limit]
        try:
            items = tuple(_inspected_retry_event(row) for row in page_rows)
        except (TypeError, ValueError) as error:
            raise WorkflowRunInspectionInvariantViolation from error
        next_cursor = None
        if len(rows) > limit and items:
            last = items[-1]
            next_cursor = RetryEventCursor(last.task_run_id, last.occurred_at, last.id)
        return InspectedRetryEventPage(items, next_cursor)

    async def transition_runnable_tasks(
        self,
        workflow_run_id: UUID,
    ) -> RunnableTransitionResult:
        """Own dependency evaluation and persist only blocked-to-runnable moves."""
        statement = _runnable_transition_statement(workflow_run_id)
        try:
            async with self._sessions.begin() as session:
                rows = (await session.execute(statement)).all()
        except DBAPIError as error:
            raise WorkflowRunPersistenceUnavailable from error

        # PostgreSQL does not guarantee RETURNING order. Stabilize the boundary.
        ordered = sorted(rows, key=lambda row: (row.step_identifier, row.id))
        return RunnableTransitionResult(
            workflow_run_id=workflow_run_id,
            transitioned_task_ids=tuple(row.id for row in ordered),
            transitioned_step_identifiers=tuple(row.step_identifier for row in ordered),
        )

    async def propagate_dependency_failures(
        self,
        workflow_run_id: UUID,
    ) -> DependencyFailurePropagationResult:
        """Own immutable dependency traversal and blocked-to-skipped persistence."""
        try:
            async with self._sessions.begin() as session:
                # The run row is the shared progression lock. It serializes this
                # operation with runnable and future run-state transitions while
                # allowing unrelated runs to progress independently.
                run = (
                    await session.execute(
                        _active_run_progression_lock_statement(workflow_run_id)
                    )
                ).one_or_none()
                if run is None:
                    rows: Sequence[Row[Any]] = ()
                else:
                    rows = (
                        await session.execute(
                            _dependency_failure_propagation_statement(
                                workflow_run_id, run.workflow_version_id
                            )
                        )
                    ).all()
        except DBAPIError as error:
            raise WorkflowRunPersistenceUnavailable from error

        # PostgreSQL does not guarantee RETURNING order. Stabilize the boundary.
        ordered = sorted(rows, key=lambda row: (row.step_identifier, row.id))
        return DependencyFailurePropagationResult(
            workflow_run_id=workflow_run_id,
            skipped_task_ids=tuple(row.id for row in ordered),
            skipped_step_identifiers=tuple(row.step_identifier for row in ordered),
        )

    async def evaluate_workflow_run_state(
        self,
        workflow_run_id: UUID,
    ) -> WorkflowRunEvaluationResult:
        """Persist at most one workflow transition under the progression lock."""
        try:
            async with self._sessions.begin() as session:
                locked = (
                    await session.execute(
                        _workflow_run_evaluation_lock_statement(workflow_run_id)
                    )
                ).one_or_none()
                if locked is None:
                    return WorkflowRunEvaluationResult(
                        workflow_run_id, False, None, None
                    )

                previous_status = WorkflowRunStatus(locked.status)
                statement: Any | None = None
                if previous_status is WorkflowRunStatus.PENDING:
                    statement = _pending_to_running_statement(workflow_run_id)
                elif previous_status is WorkflowRunStatus.RUNNING:
                    statement = _running_terminal_transition_statement(workflow_run_id)

                resulting_status = previous_status
                if statement is not None:
                    transitioned = (await session.execute(statement)).one_or_none()
                    if transitioned is not None:
                        resulting_status = WorkflowRunStatus(transitioned.status)

                # Source-status branching above deliberately permits at most one
                # guarded UPDATE. A selected transition ends this invocation.
                return WorkflowRunEvaluationResult(
                    workflow_run_id,
                    True,
                    previous_status,
                    resulting_status,
                )
        except DBAPIError as error:
            raise WorkflowRunPersistenceUnavailable from error

    async def resolve_workflow_version(
        self,
        workflow_id: UUID,
        owner_principal_id: UUID,
        selection: WorkflowVersionSelection,
    ) -> WorkflowVersionResolutionRecord | None:
        statement = _version_resolution_statement(
            workflow_id, owner_principal_id, selection
        )
        try:
            async with self._sessions() as session, session.begin():
                row = (await session.execute(statement)).one_or_none()
        except DBAPIError as error:
            raise WorkflowRunPersistenceUnavailable from error
        if row is None:
            return None
        return WorkflowVersionResolutionRecord(
            workflow_definition_id=row.workflow_definition_id,
            status=WorkflowDefinitionStatus(row.status),
            workflow_version_id=row.workflow_version_id,
            version_number=row.version_number,
        )


class SQLAlchemyWorkflowRunCreationTransaction:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions
        self._session: AsyncSession | None = None
        self._committed = False

    async def __aenter__(self) -> SQLAlchemyWorkflowRunCreationTransaction:
        self._committed = False
        session = self._sessions()
        self._session = session
        try:
            await session.begin()
        except DBAPIError as error:
            self._session = None
            try:
                await session.close()
            except BaseException:
                pass
            raise WorkflowRunPersistenceUnavailable from error
        except BaseException:
            self._session = None
            try:
                await session.close()
            except BaseException:
                pass
            raise
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exception_type, exception, traceback
        session, self._session = self._required_session(), None
        try:
            if not self._committed:
                await session.rollback()
        finally:
            await session.close()

    async def prepare_creation_target(
        self,
        workflow_id: UUID,
        owner_principal_id: UUID,
        selection: WorkflowVersionSelection,
    ) -> PreparedWorkflowRunCreation | None:
        session = self._required_session()
        try:
            definition = (
                await session.execute(
                    _definition_lock_statement(workflow_id, owner_principal_id)
                )
            ).one_or_none()
            if definition is None:
                return None
            status = WorkflowDefinitionStatus(definition.status)
            if status is not WorkflowDefinitionStatus.ENABLED:
                return PreparedWorkflowRunCreation(workflow_id, status, None)
            version = (
                await session.execute(_locked_version_statement(workflow_id, selection))
            ).one_or_none()
            if version is None:
                return PreparedWorkflowRunCreation(workflow_id, status, None)
            step_rows = (
                await session.execute(
                    select(
                        workflow_version_steps.c.step_identifier,
                        workflow_version_steps.c.execution_policy,
                    )
                    .where(workflow_version_steps.c.workflow_version_id == version.id)
                    .order_by(workflow_version_steps.c.step_identifier)
                )
            ).all()
            dependency_rows = (
                await session.execute(
                    select(
                        workflow_version_dependencies.c.predecessor_step_identifier,
                        workflow_version_dependencies.c.successor_step_identifier,
                    )
                    .where(
                        workflow_version_dependencies.c.workflow_version_id
                        == version.id
                    )
                    .order_by(
                        workflow_version_dependencies.c.predecessor_step_identifier,
                        workflow_version_dependencies.c.successor_step_identifier,
                    )
                )
            ).all()
        except DBAPIError as error:
            raise WorkflowRunPersistenceUnavailable from error
        snapshot = _creation_snapshot(version, step_rows, dependency_rows)
        return PreparedWorkflowRunCreation(workflow_id, status, snapshot)

    async def prepare_idempotent_creation(
        self,
        workflow_id: UUID,
        owner_principal_id: UUID,
        principal_id: UUID,
        selection: WorkflowVersionSelection,
        key_digest: str,
    ) -> IdempotentCreationPreparation | None:
        session = self._required_session()
        try:
            definition = (
                await session.execute(
                    _definition_lock_statement(workflow_id, owner_principal_id)
                )
            ).one_or_none()
            if definition is None:
                return None
            existing = (
                await session.execute(
                    _idempotent_run_statement(principal_id, workflow_id, key_digest)
                )
            ).one_or_none()
            if existing is not None:
                return _existing_idempotent_run(existing)
            status = WorkflowDefinitionStatus(definition.status)
            if status is not WorkflowDefinitionStatus.ENABLED:
                return PreparedWorkflowRunCreation(workflow_id, status, None)
            version = (
                await session.execute(_locked_version_statement(workflow_id, selection))
            ).one_or_none()
            if version is None:
                return PreparedWorkflowRunCreation(workflow_id, status, None)
            step_rows = (
                await session.execute(
                    select(
                        workflow_version_steps.c.step_identifier,
                        workflow_version_steps.c.execution_policy,
                    )
                    .where(workflow_version_steps.c.workflow_version_id == version.id)
                    .order_by(workflow_version_steps.c.step_identifier)
                )
            ).all()
            dependency_rows = (
                await session.execute(
                    select(
                        workflow_version_dependencies.c.predecessor_step_identifier,
                        workflow_version_dependencies.c.successor_step_identifier,
                    )
                    .where(
                        workflow_version_dependencies.c.workflow_version_id
                        == version.id
                    )
                    .order_by(
                        workflow_version_dependencies.c.predecessor_step_identifier,
                        workflow_version_dependencies.c.successor_step_identifier,
                    )
                )
            ).all()
        except DBAPIError as error:
            raise WorkflowRunPersistenceUnavailable from error
        return PreparedWorkflowRunCreation(
            workflow_id,
            status,
            _creation_snapshot(version, step_rows, dependency_rows),
        )

    async def insert_complete_run(
        self,
        prepared: PreparedWorkflowRunCreation,
        run: NewWorkflowRun,
        input_snapshot: WorkflowRunInput,
        task_run_values: tuple[NewTaskRun, ...],
        idempotency: WorkflowRunIdempotency | None = None,
    ) -> WorkflowRunTimestamps:
        snapshot = prepared.snapshot
        if snapshot is None:
            raise ValueError("prepared creation has no workflow version snapshot")
        session = self._required_session()
        try:
            timestamps = await insert_complete_workflow_run(
                session,
                prepared,
                run,
                input_snapshot,
                task_run_values,
            )
            if idempotency is not None:
                await session.execute(
                    insert(workflow_run_idempotency).values(
                        principal_id=run.requested_by_principal_id,
                        workflow_definition_id=prepared.workflow_definition_id,
                        idempotency_key_digest=idempotency.key_digest,
                        request_fingerprint=idempotency.request_fingerprint,
                        workflow_run_id=run.id,
                    )
                )
        except IntegrityError as error:
            if _is_idempotency_scope_conflict(error):
                raise WorkflowRunIdempotencyRecordConflict from error
            raise WorkflowRunRecordConflict from error
        except DBAPIError as error:
            raise WorkflowRunPersistenceUnavailable from error
        return timestamps

    async def commit(self) -> None:
        try:
            await self._required_session().commit()
        except IntegrityError as error:
            raise WorkflowRunRecordConflict from error
        except DBAPIError as error:
            raise WorkflowRunPersistenceUnavailable from error
        self._committed = True

    def _required_session(self) -> AsyncSession:
        if self._session is None:
            raise RuntimeError("workflow run creation transaction is not active")
        return self._session


def _locked_version_statement(
    workflow_id: UUID,
    selection: WorkflowVersionSelection,
) -> Select[Any]:
    statement = select(
        workflow_versions.c.id,
        workflow_versions.c.workflow_definition_id,
        workflow_versions.c.version_number,
        workflow_versions.c.execution_policy,
    ).where(workflow_versions.c.workflow_definition_id == workflow_id)
    if isinstance(selection, ExplicitWorkflowVersion):
        return statement.where(
            workflow_versions.c.version_number == selection.version_number
        )
    return statement.order_by(workflow_versions.c.version_number.desc()).limit(1)


async def load_exact_workflow_version_snapshot(
    session: AsyncSession,
    workflow_definition_id: UUID,
    workflow_version_id: UUID,
) -> WorkflowRunVersionSnapshot | None:
    """Load one exact immutable version using the caller's transaction."""
    version = (
        await session.execute(
            select(
                workflow_versions.c.id,
                workflow_versions.c.workflow_definition_id,
                workflow_versions.c.version_number,
                workflow_versions.c.execution_policy,
            ).where(
                workflow_versions.c.id == workflow_version_id,
                workflow_versions.c.workflow_definition_id == workflow_definition_id,
            )
        )
    ).one_or_none()
    if version is None:
        return None
    steps = (
        await session.execute(
            select(
                workflow_version_steps.c.step_identifier,
                workflow_version_steps.c.execution_policy,
            )
            .where(workflow_version_steps.c.workflow_version_id == workflow_version_id)
            .order_by(workflow_version_steps.c.step_identifier)
        )
    ).all()
    dependencies = (
        await session.execute(
            select(
                workflow_version_dependencies.c.predecessor_step_identifier,
                workflow_version_dependencies.c.successor_step_identifier,
            )
            .where(
                workflow_version_dependencies.c.workflow_version_id
                == workflow_version_id
            )
            .order_by(
                workflow_version_dependencies.c.predecessor_step_identifier,
                workflow_version_dependencies.c.successor_step_identifier,
            )
        )
    ).all()
    return _creation_snapshot(version, steps, dependencies)


async def insert_complete_workflow_run(
    session: AsyncSession,
    prepared: PreparedWorkflowRunCreation,
    run: NewWorkflowRun,
    input_snapshot: WorkflowRunInput,
    task_run_values: tuple[NewTaskRun, ...],
) -> WorkflowRunTimestamps:
    """Insert one complete run graph without owning or committing the transaction."""
    snapshot = prepared.snapshot
    if snapshot is None:
        raise ValueError("prepared creation has no workflow version snapshot")
    row = (
        await session.execute(
            insert(workflow_runs)
            .values(
                id=run.id,
                workflow_definition_id=prepared.workflow_definition_id,
                workflow_version_id=snapshot.workflow_version_id,
                requested_by_principal_id=run.requested_by_principal_id,
                status=run.status.value,
            )
            .returning(workflow_runs.c.created_at, workflow_runs.c.updated_at)
        )
    ).one()
    await session.execute(
        insert(workflow_run_inputs).values(
            workflow_run_id=run.id,
            payload=input_snapshot.payload,
            input_references=input_snapshot.input_references,
        )
    )
    if task_run_values:
        await session.execute(
            insert(task_runs),
            [
                {
                    "id": task.id,
                    "workflow_run_id": run.id,
                    "workflow_version_id": snapshot.workflow_version_id,
                    "step_identifier": task.step_identifier,
                    "status": task.status.value,
                    "deadline_at": (
                        row.created_at + timedelta(seconds=task.deadline_seconds)
                        if task.deadline_seconds is not None
                        else None
                    ),
                    "execution_timeout_seconds": task.execution_timeout_seconds,
                }
                for task in task_run_values
            ],
        )
    return WorkflowRunTimestamps(row.created_at, row.updated_at)


def _definition_lock_statement(
    workflow_id: UUID,
    owner_principal_id: UUID,
) -> Select[Any]:
    return (
        select(
            workflow_definitions.c.id,
            workflow_definitions.c.status,
        )
        .where(
            workflow_definitions.c.id == workflow_id,
            workflow_definitions.c.owner_principal_id == owner_principal_id,
        )
        .with_for_update()
    )


def _idempotent_run_statement(
    principal_id: UUID,
    workflow_id: UUID,
    key_digest: str,
) -> Select[Any]:
    total_tasks = (
        select(func.count())
        .select_from(task_runs)
        .where(task_runs.c.workflow_run_id == workflow_runs.c.id)
        .scalar_subquery()
    )
    runnable_tasks = (
        select(func.count())
        .select_from(task_runs)
        .where(
            task_runs.c.workflow_run_id == workflow_runs.c.id,
            task_runs.c.status == "runnable",
        )
        .scalar_subquery()
    )
    blocked_tasks = (
        select(func.count())
        .select_from(task_runs)
        .where(
            task_runs.c.workflow_run_id == workflow_runs.c.id,
            task_runs.c.status == "blocked",
        )
        .scalar_subquery()
    )
    return (
        select(
            workflow_run_idempotency.c.request_fingerprint,
            workflow_runs.c.id.label("run_id"),
            workflow_runs.c.workflow_definition_id,
            workflow_runs.c.workflow_version_id,
            workflow_runs.c.requested_by_principal_id,
            workflow_runs.c.status,
            workflow_runs.c.created_at,
            workflow_versions.c.version_number,
            total_tasks.label("task_count"),
            runnable_tasks.label("runnable_task_count"),
            blocked_tasks.label("blocked_task_count"),
        )
        .select_from(
            workflow_run_idempotency.join(
                workflow_runs,
                workflow_runs.c.id == workflow_run_idempotency.c.workflow_run_id,
            ).join(
                workflow_versions,
                workflow_versions.c.id == workflow_runs.c.workflow_version_id,
            )
        )
        .where(
            workflow_run_idempotency.c.principal_id == principal_id,
            workflow_run_idempotency.c.workflow_definition_id == workflow_id,
            workflow_run_idempotency.c.idempotency_key_digest == key_digest,
        )
    )


def _run_inspection_statement(
    run_id: UUID,
    owner_principal_id: UUID,
) -> Select[Any]:
    recovered_cancellation_count = (
        select(func.count(task_result_events.c.id))
        .select_from(
            task_result_events.join(
                task_attempts,
                task_attempts.c.id == task_result_events.c.task_attempt_id,
            )
            .join(task_runs, task_runs.c.id == task_attempts.c.task_run_id)
            .join(
                task_attempt_results,
                and_(
                    task_attempt_results.c.task_attempt_id
                    == task_result_events.c.task_attempt_id,
                    task_attempt_results.c.claim_generation
                    == task_result_events.c.claim_generation,
                ),
            )
        )
        .where(
            task_runs.c.workflow_run_id == workflow_runs.c.id,
            task_result_events.c.event_type == "result_recovered",
            task_attempt_results.c.result_kind
            == TaskExecutionResultKind.CANCELLATION.value,
        )
        .scalar_subquery()
    )
    return (
        select(
            workflow_runs.c.id,
            workflow_runs.c.workflow_definition_id,
            workflow_runs.c.workflow_version_id,
            workflow_versions.c.version_number,
            workflow_runs.c.requested_by_principal_id,
            workflow_runs.c.status,
            workflow_runs.c.created_at,
            workflow_runs.c.updated_at,
            case(
                (
                    workflow_runs.c.status == WorkflowRunStatus.FAILED.value,
                    RunFailureReason.TASK_FAILED.value,
                ),
                else_=None,
            ).label("failure_reason"),
            workflow_run_cancellation_requests.c.requested_by_principal_id.label(
                "cancellation_requested_by_principal_id"
            ),
            workflow_run_cancellation_requests.c.reason.label("cancellation_reason"),
            workflow_run_cancellation_requests.c.requested_at.label(
                "cancellation_requested_at"
            ),
            recovered_cancellation_count.label("recovered_cancellation_count"),
        )
        .select_from(
            workflow_runs.join(
                workflow_definitions,
                workflow_definitions.c.id == workflow_runs.c.workflow_definition_id,
            ).join(
                workflow_versions,
                workflow_versions.c.id == workflow_runs.c.workflow_version_id,
            ).outerjoin(
                workflow_run_cancellation_requests,
                workflow_run_cancellation_requests.c.workflow_run_id
                == workflow_runs.c.id,
            )
        )
        .where(
            workflow_runs.c.id == run_id,
            workflow_definitions.c.owner_principal_id == owner_principal_id,
        )
    )


def _owner_scoped_run_exists_statement(
    run_id: UUID,
    owner_principal_id: UUID,
) -> Select[Any]:
    return (
        select(workflow_runs.c.id)
        .select_from(
            workflow_runs.join(
                workflow_definitions,
                workflow_definitions.c.id == workflow_runs.c.workflow_definition_id,
            )
        )
        .where(
            workflow_runs.c.id == run_id,
            workflow_definitions.c.owner_principal_id == owner_principal_id,
        )
    )


def _task_run_projection() -> tuple[tuple[Any, ...], Any]:
    attempt_aggregates = (
        select(
            task_attempts.c.task_run_id,
            func.count(task_attempts.c.id).label("attempt_count"),
            func.min(task_attempts.c.attempt_number).label("minimum_attempt_number"),
            func.max(task_attempts.c.attempt_number).label("maximum_attempt_number"),
        )
        .group_by(task_attempts.c.task_run_id)
        .subquery("task_attempt_aggregates")
    )
    ranked_attempts = select(
        task_attempts.c.task_run_id,
        task_attempts.c.next_eligible_at,
        func.row_number()
        .over(
            partition_by=task_attempts.c.task_run_id,
            order_by=task_attempts.c.attempt_number.desc(),
        )
        .label("attempt_rank"),
    ).subquery("ranked_task_attempts")
    ranked_failures = (
        select(
            task_attempts.c.task_run_id,
            task_attempt_results.c.failure_kind,
            func.row_number()
            .over(
                partition_by=task_attempts.c.task_run_id,
                order_by=task_attempts.c.attempt_number.desc(),
            )
            .label("failure_rank"),
        )
        .select_from(
            task_attempts.join(
                task_attempt_results,
                task_attempt_results.c.task_attempt_id == task_attempts.c.id,
            )
        )
        .where(task_attempt_results.c.failure_kind.is_not(None))
        .subquery("ranked_task_failures")
    )
    columns = (
        task_runs.c.id,
        task_runs.c.workflow_run_id,
        task_runs.c.workflow_version_id,
        task_runs.c.step_identifier,
        task_runs.c.status,
        task_runs.c.created_at,
        task_runs.c.updated_at,
        case(
            (
                task_runs.c.status == TaskRunStatus.FAILED.value,
                RunFailureReason.TASK_FAILED.value,
            ),
            (
                task_runs.c.status == TaskRunStatus.SKIPPED.value,
                RunFailureReason.DEPENDENCY_FAILED.value,
            ),
            (
                task_runs.c.status == TaskRunStatus.CANCELLED.value,
                RunFailureReason.CANCELLATION_REQUESTED.value,
            ),
            else_=None,
        ).label("failure_reason"),
        func.coalesce(attempt_aggregates.c.attempt_count, 0).label("attempt_count"),
        attempt_aggregates.c.minimum_attempt_number,
        attempt_aggregates.c.maximum_attempt_number,
        ranked_attempts.c.next_eligible_at.label("retry_eligible_at"),
        ranked_failures.c.failure_kind.label("latest_failure_kind"),
        workflow_versions.c.execution_policy.label("workflow_execution_policy"),
        workflow_version_steps.c.execution_policy.label("step_execution_policy"),
    )
    relation = (
        task_runs.join(
            workflow_runs,
            workflow_runs.c.id == task_runs.c.workflow_run_id,
        )
        .join(
            workflow_definitions,
            workflow_definitions.c.id == workflow_runs.c.workflow_definition_id,
        )
        .join(
            workflow_versions,
            workflow_versions.c.id == task_runs.c.workflow_version_id,
        )
        .join(
            workflow_version_steps,
            and_(
                workflow_version_steps.c.workflow_version_id
                == task_runs.c.workflow_version_id,
                workflow_version_steps.c.step_identifier == task_runs.c.step_identifier,
            ),
        )
        .outerjoin(
            attempt_aggregates,
            attempt_aggregates.c.task_run_id == task_runs.c.id,
        )
        .outerjoin(
            ranked_attempts,
            and_(
                ranked_attempts.c.task_run_id == task_runs.c.id,
                ranked_attempts.c.attempt_rank == 1,
            ),
        )
        .outerjoin(
            ranked_failures,
            and_(
                ranked_failures.c.task_run_id == task_runs.c.id,
                ranked_failures.c.failure_rank == 1,
            ),
        )
    )
    return columns, relation


def _task_run_list_statement(
    run_id: UUID,
    owner_principal_id: UUID,
) -> Select[Any]:
    columns, relation = _task_run_projection()
    return (
        select(*columns)
        .select_from(relation)
        .where(
            task_runs.c.workflow_run_id == run_id,
            workflow_definitions.c.owner_principal_id == owner_principal_id,
        )
        .order_by(task_runs.c.step_identifier)
    )


def _task_run_inspection_statement(
    task_run_id: UUID,
    owner_principal_id: UUID,
) -> Select[Any]:
    columns, relation = _task_run_projection()
    return (
        select(*columns)
        .select_from(relation)
        .where(
            task_runs.c.id == task_run_id,
            workflow_definitions.c.owner_principal_id == owner_principal_id,
        )
    )


def _owner_scoped_task_exists_statement(
    task_run_id: UUID,
    owner_principal_id: UUID,
) -> Select[Any]:
    return (
        select(task_runs.c.id)
        .select_from(
            task_runs.join(
                workflow_runs,
                workflow_runs.c.id == task_runs.c.workflow_run_id,
            ).join(
                workflow_definitions,
                workflow_definitions.c.id == workflow_runs.c.workflow_definition_id,
            )
        )
        .where(
            task_runs.c.id == task_run_id,
            workflow_definitions.c.owner_principal_id == owner_principal_id,
        )
    )


def _retry_event_history_statement(
    task_run_id: UUID,
    owner_principal_id: UUID,
    *,
    limit: int,
    cursor: RetryEventCursor | None,
) -> Select[Any]:
    failed_attempt = task_attempts.alias("retry_event_failed_attempt")
    retry_attempt = task_attempts.alias("retry_event_retry_attempt")
    statement = (
        select(
            task_retry_events.c.id,
            task_runs.c.workflow_run_id,
            task_retry_events.c.task_run_id,
            task_retry_events.c.event_type,
            failed_attempt.c.id.label("failed_attempt_id"),
            task_retry_events.c.failed_attempt_number,
            retry_attempt.c.id.label("retry_attempt_id"),
            task_retry_events.c.retry_attempt_number,
            task_retry_events.c.next_eligible_at,
            task_retry_events.c.decision_reason,
            task_attempt_results.c.result_kind,
            task_attempt_results.c.failure_kind,
            task_retry_events.c.occurred_at,
        )
        .select_from(
            task_retry_events.join(
                task_runs, task_runs.c.id == task_retry_events.c.task_run_id
            )
            .join(
                workflow_runs,
                workflow_runs.c.id == task_runs.c.workflow_run_id,
            )
            .join(
                workflow_definitions,
                workflow_definitions.c.id == workflow_runs.c.workflow_definition_id,
            )
            .outerjoin(
                failed_attempt,
                and_(
                    failed_attempt.c.task_run_id == task_retry_events.c.task_run_id,
                    failed_attempt.c.attempt_number
                    == task_retry_events.c.failed_attempt_number,
                ),
            )
            .outerjoin(
                retry_attempt,
                and_(
                    retry_attempt.c.task_run_id == task_retry_events.c.task_run_id,
                    retry_attempt.c.attempt_number
                    == task_retry_events.c.retry_attempt_number,
                ),
            )
            .outerjoin(
                task_attempt_results,
                task_attempt_results.c.task_attempt_id == failed_attempt.c.id,
            )
        )
        .where(
            task_retry_events.c.task_run_id == task_run_id,
            workflow_definitions.c.owner_principal_id == owner_principal_id,
        )
    )
    if cursor is not None:
        statement = statement.where(
            tuple_(task_retry_events.c.occurred_at, task_retry_events.c.id)
            < (cursor.occurred_at, cursor.event_id)
        )
    return statement.order_by(
        task_retry_events.c.occurred_at.desc(), task_retry_events.c.id.desc()
    ).limit(limit + 1)


def _runnable_transition_statement(workflow_run_id: UUID) -> Any:
    """Build the repository's authoritative dependency-transition statement.

    Dependencies come only from the run's bound immutable workflow version. A
    missing predecessor task fails closed, and only ``succeeded`` satisfies an
    edge. The mutation repeats the ``blocked`` guard so concurrent evaluators
    cannot move an already-transitioned or progressed task.
    """
    candidate = task_runs.alias("runnable_candidate")
    run = workflow_runs.alias("candidate_run")
    edge = workflow_version_dependencies.alias("required_dependency")
    predecessor = task_runs.alias("required_predecessor")

    succeeded_predecessor_exists = exists(
        select(1)
        .where(
            predecessor.c.workflow_run_id == candidate.c.workflow_run_id,
            predecessor.c.workflow_version_id == candidate.c.workflow_version_id,
            predecessor.c.step_identifier == edge.c.predecessor_step_identifier,
            predecessor.c.status == TaskRunStatus.SUCCEEDED.value,
        )
        .correlate(candidate, edge)
    )
    unsatisfied_dependency_exists = exists(
        select(1)
        .where(
            edge.c.workflow_version_id == run.c.workflow_version_id,
            edge.c.successor_step_identifier == candidate.c.step_identifier,
            ~succeeded_predecessor_exists,
        )
        .correlate(candidate, run)
    )

    eligible_tasks = (
        select(candidate.c.id)
        .select_from(
            candidate.join(
                run,
                and_(
                    run.c.id == candidate.c.workflow_run_id,
                    run.c.workflow_version_id == candidate.c.workflow_version_id,
                ),
            )
        )
        .where(
            candidate.c.workflow_run_id == workflow_run_id,
            candidate.c.status == TaskRunStatus.BLOCKED.value,
            run.c.status.in_(
                (WorkflowRunStatus.PENDING.value, WorkflowRunStatus.RUNNING.value)
            ),
            ~unsatisfied_dependency_exists,
        )
        # Serialize this operation with future run-level transitions such as
        # cancellation. Whichever transaction locks the run first establishes
        # whether runnable promotion precedes or follows that run-state change.
        .with_for_update(of=run)
        .cte("eligible_runnable_tasks")
    )

    # Zero matching rows is a successful idempotent no-op.
    return (
        update(task_runs)
        .where(
            task_runs.c.id.in_(select(eligible_tasks.c.id)),
            task_runs.c.status == TaskRunStatus.BLOCKED.value,
        )
        .values(
            status=TaskRunStatus.RUNNABLE.value,
            updated_at=func.current_timestamp(),
        )
        .returning(task_runs.c.id, task_runs.c.step_identifier)
    )


def _active_run_progression_lock_statement(workflow_run_id: UUID) -> Select[Any]:
    """Lock one active run as the transaction boundary for task progression."""
    return (
        select(workflow_runs.c.workflow_version_id)
        .where(
            workflow_runs.c.id == workflow_run_id,
            workflow_runs.c.status.in_(
                (WorkflowRunStatus.PENDING.value, WorkflowRunStatus.RUNNING.value)
            ),
        )
        .with_for_update()
    )


def _workflow_run_evaluation_lock_statement(
    workflow_run_id: UUID,
) -> Select[Any]:
    """Lock the run first, preserving the shared progression lock order."""
    return (
        select(workflow_runs.c.status)
        .where(workflow_runs.c.id == workflow_run_id)
        .with_for_update()
    )


def _workflow_cancellation_unsettled_exists_statement(
    workflow_run_id: UUID,
) -> Select[Any]:
    terminal_statuses = (
        TaskRunStatus.SUCCEEDED.value,
        TaskRunStatus.FAILED.value,
        TaskRunStatus.SKIPPED.value,
        TaskRunStatus.CANCELLED.value,
    )
    nonterminal_task = exists(
        select(task_runs.c.id).where(
            task_runs.c.workflow_run_id == workflow_run_id,
            task_runs.c.status.not_in(terminal_statuses),
        )
    )
    claim_for_run = task_attempt_claims.join(
        task_attempts,
        task_attempts.c.id == task_attempt_claims.c.task_attempt_id,
    ).join(task_runs, task_runs.c.id == task_attempts.c.task_run_id)
    unterminated_claim = exists(
        select(task_attempt_claims.c.task_attempt_id)
        .select_from(claim_for_run)
        .where(
            task_runs.c.workflow_run_id == workflow_run_id,
            task_attempt_claims.c.terminated_at.is_(None),
        )
    )
    return select(nonterminal_task | unterminated_claim)


def _workflow_cancellation_claim_result_violation_statement(
    workflow_run_id: UUID,
) -> Select[Any]:
    claim_for_run = task_attempt_claims.join(
        task_attempts,
        task_attempts.c.id == task_attempt_claims.c.task_attempt_id,
    ).join(task_runs, task_runs.c.id == task_attempts.c.task_run_id)
    terminated_without_exact_result = exists(
        select(task_attempt_claims.c.task_attempt_id)
        .select_from(claim_for_run)
        .where(
            task_runs.c.workflow_run_id == workflow_run_id,
            task_attempt_claims.c.terminated_at.is_not(None),
            ~exists(
                select(task_attempt_results.c.task_attempt_id).where(
                    task_attempt_results.c.task_attempt_id
                    == task_attempt_claims.c.task_attempt_id,
                    task_attempt_results.c.claim_generation
                    == task_attempt_claims.c.generation,
                )
            ),
        )
    )
    return select(terminated_without_exact_result)


def _workflow_run_cancellation_lock_statement(
    workflow_run_id: UUID,
    owner_filter: OwnerFilter,
) -> Select[Any]:
    conditions: list[Any] = [workflow_runs.c.id == workflow_run_id]
    if not owner_filter.unrestricted:
        conditions.append(
            workflow_definitions.c.owner_principal_id == owner_filter.principal_id
        )
    return (
        select(workflow_runs.c.id, workflow_runs.c.status)
        .select_from(
            workflow_runs.join(
                workflow_definitions,
                workflow_definitions.c.id == workflow_runs.c.workflow_definition_id,
            )
        )
        .where(*conditions)
        .with_for_update(of=workflow_runs)
    )


def _workflow_run_cancellation_request_statement(
    workflow_run_id: UUID,
) -> Select[Any]:
    return select(*workflow_run_cancellation_requests.c).where(
        workflow_run_cancellation_requests.c.workflow_run_id == workflow_run_id
    )


def _workflow_run_to_cancelling_statement(
    workflow_run_id: UUID,
    expected_status: WorkflowRunStatus,
    requested_at: datetime,
) -> Any:
    return (
        update(workflow_runs)
        .where(
            workflow_runs.c.id == workflow_run_id,
            workflow_runs.c.status == expected_status.value,
        )
        .values(
            status=WorkflowRunStatus.CANCELLING.value,
            updated_at=requested_at,
        )
        .returning(workflow_runs.c.status)
    )


async def _suppress_unstarted_task_rows(
    session: AsyncSession,
    workflow_run_id: UUID,
) -> Sequence[Row[Any]]:
    """Cancel every pre-dispatch task while the caller holds the run lock."""
    return (
        await session.execute(_unstarted_task_suppression_statement(workflow_run_id))
    ).all()


def _unstarted_task_suppression_statement(workflow_run_id: UUID) -> Any:
    return (
        update(task_runs)
        .where(
            task_runs.c.workflow_run_id == workflow_run_id,
            task_runs.c.status.in_(
                (
                    TaskRunStatus.BLOCKED.value,
                    TaskRunStatus.RUNNABLE.value,
                    TaskRunStatus.RETRY_PENDING.value,
                    TaskRunStatus.RETRY_SCHEDULED.value,
                )
            ),
        )
        .values(
            status=TaskRunStatus.CANCELLED.value,
            updated_at=func.statement_timestamp(),
        )
        .returning(task_runs.c.id, task_runs.c.step_identifier)
    )


async def _settle_dispatched_task_rows(
    session: AsyncSession,
    workflow_run_id: UUID,
) -> Sequence[Row[Any]]:
    """Cancel dispatched tasks after the caller serializes on their run."""
    active_claim_exists = exists(
        select(task_attempt_claims.c.task_attempt_id)
        .select_from(
            task_attempts.join(
                task_attempt_claims,
                task_attempt_claims.c.task_attempt_id == task_attempts.c.id,
            )
        )
        .where(
            task_attempts.c.task_run_id == task_runs.c.id,
            task_attempt_claims.c.terminated_at.is_(None),
        )
    )
    corrupt = await session.scalar(
        select(task_runs.c.id)
        .where(
            task_runs.c.workflow_run_id == workflow_run_id,
            task_runs.c.status == TaskRunStatus.DISPATCHED.value,
            active_claim_exists,
        )
        .limit(1)
    )
    if corrupt is not None:
        raise WorkflowRunCancellationPersistenceInvariantViolation
    return (
        await session.execute(
            update(task_runs)
            .where(
                task_runs.c.workflow_run_id == workflow_run_id,
                task_runs.c.status == TaskRunStatus.DISPATCHED.value,
                ~active_claim_exists,
            )
            .values(
                status=TaskRunStatus.CANCELLED.value,
                updated_at=func.statement_timestamp(),
            )
            .returning(task_runs.c.id, task_runs.c.step_identifier)
        )
    ).all()


def _pending_to_running_statement(workflow_run_id: UUID) -> Any:
    """Build the sole transition valid for a pending workflow run."""
    execution_progress_exists = exists(
        select(1).where(
            task_runs.c.workflow_run_id == workflow_runs.c.id,
            task_runs.c.status.in_(
                (
                    TaskRunStatus.RUNNABLE.value,
                    TaskRunStatus.DISPATCHED.value,
                    TaskRunStatus.CLAIMED.value,
                    TaskRunStatus.RUNNING.value,
                    TaskRunStatus.RETRY_PENDING.value,
                    TaskRunStatus.RETRY_SCHEDULED.value,
                    TaskRunStatus.SUCCEEDED.value,
                    TaskRunStatus.FAILED.value,
                )
            ),
        )
    )
    return (
        update(workflow_runs)
        .where(
            workflow_runs.c.id == workflow_run_id,
            workflow_runs.c.status == WorkflowRunStatus.PENDING.value,
            execution_progress_exists,
        )
        .values(
            status=WorkflowRunStatus.RUNNING.value,
            updated_at=func.current_timestamp(),
        )
        .returning(workflow_runs.c.status)
    )


def _running_terminal_transition_statement(workflow_run_id: UUID) -> Any:
    """Build failure-first terminal evaluation for a running workflow run."""
    has_tasks = exists(
        select(1).where(task_runs.c.workflow_run_id == workflow_runs.c.id)
    )
    has_failed_task = exists(
        select(1).where(
            task_runs.c.workflow_run_id == workflow_runs.c.id,
            task_runs.c.status == TaskRunStatus.FAILED.value,
        )
    )
    has_unsettled_failure_task = exists(
        select(1).where(
            task_runs.c.workflow_run_id == workflow_runs.c.id,
            task_runs.c.status.not_in(
                (
                    TaskRunStatus.SUCCEEDED.value,
                    TaskRunStatus.FAILED.value,
                    TaskRunStatus.SKIPPED.value,
                )
            ),
        )
    )
    has_non_succeeded_task = exists(
        select(1).where(
            task_runs.c.workflow_run_id == workflow_runs.c.id,
            task_runs.c.status != TaskRunStatus.SUCCEEDED.value,
        )
    )
    terminal_failure = and_(has_failed_task, ~has_unsettled_failure_task)
    terminal_success = and_(has_tasks, ~has_non_succeeded_task)

    # Failure precedence is explicit. Valid state cannot satisfy both predicates,
    # but CASE ordering makes defensive behavior deterministic.
    target_status = cast(
        case(
            (terminal_failure, WorkflowRunStatus.FAILED.value),
            (terminal_success, WorkflowRunStatus.SUCCEEDED.value),
        ),
        workflow_runs.c.status.type,
    )
    return (
        update(workflow_runs)
        .where(
            workflow_runs.c.id == workflow_run_id,
            workflow_runs.c.status == WorkflowRunStatus.RUNNING.value,
            or_(terminal_failure, terminal_success),
        )
        .values(status=target_status, updated_at=func.current_timestamp())
        .returning(workflow_runs.c.status)
    )


def _dependency_failure_propagation_statement(
    workflow_run_id: UUID,
    workflow_version_id: UUID,
) -> Any:
    """Build authoritative AND-only dependency-failure propagation SQL.

    Every immutable incoming edge is required. Therefore one failed or skipped
    predecessor makes a still-blocked successor unreachable. Traversal continues
    only through blocked or already-skipped task runs; progressed states are a
    conservative boundary. Missing task rows create no failure fact.
    """
    predecessor = task_runs.alias("dependency_failed_predecessor")
    edge = workflow_version_dependencies.alias("failure_seed_dependency")
    successor = task_runs.alias("failure_seed_successor")
    traversable_statuses = (
        TaskRunStatus.BLOCKED.value,
        TaskRunStatus.SKIPPED.value,
    )
    failure_statuses = (
        TaskRunStatus.FAILED.value,
        TaskRunStatus.SKIPPED.value,
    )

    affected_descendants = (
        select(successor.c.step_identifier)
        .select_from(
            predecessor.join(
                edge,
                and_(
                    edge.c.workflow_version_id == workflow_version_id,
                    edge.c.predecessor_step_identifier == predecessor.c.step_identifier,
                ),
            ).join(
                successor,
                and_(
                    successor.c.workflow_run_id == workflow_run_id,
                    successor.c.workflow_version_id == workflow_version_id,
                    successor.c.step_identifier == edge.c.successor_step_identifier,
                ),
            )
        )
        .where(
            predecessor.c.workflow_run_id == workflow_run_id,
            predecessor.c.workflow_version_id == workflow_version_id,
            predecessor.c.status.in_(failure_statuses),
            successor.c.status.in_(traversable_statuses),
        )
        .cte("dependency_failed_descendants", recursive=True)
    )

    recursive_edge = workflow_version_dependencies.alias(
        "propagated_failure_dependency"
    )
    recursive_successor = task_runs.alias("propagated_failure_successor")
    affected_descendants = affected_descendants.union(
        select(recursive_successor.c.step_identifier)
        .select_from(
            affected_descendants.join(
                recursive_edge,
                and_(
                    recursive_edge.c.workflow_version_id == workflow_version_id,
                    recursive_edge.c.predecessor_step_identifier
                    == affected_descendants.c.step_identifier,
                ),
            ).join(
                recursive_successor,
                and_(
                    recursive_successor.c.workflow_run_id == workflow_run_id,
                    recursive_successor.c.workflow_version_id == workflow_version_id,
                    recursive_successor.c.step_identifier
                    == recursive_edge.c.successor_step_identifier,
                ),
            )
        )
        .where(recursive_successor.c.status.in_(traversable_statuses))
    )

    # Only blocked task runs transition. Existing skipped intermediates are used
    # for reachability but are neither returned nor timestamp-rewritten. Zero
    # matching rows is a successful idempotent no-op.
    return (
        update(task_runs)
        .where(
            task_runs.c.workflow_run_id == workflow_run_id,
            task_runs.c.workflow_version_id == workflow_version_id,
            task_runs.c.status == TaskRunStatus.BLOCKED.value,
            task_runs.c.step_identifier.in_(
                select(affected_descendants.c.step_identifier)
            ),
        )
        .values(
            status=TaskRunStatus.SKIPPED.value,
            updated_at=func.current_timestamp(),
        )
        .returning(task_runs.c.id, task_runs.c.step_identifier)
    )


def _inspected_run(row: Row[Any]) -> InspectedWorkflowRun:
    cancellation = None
    if row.cancellation_requested_by_principal_id is not None:
        cancellation = InspectedWorkflowRunCancellation(
            requested_by_principal_id=row.cancellation_requested_by_principal_id,
            reason=row.cancellation_reason,
            requested_at=row.cancellation_requested_at,
            recovered_cancellation_count=row.recovered_cancellation_count,
            caveats=(
                WorkflowRunCancellationCaveat.EXTERNAL_EFFECTS_MAY_PERSIST,
                WorkflowRunCancellationCaveat.PHYSICAL_EXECUTION_MAY_CONTINUE_AFTER_AUTHORITY_LOSS,
                WorkflowRunCancellationCaveat.COMPLETED_TASK_OUTCOMES_ARE_PRESERVED,
            ),
        )
    return InspectedWorkflowRun(
        id=row.id,
        workflow_definition_id=row.workflow_definition_id,
        workflow_version_id=row.workflow_version_id,
        version_number=row.version_number,
        requested_by_principal_id=row.requested_by_principal_id,
        status=WorkflowRunStatus(row.status),
        created_at=row.created_at,
        updated_at=row.updated_at,
        failure_reason=(
            RunFailureReason(row.failure_reason)
            if row.failure_reason is not None
            else None
        ),
        cancellation=cancellation,
    )


def _inspected_task_run(row: Row[Any]) -> InspectedTaskRun:
    attempt_count = row.attempt_count
    if attempt_count == 0:
        if (
            row.minimum_attempt_number is not None
            or row.maximum_attempt_number is not None
        ):
            raise WorkflowRunInspectionInvariantViolation
    elif row.minimum_attempt_number != 1 or row.maximum_attempt_number != attempt_count:
        raise WorkflowRunInspectionInvariantViolation
    try:
        policy = resolve_persisted_retry_policy(
            row.workflow_execution_policy,
            row.step_execution_policy,
        )
        latest_failure_kind = (
            TaskExecutionFailureKind(row.latest_failure_kind)
            if row.latest_failure_kind is not None
            else None
        )
    except (InvalidPersistedRetryPolicy, TypeError, ValueError) as error:
        raise WorkflowRunInspectionInvariantViolation from error
    return InspectedTaskRun(
        id=row.id,
        workflow_run_id=row.workflow_run_id,
        workflow_version_id=row.workflow_version_id,
        step_identifier=row.step_identifier,
        status=TaskRunStatus(row.status),
        created_at=row.created_at,
        updated_at=row.updated_at,
        failure_reason=(
            RunFailureReason(row.failure_reason)
            if row.failure_reason is not None
            else None
        ),
        attempt_count=attempt_count,
        retry_attempt_count=max(attempt_count - 1, 0),
        maximum_attempts=(policy.maximum_attempts if policy is not None else None),
        retry_eligible_at=row.retry_eligible_at,
        latest_failure_kind=latest_failure_kind,
    )


def _inspected_retry_event(row: Row[Any]) -> InspectedRetryEvent:
    event_type = RetryEventType(row.event_type)
    if event_type in (
        RetryEventType.RETRY_SCHEDULED,
        RetryEventType.RETRY_NOT_SCHEDULED,
    ) and (
        row.failed_attempt_id is None
        or row.result_kind != TaskExecutionResultKind.RETRYABLE_FAILURE.value
        or row.failure_kind is None
    ):
        raise WorkflowRunInspectionInvariantViolation
    return InspectedRetryEvent(
        id=row.id,
        workflow_run_id=row.workflow_run_id,
        task_run_id=row.task_run_id,
        event_type=event_type,
        failed_attempt_id=row.failed_attempt_id,
        failed_attempt_number=row.failed_attempt_number,
        retry_attempt_id=row.retry_attempt_id,
        retry_attempt_number=row.retry_attempt_number,
        next_eligible_at=row.next_eligible_at,
        decision_reason=(
            RetryNotScheduledReason(row.decision_reason)
            if row.decision_reason is not None
            else None
        ),
        failure_kind=(
            TaskExecutionFailureKind(row.failure_kind)
            if row.failure_kind is not None
            else None
        ),
        occurred_at=row.occurred_at,
    )


def _existing_idempotent_run(row: Row[Any]) -> ExistingIdempotentWorkflowRun:
    return ExistingIdempotentWorkflowRun(
        request_fingerprint=row.request_fingerprint,
        run=CreatedWorkflowRun(
            id=row.run_id,
            workflow_definition_id=row.workflow_definition_id,
            workflow_version_id=row.workflow_version_id,
            version_number=row.version_number,
            requested_by_principal_id=row.requested_by_principal_id,
            status=WorkflowRunStatus(row.status),
            created_at=row.created_at,
            task_count=row.task_count,
            runnable_task_count=row.runnable_task_count,
            blocked_task_count=row.blocked_task_count,
        ),
    )


def _accepted_cancellation(row: Row[Any]) -> AcceptedWorkflowRunCancellation:
    try:
        return AcceptedWorkflowRunCancellation(
            row.requested_by_principal_id,
            row.reason,
            row.requested_at,
        )
    except (TypeError, ValueError) as error:
        raise WorkflowRunCancellationPersistenceInvariantViolation from error


def _is_idempotency_scope_conflict(error: IntegrityError) -> bool:
    sqlstate: str | None = None
    constraint_name: str | None = None
    pending: list[BaseException] = [error]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        candidate_state = getattr(current, "sqlstate", None)
        candidate_constraint = getattr(current, "constraint_name", None)
        if isinstance(candidate_state, str):
            sqlstate = candidate_state
        if isinstance(candidate_constraint, str):
            constraint_name = candidate_constraint
        if current.__cause__ is not None:
            pending.append(current.__cause__)
        if current.__context__ is not None:
            pending.append(current.__context__)
        original = getattr(current, "orig", None)
        if isinstance(original, BaseException):
            pending.append(original)
    return (
        sqlstate == POSTGRES_UNIQUE_VIOLATION
        and constraint_name == IDEMPOTENCY_SCOPE_CONSTRAINT
    )


def _creation_snapshot(
    version: Row[Any],
    step_rows: Sequence[Row[Any]],
    dependency_rows: Sequence[Row[Any]],
) -> WorkflowRunVersionSnapshot:
    return WorkflowRunVersionSnapshot(
        workflow_definition_id=version.workflow_definition_id,
        workflow_version_id=version.id,
        version_number=version.version_number,
        steps=tuple(
            WorkflowRunVersionStep(
                row.step_identifier,
                resolve_deadline_seconds(
                    getattr(version, "execution_policy", None),
                    getattr(row, "execution_policy", None),
                ),
                resolve_execution_timeout_seconds(
                    getattr(version, "execution_policy", None),
                    getattr(row, "execution_policy", None),
                ),
            )
            for row in step_rows
        ),
        dependencies=tuple(
            WorkflowRunVersionDependency(
                row.predecessor_step_identifier,
                row.successor_step_identifier,
            )
            for row in dependency_rows
        ),
    )


def _version_resolution_statement(
    workflow_id: UUID,
    owner_principal_id: UUID,
    selection: WorkflowVersionSelection,
) -> Select[Any]:
    version_query = select(
        workflow_versions.c.id.label("workflow_version_id"),
        workflow_versions.c.version_number,
    ).where(workflow_versions.c.workflow_definition_id == workflow_id)
    if isinstance(selection, ExplicitWorkflowVersion):
        version_query = version_query.where(
            workflow_versions.c.version_number == selection.version_number
        )
    else:
        version_query = version_query.order_by(
            workflow_versions.c.version_number.desc()
        )
    selected_version = version_query.limit(1).lateral("selected_version")
    return (
        select(
            workflow_definitions.c.id.label("workflow_definition_id"),
            workflow_definitions.c.status,
            selected_version.c.workflow_version_id,
            selected_version.c.version_number,
        )
        .select_from(workflow_definitions.outerjoin(selected_version, true()))
        .where(
            and_(
                workflow_definitions.c.id == workflow_id,
                workflow_definitions.c.owner_principal_id == owner_principal_id,
            )
        )
    )
