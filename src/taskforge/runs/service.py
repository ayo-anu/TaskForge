"""Application service for workflow run target resolution."""

from __future__ import annotations

from typing import cast
from uuid import UUID, uuid4

from taskforge.identity.authorization import OwnerFilter
from taskforge.retries.domain import InspectedRetryEventPage, RetryEventCursor
from taskforge.runs.domain import (
    MAX_WORKFLOW_RECONCILIATION_ITERATIONS,
    CancellationFinalizationResult,
    CancellationPropagationResult,
    CancellationSettlementResult,
    CreatedFullWorkflowReplay,
    CreatedWorkflowRun,
    DependencyFailurePropagationResult,
    InspectedTaskRun,
    InspectedWorkflowRun,
    NewTaskRun,
    NewWorkflowRun,
    ResolvedWorkflowVersion,
    RunnableTransitionResult,
    TaskRunStatus,
    WorkflowReplayMode,
    WorkflowRunCancellationIdempotencyConflict,
    WorkflowRunCancellationOutcome,
    WorkflowRunCancellationResult,
    WorkflowRunEvaluationResult,
    WorkflowRunIdempotency,
    WorkflowRunIdempotencyConflict,
    WorkflowRunInput,
    WorkflowRunReconciliationResult,
    WorkflowRunStatus,
    WorkflowVersionSelection,
    create_workflow_run_cancellation_command,
    create_workflow_run_idempotency,
    create_workflow_run_input,
    idempotency_fingerprints_match,
    materialize_initial_tasks,
    require_full_replay_source_terminal,
    require_run_available,
)
from taskforge.runs.persistence_ports import (
    ExistingIdempotentWorkflowRun,
    PersistedCancellationOutcome,
    PreparedFullWorkflowReplay,
    PreparedWorkflowRunCreation,
    RetryEventInspectionRepository,
    WorkflowRunCancellationPersistenceInvariantViolation,
    WorkflowRunCreationTransaction,
    WorkflowRunIdempotencyRecordConflict,
    WorkflowRunInspectionInvariantViolation,
    WorkflowRunPersistenceUnavailable,
    WorkflowRunRecordConflict,
    WorkflowRunRepository,
)


class WorkflowRunTargetNotFound(Exception):
    """A workflow is absent from the requested owner's scope."""


class WorkflowVersionUnavailable(Exception):
    """The selected version is not published for the workflow."""


class WorkflowRunServiceUnavailable(Exception):
    """Workflow run target persistence was operationally unavailable."""


class WorkflowRunPersistenceConflict(Exception):
    """A complete workflow run could not be persisted."""


class WorkflowRunNotFound(Exception):
    """A workflow run is absent from the requested owner's scope."""


class TaskRunNotFound(Exception):
    """A task run is absent from the requested owner's scope."""


class WorkflowRunInspectionInvariantError(Exception):
    """Durable workflow-run inspection facts are inconsistent."""


class WorkflowRunCancellationInvariantError(Exception):
    """Durable cancellation facts are internally inconsistent."""


class WorkflowRunReplayInvariantError(Exception):
    """Durable replay source facts are internally inconsistent."""


class WorkflowRunService:
    def __init__(self, repository: WorkflowRunRepository) -> None:
        self._repository = repository

    async def create_full_replay(
        self,
        source_workflow_run_id: UUID,
        owner_filter: OwnerFilter,
        *,
        requested_by_principal_id: UUID,
    ) -> CreatedFullWorkflowReplay:
        """Atomically create a fresh complete run from one exact terminal source."""
        try:
            async with self._repository.creation_transaction() as transaction:
                prepared = await transaction.prepare_full_replay(
                    source_workflow_run_id, owner_filter
                )
                if prepared is None:
                    raise WorkflowRunNotFound
                require_full_replay_source_terminal(prepared.source_status)
                accepted_input = create_workflow_run_input(
                    prepared.input_snapshot.payload,
                    prepared.input_snapshot.input_references,
                )
                created = await _create_prepared_full_replay(
                    transaction,
                    prepared,
                    accepted_input,
                    requested_by_principal_id,
                )
        except WorkflowRunRecordConflict as error:
            raise WorkflowRunPersistenceConflict from error
        except WorkflowRunPersistenceUnavailable as error:
            raise WorkflowRunServiceUnavailable from error
        return CreatedFullWorkflowReplay(
            source_workflow_run_id,
            WorkflowReplayMode.FULL,
            created,
        )

    async def cancel_run(
        self,
        workflow_run_id: UUID,
        owner_filter: OwnerFilter,
        *,
        requested_by_principal_id: UUID,
        idempotency_key: object,
        reason: object,
    ) -> WorkflowRunCancellationResult:
        """Accept or replay one owner-scoped workflow cancellation intention."""
        command = create_workflow_run_cancellation_command(
            workflow_run_id,
            requested_by_principal_id,
            idempotency_key=idempotency_key,
            reason=reason,
        )
        try:
            persisted = await self._repository.cancel_run(
                workflow_run_id, owner_filter, command
            )
        except WorkflowRunCancellationPersistenceInvariantViolation as error:
            raise WorkflowRunCancellationInvariantError from error
        except WorkflowRunPersistenceUnavailable as error:
            raise WorkflowRunServiceUnavailable from error
        if persisted is None:
            raise WorkflowRunNotFound
        if persisted.outcome is PersistedCancellationOutcome.IDEMPOTENCY_CONFLICT:
            raise WorkflowRunCancellationIdempotencyConflict
        outcome = WorkflowRunCancellationOutcome(persisted.outcome.value)
        disclosed = (
            persisted.canonical_request
            if outcome
            in (
                WorkflowRunCancellationOutcome.NEWLY_ACCEPTED,
                WorkflowRunCancellationOutcome.EXACT_RETRY,
            )
            else None
        )
        return WorkflowRunCancellationResult(
            workflow_run_id,
            outcome,
            persisted.status,
            disclosed,
        )

    async def resolve_version(
        self,
        workflow_id: UUID,
        *,
        owner_principal_id: UUID,
        selection: WorkflowVersionSelection,
    ) -> ResolvedWorkflowVersion:
        """Resolve a target valid at lookup time, without admitting a run."""
        try:
            record = await self._repository.resolve_workflow_version(
                workflow_id,
                owner_principal_id,
                selection,
            )
        except WorkflowRunPersistenceUnavailable as error:
            raise WorkflowRunServiceUnavailable from error
        if record is None:
            raise WorkflowRunTargetNotFound
        require_run_available(record.status)
        if record.workflow_version_id is None or record.version_number is None:
            raise WorkflowVersionUnavailable
        return ResolvedWorkflowVersion(
            workflow_definition_id=record.workflow_definition_id,
            workflow_version_id=record.workflow_version_id,
            version_number=record.version_number,
        )

    async def get_run(
        self,
        run_id: UUID,
        *,
        owner_principal_id: UUID,
    ) -> InspectedWorkflowRun:
        try:
            run = await self._repository.get_run(run_id, owner_principal_id)
        except WorkflowRunPersistenceUnavailable as error:
            raise WorkflowRunServiceUnavailable from error
        if run is None:
            raise WorkflowRunNotFound
        return run

    async def list_task_runs(
        self,
        run_id: UUID,
        *,
        owner_principal_id: UUID,
    ) -> tuple[InspectedTaskRun, ...]:
        try:
            tasks = await self._repository.list_task_runs(run_id, owner_principal_id)
        except WorkflowRunInspectionInvariantViolation as error:
            raise WorkflowRunInspectionInvariantError from error
        except WorkflowRunPersistenceUnavailable as error:
            raise WorkflowRunServiceUnavailable from error
        if tasks is None:
            raise WorkflowRunNotFound
        return tasks

    async def get_task_run(
        self,
        task_run_id: UUID,
        *,
        owner_principal_id: UUID,
    ) -> InspectedTaskRun:
        try:
            task = await self._repository.get_task_run(task_run_id, owner_principal_id)
        except WorkflowRunInspectionInvariantViolation as error:
            raise WorkflowRunInspectionInvariantError from error
        except WorkflowRunPersistenceUnavailable as error:
            raise WorkflowRunServiceUnavailable from error
        if task is None:
            raise TaskRunNotFound
        return task

    async def list_retry_events(
        self,
        task_run_id: UUID,
        *,
        owner_principal_id: UUID,
        limit: int,
        cursor: RetryEventCursor | None,
    ) -> InspectedRetryEventPage:
        try:
            page = await cast(
                RetryEventInspectionRepository, self._repository
            ).list_retry_events(
                task_run_id,
                owner_principal_id,
                limit=limit,
                cursor=cursor,
            )
        except WorkflowRunInspectionInvariantViolation as error:
            raise WorkflowRunInspectionInvariantError from error
        except WorkflowRunPersistenceUnavailable as error:
            raise WorkflowRunServiceUnavailable from error
        if page is None:
            raise TaskRunNotFound
        return page

    async def transition_runnable_tasks(
        self,
        workflow_run_id: UUID,
    ) -> RunnableTransitionResult:
        """Delegate authoritative dependency evaluation and transition persistence."""
        try:
            return await self._repository.transition_runnable_tasks(workflow_run_id)
        except WorkflowRunPersistenceUnavailable as error:
            raise WorkflowRunServiceUnavailable from error

    async def suppress_unstarted_tasks(
        self,
        workflow_run_id: UUID,
    ) -> CancellationPropagationResult:
        """Cancel pre-dispatch task states under the workflow progression lock."""
        try:
            return await self._repository.suppress_unstarted_tasks(workflow_run_id)
        except WorkflowRunCancellationPersistenceInvariantViolation as error:
            raise WorkflowRunCancellationInvariantError from error
        except WorkflowRunPersistenceUnavailable as error:
            raise WorkflowRunServiceUnavailable from error

    async def propagate_dependency_failures(
        self,
        workflow_run_id: UUID,
    ) -> DependencyFailurePropagationResult:
        """Delegate authoritative dependency-failure propagation."""
        try:
            return await self._repository.propagate_dependency_failures(workflow_run_id)
        except WorkflowRunPersistenceUnavailable as error:
            raise WorkflowRunServiceUnavailable from error

    async def settle_dispatched_tasks(
        self,
        workflow_run_id: UUID,
    ) -> CancellationSettlementResult:
        try:
            return await self._repository.settle_dispatched_tasks(workflow_run_id)
        except WorkflowRunCancellationPersistenceInvariantViolation as error:
            raise WorkflowRunCancellationInvariantError from error
        except WorkflowRunPersistenceUnavailable as error:
            raise WorkflowRunServiceUnavailable from error

    async def finalize_workflow_run_cancellation(
        self,
        workflow_run_id: UUID,
    ) -> CancellationFinalizationResult:
        try:
            return await self._repository.finalize_workflow_run_cancellation(
                workflow_run_id
            )
        except WorkflowRunCancellationPersistenceInvariantViolation as error:
            raise WorkflowRunCancellationInvariantError from error
        except WorkflowRunPersistenceUnavailable as error:
            raise WorkflowRunServiceUnavailable from error

    async def evaluate_workflow_run_state(
        self,
        workflow_run_id: UUID,
    ) -> WorkflowRunEvaluationResult:
        """Delegate authoritative workflow-run state evaluation."""
        try:
            return await self._repository.evaluate_workflow_run_state(workflow_run_id)
        except WorkflowRunPersistenceUnavailable as error:
            raise WorkflowRunServiceUnavailable from error

    async def reconcile_workflow_run(
        self,
        workflow_run_id: UUID,
    ) -> WorkflowRunReconciliationResult:
        """Boundedly compose cancellation-first authoritative progression."""
        runnable_count = 0
        skipped_count = 0
        workflow_transition_count = 0
        cancelled_count = 0
        last_status: WorkflowRunStatus | None = None
        terminal_statuses = (
            WorkflowRunStatus.SUCCEEDED,
            WorkflowRunStatus.FAILED,
            WorkflowRunStatus.CANCELLED,
        )

        for iteration in range(1, MAX_WORKFLOW_RECONCILIATION_ITERATIONS + 1):
            cancellation = await self.suppress_unstarted_tasks(workflow_run_id)
            cancelled_count += cancellation.cancelled_count
            if not cancellation.found:
                return WorkflowRunReconciliationResult(
                    workflow_run_id,
                    False,
                    iteration,
                    runnable_count,
                    skipped_count,
                    workflow_transition_count,
                    cancelled_count,
                    None,
                    False,
                    False,
                )
            settlement = await self.settle_dispatched_tasks(workflow_run_id)
            if not settlement.found:
                return WorkflowRunReconciliationResult(
                    workflow_run_id,
                    False,
                    iteration,
                    runnable_count,
                    skipped_count,
                    workflow_transition_count,
                    cancelled_count,
                    None,
                    False,
                    False,
                )
            cancelled_count += settlement.settled_count
            assert settlement.workflow_status is not None
            last_status = settlement.workflow_status
            if last_status in (
                WorkflowRunStatus.CANCELLING,
                WorkflowRunStatus.CANCELLED,
            ):
                finalization = await self.finalize_workflow_run_cancellation(
                    workflow_run_id
                )
                if not finalization.found:
                    return WorkflowRunReconciliationResult(
                        workflow_run_id,
                        False,
                        iteration,
                        runnable_count,
                        skipped_count,
                        workflow_transition_count,
                        cancelled_count,
                        None,
                        False,
                        False,
                    )
                workflow_transition_count += int(finalization.transitioned)
                assert finalization.resulting_status is not None
                last_status = finalization.resulting_status
                return WorkflowRunReconciliationResult(
                    workflow_run_id,
                    True,
                    iteration,
                    runnable_count,
                    skipped_count,
                    workflow_transition_count,
                    cancelled_count,
                    last_status,
                    True,
                    False,
                )
            if last_status in terminal_statuses:
                return WorkflowRunReconciliationResult(
                    workflow_run_id,
                    True,
                    iteration,
                    runnable_count,
                    skipped_count,
                    workflow_transition_count,
                    cancelled_count,
                    last_status,
                    True,
                    False,
                )

            runnable = await self.transition_runnable_tasks(workflow_run_id)
            skipped = await self.propagate_dependency_failures(workflow_run_id)
            workflow = await self.evaluate_workflow_run_state(workflow_run_id)
            runnable_count += runnable.transitioned_count
            skipped_count += skipped.skipped_count
            workflow_transition_count += int(workflow.transitioned)

            if not workflow.found:
                return WorkflowRunReconciliationResult(
                    workflow_run_id,
                    False,
                    iteration,
                    runnable_count,
                    skipped_count,
                    workflow_transition_count,
                    cancelled_count,
                    None,
                    False,
                    False,
                )

            assert workflow.resulting_status is not None
            last_status = workflow.resulting_status
            if last_status in terminal_statuses:
                return WorkflowRunReconciliationResult(
                    workflow_run_id,
                    True,
                    iteration,
                    runnable_count,
                    skipped_count,
                    workflow_transition_count,
                    cancelled_count,
                    last_status,
                    True,
                    False,
                )

            if last_status is WorkflowRunStatus.CANCELLING:
                if iteration == MAX_WORKFLOW_RECONCILIATION_ITERATIONS:
                    return WorkflowRunReconciliationResult(
                        workflow_run_id,
                        True,
                        iteration,
                        runnable_count,
                        skipped_count,
                        workflow_transition_count,
                        cancelled_count,
                        last_status,
                        True,
                        False,
                    )
                continue

            iteration_made_progress = (
                cancellation.made_progress
                or settlement.made_progress
                or runnable.made_progress
                or skipped.made_progress
                or workflow.made_progress
            )
            if not iteration_made_progress:
                return WorkflowRunReconciliationResult(
                    workflow_run_id,
                    True,
                    iteration,
                    runnable_count,
                    skipped_count,
                    workflow_transition_count,
                    cancelled_count,
                    last_status,
                    True,
                    False,
                )

        assert last_status is not None
        return WorkflowRunReconciliationResult(
            workflow_run_id,
            True,
            MAX_WORKFLOW_RECONCILIATION_ITERATIONS,
            runnable_count,
            skipped_count,
            workflow_transition_count,
            cancelled_count,
            last_status,
            False,
            True,
        )

    async def create_run(
        self,
        workflow_id: UUID,
        *,
        owner_principal_id: UUID,
        requested_by_principal_id: UUID,
        selection: WorkflowVersionSelection,
        input_snapshot: WorkflowRunInput,
    ) -> CreatedWorkflowRun:
        """Atomically create one pending run and its complete initial task graph."""
        accepted_input = create_workflow_run_input(
            input_snapshot.payload,
            input_snapshot.input_references,
        )
        run = NewWorkflowRun(
            id=uuid4(),
            requested_by_principal_id=requested_by_principal_id,
        )
        try:
            async with self._repository.creation_transaction() as transaction:
                prepared = await transaction.prepare_creation_target(
                    workflow_id, owner_principal_id, selection
                )
                if prepared is None:
                    raise WorkflowRunTargetNotFound
                created = await _create_prepared_run(
                    transaction,
                    prepared,
                    accepted_input,
                    requested_by_principal_id,
                    run=run,
                )
        except WorkflowRunRecordConflict as error:
            raise WorkflowRunPersistenceConflict from error
        except WorkflowRunPersistenceUnavailable as error:
            raise WorkflowRunServiceUnavailable from error
        return created

    async def create_idempotent_run(
        self,
        workflow_id: UUID,
        *,
        owner_principal_id: UUID,
        requested_by_principal_id: UUID,
        selection: WorkflowVersionSelection,
        input_snapshot: WorkflowRunInput,
        idempotency_key: object,
    ) -> CreatedWorkflowRun:
        """Create or replay one scoped idempotent workflow run."""
        accepted_input = create_workflow_run_input(
            input_snapshot.payload,
            input_snapshot.input_references,
        )
        idempotency = create_workflow_run_idempotency(
            idempotency_key,
            workflow_definition_id=workflow_id,
            requested_by_principal_id=requested_by_principal_id,
            selection=selection,
            input_snapshot=accepted_input,
        )
        try:
            async with self._repository.creation_transaction() as transaction:
                preparation = await transaction.prepare_idempotent_creation(
                    workflow_id,
                    owner_principal_id,
                    requested_by_principal_id,
                    selection,
                    idempotency.key_digest,
                )
                if preparation is None:
                    raise WorkflowRunTargetNotFound
                if isinstance(preparation, ExistingIdempotentWorkflowRun):
                    return _replay_idempotent_run(preparation, idempotency)
                created = await _create_prepared_run(
                    transaction,
                    preparation,
                    accepted_input,
                    requested_by_principal_id,
                    idempotency=idempotency,
                )
        except WorkflowRunIdempotencyRecordConflict:
            return await self._recover_idempotency_conflict(
                requested_by_principal_id,
                workflow_id,
                idempotency,
            )
        except WorkflowRunRecordConflict as error:
            raise WorkflowRunPersistenceConflict from error
        except WorkflowRunPersistenceUnavailable as error:
            raise WorkflowRunServiceUnavailable from error
        return created

    async def _recover_idempotency_conflict(
        self,
        principal_id: UUID,
        workflow_id: UUID,
        idempotency: WorkflowRunIdempotency,
    ) -> CreatedWorkflowRun:
        try:
            existing = await self._repository.find_idempotent_run(
                principal_id,
                workflow_id,
                idempotency.key_digest,
            )
        except WorkflowRunPersistenceUnavailable as error:
            raise WorkflowRunServiceUnavailable from error
        if existing is None:
            raise WorkflowRunPersistenceConflict(
                "idempotency winner was not found after uniqueness conflict"
            )
        return _replay_idempotent_run(existing, idempotency)


def _replay_idempotent_run(
    existing: ExistingIdempotentWorkflowRun,
    idempotency: WorkflowRunIdempotency,
) -> CreatedWorkflowRun:
    if not idempotency_fingerprints_match(
        existing.request_fingerprint,
        idempotency.request_fingerprint,
    ):
        raise WorkflowRunIdempotencyConflict
    return existing.run


async def _create_prepared_run(
    transaction: WorkflowRunCreationTransaction,
    prepared: PreparedWorkflowRunCreation,
    input_snapshot: WorkflowRunInput,
    requested_by_principal_id: UUID,
    *,
    run: NewWorkflowRun | None = None,
    idempotency: WorkflowRunIdempotency | None = None,
) -> CreatedWorkflowRun:
    require_run_available(prepared.status)
    if prepared.snapshot is None:
        raise WorkflowVersionUnavailable
    initial_tasks = materialize_initial_tasks(prepared.snapshot)
    if run is None:
        run = NewWorkflowRun(
            id=uuid4(),
            requested_by_principal_id=requested_by_principal_id,
        )
    task_values = tuple(
        NewTaskRun(
            uuid4(),
            task.step_identifier,
            task.status,
            task.deadline_seconds,
            task.execution_timeout_seconds,
        )
        for task in initial_tasks
    )
    timestamps = await transaction.insert_complete_run(
        prepared,
        run,
        input_snapshot,
        task_values,
        idempotency,
    )
    await transaction.commit()
    runnable_count = sum(task.status is TaskRunStatus.RUNNABLE for task in task_values)
    return CreatedWorkflowRun(
        id=run.id,
        workflow_definition_id=prepared.workflow_definition_id,
        workflow_version_id=prepared.snapshot.workflow_version_id,
        version_number=prepared.snapshot.version_number,
        requested_by_principal_id=run.requested_by_principal_id,
        status=WorkflowRunStatus.PENDING,
        created_at=timestamps.created_at,
        task_count=len(task_values),
        runnable_task_count=runnable_count,
        blocked_task_count=len(task_values) - runnable_count,
    )


async def _create_prepared_full_replay(
    transaction: WorkflowRunCreationTransaction,
    prepared: PreparedFullWorkflowReplay,
    input_snapshot: WorkflowRunInput,
    requested_by_principal_id: UUID,
) -> CreatedWorkflowRun:
    snapshot = prepared.creation.snapshot
    if snapshot is None:
        raise WorkflowRunReplayInvariantError
    initial_tasks = materialize_initial_tasks(snapshot)
    run = NewWorkflowRun(uuid4(), requested_by_principal_id)
    task_values = tuple(
        NewTaskRun(
            uuid4(),
            task.step_identifier,
            task.status,
            task.deadline_seconds,
            task.execution_timeout_seconds,
        )
        for task in initial_tasks
    )
    timestamps = await transaction.insert_full_replay(
        prepared, run, input_snapshot, task_values
    )
    await transaction.commit()
    runnable_count = sum(task.status is TaskRunStatus.RUNNABLE for task in task_values)
    return CreatedWorkflowRun(
        id=run.id,
        workflow_definition_id=prepared.creation.workflow_definition_id,
        workflow_version_id=snapshot.workflow_version_id,
        version_number=snapshot.version_number,
        requested_by_principal_id=requested_by_principal_id,
        status=WorkflowRunStatus.PENDING,
        created_at=timestamps.created_at,
        task_count=len(task_values),
        runnable_task_count=runnable_count,
        blocked_task_count=len(task_values) - runnable_count,
    )
