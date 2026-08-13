"""Application service for workflow run target resolution."""

from __future__ import annotations

from uuid import UUID, uuid4

from taskforge.runs.domain import (
    MAX_WORKFLOW_RECONCILIATION_ITERATIONS,
    CreatedWorkflowRun,
    DependencyFailurePropagationResult,
    InspectedTaskRun,
    InspectedWorkflowRun,
    NewTaskRun,
    NewWorkflowRun,
    ResolvedWorkflowVersion,
    RunnableTransitionResult,
    TaskRunStatus,
    WorkflowRunEvaluationResult,
    WorkflowRunIdempotency,
    WorkflowRunIdempotencyConflict,
    WorkflowRunInput,
    WorkflowRunReconciliationResult,
    WorkflowRunStatus,
    WorkflowVersionSelection,
    create_workflow_run_idempotency,
    create_workflow_run_input,
    idempotency_fingerprints_match,
    materialize_initial_tasks,
    require_run_available,
)
from taskforge.runs.persistence_ports import (
    ExistingIdempotentWorkflowRun,
    PreparedWorkflowRunCreation,
    WorkflowRunCreationTransaction,
    WorkflowRunIdempotencyRecordConflict,
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


class WorkflowRunService:
    def __init__(self, repository: WorkflowRunRepository) -> None:
        self._repository = repository

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
        except WorkflowRunPersistenceUnavailable as error:
            raise WorkflowRunServiceUnavailable from error
        if task is None:
            raise TaskRunNotFound
        return task

    async def transition_runnable_tasks(
        self,
        workflow_run_id: UUID,
    ) -> RunnableTransitionResult:
        """Delegate authoritative dependency evaluation and transition persistence."""
        try:
            return await self._repository.transition_runnable_tasks(workflow_run_id)
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
        """Boundedly compose the three authoritative progression operations."""
        runnable_count = 0
        skipped_count = 0
        workflow_transition_count = 0
        last_status: WorkflowRunStatus | None = None
        inactive_statuses = (
            WorkflowRunStatus.CANCELLING,
            WorkflowRunStatus.SUCCEEDED,
            WorkflowRunStatus.FAILED,
            WorkflowRunStatus.CANCELLED,
        )

        for iteration in range(1, MAX_WORKFLOW_RECONCILIATION_ITERATIONS + 1):
            # A terminal/cancelling status observed by the prior iteration must
            # never begin another Task 1 -> Task 2 -> Task 3 cycle.
            if last_status in inactive_statuses:
                assert last_status is not None
                return WorkflowRunReconciliationResult(
                    workflow_run_id,
                    True,
                    iteration - 1,
                    runnable_count,
                    skipped_count,
                    workflow_transition_count,
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
                    None,
                    False,
                    False,
                )

            assert workflow.resulting_status is not None
            last_status = workflow.resulting_status
            if last_status in inactive_statuses:
                return WorkflowRunReconciliationResult(
                    workflow_run_id,
                    True,
                    iteration,
                    runnable_count,
                    skipped_count,
                    workflow_transition_count,
                    last_status,
                    True,
                    False,
                )

            iteration_made_progress = (
                runnable.made_progress
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
