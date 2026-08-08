"""Application service for workflow run target resolution."""

from __future__ import annotations

from uuid import UUID, uuid4

from taskforge.runs.domain import (
    CreatedWorkflowRun,
    NewTaskRun,
    NewWorkflowRun,
    ResolvedWorkflowVersion,
    TaskRunStatus,
    WorkflowRunIdempotency,
    WorkflowRunIdempotencyConflict,
    WorkflowRunInput,
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
        NewTaskRun(uuid4(), task.step_identifier, task.status) for task in initial_tasks
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
