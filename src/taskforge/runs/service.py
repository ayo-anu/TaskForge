"""Application service for workflow run target resolution."""

from __future__ import annotations

from uuid import UUID, uuid4

from taskforge.runs.domain import (
    CreatedWorkflowRun,
    NewTaskRun,
    NewWorkflowRun,
    ResolvedWorkflowVersion,
    TaskRunStatus,
    WorkflowRunInput,
    WorkflowRunStatus,
    WorkflowVersionSelection,
    create_workflow_run_input,
    materialize_initial_tasks,
    require_run_available,
)
from taskforge.runs.persistence_ports import (
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
                require_run_available(prepared.status)
                if prepared.snapshot is None:
                    raise WorkflowVersionUnavailable
                initial_tasks = materialize_initial_tasks(prepared.snapshot)
                task_values = tuple(
                    NewTaskRun(uuid4(), task.step_identifier, task.status)
                    for task in initial_tasks
                )
                timestamps = await transaction.insert_complete_run(
                    prepared,
                    run,
                    accepted_input,
                    task_values,
                )
                await transaction.commit()
        except WorkflowRunRecordConflict as error:
            raise WorkflowRunPersistenceConflict from error
        except WorkflowRunPersistenceUnavailable as error:
            raise WorkflowRunServiceUnavailable from error
        runnable_count = sum(
            task.status is TaskRunStatus.RUNNABLE for task in task_values
        )
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
