"""Application service for workflow run target resolution."""

from __future__ import annotations

from typing import cast
from uuid import UUID, uuid4

from taskforge.audit.domain import (
    AuditActor,
    AuditActorKind,
    AuditOutcome,
    AuditRecord,
    AuditRejected,
    bounded_string_set,
)
from taskforge.identity.authorization import OwnerFilter
from taskforge.persistence.audit import RejectedAuditRecorder
from taskforge.retries.domain import InspectedRetryEventPage, RetryEventCursor
from taskforge.runs.domain import (
    MAX_WORKFLOW_RECONCILIATION_ITERATIONS,
    CancellationFinalizationResult,
    CancellationPropagationResult,
    CancellationSettlementResult,
    CreatedFailedSubgraphWorkflowReplay,
    CreatedFullWorkflowReplay,
    CreatedWorkflowRun,
    DependencyFailurePropagationResult,
    FailedSubgraphReplaySelectionInvalid,
    InitialTaskRun,
    InspectedTaskRun,
    InspectedWorkflowRun,
    InvalidFailedSubgraphReplayRequest,
    InvalidWorkflowRunCancellationIdempotencyKey,
    InvalidWorkflowRunCancellationReason,
    InvalidWorkflowRunIdempotencyKey,
    NewTaskRun,
    NewWorkflowRun,
    ResolvedWorkflowVersion,
    RunnableTransitionResult,
    TaskRunStatus,
    WorkflowReplayIdempotency,
    WorkflowReplayIdempotencyConflict,
    WorkflowReplayMode,
    WorkflowRunCancellationIdempotencyConflict,
    WorkflowRunCancellationOutcome,
    WorkflowRunCancellationResult,
    WorkflowRunEvaluationResult,
    WorkflowRunIdempotency,
    WorkflowRunIdempotencyConflict,
    WorkflowRunInput,
    WorkflowRunReconciliationResult,
    WorkflowRunReplayNotEligible,
    WorkflowRunReplaySourceInvalid,
    WorkflowRunStatus,
    WorkflowVersionSelection,
    canonicalize_failed_subgraph_replay_request,
    create_workflow_replay_idempotency,
    create_workflow_replay_idempotency_from_digest,
    create_workflow_replay_key_digest,
    create_workflow_run_cancellation_command,
    create_workflow_run_idempotency,
    create_workflow_run_input,
    idempotency_fingerprints_match,
    materialize_failed_subgraph_replay_tasks,
    materialize_initial_tasks,
    reconstruct_failed_subgraph_selection,
    require_full_replay_source_terminal,
    require_run_available,
)
from taskforge.runs.persistence_ports import (
    ExistingIdempotentWorkflowReplay,
    ExistingIdempotentWorkflowRun,
    PersistedCancellationOutcome,
    PreparedFailedSubgraphWorkflowReplay,
    PreparedFullWorkflowReplay,
    PreparedWorkflowRunCreation,
    RetryEventInspectionRepository,
    WorkflowRunCancellationPersistenceInvariantViolation,
    WorkflowRunCreationTransaction,
    WorkflowRunIdempotencyRecordConflict,
    WorkflowRunInspectionInvariantViolation,
    WorkflowRunPersistenceUnavailable,
    WorkflowRunRecordConflict,
    WorkflowRunReplayPersistenceInvariantViolation,
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


_REPLAY_AUDIT_REASONS: dict[type[Exception], str] = {
    InvalidWorkflowRunIdempotencyKey: "invalid_idempotency_key",
    WorkflowRunNotFound: "source_not_visible",
    WorkflowRunReplayNotEligible: "source_not_replayable",
    InvalidFailedSubgraphReplayRequest: "invalid_failed_subgraph_request",
    FailedSubgraphReplaySelectionInvalid: "failed_subgraph_invalid",
    WorkflowReplayIdempotencyConflict: "idempotency_conflict",
    WorkflowRunPersistenceConflict: "persistence_conflict",
}
_CANCELLATION_AUDIT_REASONS: dict[type[Exception], str] = {
    InvalidWorkflowRunCancellationIdempotencyKey: "invalid_idempotency_key",
    InvalidWorkflowRunCancellationReason: "invalid_reason",
    WorkflowRunNotFound: "workflow_run_not_visible",
    WorkflowRunCancellationIdempotencyConflict: "idempotency_conflict",
}
_RUN_CREATION_AUDIT_REASONS: dict[type[Exception], str] = {
    WorkflowRunIdempotencyConflict: "idempotency_conflict",
}


class WorkflowRunService:
    def __init__(
        self,
        repository: WorkflowRunRepository,
        rejected_audit: RejectedAuditRecorder | None = None,
    ) -> None:
        self._repository = repository
        self._rejected_audit = rejected_audit

    async def _audit_replay_rejection(
        self,
        error: Exception,
        source_id: UUID,
        principal_id: UUID,
        correlation_id: UUID,
        failed_steps: object = None,
    ) -> None:
        if self._rejected_audit is None:
            return
        reason = _REPLAY_AUDIT_REASONS[type(error)]
        provenance: dict[str, object] = {}
        if isinstance(failed_steps, (list, tuple)) and all(
            isinstance(item, str) for item in failed_steps
        ):
            provenance["failed_steps"] = bounded_string_set(tuple(failed_steps))
        try:
            await self._rejected_audit.record(
                AuditRecord(
                    uuid4(),
                    AuditActor(
                        AuditActorKind.API_PRINCIPAL,
                        api_principal_id=principal_id,
                    ),
                    "workflow_run.replay",
                    AuditOutcome.REJECTED,
                    "workflow_run",
                    source_id,
                    str(correlation_id),
                    provenance,
                    reason,
                )
            )
        except AuditRejected as audit_error:
            raise WorkflowRunServiceUnavailable from audit_error

    async def create_full_replay(
        self,
        source_workflow_run_id: UUID,
        owner_filter: OwnerFilter,
        *,
        requested_by_principal_id: UUID,
        correlation_id: UUID,
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
                    correlation_id,
                )
        except WorkflowRunReplayPersistenceInvariantViolation as error:
            raise WorkflowRunReplayInvariantError from error
        except WorkflowRunRecordConflict as error:
            rejection = WorkflowRunPersistenceConflict()
            await self._audit_replay_rejection(
                rejection,
                source_workflow_run_id,
                requested_by_principal_id,
                correlation_id,
            )
            raise rejection from error
        except WorkflowRunPersistenceUnavailable as error:
            raise WorkflowRunServiceUnavailable from error
        except (WorkflowRunNotFound, WorkflowRunReplayNotEligible) as error:
            await self._audit_replay_rejection(
                error, source_workflow_run_id, requested_by_principal_id, correlation_id
            )
            raise
        return CreatedFullWorkflowReplay(
            source_workflow_run_id,
            WorkflowReplayMode.FULL,
            created,
        )

    async def create_idempotent_full_replay(
        self,
        source_workflow_run_id: UUID,
        owner_filter: OwnerFilter,
        *,
        requested_by_principal_id: UUID,
        idempotency_key: object,
        correlation_id: UUID,
    ) -> CreatedFullWorkflowReplay:
        """Create or recover one requester/source-scoped full replay."""
        prepared: PreparedFullWorkflowReplay | None = None
        idempotency: WorkflowReplayIdempotency | None = None
        try:
            async with self._repository.creation_transaction() as transaction:
                prepared = await transaction.prepare_replay_source(
                    source_workflow_run_id, owner_filter
                )
                if prepared is None:
                    raise WorkflowRunNotFound
                idempotency = create_workflow_replay_idempotency(
                    idempotency_key,
                    source_workflow_run_id=source_workflow_run_id,
                    requested_by_principal_id=requested_by_principal_id,
                    mode=WorkflowReplayMode.FULL,
                    requested_scope={},
                )
                existing = await transaction.find_idempotent_replay(
                    requested_by_principal_id,
                    prepared.creation.workflow_definition_id,
                    idempotency.key_digest,
                )
                if existing is not None:
                    return _replay_idempotent_full_replay(
                        existing, idempotency, prepared
                    )
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
                    correlation_id,
                    idempotency=idempotency,
                )
        except WorkflowRunIdempotencyRecordConflict:
            assert prepared is not None
            assert idempotency is not None
            try:
                return await self._recover_idempotent_full_replay(
                    prepared, requested_by_principal_id, idempotency
                )
            except WorkflowReplayIdempotencyConflict as error:
                await self._audit_replay_rejection(
                    error,
                    source_workflow_run_id,
                    requested_by_principal_id,
                    correlation_id,
                )
                raise
        except WorkflowRunReplayPersistenceInvariantViolation as error:
            raise WorkflowRunReplayInvariantError from error
        except WorkflowRunRecordConflict as error:
            rejection = WorkflowRunPersistenceConflict()
            await self._audit_replay_rejection(
                rejection,
                source_workflow_run_id,
                requested_by_principal_id,
                correlation_id,
            )
            raise rejection from error
        except WorkflowRunPersistenceUnavailable as error:
            raise WorkflowRunServiceUnavailable from error
        except (
            WorkflowRunNotFound,
            WorkflowRunReplayNotEligible,
            InvalidWorkflowRunIdempotencyKey,
            WorkflowReplayIdempotencyConflict,
        ) as error:
            await self._audit_replay_rejection(
                error, source_workflow_run_id, requested_by_principal_id, correlation_id
            )
            raise
        return CreatedFullWorkflowReplay(
            source_workflow_run_id, WorkflowReplayMode.FULL, created
        )

    async def create_failed_subgraph_replay(
        self,
        source_workflow_run_id: UUID,
        owner_filter: OwnerFilter,
        *,
        requested_by_principal_id: UUID,
        failed_step_identifiers: object,
        correlation_id: UUID,
    ) -> CreatedFailedSubgraphWorkflowReplay:
        """Create one fresh dependency-safe replay of requested failed roots."""
        try:
            async with self._repository.creation_transaction() as transaction:
                prepared = await transaction.prepare_failed_subgraph_replay(
                    source_workflow_run_id, owner_filter
                )
                if prepared is None:
                    raise WorkflowRunNotFound
                snapshot = prepared.creation.snapshot
                if snapshot is None:
                    raise WorkflowRunReplayInvariantError
                try:
                    selection = materialize_failed_subgraph_replay_tasks(
                        snapshot,
                        prepared.source_status,
                        prepared.source_tasks,
                        failed_step_identifiers,
                    )
                except WorkflowRunReplaySourceInvalid as error:
                    raise WorkflowRunReplayInvariantError from error
                accepted_input = create_workflow_run_input(
                    prepared.input_snapshot.payload,
                    prepared.input_snapshot.input_references,
                )
                created = await _create_prepared_failed_subgraph_replay(
                    transaction,
                    prepared,
                    accepted_input,
                    requested_by_principal_id,
                    selection.initial_tasks,
                    selection.requested_scope,
                    correlation_id,
                )
        except WorkflowRunReplayPersistenceInvariantViolation as error:
            raise WorkflowRunReplayInvariantError from error
        except WorkflowRunRecordConflict as error:
            rejection = WorkflowRunPersistenceConflict()
            await self._audit_replay_rejection(
                rejection,
                source_workflow_run_id,
                requested_by_principal_id,
                correlation_id,
                failed_step_identifiers,
            )
            raise rejection from error
        except WorkflowRunPersistenceUnavailable as error:
            raise WorkflowRunServiceUnavailable from error
        except (
            WorkflowRunNotFound,
            InvalidFailedSubgraphReplayRequest,
            FailedSubgraphReplaySelectionInvalid,
        ) as error:
            await self._audit_replay_rejection(
                error,
                source_workflow_run_id,
                requested_by_principal_id,
                correlation_id,
                failed_step_identifiers,
            )
            raise
        return CreatedFailedSubgraphWorkflowReplay(
            source_workflow_run_id,
            WorkflowReplayMode.FAILED_SUBGRAPH,
            selection.canonical_failed_step_identifiers,
            selection.selected_step_identifiers,
            created,
        )

    async def create_idempotent_failed_subgraph_replay(
        self,
        source_workflow_run_id: UUID,
        owner_filter: OwnerFilter,
        *,
        requested_by_principal_id: UUID,
        failed_step_identifiers: object,
        idempotency_key: object,
        correlation_id: UUID,
    ) -> CreatedFailedSubgraphWorkflowReplay:
        """Create or recover one requester/source-scoped failed replay."""
        prepared: PreparedFullWorkflowReplay | None = None
        idempotency: WorkflowReplayIdempotency | None = None
        canonical_roots: tuple[str, ...] | None = None
        try:
            async with self._repository.creation_transaction() as transaction:
                prepared = await transaction.prepare_replay_source(
                    source_workflow_run_id, owner_filter
                )
                if prepared is None:
                    raise WorkflowRunNotFound
                key_digest = create_workflow_replay_key_digest(
                    idempotency_key, source_workflow_run_id
                )
                canonical_roots = canonicalize_failed_subgraph_replay_request(
                    failed_step_identifiers
                )
                scope: dict[str, object] = {
                    "failed_step_identifiers": list(canonical_roots)
                }
                idempotency = create_workflow_replay_idempotency_from_digest(
                    key_digest,
                    source_workflow_run_id=source_workflow_run_id,
                    requested_by_principal_id=requested_by_principal_id,
                    mode=WorkflowReplayMode.FAILED_SUBGRAPH,
                    requested_scope=scope,
                )
                existing = await transaction.find_idempotent_replay(
                    requested_by_principal_id,
                    prepared.creation.workflow_definition_id,
                    idempotency.key_digest,
                )
                if existing is not None:
                    return _replay_idempotent_failed_subgraph_replay(
                        existing, idempotency, prepared, canonical_roots
                    )
                source_tasks = await transaction.load_replay_source_tasks(
                    source_workflow_run_id
                )
                failed_prepared = PreparedFailedSubgraphWorkflowReplay(
                    source_workflow_run_id,
                    prepared.source_status,
                    prepared.creation,
                    prepared.input_snapshot,
                    source_tasks,
                )
                snapshot = prepared.creation.snapshot
                if snapshot is None:
                    raise WorkflowRunReplayInvariantError
                try:
                    selection = materialize_failed_subgraph_replay_tasks(
                        snapshot,
                        prepared.source_status,
                        source_tasks,
                        canonical_roots,
                    )
                except WorkflowRunReplaySourceInvalid as error:
                    raise WorkflowRunReplayInvariantError from error
                accepted_input = create_workflow_run_input(
                    prepared.input_snapshot.payload,
                    prepared.input_snapshot.input_references,
                )
                created = await _create_prepared_failed_subgraph_replay(
                    transaction,
                    failed_prepared,
                    accepted_input,
                    requested_by_principal_id,
                    selection.initial_tasks,
                    selection.requested_scope,
                    correlation_id,
                    idempotency=idempotency,
                )
        except WorkflowRunIdempotencyRecordConflict:
            assert prepared is not None
            assert idempotency is not None
            assert canonical_roots is not None
            try:
                return await self._recover_idempotent_failed_subgraph_replay(
                    prepared, requested_by_principal_id, idempotency, canonical_roots
                )
            except WorkflowReplayIdempotencyConflict as error:
                await self._audit_replay_rejection(
                    error,
                    source_workflow_run_id,
                    requested_by_principal_id,
                    correlation_id,
                    canonical_roots,
                )
                raise
        except WorkflowRunReplayPersistenceInvariantViolation as error:
            raise WorkflowRunReplayInvariantError from error
        except WorkflowRunRecordConflict as error:
            rejection = WorkflowRunPersistenceConflict()
            await self._audit_replay_rejection(
                rejection,
                source_workflow_run_id,
                requested_by_principal_id,
                correlation_id,
                failed_step_identifiers,
            )
            raise rejection from error
        except WorkflowRunPersistenceUnavailable as error:
            raise WorkflowRunServiceUnavailable from error
        except (
            WorkflowRunNotFound,
            InvalidFailedSubgraphReplayRequest,
            FailedSubgraphReplaySelectionInvalid,
            InvalidWorkflowRunIdempotencyKey,
            WorkflowReplayIdempotencyConflict,
        ) as error:
            await self._audit_replay_rejection(
                error,
                source_workflow_run_id,
                requested_by_principal_id,
                correlation_id,
                failed_step_identifiers,
            )
            raise
        return CreatedFailedSubgraphWorkflowReplay(
            source_workflow_run_id,
            WorkflowReplayMode.FAILED_SUBGRAPH,
            selection.canonical_failed_step_identifiers,
            selection.selected_step_identifiers,
            created,
        )

    async def _recover_idempotent_full_replay(
        self,
        prepared: PreparedFullWorkflowReplay,
        principal_id: UUID,
        idempotency: WorkflowReplayIdempotency,
    ) -> CreatedFullWorkflowReplay:
        existing = await self._find_idempotent_replay(
            principal_id, prepared.creation.workflow_definition_id, idempotency
        )
        return _replay_idempotent_full_replay(existing, idempotency, prepared)

    async def _recover_idempotent_failed_subgraph_replay(
        self,
        prepared: PreparedFullWorkflowReplay,
        principal_id: UUID,
        idempotency: WorkflowReplayIdempotency,
        canonical_roots: tuple[str, ...],
    ) -> CreatedFailedSubgraphWorkflowReplay:
        existing = await self._find_idempotent_replay(
            principal_id, prepared.creation.workflow_definition_id, idempotency
        )
        return _replay_idempotent_failed_subgraph_replay(
            existing, idempotency, prepared, canonical_roots
        )

    async def _find_idempotent_replay(
        self,
        principal_id: UUID,
        workflow_id: UUID,
        idempotency: WorkflowReplayIdempotency,
    ) -> ExistingIdempotentWorkflowReplay:
        try:
            existing = await self._repository.find_idempotent_replay(
                principal_id, workflow_id, idempotency.key_digest
            )
        except WorkflowRunReplayPersistenceInvariantViolation as error:
            raise WorkflowRunReplayInvariantError from error
        except WorkflowRunPersistenceUnavailable as error:
            raise WorkflowRunServiceUnavailable from error
        if existing is None:
            raise WorkflowRunPersistenceConflict(
                "idempotency winner was not found after uniqueness conflict"
            )
        return existing

    async def cancel_run(
        self,
        workflow_run_id: UUID,
        owner_filter: OwnerFilter,
        *,
        requested_by_principal_id: UUID,
        idempotency_key: object,
        reason: object,
        correlation_id: UUID | None = None,
    ) -> WorkflowRunCancellationResult:
        """Accept or replay one owner-scoped workflow cancellation intention."""
        try:
            command = create_workflow_run_cancellation_command(
                workflow_run_id,
                requested_by_principal_id,
                idempotency_key=idempotency_key,
                reason=reason,
                correlation_id=correlation_id,
            )
            persisted = await self._repository.cancel_run(
                workflow_run_id, owner_filter, command
            )
        except (
            InvalidWorkflowRunCancellationIdempotencyKey,
            InvalidWorkflowRunCancellationReason,
        ) as error:
            await self._audit_command_rejection(
                error,
                action="workflow_run.cancel",
                resource_id=workflow_run_id,
                principal_id=requested_by_principal_id,
                correlation_id=correlation_id,
                reasons=_CANCELLATION_AUDIT_REASONS,
            )
            raise
        except WorkflowRunCancellationPersistenceInvariantViolation as error:
            raise WorkflowRunCancellationInvariantError from error
        except WorkflowRunPersistenceUnavailable as error:
            raise WorkflowRunServiceUnavailable from error
        if persisted is None:
            await self._audit_command_rejection(
                WorkflowRunNotFound(),
                action="workflow_run.cancel",
                resource_id=workflow_run_id,
                principal_id=requested_by_principal_id,
                correlation_id=correlation_id,
                reasons=_CANCELLATION_AUDIT_REASONS,
            )
            raise WorkflowRunNotFound
        if persisted.outcome is PersistedCancellationOutcome.IDEMPOTENCY_CONFLICT:
            await self._audit_command_rejection(
                WorkflowRunCancellationIdempotencyConflict(),
                action="workflow_run.cancel",
                resource_id=workflow_run_id,
                principal_id=requested_by_principal_id,
                correlation_id=correlation_id,
                reasons=_CANCELLATION_AUDIT_REASONS,
            )
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

    async def _audit_command_rejection(
        self,
        error: Exception,
        *,
        action: str,
        resource_id: UUID,
        principal_id: UUID,
        correlation_id: UUID | None,
        reasons: dict[type[Exception], str],
        provenance: dict[str, object] | None = None,
        resource_type: str = "workflow_run",
    ) -> None:
        if self._rejected_audit is None:
            return
        try:
            await self._rejected_audit.record(
                AuditRecord(
                    uuid4(),
                    AuditActor(
                        AuditActorKind.API_PRINCIPAL, api_principal_id=principal_id
                    ),
                    action,
                    AuditOutcome.REJECTED,
                    resource_type,
                    resource_id,
                    str(correlation_id) if correlation_id else None,
                    provenance or {},
                    reasons[type(error)],
                )
            )
        except AuditRejected as audit_error:
            raise WorkflowRunServiceUnavailable from audit_error

    async def resolve_version(
        self,
        workflow_id: UUID,
        *,
        owner_filter: OwnerFilter,
        selection: WorkflowVersionSelection,
    ) -> ResolvedWorkflowVersion:
        """Resolve a target valid at lookup time, without admitting a run."""
        try:
            record = await self._repository.resolve_workflow_version(
                workflow_id,
                owner_filter,
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
        owner_filter: OwnerFilter,
    ) -> InspectedWorkflowRun:
        try:
            run = await self._repository.get_run(run_id, owner_filter)
        except WorkflowRunPersistenceUnavailable as error:
            raise WorkflowRunServiceUnavailable from error
        if run is None:
            raise WorkflowRunNotFound
        return run

    async def list_task_runs(
        self,
        run_id: UUID,
        *,
        owner_filter: OwnerFilter,
    ) -> tuple[InspectedTaskRun, ...]:
        try:
            tasks = await self._repository.list_task_runs(run_id, owner_filter)
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
        owner_filter: OwnerFilter,
    ) -> InspectedTaskRun:
        try:
            task = await self._repository.get_task_run(task_run_id, owner_filter)
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
        owner_filter: OwnerFilter,
        limit: int,
        cursor: RetryEventCursor | None,
    ) -> InspectedRetryEventPage:
        try:
            page = await cast(
                RetryEventInspectionRepository, self._repository
            ).list_retry_events(
                task_run_id,
                owner_filter,
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
        owner_filter: OwnerFilter,
        requested_by_principal_id: UUID,
        selection: WorkflowVersionSelection,
        input_snapshot: WorkflowRunInput,
        correlation_id: UUID | None = None,
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
                    workflow_id, owner_filter, selection
                )
                if prepared is None:
                    raise WorkflowRunTargetNotFound
                created = await _create_prepared_run(
                    transaction,
                    prepared,
                    accepted_input,
                    requested_by_principal_id,
                    run=run,
                    correlation_id=correlation_id,
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
        owner_filter: OwnerFilter,
        requested_by_principal_id: UUID,
        selection: WorkflowVersionSelection,
        input_snapshot: WorkflowRunInput,
        idempotency_key: object,
        correlation_id: UUID | None = None,
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
                    owner_filter,
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
                    correlation_id=correlation_id,
                    idempotency=idempotency,
                )
        except WorkflowRunIdempotencyConflict as error:
            await self._audit_command_rejection(
                error,
                action="workflow_run.create",
                resource_id=workflow_id,
                resource_type="workflow",
                principal_id=requested_by_principal_id,
                correlation_id=correlation_id,
                reasons=_RUN_CREATION_AUDIT_REASONS,
            )
            raise
        except WorkflowRunIdempotencyRecordConflict:
            try:
                return await self._recover_idempotency_conflict(
                    requested_by_principal_id,
                    workflow_id,
                    idempotency,
                )
            except WorkflowRunIdempotencyConflict as error:
                await self._audit_command_rejection(
                    error,
                    action="workflow_run.create",
                    resource_id=workflow_id,
                    resource_type="workflow",
                    principal_id=requested_by_principal_id,
                    correlation_id=correlation_id,
                    reasons=_RUN_CREATION_AUDIT_REASONS,
                )
                raise
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


def _verify_idempotent_replay(
    existing: ExistingIdempotentWorkflowReplay,
    idempotency: WorkflowReplayIdempotency,
    prepared: PreparedFullWorkflowReplay,
    expected_mode: WorkflowReplayMode,
    expected_scope: dict[str, object],
) -> None:
    if not idempotency_fingerprints_match(
        existing.request_fingerprint,
        idempotency.request_fingerprint,
    ):
        raise WorkflowReplayIdempotencyConflict
    if (
        existing.source_workflow_run_id != prepared.source_workflow_run_id
        or existing.mode is not expected_mode
        or existing.requested_scope != expected_scope
        or existing.run.workflow_definition_id
        != prepared.creation.workflow_definition_id
    ):
        raise WorkflowRunReplayInvariantError


def _replay_idempotent_full_replay(
    existing: ExistingIdempotentWorkflowReplay,
    idempotency: WorkflowReplayIdempotency,
    prepared: PreparedFullWorkflowReplay,
) -> CreatedFullWorkflowReplay:
    _verify_idempotent_replay(
        existing, idempotency, prepared, WorkflowReplayMode.FULL, {}
    )
    return CreatedFullWorkflowReplay(
        prepared.source_workflow_run_id, WorkflowReplayMode.FULL, existing.run
    )


def _replay_idempotent_failed_subgraph_replay(
    existing: ExistingIdempotentWorkflowReplay,
    idempotency: WorkflowReplayIdempotency,
    prepared: PreparedFullWorkflowReplay,
    canonical_roots: tuple[str, ...],
) -> CreatedFailedSubgraphWorkflowReplay:
    scope: dict[str, object] = {"failed_step_identifiers": list(canonical_roots)}
    _verify_idempotent_replay(
        existing,
        idempotency,
        prepared,
        WorkflowReplayMode.FAILED_SUBGRAPH,
        scope,
    )
    snapshot = prepared.creation.snapshot
    if snapshot is None:
        raise WorkflowRunReplayInvariantError
    try:
        selected = reconstruct_failed_subgraph_selection(snapshot, canonical_roots)
    except WorkflowRunReplaySourceInvalid as error:
        raise WorkflowRunReplayInvariantError from error
    return CreatedFailedSubgraphWorkflowReplay(
        prepared.source_workflow_run_id,
        WorkflowReplayMode.FAILED_SUBGRAPH,
        canonical_roots,
        selected,
        existing.run,
    )


async def _create_prepared_run(
    transaction: WorkflowRunCreationTransaction,
    prepared: PreparedWorkflowRunCreation,
    input_snapshot: WorkflowRunInput,
    requested_by_principal_id: UUID,
    *,
    run: NewWorkflowRun | None = None,
    correlation_id: UUID | None = None,
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
        correlation_id,
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
    correlation_id: UUID,
    *,
    idempotency: WorkflowReplayIdempotency | None = None,
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
        prepared, run, input_snapshot, task_values, correlation_id, idempotency
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


async def _create_prepared_failed_subgraph_replay(
    transaction: WorkflowRunCreationTransaction,
    prepared: PreparedFailedSubgraphWorkflowReplay,
    input_snapshot: WorkflowRunInput,
    requested_by_principal_id: UUID,
    initial_tasks: tuple[InitialTaskRun, ...],
    requested_scope: dict[str, object],
    correlation_id: UUID,
    *,
    idempotency: WorkflowReplayIdempotency | None = None,
) -> CreatedWorkflowRun:
    snapshot = prepared.creation.snapshot
    if snapshot is None:
        raise WorkflowRunReplayInvariantError
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
    timestamps = await transaction.insert_failed_subgraph_replay(
        prepared,
        run,
        input_snapshot,
        task_values,
        requested_scope,
        correlation_id,
        idempotency,
    )
    await transaction.commit()
    runnable_count = sum(task.status is TaskRunStatus.RUNNABLE for task in task_values)
    blocked_count = sum(task.status is TaskRunStatus.BLOCKED for task in task_values)
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
        blocked_task_count=blocked_count,
    )
