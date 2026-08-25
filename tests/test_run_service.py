"""Workflow run target resolution service tests."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from types import TracebackType
from typing import TypeVar
from uuid import UUID, uuid4

import pytest

from taskforge.identity.authorization import OwnerFilter
from taskforge.runs.domain import (
    MAX_WORKFLOW_RECONCILIATION_ITERATIONS,
    CancellationFinalizationOutcome,
    CancellationFinalizationResult,
    CancellationPropagationResult,
    CancellationSettlementResult,
    CreatedWorkflowRun,
    DependencyFailurePropagationResult,
    ExplicitWorkflowVersion,
    InspectedTaskRun,
    InspectedWorkflowRun,
    InvalidWorkflowRunIdempotencyKey,
    InvalidWorkflowRunInput,
    LatestWorkflowVersion,
    NewTaskRun,
    NewWorkflowRun,
    RunnableTransitionResult,
    SourceTaskRunState,
    TaskRunStatus,
    WorkflowReplayIdempotency,
    WorkflowReplayIdempotencyConflict,
    WorkflowReplayMode,
    WorkflowRunCancellationCommand,
    WorkflowRunEvaluationResult,
    WorkflowRunIdempotency,
    WorkflowRunIdempotencyConflict,
    WorkflowRunInput,
    WorkflowRunReconciliationResult,
    WorkflowRunReplayNotEligible,
    WorkflowRunStatus,
    WorkflowRunTargetUnavailable,
    WorkflowRunVersionDependency,
    WorkflowRunVersionSnapshot,
    WorkflowRunVersionStep,
    WorkflowVersionSelection,
    create_workflow_replay_idempotency,
    create_workflow_run_idempotency,
)
from taskforge.runs.persistence_ports import (
    ExistingIdempotentWorkflowReplay,
    ExistingIdempotentWorkflowRun,
    IdempotentCreationPreparation,
    PersistedWorkflowRunCancellation,
    PreparedFailedSubgraphWorkflowReplay,
    PreparedFullWorkflowReplay,
    PreparedWorkflowRunCreation,
    WorkflowRunCreationTransaction,
    WorkflowRunCreationTransactionContext,
    WorkflowRunIdempotencyRecordConflict,
    WorkflowRunPersistenceUnavailable,
    WorkflowRunRecordConflict,
    WorkflowRunReplayPersistenceInvariantViolation,
    WorkflowRunTimestamps,
    WorkflowVersionResolutionRecord,
)
from taskforge.runs.service import (
    TaskRunNotFound,
    WorkflowRunNotFound,
    WorkflowRunPersistenceConflict,
    WorkflowRunReplayInvariantError,
    WorkflowRunService,
    WorkflowRunServiceUnavailable,
    WorkflowRunTargetNotFound,
    WorkflowVersionUnavailable,
)
from taskforge.workflows.domain import WorkflowDefinitionStatus

T = TypeVar("T")


@dataclass
class FakeRepository:
    result: WorkflowVersionResolutionRecord | None = None
    failure: BaseException | None = None
    run_result: InspectedWorkflowRun | None = None
    task_results: tuple[InspectedTaskRun, ...] | None = None
    task_result: InspectedTaskRun | None = None
    transition_result: RunnableTransitionResult | None = None
    propagation_result: DependencyFailurePropagationResult | None = None
    evaluation_result: WorkflowRunEvaluationResult | None = None
    suppression_result: CancellationPropagationResult | None = None

    def __post_init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    def creation_transaction(self) -> WorkflowRunCreationTransactionContext:
        raise AssertionError("creation transaction was not expected")

    async def cancel_run(
        self,
        workflow_run_id: UUID,
        owner_filter: OwnerFilter,
        command: WorkflowRunCancellationCommand,
    ) -> PersistedWorkflowRunCancellation | None:
        del workflow_run_id, owner_filter, command
        raise AssertionError("cancellation was not expected")

    async def find_idempotent_run(
        self, principal_id: UUID, workflow_id: UUID, key_digest: str
    ) -> ExistingIdempotentWorkflowRun | None:
        del principal_id, workflow_id, key_digest
        raise AssertionError("idempotency recovery was not expected")

    async def get_run(
        self, run_id: UUID, owner_principal_id: UUID
    ) -> InspectedWorkflowRun | None:
        del run_id, owner_principal_id
        if self.failure is not None:
            raise self.failure
        return self.run_result

    async def list_task_runs(
        self, run_id: UUID, owner_principal_id: UUID
    ) -> tuple[InspectedTaskRun, ...] | None:
        del run_id, owner_principal_id
        if self.failure is not None:
            raise self.failure
        return self.task_results

    async def get_task_run(
        self, task_run_id: UUID, owner_principal_id: UUID
    ) -> InspectedTaskRun | None:
        del task_run_id, owner_principal_id
        if self.failure is not None:
            raise self.failure
        return self.task_result

    async def transition_runnable_tasks(
        self, workflow_run_id: UUID
    ) -> RunnableTransitionResult:
        self.calls.append(("transition_runnable_tasks", workflow_run_id))
        if self.failure is not None:
            raise self.failure
        return self.transition_result or RunnableTransitionResult(
            workflow_run_id, (), ()
        )

    async def suppress_unstarted_tasks(
        self, workflow_run_id: UUID
    ) -> CancellationPropagationResult:
        self.calls.append(("suppress_unstarted_tasks", workflow_run_id))
        if self.failure is not None:
            raise self.failure
        return self.suppression_result or CancellationPropagationResult(
            workflow_run_id, True, WorkflowRunStatus.RUNNING, (), ()
        )

    async def settle_dispatched_tasks(
        self, workflow_run_id: UUID
    ) -> CancellationSettlementResult:
        del workflow_run_id
        raise AssertionError("cancellation settlement was not expected")

    async def finalize_workflow_run_cancellation(
        self, workflow_run_id: UUID
    ) -> CancellationFinalizationResult:
        del workflow_run_id
        raise AssertionError("cancellation finalization was not expected")

    async def propagate_dependency_failures(
        self, workflow_run_id: UUID
    ) -> DependencyFailurePropagationResult:
        self.calls.append(("propagate_dependency_failures", workflow_run_id))
        if self.failure is not None:
            raise self.failure
        return self.propagation_result or DependencyFailurePropagationResult(
            workflow_run_id, (), ()
        )

    async def evaluate_workflow_run_state(
        self, workflow_run_id: UUID
    ) -> WorkflowRunEvaluationResult:
        self.calls.append(("evaluate_workflow_run_state", workflow_run_id))
        if self.failure is not None:
            raise self.failure
        return self.evaluation_result or WorkflowRunEvaluationResult(
            workflow_run_id,
            False,
            None,
            None,
        )

    async def resolve_workflow_version(
        self,
        workflow_id: UUID,
        owner_principal_id: UUID,
        selection: WorkflowVersionSelection,
    ) -> WorkflowVersionResolutionRecord | None:
        self.calls.append((workflow_id, owner_principal_id, selection))
        if self.failure is not None:
            raise self.failure
        return self.result


def record(
    status: WorkflowDefinitionStatus = WorkflowDefinitionStatus.ENABLED,
    *,
    with_version: bool = True,
) -> WorkflowVersionResolutionRecord:
    return WorkflowVersionResolutionRecord(
        workflow_definition_id=uuid4(),
        status=status,
        workflow_version_id=uuid4() if with_version else None,
        version_number=3 if with_version else None,
    )


@pytest.mark.parametrize(
    "selection", (ExplicitWorkflowVersion(2), LatestWorkflowVersion())
)
def test_service_resolves_explicit_and_latest_owner_scoped_targets(
    selection: WorkflowVersionSelection,
) -> None:
    repository = FakeRepository(record())
    service = WorkflowRunService(repository)
    workflow_id, owner_id = uuid4(), uuid4()

    resolved = asyncio.run(
        service.resolve_version(
            workflow_id, owner_principal_id=owner_id, selection=selection
        )
    )

    assert repository.calls == [(workflow_id, owner_id, selection)]
    assert resolved.workflow_definition_id == repository.result.workflow_definition_id  # type: ignore[union-attr]
    assert resolved.workflow_version_id == repository.result.workflow_version_id  # type: ignore[union-attr]
    assert resolved.version_number == 3


def test_absent_or_cross_owner_target_is_concealed_as_not_found() -> None:
    service = WorkflowRunService(FakeRepository())

    with pytest.raises(WorkflowRunTargetNotFound):
        asyncio.run(
            service.resolve_version(
                uuid4(),
                owner_principal_id=uuid4(),
                selection=LatestWorkflowVersion(),
            )
        )


@pytest.mark.parametrize(
    "status",
    (
        WorkflowDefinitionStatus.DRAFT,
        WorkflowDefinitionStatus.DISABLED,
        WorkflowDefinitionStatus.ARCHIVED,
    ),
)
def test_unavailable_status_takes_precedence_over_absent_version(
    status: WorkflowDefinitionStatus,
) -> None:
    service = WorkflowRunService(FakeRepository(record(status, with_version=False)))

    with pytest.raises(WorkflowRunTargetUnavailable) as caught:
        asyncio.run(
            service.resolve_version(
                uuid4(),
                owner_principal_id=uuid4(),
                selection=LatestWorkflowVersion(),
            )
        )

    assert caught.value.status is status


def test_enabled_target_without_selected_version_is_unavailable() -> None:
    service = WorkflowRunService(FakeRepository(record(with_version=False)))

    with pytest.raises(WorkflowVersionUnavailable):
        asyncio.run(
            service.resolve_version(
                uuid4(),
                owner_principal_id=uuid4(),
                selection=ExplicitWorkflowVersion(9),
            )
        )


def test_persistence_unavailability_is_normalized() -> None:
    service = WorkflowRunService(
        FakeRepository(failure=WorkflowRunPersistenceUnavailable())
    )

    with pytest.raises(WorkflowRunServiceUnavailable):
        asyncio.run(
            service.resolve_version(
                uuid4(),
                owner_principal_id=uuid4(),
                selection=LatestWorkflowVersion(),
            )
        )


def inspected_values() -> tuple[InspectedWorkflowRun, InspectedTaskRun]:
    now = datetime.now(UTC)
    run_id, workflow_id, version_id, principal_id = (
        uuid4(),
        uuid4(),
        uuid4(),
        uuid4(),
    )
    return (
        InspectedWorkflowRun(
            run_id,
            workflow_id,
            version_id,
            1,
            principal_id,
            WorkflowRunStatus.PENDING,
            now,
            now,
        ),
        InspectedTaskRun(
            uuid4(),
            run_id,
            version_id,
            "root",
            TaskRunStatus.RUNNABLE,
            now,
            now,
        ),
    )


def test_service_returns_owner_scoped_run_and_task_inspection() -> None:
    run, task = inspected_values()
    service = WorkflowRunService(
        FakeRepository(run_result=run, task_results=(task,), task_result=task)
    )

    assert (
        asyncio.run(
            service.get_run(run.id, owner_principal_id=run.requested_by_principal_id)
        )
        is run
    )
    assert asyncio.run(
        service.list_task_runs(run.id, owner_principal_id=run.requested_by_principal_id)
    ) == (task,)
    assert (
        asyncio.run(
            service.get_task_run(
                task.id, owner_principal_id=run.requested_by_principal_id
            )
        )
        is task
    )


def test_inspection_not_found_and_unavailability_are_normalized() -> None:
    service = WorkflowRunService(FakeRepository())

    with pytest.raises(WorkflowRunNotFound):
        asyncio.run(service.get_run(uuid4(), owner_principal_id=uuid4()))
    with pytest.raises(WorkflowRunNotFound):
        asyncio.run(service.list_task_runs(uuid4(), owner_principal_id=uuid4()))
    with pytest.raises(TaskRunNotFound):
        asyncio.run(service.get_task_run(uuid4(), owner_principal_id=uuid4()))

    unavailable = WorkflowRunService(
        FakeRepository(failure=WorkflowRunPersistenceUnavailable())
    )
    with pytest.raises(WorkflowRunServiceUnavailable):
        asyncio.run(unavailable.get_run(uuid4(), owner_principal_id=uuid4()))


def test_service_delegates_runnable_transition_without_reinterpreting_result() -> None:
    run_id, task_id = uuid4(), uuid4()
    result = RunnableTransitionResult(run_id, (task_id,), ("leaf",))
    repository = FakeRepository(transition_result=result)

    actual = asyncio.run(
        WorkflowRunService(repository).transition_runnable_tasks(run_id)
    )

    assert actual is result
    assert repository.calls == [("transition_runnable_tasks", run_id)]


def test_empty_runnable_transition_is_successful_and_unavailability_is_normalized() -> (
    None
):
    run_id = uuid4()
    empty = asyncio.run(
        WorkflowRunService(FakeRepository()).transition_runnable_tasks(run_id)
    )
    assert empty == RunnableTransitionResult(run_id, (), ())

    with pytest.raises(WorkflowRunServiceUnavailable):
        asyncio.run(
            WorkflowRunService(
                FakeRepository(failure=WorkflowRunPersistenceUnavailable())
            ).transition_runnable_tasks(run_id)
        )


def test_service_delegates_dependency_failure_propagation_unchanged() -> None:
    run_id, task_id = uuid4(), uuid4()
    result = DependencyFailurePropagationResult(run_id, (task_id,), ("leaf",))
    repository = FakeRepository(propagation_result=result)

    actual = asyncio.run(
        WorkflowRunService(repository).propagate_dependency_failures(run_id)
    )

    assert actual is result
    assert repository.calls == [("propagate_dependency_failures", run_id)]


def test_empty_dependency_failure_propagation_is_successful_and_normalized() -> None:
    run_id = uuid4()
    empty = asyncio.run(
        WorkflowRunService(FakeRepository()).propagate_dependency_failures(run_id)
    )
    assert empty == DependencyFailurePropagationResult(run_id, (), ())

    with pytest.raises(WorkflowRunServiceUnavailable):
        asyncio.run(
            WorkflowRunService(
                FakeRepository(failure=WorkflowRunPersistenceUnavailable())
            ).propagate_dependency_failures(run_id)
        )


@pytest.mark.parametrize("failure", (RuntimeError("bug"), asyncio.CancelledError()))
def test_dependency_failure_propagation_preserves_unexpected_failures(
    failure: BaseException,
) -> None:
    with pytest.raises(type(failure)):
        asyncio.run(
            WorkflowRunService(
                FakeRepository(failure=failure)
            ).propagate_dependency_failures(uuid4())
        )


def test_service_delegates_workflow_run_evaluation_unchanged() -> None:
    run_id = uuid4()
    result = WorkflowRunEvaluationResult(
        run_id, True, WorkflowRunStatus.PENDING, WorkflowRunStatus.RUNNING
    )
    repository = FakeRepository(evaluation_result=result)

    actual = asyncio.run(
        WorkflowRunService(repository).evaluate_workflow_run_state(run_id)
    )

    assert actual is result
    assert repository.calls == [("evaluate_workflow_run_state", run_id)]


def test_missing_workflow_run_evaluation_is_successful_and_normalized() -> None:
    run_id = uuid4()
    missing = asyncio.run(
        WorkflowRunService(FakeRepository()).evaluate_workflow_run_state(run_id)
    )
    assert missing == WorkflowRunEvaluationResult(run_id, False, None, None)

    with pytest.raises(WorkflowRunServiceUnavailable):
        asyncio.run(
            WorkflowRunService(
                FakeRepository(failure=WorkflowRunPersistenceUnavailable())
            ).evaluate_workflow_run_state(run_id)
        )


@pytest.mark.parametrize("failure", (RuntimeError("bug"), asyncio.CancelledError()))
def test_unexpected_and_cancellation_failures_are_not_normalized(
    failure: BaseException,
) -> None:
    service = WorkflowRunService(FakeRepository(failure=failure))

    with pytest.raises(type(failure)):
        asyncio.run(
            service.resolve_version(
                uuid4(),
                owner_principal_id=uuid4(),
                selection=LatestWorkflowVersion(),
            )
        )


class FakeCreationTransaction:
    def __init__(
        self,
        prepared: (
            IdempotentCreationPreparation
            | PreparedFullWorkflowReplay
            | PreparedFailedSubgraphWorkflowReplay
            | None
        ),
    ) -> None:
        self.prepared = prepared
        self.calls: list[tuple[object, ...]] = []
        self.inserted: (
            tuple[
                PreparedWorkflowRunCreation,
                NewWorkflowRun,
                WorkflowRunInput,
                tuple[NewTaskRun, ...],
                WorkflowRunIdempotency | None,
            ]
            | None
        ) = None
        self.failure_for: str | None = None
        self.failure: BaseException | None = None
        self.existing_replay: ExistingIdempotentWorkflowReplay | None = None
        self.replay_inserted: (
            tuple[
                PreparedFullWorkflowReplay,
                NewWorkflowRun,
                WorkflowRunInput,
                tuple[NewTaskRun, ...],
                UUID,
                WorkflowReplayIdempotency | None,
            ]
            | None
        ) = None
        self.failed_replay_inserted: (
            tuple[
                PreparedFailedSubgraphWorkflowReplay,
                NewWorkflowRun,
                WorkflowRunInput,
                tuple[NewTaskRun, ...],
                dict[str, object],
                UUID,
                WorkflowReplayIdempotency | None,
            ]
            | None
        ) = None

    async def __aenter__(self) -> WorkflowRunCreationTransaction:
        self._record("enter")
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exception_type, exception, traceback
        self.calls.append(("exit",))

    async def prepare_creation_target(
        self,
        workflow_id: UUID,
        owner_principal_id: UUID,
        selection: WorkflowVersionSelection,
    ) -> PreparedWorkflowRunCreation | None:
        self._record("prepare", workflow_id, owner_principal_id, selection)
        return (
            self.prepared
            if isinstance(self.prepared, PreparedWorkflowRunCreation)
            else None
        )

    async def prepare_full_replay(
        self,
        source_workflow_run_id: UUID,
        owner_filter: OwnerFilter,
    ) -> PreparedFullWorkflowReplay | None:
        self._record("prepare_full_replay", source_workflow_run_id, owner_filter)
        return (
            self.prepared
            if isinstance(self.prepared, PreparedFullWorkflowReplay)
            else None
        )

    async def prepare_replay_source(
        self,
        source_workflow_run_id: UUID,
        owner_filter: OwnerFilter,
    ) -> PreparedFullWorkflowReplay | None:
        self._record("prepare_replay_source", source_workflow_run_id, owner_filter)
        if isinstance(self.prepared, PreparedFullWorkflowReplay):
            return self.prepared
        if isinstance(self.prepared, PreparedFailedSubgraphWorkflowReplay):
            return PreparedFullWorkflowReplay(
                self.prepared.source_workflow_run_id,
                self.prepared.source_status,
                self.prepared.creation,
                self.prepared.input_snapshot,
            )
        return None

    async def load_replay_source_tasks(
        self,
        source_workflow_run_id: UUID,
    ) -> tuple[SourceTaskRunState, ...]:
        self._record("load_replay_source_tasks", source_workflow_run_id)
        if isinstance(self.prepared, PreparedFailedSubgraphWorkflowReplay):
            return self.prepared.source_tasks
        raise AssertionError("failed replay source tasks were unavailable")

    async def find_idempotent_replay(
        self,
        principal_id: UUID,
        workflow_id: UUID,
        key_digest: str,
    ) -> ExistingIdempotentWorkflowReplay | None:
        self._record("find_idempotent_replay", principal_id, workflow_id, key_digest)
        return self.existing_replay

    async def prepare_failed_subgraph_replay(
        self,
        source_workflow_run_id: UUID,
        owner_filter: OwnerFilter,
    ) -> PreparedFailedSubgraphWorkflowReplay | None:
        self._record(
            "prepare_failed_subgraph_replay", source_workflow_run_id, owner_filter
        )
        return (
            self.prepared
            if isinstance(self.prepared, PreparedFailedSubgraphWorkflowReplay)
            else None
        )

    async def prepare_idempotent_creation(
        self,
        workflow_id: UUID,
        owner_principal_id: UUID,
        principal_id: UUID,
        selection: WorkflowVersionSelection,
        key_digest: str,
    ) -> IdempotentCreationPreparation | None:
        self._record(
            "prepare_idempotent",
            workflow_id,
            owner_principal_id,
            principal_id,
            selection,
            key_digest,
        )
        return (
            self.prepared
            if isinstance(
                self.prepared,
                (PreparedWorkflowRunCreation, ExistingIdempotentWorkflowRun),
            )
            else None
        )

    async def insert_complete_run(
        self,
        prepared: PreparedWorkflowRunCreation,
        run: NewWorkflowRun,
        input_snapshot: WorkflowRunInput,
        task_run_values: tuple[NewTaskRun, ...],
        idempotency: WorkflowRunIdempotency | None = None,
    ) -> WorkflowRunTimestamps:
        self._record("insert_complete_run")
        self.inserted = (
            prepared,
            run,
            input_snapshot,
            task_run_values,
            idempotency,
        )
        now = datetime.now(UTC)
        return WorkflowRunTimestamps(now, now)

    async def insert_full_replay(
        self,
        prepared: PreparedFullWorkflowReplay,
        run: NewWorkflowRun,
        input_snapshot: WorkflowRunInput,
        task_run_values: tuple[NewTaskRun, ...],
        correlation_id: UUID,
        idempotency: WorkflowReplayIdempotency | None = None,
    ) -> WorkflowRunTimestamps:
        self._record("insert_full_replay")
        self.replay_inserted = (
            prepared,
            run,
            input_snapshot,
            task_run_values,
            correlation_id,
            idempotency,
        )
        now = datetime.now(UTC)
        return WorkflowRunTimestamps(now, now)

    async def insert_failed_subgraph_replay(
        self,
        prepared: PreparedFailedSubgraphWorkflowReplay,
        run: NewWorkflowRun,
        input_snapshot: WorkflowRunInput,
        task_run_values: tuple[NewTaskRun, ...],
        requested_scope: dict[str, object],
        correlation_id: UUID,
        idempotency: WorkflowReplayIdempotency | None = None,
    ) -> WorkflowRunTimestamps:
        self._record("insert_failed_subgraph_replay")
        self.failed_replay_inserted = (
            prepared,
            run,
            input_snapshot,
            task_run_values,
            requested_scope,
            correlation_id,
            idempotency,
        )
        now = datetime.now(UTC)
        return WorkflowRunTimestamps(now, now)

    async def commit(self) -> None:
        self._record("commit")

    def _record(self, name: str, *values: object) -> None:
        self.calls.append((name, *values))
        if self.failure_for == name and self.failure is not None:
            raise self.failure


@dataclass
class CreationRepository:
    transaction: FakeCreationTransaction
    recovery: ExistingIdempotentWorkflowRun | None = None
    recovery_failure: BaseException | None = None
    replay_recovery: ExistingIdempotentWorkflowReplay | None = None

    def creation_transaction(self) -> WorkflowRunCreationTransactionContext:
        return self.transaction

    async def cancel_run(
        self,
        workflow_run_id: UUID,
        owner_filter: OwnerFilter,
        command: WorkflowRunCancellationCommand,
    ) -> PersistedWorkflowRunCancellation | None:
        del workflow_run_id, owner_filter, command
        raise AssertionError("cancellation was not expected")

    async def resolve_workflow_version(
        self,
        workflow_id: UUID,
        owner_principal_id: UUID,
        selection: WorkflowVersionSelection,
    ) -> WorkflowVersionResolutionRecord | None:
        del workflow_id, owner_principal_id, selection
        raise AssertionError("unlocked Task 2 resolution must not admit a run")

    async def find_idempotent_run(
        self, principal_id: UUID, workflow_id: UUID, key_digest: str
    ) -> ExistingIdempotentWorkflowRun | None:
        self.transaction.calls.append(
            ("recover", principal_id, workflow_id, key_digest)
        )
        if self.recovery_failure is not None:
            raise self.recovery_failure
        return self.recovery

    async def find_idempotent_replay(
        self,
        principal_id: UUID,
        workflow_id: UUID,
        key_digest: str,
    ) -> ExistingIdempotentWorkflowReplay | None:
        self.transaction.calls.append(
            ("recover_replay", principal_id, workflow_id, key_digest)
        )
        if self.recovery_failure is not None:
            raise self.recovery_failure
        return self.replay_recovery

    async def get_run(self, run_id: UUID, owner_principal_id: UUID) -> None:
        del run_id, owner_principal_id
        raise AssertionError("run inspection was not expected")

    async def list_task_runs(self, run_id: UUID, owner_principal_id: UUID) -> None:
        del run_id, owner_principal_id
        raise AssertionError("task inspection was not expected")

    async def get_task_run(self, task_run_id: UUID, owner_principal_id: UUID) -> None:
        del task_run_id, owner_principal_id
        raise AssertionError("task inspection was not expected")

    async def transition_runnable_tasks(
        self, workflow_run_id: UUID
    ) -> RunnableTransitionResult:
        raise AssertionError("runnable transition was not expected")

    async def suppress_unstarted_tasks(
        self, workflow_run_id: UUID
    ) -> CancellationPropagationResult:
        del workflow_run_id
        raise AssertionError("cancellation suppression was not expected")

    async def settle_dispatched_tasks(
        self, workflow_run_id: UUID
    ) -> CancellationSettlementResult:
        del workflow_run_id
        raise AssertionError("cancellation settlement was not expected")

    async def finalize_workflow_run_cancellation(
        self, workflow_run_id: UUID
    ) -> CancellationFinalizationResult:
        del workflow_run_id
        raise AssertionError("cancellation finalization was not expected")

    async def propagate_dependency_failures(
        self, workflow_run_id: UUID
    ) -> DependencyFailurePropagationResult:
        raise AssertionError("dependency failure propagation was not expected")

    async def evaluate_workflow_run_state(
        self, workflow_run_id: UUID
    ) -> WorkflowRunEvaluationResult:
        raise AssertionError("workflow run evaluation was not expected")


def prepared_creation(
    status: WorkflowDefinitionStatus = WorkflowDefinitionStatus.ENABLED,
    *,
    with_snapshot: bool = True,
) -> PreparedWorkflowRunCreation:
    workflow_id, version_id = uuid4(), uuid4()
    snapshot = (
        WorkflowRunVersionSnapshot(
            workflow_definition_id=workflow_id,
            workflow_version_id=version_id,
            version_number=4,
            steps=(WorkflowRunVersionStep("leaf"), WorkflowRunVersionStep("root")),
            dependencies=(WorkflowRunVersionDependency("root", "leaf"),),
        )
        if with_snapshot
        else None
    )
    return PreparedWorkflowRunCreation(workflow_id, status, snapshot)


def prepared_full_replay(
    status: WorkflowRunStatus,
) -> PreparedFullWorkflowReplay:
    return PreparedFullWorkflowReplay(
        source_workflow_run_id=uuid4(),
        source_status=status,
        creation=prepared_creation(WorkflowDefinitionStatus.ARCHIVED),
        input_snapshot=WorkflowRunInput(
            {"ordinary": {"value": 1}},
            {"database_password": {"secret_ref": "vault://taskforge/database"}},
        ),
    )


def prepared_failed_subgraph_replay(
    status: WorkflowRunStatus = WorkflowRunStatus.FAILED,
) -> PreparedFailedSubgraphWorkflowReplay:
    creation = prepared_creation(WorkflowDefinitionStatus.ARCHIVED)
    return PreparedFailedSubgraphWorkflowReplay(
        source_workflow_run_id=uuid4(),
        source_status=status,
        creation=creation,
        input_snapshot=WorkflowRunInput(
            {"ordinary": {"value": 1}},
            {"database_password": {"secret_ref": "vault://taskforge/database"}},
        ),
        source_tasks=(
            SourceTaskRunState("root", TaskRunStatus.SUCCEEDED),
            SourceTaskRunState("leaf", TaskRunStatus.FAILED),
        ),
    )


def test_failed_subgraph_replay_creates_carried_forward_complete_graph() -> None:
    prepared = prepared_failed_subgraph_replay()
    transaction = FakeCreationTransaction(prepared)
    requester_id = uuid4()

    replay = asyncio.run(
        WorkflowRunService(
            CreationRepository(transaction)
        ).create_failed_subgraph_replay(
            prepared.source_workflow_run_id,
            OwnerFilter.only(uuid4()),
            requested_by_principal_id=requester_id,
            failed_step_identifiers=("leaf",),
            correlation_id=uuid4(),
        )
    )

    assert replay.mode is WorkflowReplayMode.FAILED_SUBGRAPH
    assert replay.canonical_failed_step_identifiers == ("leaf",)
    assert replay.selected_step_identifiers == ("leaf",)
    assert transaction.failed_replay_inserted is not None
    _, run, copied_input, tasks, scope, _, idempotency = (
        transaction.failed_replay_inserted
    )
    assert run.id == replay.run.id != prepared.source_workflow_run_id
    assert copied_input == prepared.input_snapshot
    assert copied_input is not prepared.input_snapshot
    assert [(task.step_identifier, task.status) for task in tasks] == [
        ("root", TaskRunStatus.SUCCEEDED),
        ("leaf", TaskRunStatus.RUNNABLE),
    ]
    assert len({task.id for task in tasks}) == 2
    assert scope == {"failed_step_identifiers": ["leaf"]}
    assert idempotency is None
    assert [call[0] for call in transaction.calls] == [
        "enter",
        "prepare_failed_subgraph_replay",
        "insert_failed_subgraph_replay",
        "commit",
        "exit",
    ]
    rendered = repr(prepared) + repr(replay)
    assert "ordinary" not in rendered
    assert "vault://taskforge/database" not in rendered


@pytest.mark.parametrize(
    "source_status",
    (
        WorkflowRunStatus.SUCCEEDED,
        WorkflowRunStatus.FAILED,
        WorkflowRunStatus.CANCELLED,
    ),
)
def test_full_replay_creates_fresh_complete_exact_version_graph(
    source_status: WorkflowRunStatus,
) -> None:
    prepared = prepared_full_replay(source_status)
    transaction = FakeCreationTransaction(prepared)
    service = WorkflowRunService(CreationRepository(transaction))
    requester_id = uuid4()

    replay = asyncio.run(
        service.create_full_replay(
            prepared.source_workflow_run_id,
            OwnerFilter.only(uuid4()),
            requested_by_principal_id=requester_id,
            correlation_id=uuid4(),
        )
    )

    assert replay.source_workflow_run_id == prepared.source_workflow_run_id
    assert replay.mode is WorkflowReplayMode.FULL
    assert replay.run.id != prepared.source_workflow_run_id
    snapshot = prepared.creation.snapshot
    assert snapshot is not None
    assert replay.run.workflow_version_id == snapshot.workflow_version_id
    assert replay.run.requested_by_principal_id == requester_id
    assert transaction.replay_inserted is not None
    _, inserted_run, copied_input, tasks, _, idempotency = transaction.replay_inserted
    assert inserted_run.id == replay.run.id
    assert copied_input == prepared.input_snapshot
    assert copied_input is not prepared.input_snapshot
    assert [(task.step_identifier, task.status) for task in tasks] == [
        ("leaf", TaskRunStatus.BLOCKED),
        ("root", TaskRunStatus.RUNNABLE),
    ]
    assert len({task.id for task in tasks}) == 2
    assert idempotency is None
    assert [call[0] for call in transaction.calls] == [
        "enter",
        "prepare_full_replay",
        "insert_full_replay",
        "commit",
        "exit",
    ]
    rendered = repr(prepared) + repr(replay)
    assert "ordinary" not in rendered
    assert "vault://taskforge/database" not in rendered


@pytest.mark.parametrize(
    "source_status",
    (
        WorkflowRunStatus.PENDING,
        WorkflowRunStatus.RUNNING,
        WorkflowRunStatus.CANCELLING,
    ),
)
def test_full_replay_rejects_nonterminal_source_without_writes(
    source_status: WorkflowRunStatus,
) -> None:
    prepared = prepared_full_replay(source_status)
    transaction = FakeCreationTransaction(prepared)

    with pytest.raises(WorkflowRunReplayNotEligible):
        asyncio.run(
            WorkflowRunService(CreationRepository(transaction)).create_full_replay(
                prepared.source_workflow_run_id,
                OwnerFilter.all_owners(),
                requested_by_principal_id=uuid4(),
                correlation_id=uuid4(),
            )
        )

    assert [call[0] for call in transaction.calls] == [
        "enter",
        "prepare_full_replay",
        "exit",
    ]


def test_create_run_inserts_one_pending_complete_graph_then_commits() -> None:
    transaction = FakeCreationTransaction(prepared_creation())
    service = WorkflowRunService(CreationRepository(transaction))
    workflow_id, owner_id, requester_id = uuid4(), uuid4(), uuid4()
    accepted_input = WorkflowRunInput({"value": 1}, {"artifact": "ref"})

    created = asyncio.run(
        service.create_run(
            workflow_id,
            owner_principal_id=owner_id,
            requested_by_principal_id=requester_id,
            selection=LatestWorkflowVersion(),
            input_snapshot=accepted_input,
        )
    )

    assert [call[0] for call in transaction.calls] == [
        "enter",
        "prepare",
        "insert_complete_run",
        "commit",
        "exit",
    ]
    assert transaction.inserted is not None
    _, run, stored_input, tasks, idempotency = transaction.inserted
    assert run.status is WorkflowRunStatus.PENDING
    assert run.requested_by_principal_id == requester_id
    assert stored_input == accepted_input
    assert stored_input is not accepted_input
    assert [(task.step_identifier, task.status) for task in tasks] == [
        ("leaf", TaskRunStatus.BLOCKED),
        ("root", TaskRunStatus.RUNNABLE),
    ]
    assert len({task.id for task in tasks}) == 2
    assert idempotency is None
    assert created.id == run.id
    assert created.status is WorkflowRunStatus.PENDING
    assert created.task_count == 2
    assert created.runnable_task_count == 1
    assert created.blocked_task_count == 1


@pytest.mark.parametrize(
    "prepared",
    (
        None,
        prepared_creation(WorkflowDefinitionStatus.DRAFT, with_snapshot=False),
        prepared_creation(WorkflowDefinitionStatus.DISABLED, with_snapshot=False),
        prepared_creation(WorkflowDefinitionStatus.ARCHIVED, with_snapshot=False),
        prepared_creation(with_snapshot=False),
    ),
)
def test_rejected_creation_exits_without_inserting_or_committing(
    prepared: PreparedWorkflowRunCreation | None,
) -> None:
    transaction = FakeCreationTransaction(prepared)
    service = WorkflowRunService(CreationRepository(transaction))

    with pytest.raises(
        (
            WorkflowRunTargetNotFound,
            WorkflowRunTargetUnavailable,
            WorkflowVersionUnavailable,
        )
    ):
        asyncio.run(
            service.create_run(
                uuid4(),
                owner_principal_id=uuid4(),
                requested_by_principal_id=uuid4(),
                selection=ExplicitWorkflowVersion(1),
                input_snapshot=WorkflowRunInput({}, {}),
            )
        )

    assert [call[0] for call in transaction.calls] == ["enter", "prepare", "exit"]


def test_create_run_revalidates_input_before_opening_transaction() -> None:
    transaction = FakeCreationTransaction(prepared_creation())
    service = WorkflowRunService(CreationRepository(transaction))

    with pytest.raises(InvalidWorkflowRunInput):
        asyncio.run(
            service.create_run(
                uuid4(),
                owner_principal_id=uuid4(),
                requested_by_principal_id=uuid4(),
                selection=LatestWorkflowVersion(),
                input_snapshot=WorkflowRunInput({"bad": float("nan")}, {}),
            )
        )

    assert transaction.calls == []


def test_creation_transaction_entry_unavailability_is_service_normalized() -> None:
    transaction = FakeCreationTransaction(prepared_creation())
    transaction.failure_for = "enter"
    transaction.failure = WorkflowRunPersistenceUnavailable()
    service = WorkflowRunService(CreationRepository(transaction))

    with pytest.raises(WorkflowRunServiceUnavailable):
        asyncio.run(
            service.create_run(
                uuid4(),
                owner_principal_id=uuid4(),
                requested_by_principal_id=uuid4(),
                selection=LatestWorkflowVersion(),
                input_snapshot=WorkflowRunInput({}, {}),
            )
        )

    assert transaction.calls == [("enter",)]


def existing_idempotent(
    request_fingerprint: str,
) -> ExistingIdempotentWorkflowRun:
    return ExistingIdempotentWorkflowRun(
        request_fingerprint=request_fingerprint,
        run=CreatedWorkflowRun(
            id=uuid4(),
            workflow_definition_id=uuid4(),
            workflow_version_id=uuid4(),
            version_number=2,
            requested_by_principal_id=uuid4(),
            status=WorkflowRunStatus.PENDING,
            created_at=datetime.now(UTC),
            task_count=2,
            runnable_task_count=1,
            blocked_task_count=1,
        ),
    )


def test_identical_idempotent_replay_returns_existing_without_generating_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workflow_id, principal_id = uuid4(), uuid4()
    accepted = WorkflowRunInput({}, {})
    request = create_workflow_run_idempotency(
        "abcdefghijklmnop",
        workflow_definition_id=workflow_id,
        requested_by_principal_id=principal_id,
        selection=LatestWorkflowVersion(),
        input_snapshot=accepted,
    )
    existing = existing_idempotent(request.request_fingerprint)
    transaction = FakeCreationTransaction(existing)
    service = WorkflowRunService(CreationRepository(transaction))
    monkeypatch.setattr(
        "taskforge.runs.service.uuid4",
        lambda: pytest.fail("replay must not generate identifiers"),
    )

    result = asyncio.run(
        service.create_idempotent_run(
            workflow_id,
            owner_principal_id=principal_id,
            requested_by_principal_id=principal_id,
            selection=LatestWorkflowVersion(),
            input_snapshot=accepted,
            idempotency_key="abcdefghijklmnop",
        )
    )

    assert result is existing.run
    assert [call[0] for call in transaction.calls] == [
        "enter",
        "prepare_idempotent",
        "exit",
    ]


def test_conflicting_idempotent_reuse_writes_nothing_and_generates_no_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workflow_id, owner_id, principal_id = uuid4(), uuid4(), uuid4()
    transaction = FakeCreationTransaction(existing_idempotent("sha256:v1:other"))
    service = WorkflowRunService(CreationRepository(transaction))
    monkeypatch.setattr(
        "taskforge.runs.service.uuid4",
        lambda: pytest.fail("conflict must not generate identifiers"),
    )

    with pytest.raises(WorkflowRunIdempotencyConflict):
        asyncio.run(
            service.create_idempotent_run(
                workflow_id,
                owner_principal_id=owner_id,
                requested_by_principal_id=principal_id,
                selection=LatestWorkflowVersion(),
                input_snapshot=WorkflowRunInput({}, {}),
                idempotency_key="abcdefghijklmnop",
            )
        )

    assert [call[0] for call in transaction.calls] == [
        "enter",
        "prepare_idempotent",
        "exit",
    ]


def test_first_idempotent_use_inserts_fact_with_complete_graph() -> None:
    transaction = FakeCreationTransaction(prepared_creation())
    service = WorkflowRunService(CreationRepository(transaction))

    created = asyncio.run(
        service.create_idempotent_run(
            uuid4(),
            owner_principal_id=uuid4(),
            requested_by_principal_id=uuid4(),
            selection=ExplicitWorkflowVersion(4),
            input_snapshot=WorkflowRunInput({"value": 1}, {}),
            idempotency_key="abcdefghijklmnop",
        )
    )

    assert transaction.inserted is not None
    _, run, _, tasks, idempotency = transaction.inserted
    assert created.id == run.id
    assert len(tasks) == 2
    assert idempotency is not None
    assert idempotency.key_digest.startswith("sha256:v1:")
    assert [call[0] for call in transaction.calls] == [
        "enter",
        "prepare_idempotent",
        "insert_complete_run",
        "commit",
        "exit",
    ]


def test_uniqueness_conflict_recovers_identical_winner_once() -> None:
    transaction = FakeCreationTransaction(prepared_creation())
    transaction.failure_for = "insert_complete_run"
    transaction.failure = WorkflowRunIdempotencyRecordConflict()
    repository = CreationRepository(transaction)
    service = WorkflowRunService(repository)
    workflow_id, principal_id = uuid4(), uuid4()
    request = create_workflow_run_idempotency(
        "abcdefghijklmnop",
        workflow_definition_id=workflow_id,
        requested_by_principal_id=principal_id,
        selection=LatestWorkflowVersion(),
        input_snapshot=WorkflowRunInput({}, {}),
    )
    repository.recovery = existing_idempotent(request.request_fingerprint)

    result = asyncio.run(
        service.create_idempotent_run(
            workflow_id,
            owner_principal_id=principal_id,
            requested_by_principal_id=principal_id,
            selection=LatestWorkflowVersion(),
            input_snapshot=WorkflowRunInput({}, {}),
            idempotency_key="abcdefghijklmnop",
        )
    )

    assert result is repository.recovery.run
    assert sum(call[0] == "recover" for call in transaction.calls) == 1


@pytest.mark.parametrize(
    ("prepared", "expected_error"),
    (
        (None, WorkflowRunTargetNotFound),
        (
            prepared_creation(WorkflowDefinitionStatus.DISABLED, with_snapshot=False),
            WorkflowRunTargetUnavailable,
        ),
        (prepared_creation(with_snapshot=False), WorkflowVersionUnavailable),
    ),
)
def test_idempotent_first_use_rejects_unavailable_targets_before_writing(
    prepared: PreparedWorkflowRunCreation | None,
    expected_error: type[Exception],
) -> None:
    transaction = FakeCreationTransaction(prepared)
    service = WorkflowRunService(CreationRepository(transaction))

    with pytest.raises(expected_error):
        asyncio.run(
            service.create_idempotent_run(
                uuid4(),
                owner_principal_id=uuid4(),
                requested_by_principal_id=uuid4(),
                selection=LatestWorkflowVersion(),
                input_snapshot=WorkflowRunInput({}, {}),
                idempotency_key="abcdefghijklmnop",
            )
        )

    assert [call[0] for call in transaction.calls] == [
        "enter",
        "prepare_idempotent",
        "exit",
    ]


@pytest.mark.parametrize(
    ("failure", "expected_error"),
    (
        (WorkflowRunRecordConflict(), WorkflowRunPersistenceConflict),
        (WorkflowRunPersistenceUnavailable(), WorkflowRunServiceUnavailable),
    ),
)
def test_idempotent_creation_normalizes_persistence_failures(
    failure: BaseException,
    expected_error: type[Exception],
) -> None:
    transaction = FakeCreationTransaction(prepared_creation())
    transaction.failure_for = "insert_complete_run"
    transaction.failure = failure
    service = WorkflowRunService(CreationRepository(transaction))

    with pytest.raises(expected_error):
        asyncio.run(
            service.create_idempotent_run(
                uuid4(),
                owner_principal_id=uuid4(),
                requested_by_principal_id=uuid4(),
                selection=LatestWorkflowVersion(),
                input_snapshot=WorkflowRunInput({}, {}),
                idempotency_key="abcdefghijklmnop",
            )
        )


@pytest.mark.parametrize(
    ("recovery_failure", "expected_error"),
    (
        (None, WorkflowRunPersistenceConflict),
        (WorkflowRunPersistenceUnavailable(), WorkflowRunServiceUnavailable),
    ),
)
def test_idempotency_conflict_recovery_fails_closed(
    recovery_failure: BaseException | None,
    expected_error: type[Exception],
) -> None:
    transaction = FakeCreationTransaction(prepared_creation())
    transaction.failure_for = "insert_complete_run"
    transaction.failure = WorkflowRunIdempotencyRecordConflict()
    repository = CreationRepository(
        transaction,
        recovery_failure=recovery_failure,
    )
    service = WorkflowRunService(repository)

    with pytest.raises(expected_error):
        asyncio.run(
            service.create_idempotent_run(
                uuid4(),
                owner_principal_id=uuid4(),
                requested_by_principal_id=uuid4(),
                selection=LatestWorkflowVersion(),
                input_snapshot=WorkflowRunInput({}, {}),
                idempotency_key="abcdefghijklmnop",
            )
        )

    assert sum(call[0] == "recover" for call in transaction.calls) == 1


class ScriptedProgressionRepository:
    def __init__(
        self,
        run_id: UUID,
        runnable: list[RunnableTransitionResult],
        skipped: list[DependencyFailurePropagationResult],
        workflow: list[WorkflowRunEvaluationResult],
        *,
        cancellation: list[CancellationPropagationResult] | None = None,
        settlement: list[CancellationSettlementResult] | None = None,
        finalization: list[CancellationFinalizationResult] | None = None,
        failure_at: tuple[str, int] | None = None,
    ) -> None:
        self.run_id = run_id
        self.runnable = runnable
        self.skipped = skipped
        self.workflow = workflow
        self.cancellation = cancellation or [
            CancellationPropagationResult(
                run_id, True, WorkflowRunStatus.RUNNING, (), ()
            )
            for _ in range(max(len(runnable), 1))
        ]
        self.settlement = settlement or [
            CancellationSettlementResult(
                run_id, True, WorkflowRunStatus.RUNNING, (), ()
            )
            for _ in range(max(len(runnable), 1))
        ]
        self.finalization = finalization or []
        self.failure_at = failure_at
        self.calls: list[str] = []

    def _take(self, name: str, values: list[T]) -> T:
        self.calls.append(name)
        occurrence = self.calls.count(name)
        if self.failure_at == (name, occurrence):
            raise WorkflowRunPersistenceUnavailable
        return values.pop(0)

    async def transition_runnable_tasks(
        self, workflow_run_id: UUID
    ) -> RunnableTransitionResult:
        assert workflow_run_id == self.run_id
        return self._take("runnable", self.runnable)

    async def suppress_unstarted_tasks(
        self, workflow_run_id: UUID
    ) -> CancellationPropagationResult:
        assert workflow_run_id == self.run_id
        return self._take("cancellation", self.cancellation)

    async def settle_dispatched_tasks(
        self, workflow_run_id: UUID
    ) -> CancellationSettlementResult:
        assert workflow_run_id == self.run_id
        return self._take("settlement", self.settlement)

    async def finalize_workflow_run_cancellation(
        self, workflow_run_id: UUID
    ) -> CancellationFinalizationResult:
        assert workflow_run_id == self.run_id
        return self._take("finalization", self.finalization)

    async def propagate_dependency_failures(
        self, workflow_run_id: UUID
    ) -> DependencyFailurePropagationResult:
        assert workflow_run_id == self.run_id
        return self._take("skipped", self.skipped)

    async def evaluate_workflow_run_state(
        self, workflow_run_id: UUID
    ) -> WorkflowRunEvaluationResult:
        assert workflow_run_id == self.run_id
        return self._take("workflow", self.workflow)


def empty_runnable(run_id: UUID) -> RunnableTransitionResult:
    return RunnableTransitionResult(run_id, (), ())


def progressed_runnable(run_id: UUID, step: str) -> RunnableTransitionResult:
    return RunnableTransitionResult(run_id, (uuid4(),), (step,))


def empty_skipped(run_id: UUID) -> DependencyFailurePropagationResult:
    return DependencyFailurePropagationResult(run_id, (), ())


def workflow_result(
    run_id: UUID,
    previous: WorkflowRunStatus,
    resulting: WorkflowRunStatus | None = None,
) -> WorkflowRunEvaluationResult:
    return WorkflowRunEvaluationResult(
        run_id,
        True,
        previous,
        resulting or previous,
    )


def reconciliation_service(
    repository: ScriptedProgressionRepository,
) -> WorkflowRunService:
    return WorkflowRunService(repository)  # type: ignore[arg-type]


def test_reconciler_calls_existing_operations_in_order_and_stops_at_quiescence() -> (
    None
):
    run_id = uuid4()
    repository = ScriptedProgressionRepository(
        run_id,
        [empty_runnable(run_id)],
        [empty_skipped(run_id)],
        [workflow_result(run_id, WorkflowRunStatus.RUNNING)],
    )

    result = asyncio.run(
        reconciliation_service(repository).reconcile_workflow_run(run_id)
    )

    assert result == WorkflowRunReconciliationResult(
        run_id, True, 1, 0, 0, 0, 0, WorkflowRunStatus.RUNNING, True, False
    )
    assert repository.calls == [
        "cancellation",
        "settlement",
        "runnable",
        "skipped",
        "workflow",
    ]


def test_reconciler_completes_late_pending_success_without_post_terminal_cycle() -> (
    None
):
    run_id = uuid4()
    repository = ScriptedProgressionRepository(
        run_id,
        [empty_runnable(run_id), empty_runnable(run_id)],
        [empty_skipped(run_id), empty_skipped(run_id)],
        [
            workflow_result(
                run_id, WorkflowRunStatus.PENDING, WorkflowRunStatus.RUNNING
            ),
            workflow_result(
                run_id, WorkflowRunStatus.RUNNING, WorkflowRunStatus.SUCCEEDED
            ),
        ],
    )

    result = asyncio.run(
        reconciliation_service(repository).reconcile_workflow_run(run_id)
    )

    assert result.iterations == 2
    assert result.workflow_transition_count == 2
    assert result.final_status is WorkflowRunStatus.SUCCEEDED
    assert result.quiescent and not result.bound_reached
    assert (
        repository.calls
        == [
            "cancellation",
            "settlement",
            "runnable",
            "skipped",
            "workflow",
        ]
        * 2
    )


def test_reconciler_reports_missing_and_cancelling_without_another_cycle() -> None:
    run_id = uuid4()
    missing_repository = ScriptedProgressionRepository(
        run_id,
        [empty_runnable(run_id)],
        [empty_skipped(run_id)],
        [WorkflowRunEvaluationResult(run_id, False, None, None)],
        cancellation=[CancellationPropagationResult(run_id, False, None, (), ())],
    )
    missing = asyncio.run(
        reconciliation_service(missing_repository).reconcile_workflow_run(run_id)
    )
    assert not missing.found and not missing.quiescent

    cancelling_repository = ScriptedProgressionRepository(
        run_id,
        [empty_runnable(run_id)],
        [empty_skipped(run_id)],
        [workflow_result(run_id, WorkflowRunStatus.CANCELLING)],
        cancellation=[
            CancellationPropagationResult(
                run_id, True, WorkflowRunStatus.CANCELLING, (), ()
            )
        ],
        settlement=[
            CancellationSettlementResult(
                run_id, True, WorkflowRunStatus.CANCELLING, (), ()
            )
        ],
        finalization=[
            CancellationFinalizationResult(
                run_id,
                True,
                WorkflowRunStatus.CANCELLING,
                WorkflowRunStatus.CANCELLING,
                CancellationFinalizationOutcome.AWAITING_TASK_SETTLEMENT,
            )
        ],
    )
    cancelling = asyncio.run(
        reconciliation_service(cancelling_repository).reconcile_workflow_run(run_id)
    )
    assert cancelling.final_status is WorkflowRunStatus.CANCELLING
    assert cancelling.quiescent
    assert cancelling_repository.calls == [
        "cancellation",
        "settlement",
        "finalization",
    ]


def test_reconciler_enforces_behavioral_iteration_budget_without_ninth_cycle() -> None:
    run_id = uuid4()
    repository = ScriptedProgressionRepository(
        run_id,
        [
            progressed_runnable(run_id, f"step-{iteration}")
            for iteration in range(MAX_WORKFLOW_RECONCILIATION_ITERATIONS)
        ],
        [empty_skipped(run_id) for _ in range(MAX_WORKFLOW_RECONCILIATION_ITERATIONS)],
        [
            workflow_result(run_id, WorkflowRunStatus.RUNNING)
            for _ in range(MAX_WORKFLOW_RECONCILIATION_ITERATIONS)
        ],
    )

    result = asyncio.run(
        reconciliation_service(repository).reconcile_workflow_run(run_id)
    )

    assert result.iterations == MAX_WORKFLOW_RECONCILIATION_ITERATIONS
    assert result.runnable_transition_count == MAX_WORKFLOW_RECONCILIATION_ITERATIONS
    assert result.bound_reached and not result.quiescent
    assert len(repository.calls) == MAX_WORKFLOW_RECONCILIATION_ITERATIONS * 5


def test_reconciler_can_prove_quiescence_on_final_budgeted_iteration() -> None:
    run_id = uuid4()
    progress_iterations = MAX_WORKFLOW_RECONCILIATION_ITERATIONS - 1
    repository = ScriptedProgressionRepository(
        run_id,
        [
            *(
                progressed_runnable(run_id, f"step-{iteration}")
                for iteration in range(progress_iterations)
            ),
            empty_runnable(run_id),
        ],
        [empty_skipped(run_id) for _ in range(MAX_WORKFLOW_RECONCILIATION_ITERATIONS)],
        [
            workflow_result(run_id, WorkflowRunStatus.RUNNING)
            for _ in range(MAX_WORKFLOW_RECONCILIATION_ITERATIONS)
        ],
    )

    result = asyncio.run(
        reconciliation_service(repository).reconcile_workflow_run(run_id)
    )

    assert result.iterations == MAX_WORKFLOW_RECONCILIATION_ITERATIONS
    assert result.quiescent and not result.bound_reached


@pytest.mark.parametrize(
    "failure_operation",
    ("cancellation", "settlement", "runnable", "skipped", "workflow"),
)
def test_reconciler_propagates_operation_failure_and_stops_iteration(
    failure_operation: str,
) -> None:
    run_id = uuid4()
    repository = ScriptedProgressionRepository(
        run_id,
        [empty_runnable(run_id)],
        [empty_skipped(run_id)],
        [workflow_result(run_id, WorkflowRunStatus.RUNNING)],
        failure_at=(failure_operation, 1),
    )

    with pytest.raises(WorkflowRunServiceUnavailable):
        asyncio.run(reconciliation_service(repository).reconcile_workflow_run(run_id))

    expected = ["cancellation", "settlement", "runnable", "skipped", "workflow"]
    assert repository.calls == expected[: expected.index(failure_operation) + 1]


def existing_replay(
    prepared: PreparedFullWorkflowReplay,
    requester_id: UUID,
    idempotency: WorkflowReplayIdempotency,
    mode: WorkflowReplayMode,
    scope: dict[str, object],
) -> ExistingIdempotentWorkflowReplay:
    snapshot = prepared.creation.snapshot
    assert snapshot is not None
    return ExistingIdempotentWorkflowReplay(
        idempotency.request_fingerprint,
        prepared.source_workflow_run_id,
        mode,
        scope,
        CreatedWorkflowRun(
            uuid4(),
            prepared.creation.workflow_definition_id,
            snapshot.workflow_version_id,
            snapshot.version_number,
            requester_id,
            WorkflowRunStatus.PENDING,
            datetime.now(UTC),
            len(snapshot.steps),
            1,
            len(snapshot.steps) - 1,
        ),
    )


def test_first_idempotent_full_replay_inserts_atomic_idempotency_fact() -> None:
    prepared = prepared_full_replay(WorkflowRunStatus.FAILED)
    requester = uuid4()
    transaction = FakeCreationTransaction(prepared)

    result = asyncio.run(
        WorkflowRunService(
            CreationRepository(transaction)
        ).create_idempotent_full_replay(
            prepared.source_workflow_run_id,
            OwnerFilter.all_owners(),
            requested_by_principal_id=requester,
            idempotency_key="replay-key-00001",
            correlation_id=uuid4(),
        )
    )

    assert result.run.id != prepared.source_workflow_run_id
    assert transaction.replay_inserted is not None
    assert transaction.replay_inserted[-1] is not None
    assert [call[0] for call in transaction.calls] == [
        "enter",
        "prepare_replay_source",
        "find_idempotent_replay",
        "insert_full_replay",
        "commit",
        "exit",
    ]


def test_failed_replay_hit_uses_canonical_scope_without_new_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failed_prepared = prepared_failed_subgraph_replay()
    prepared = PreparedFullWorkflowReplay(
        failed_prepared.source_workflow_run_id,
        failed_prepared.source_status,
        failed_prepared.creation,
        failed_prepared.input_snapshot,
    )
    requester = uuid4()
    scope = {"failed_step_identifiers": ["leaf"]}
    idempotency = create_workflow_replay_idempotency(
        "replay-key-00001",
        source_workflow_run_id=prepared.source_workflow_run_id,
        requested_by_principal_id=requester,
        mode=WorkflowReplayMode.FAILED_SUBGRAPH,
        requested_scope=scope,
    )
    transaction = FakeCreationTransaction(failed_prepared)
    expected = existing_replay(
        prepared,
        requester,
        idempotency,
        WorkflowReplayMode.FAILED_SUBGRAPH,
        scope,
    )
    transaction.existing_replay = expected
    monkeypatch.setattr(
        "taskforge.runs.service.materialize_failed_subgraph_replay_tasks",
        lambda *args: pytest.fail("new replay admission must not run on a hit"),
    )

    result = asyncio.run(
        WorkflowRunService(
            CreationRepository(transaction)
        ).create_idempotent_failed_subgraph_replay(
            prepared.source_workflow_run_id,
            OwnerFilter.all_owners(),
            requested_by_principal_id=requester,
            failed_step_identifiers=["leaf"],
            idempotency_key="replay-key-00001",
            correlation_id=uuid4(),
        )
    )

    assert result.run.id == expected.run.id
    assert result.selected_step_identifiers == ("leaf",)
    assert [call[0] for call in transaction.calls] == [
        "enter",
        "prepare_replay_source",
        "find_idempotent_replay",
        "exit",
    ]


def test_failed_replay_invalid_key_precedes_invalid_root_request() -> None:
    prepared = prepared_failed_subgraph_replay()
    transaction = FakeCreationTransaction(prepared)

    with pytest.raises(InvalidWorkflowRunIdempotencyKey):
        asyncio.run(
            WorkflowRunService(
                CreationRepository(transaction)
            ).create_idempotent_failed_subgraph_replay(
                prepared.source_workflow_run_id,
                OwnerFilter.all_owners(),
                requested_by_principal_id=uuid4(),
                failed_step_identifiers=[],
                idempotency_key="short",
                correlation_id=uuid4(),
            )
        )

    assert [call[0] for call in transaction.calls] == [
        "enter",
        "prepare_replay_source",
        "exit",
    ]


def test_idempotent_replay_conflict_and_corrupt_lineage_are_distinct() -> None:
    prepared = prepared_full_replay(WorkflowRunStatus.FAILED)
    requester = uuid4()
    expected = create_workflow_replay_idempotency(
        "replay-key-00001",
        source_workflow_run_id=prepared.source_workflow_run_id,
        requested_by_principal_id=requester,
        mode=WorkflowReplayMode.FULL,
        requested_scope={},
    )
    transaction = FakeCreationTransaction(prepared)
    transaction.existing_replay = existing_replay(
        prepared,
        requester,
        WorkflowReplayIdempotency(expected.key_digest, "sha256:v1:different"),
        WorkflowReplayMode.FULL,
        {},
    )
    with pytest.raises(WorkflowReplayIdempotencyConflict):
        asyncio.run(
            WorkflowRunService(
                CreationRepository(transaction)
            ).create_idempotent_full_replay(
                prepared.source_workflow_run_id,
                OwnerFilter.all_owners(),
                requested_by_principal_id=requester,
                idempotency_key="replay-key-00001",
                correlation_id=uuid4(),
            )
        )


def test_replay_idempotency_uniqueness_loss_recovers_committed_winner() -> None:
    prepared = prepared_full_replay(WorkflowRunStatus.FAILED)
    requester = uuid4()
    idempotency = create_workflow_replay_idempotency(
        "replay-key-00001",
        source_workflow_run_id=prepared.source_workflow_run_id,
        requested_by_principal_id=requester,
        mode=WorkflowReplayMode.FULL,
        requested_scope={},
    )
    transaction = FakeCreationTransaction(prepared)
    transaction.failure_for = "insert_full_replay"
    transaction.failure = WorkflowRunIdempotencyRecordConflict()
    winner = existing_replay(
        prepared, requester, idempotency, WorkflowReplayMode.FULL, {}
    )
    repository = CreationRepository(transaction, replay_recovery=winner)

    result = asyncio.run(
        WorkflowRunService(repository).create_idempotent_full_replay(
            prepared.source_workflow_run_id,
            OwnerFilter.all_owners(),
            requested_by_principal_id=requester,
            idempotency_key="replay-key-00001",
            correlation_id=uuid4(),
        )
    )

    assert result.run.id == winner.run.id
    assert [call[0] for call in transaction.calls][-2:] == [
        "exit",
        "recover_replay",
    ]


def test_replay_idempotency_missing_uniqueness_winner_is_persistence_conflict() -> None:
    prepared = prepared_full_replay(WorkflowRunStatus.FAILED)
    transaction = FakeCreationTransaction(prepared)
    transaction.failure_for = "insert_full_replay"
    transaction.failure = WorkflowRunIdempotencyRecordConflict()

    with pytest.raises(WorkflowRunPersistenceConflict, match="winner was not found"):
        asyncio.run(
            WorkflowRunService(
                CreationRepository(transaction)
            ).create_idempotent_full_replay(
                prepared.source_workflow_run_id,
                OwnerFilter.all_owners(),
                requested_by_principal_id=uuid4(),
                idempotency_key="replay-key-00001",
                correlation_id=uuid4(),
            )
        )


def test_replay_idempotency_corrupt_persisted_lineage_is_invariant_error() -> None:
    prepared = prepared_full_replay(WorkflowRunStatus.FAILED)
    corrupt = FakeCreationTransaction(prepared)
    corrupt.failure_for = "find_idempotent_replay"
    corrupt.failure = WorkflowRunReplayPersistenceInvariantViolation()

    with pytest.raises(WorkflowRunReplayInvariantError):
        asyncio.run(
            WorkflowRunService(
                CreationRepository(corrupt)
            ).create_idempotent_full_replay(
                prepared.source_workflow_run_id,
                OwnerFilter.all_owners(),
                requested_by_principal_id=uuid4(),
                idempotency_key="replay-key-00001",
                correlation_id=uuid4(),
            )
        )


def test_keyless_replay_event_invariant_failure_is_normalized() -> None:
    prepared = prepared_full_replay(WorkflowRunStatus.FAILED)
    transaction = FakeCreationTransaction(prepared)
    transaction.failure_for = "insert_full_replay"
    transaction.failure = WorkflowRunReplayPersistenceInvariantViolation()

    with pytest.raises(WorkflowRunReplayInvariantError):
        asyncio.run(
            WorkflowRunService(CreationRepository(transaction)).create_full_replay(
                prepared.source_workflow_run_id,
                OwnerFilter.all_owners(),
                requested_by_principal_id=uuid4(),
                correlation_id=uuid4(),
            )
        )
