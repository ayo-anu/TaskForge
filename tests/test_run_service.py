"""Workflow run target resolution service tests."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from types import TracebackType
from uuid import UUID, uuid4

import pytest

from taskforge.runs.domain import (
    CreatedWorkflowRun,
    ExplicitWorkflowVersion,
    InspectedTaskRun,
    InspectedWorkflowRun,
    InvalidWorkflowRunInput,
    LatestWorkflowVersion,
    NewTaskRun,
    NewWorkflowRun,
    RunnableTransitionResult,
    TaskRunStatus,
    WorkflowRunIdempotency,
    WorkflowRunIdempotencyConflict,
    WorkflowRunInput,
    WorkflowRunStatus,
    WorkflowRunTargetUnavailable,
    WorkflowRunVersionDependency,
    WorkflowRunVersionSnapshot,
    WorkflowVersionSelection,
    create_workflow_run_idempotency,
)
from taskforge.runs.persistence_ports import (
    ExistingIdempotentWorkflowRun,
    IdempotentCreationPreparation,
    PreparedWorkflowRunCreation,
    WorkflowRunCreationTransaction,
    WorkflowRunCreationTransactionContext,
    WorkflowRunIdempotencyRecordConflict,
    WorkflowRunPersistenceUnavailable,
    WorkflowRunRecordConflict,
    WorkflowRunTimestamps,
    WorkflowVersionResolutionRecord,
)
from taskforge.runs.service import (
    TaskRunNotFound,
    WorkflowRunNotFound,
    WorkflowRunPersistenceConflict,
    WorkflowRunService,
    WorkflowRunServiceUnavailable,
    WorkflowRunTargetNotFound,
    WorkflowVersionUnavailable,
)
from taskforge.workflows.domain import WorkflowDefinitionStatus


@dataclass
class FakeRepository:
    result: WorkflowVersionResolutionRecord | None = None
    failure: BaseException | None = None
    run_result: InspectedWorkflowRun | None = None
    task_results: tuple[InspectedTaskRun, ...] | None = None
    task_result: InspectedTaskRun | None = None
    transition_result: RunnableTransitionResult | None = None

    def __post_init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    def creation_transaction(self) -> WorkflowRunCreationTransactionContext:
        raise AssertionError("creation transaction was not expected")

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


def test_empty_runnable_transition_is_successful_and_unavailability_is_normalized() -> None:
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
    def __init__(self, prepared: IdempotentCreationPreparation | None) -> None:
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
        return self.prepared

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

    def creation_transaction(self) -> WorkflowRunCreationTransactionContext:
        return self.transaction

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
            step_identifiers=("leaf", "root"),
            dependencies=(WorkflowRunVersionDependency("root", "leaf"),),
        )
        if with_snapshot
        else None
    )
    return PreparedWorkflowRunCreation(workflow_id, status, snapshot)


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
