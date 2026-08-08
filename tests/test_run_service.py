"""Workflow run target resolution service tests."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from types import TracebackType
from uuid import UUID, uuid4

import pytest

from taskforge.runs.domain import (
    ExplicitWorkflowVersion,
    InvalidWorkflowRunInput,
    LatestWorkflowVersion,
    NewTaskRun,
    NewWorkflowRun,
    TaskRunStatus,
    WorkflowRunInput,
    WorkflowRunStatus,
    WorkflowRunTargetUnavailable,
    WorkflowRunVersionDependency,
    WorkflowRunVersionSnapshot,
    WorkflowVersionSelection,
)
from taskforge.runs.persistence_ports import (
    PreparedWorkflowRunCreation,
    WorkflowRunCreationTransactionContext,
    WorkflowRunPersistenceUnavailable,
    WorkflowRunTimestamps,
    WorkflowVersionResolutionRecord,
)
from taskforge.runs.service import (
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

    def __post_init__(self) -> None:
        self.calls: list[tuple[UUID, UUID, WorkflowVersionSelection]] = []

    def creation_transaction(self) -> WorkflowRunCreationTransactionContext:
        raise AssertionError("creation transaction was not expected")

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
    def __init__(self, prepared: PreparedWorkflowRunCreation | None) -> None:
        self.prepared = prepared
        self.calls: list[tuple[object, ...]] = []
        self.inserted: (
            tuple[
                PreparedWorkflowRunCreation,
                NewWorkflowRun,
                WorkflowRunInput,
                tuple[NewTaskRun, ...],
            ]
            | None
        ) = None
        self.failure_for: str | None = None
        self.failure: BaseException | None = None

    async def __aenter__(self) -> FakeCreationTransaction:
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
        return self.prepared

    async def insert_complete_run(
        self,
        prepared: PreparedWorkflowRunCreation,
        run: NewWorkflowRun,
        input_snapshot: WorkflowRunInput,
        task_run_values: tuple[NewTaskRun, ...],
    ) -> WorkflowRunTimestamps:
        self._record("insert_complete_run")
        self.inserted = (prepared, run, input_snapshot, task_run_values)
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
    _, run, stored_input, tasks = transaction.inserted
    assert run.status is WorkflowRunStatus.PENDING
    assert run.requested_by_principal_id == requester_id
    assert stored_input == accepted_input
    assert stored_input is not accepted_input
    assert [(task.step_identifier, task.status) for task in tasks] == [
        ("leaf", TaskRunStatus.BLOCKED),
        ("root", TaskRunStatus.RUNNABLE),
    ]
    assert len({task.id for task in tasks}) == 2
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
