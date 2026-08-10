"""Atomic task dispatch application-service tests."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from types import TracebackType
from uuid import UUID, uuid4

import pytest

from taskforge.dispatch.persistence_ports import (
    NewTaskAttempt,
    NewTaskDispatchOutbox,
    PreparedTaskDispatch,
    TaskDispatchPersistenceConflict,
    TaskDispatchPersistenceUnavailable,
    TaskDispatchStateConflict,
)
from taskforge.dispatch.service import (
    TaskDispatchConfigurationInvalid,
    TaskDispatchConflict,
    TaskDispatchNotEligible,
    TaskDispatchService,
    TaskDispatchServiceUnavailable,
)
from taskforge.workflows.task_types import (
    JSONMapping,
    TaskTypeDefinition,
    TaskTypeRegistry,
    WorkflowValidationIssue,
)


@dataclass(frozen=True)
class AcceptParameters:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        del parameters
        return ()


@dataclass
class FakeTransaction:
    prepared: PreparedTaskDispatch | None
    failure: Exception | None = None
    persisted: list[tuple[NewTaskAttempt, NewTaskDispatchOutbox]] = field(
        default_factory=list
    )
    committed: bool = False

    async def __aenter__(self) -> FakeTransaction:
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exception_type, exception, traceback

    async def prepare_dispatch(
        self, workflow_run_id: UUID, task_run_id: UUID
    ) -> PreparedTaskDispatch | None:
        del workflow_run_id, task_run_id
        return self.prepared

    async def persist_dispatch(
        self,
        prepared: PreparedTaskDispatch,
        attempt: NewTaskAttempt,
        outbox: NewTaskDispatchOutbox,
    ) -> None:
        del prepared
        if self.failure is not None:
            raise self.failure
        self.persisted.append((attempt, outbox))

    async def commit(self) -> None:
        self.committed = True


@dataclass(frozen=True)
class FakeRepository:
    transaction: FakeTransaction

    def dispatch_transaction(self) -> FakeTransaction:
        return self.transaction


def prepared_dispatch() -> PreparedTaskDispatch:
    return PreparedTaskDispatch(
        uuid4(), uuid4(), uuid4(), "extract", "document.extract", {"page": 2}, 1
    )


def service(transaction: FakeTransaction) -> TaskDispatchService:
    registry = TaskTypeRegistry(
        (
            TaskTypeDefinition(
                "document.extract", "document-workers", AcceptParameters()
            ),
        )
    )
    return TaskDispatchService(FakeRepository(transaction), registry)


def test_dispatch_persists_validated_snapshot_and_commits() -> None:
    prepared = prepared_dispatch()
    transaction = FakeTransaction(prepared)

    result = asyncio.run(
        service(transaction).dispatch_task(
            prepared.workflow_run_id, prepared.task_run_id
        )
    )

    attempt, outbox = transaction.persisted[0]
    assert transaction.committed
    assert result.task_attempt_id == attempt.id == outbox.task_attempt_id
    assert result.dispatch_id == outbox.id
    assert outbox.route == "capability.document-workers"
    assert outbox.payload["task_payload"] == {"page": 2}
    assert outbox.payload["references"] == {}
    assert outbox.payload["required_capability"] == "document-workers"


def test_missing_or_nonrunnable_task_is_safely_suppressed() -> None:
    transaction = FakeTransaction(None)

    with pytest.raises(TaskDispatchNotEligible):
        asyncio.run(service(transaction).dispatch_task(uuid4(), uuid4()))

    assert transaction.persisted == []
    assert not transaction.committed


def test_unknown_persisted_task_type_rolls_back_without_writes() -> None:
    prepared = prepared_dispatch()
    prepared = PreparedTaskDispatch(
        prepared.workflow_run_id,
        prepared.task_run_id,
        prepared.workflow_version_id,
        prepared.step_identifier,
        "unknown.task",
        prepared.task_parameters,
        prepared.attempt_number,
    )
    transaction = FakeTransaction(prepared)

    with pytest.raises(TaskDispatchConfigurationInvalid):
        asyncio.run(
            service(transaction).dispatch_task(
                prepared.workflow_run_id, prepared.task_run_id
            )
        )

    assert transaction.persisted == []
    assert not transaction.committed


def test_final_state_conflict_becomes_duplicate_suppression() -> None:
    prepared = prepared_dispatch()
    transaction = FakeTransaction(prepared, TaskDispatchStateConflict())

    with pytest.raises(TaskDispatchNotEligible):
        asyncio.run(
            service(transaction).dispatch_task(
                prepared.workflow_run_id, prepared.task_run_id
            )
        )

    assert not transaction.committed


def test_persistence_unavailability_is_translated() -> None:
    prepared = prepared_dispatch()
    transaction = FakeTransaction(prepared, TaskDispatchPersistenceUnavailable())

    with pytest.raises(TaskDispatchServiceUnavailable):
        asyncio.run(
            service(transaction).dispatch_task(
                prepared.workflow_run_id, prepared.task_run_id
            )
        )

    assert not transaction.committed


def test_database_invariant_conflict_is_translated() -> None:
    prepared = prepared_dispatch()
    transaction = FakeTransaction(prepared, TaskDispatchPersistenceConflict())

    with pytest.raises(TaskDispatchConflict):
        asyncio.run(
            service(transaction).dispatch_task(
                prepared.workflow_run_id, prepared.task_run_id
            )
        )

    assert not transaction.committed


def test_invalid_persisted_parameters_roll_back_without_writes() -> None:
    prepared = prepared_dispatch()
    invalid = PreparedTaskDispatch(
        prepared.workflow_run_id,
        prepared.task_run_id,
        prepared.workflow_version_id,
        prepared.step_identifier,
        prepared.task_type,
        {"oversized": "x" * (16 * 1024)},
        prepared.attempt_number,
    )
    transaction = FakeTransaction(invalid)

    with pytest.raises(TaskDispatchConfigurationInvalid):
        asyncio.run(
            service(transaction).dispatch_task(
                invalid.workflow_run_id, invalid.task_run_id
            )
        )

    assert transaction.persisted == []
    assert not transaction.committed
