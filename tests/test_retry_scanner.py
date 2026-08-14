"""Focused tests for bounded due-retry dispatch scanning."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import TracebackType
from uuid import UUID, uuid4

import pytest
from sqlalchemy.dialects import postgresql

from taskforge.dispatch.envelope import (
    create_dispatch_envelope,
    dispatch_envelope_to_mapping,
)
from taskforge.persistence.retries import _next_due_retry_workflow_lock_statement
from taskforge.retries.persistence_ports import (
    DueRetryPersistenceInvariantViolation,
    DueRetryPersistenceUnavailable,
    DueRetryPreparation,
    PreparedDueRetryDispatch,
    SkippedDueRetryCandidate,
)
from taskforge.retries.scanner import (
    MAX_DUE_RETRY_BATCH_SIZE,
    DueRetryScanInvariantError,
    DueRetryScanner,
    DueRetryScanServiceUnavailable,
)
from taskforge.workflows.task_types import (
    JSONMapping,
    TaskTypeDefinition,
    TaskTypeRegistry,
    WorkflowValidationIssue,
)

NOW = datetime(2026, 8, 14, 12, 30, tzinfo=UTC)


@dataclass(frozen=True)
class AcceptParameters:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        del parameters
        return ()


def prepared_due(*, attempt_number: int = 2) -> PreparedDueRetryDispatch:
    workflow_run_id, task_run_id = uuid4(), uuid4()
    predecessor_attempt_id, predecessor_dispatch_id = uuid4(), uuid4()
    task_parameters: JSONMapping = {"source": "immutable"}
    predecessor = create_dispatch_envelope(
        dispatch_id=predecessor_dispatch_id,
        task_attempt_id=predecessor_attempt_id,
        task_run_id=task_run_id,
        workflow_run_id=workflow_run_id,
        attempt_number=attempt_number - 1,
        task_type="document.extract",
        required_capability="old-workers",
        task_payload=task_parameters,
        references={"object": "stable-reference"},
        correlation_id="correlation-1",
        trace_context={
            "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        },
    )
    return PreparedDueRetryDispatch(
        workflow_run_id,
        task_run_id,
        uuid4(),
        "extract",
        uuid4(),
        attempt_number,
        NOW,
        "document.extract",
        task_parameters,
        None,
        None,
        predecessor_attempt_id,
        attempt_number - 1,
        predecessor_dispatch_id,
        predecessor.route,
        dispatch_envelope_to_mapping(predecessor),
    )


@dataclass
class FakeTransaction:
    preparation: DueRetryPreparation | Exception
    persisted: list[tuple[PreparedDueRetryDispatch, UUID, str, dict[str, object]]] = (
        field(default_factory=list)
    )
    exited_with: type[BaseException] | str | None = "not-exited"

    async def __aenter__(self) -> FakeTransaction:
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exception, traceback
        self.exited_with = exception_type

    async def prepare_next_due(self) -> DueRetryPreparation:
        if isinstance(self.preparation, Exception):
            raise self.preparation
        return self.preparation

    async def persist_dispatch(
        self,
        prepared: PreparedDueRetryDispatch,
        outbox_id: UUID,
        route: str,
        payload: dict[str, object],
    ) -> None:
        self.persisted.append((prepared, outbox_id, route, payload))


@dataclass
class FakeRepository:
    preparations: list[DueRetryPreparation | Exception]
    transactions: list[FakeTransaction] = field(default_factory=list)

    def due_dispatch_transaction(self) -> FakeTransaction:
        transaction = FakeTransaction(self.preparations.pop(0))
        self.transactions.append(transaction)
        return transaction


def registry() -> TaskTypeRegistry:
    return TaskTypeRegistry(
        (TaskTypeDefinition("document.extract", "current-workers", AcceptParameters()),)
    )


@pytest.mark.parametrize("batch_size", (True, 0, -1, 1.5, MAX_DUE_RETRY_BATCH_SIZE + 1))
def test_batch_size_is_an_exact_bounded_integer(batch_size: object) -> None:
    scanner = DueRetryScanner(FakeRepository([]), registry())
    with pytest.raises(ValueError):
        asyncio.run(scanner.scan_due_retries(batch_size=batch_size))  # type: ignore[arg-type]


def test_candidate_query_is_due_ordered_latest_and_workflow_skip_locked() -> None:
    statement = str(
        _next_due_retry_workflow_lock_statement().compile(
            dialect=postgresql.dialect(),  # type: ignore[no-untyped-call]
            compile_kwargs={"literal_binds": True},
        )
    )
    assert "task_runs.status = 'retry_scheduled'" in statement
    assert "statement_timestamp()" in statement
    assert (
        "later_retry_attempt.attempt_number > task_attempts.attempt_number" in statement
    )
    assert "ORDER BY task_attempts.next_eligible_at, task_attempts.id" in statement
    assert "LIMIT 1" in statement
    assert "FOR UPDATE OF workflow_runs SKIP LOCKED" in statement


def test_scan_is_bounded_and_preserves_existing_attempt_identity() -> None:
    first, second, unexamined = prepared_due(), prepared_due(), prepared_due()
    repository = FakeRepository([first, second, unexamined])

    result = asyncio.run(
        DueRetryScanner(repository, registry()).scan_due_retries(batch_size=2)
    )

    assert result.examined == result.dispatched == 2
    assert result.skipped == 0
    assert result.dispatched_attempt_ids == (
        first.task_attempt_id,
        second.task_attempt_id,
    )
    assert len(repository.preparations) == 1
    for transaction, prepared in zip(
        repository.transactions, (first, second), strict=True
    ):
        assert transaction.exited_with is None
        stored, dispatch_id, route, payload = transaction.persisted[0]
        assert stored is prepared
        assert UUID(str(payload["dispatch_id"])) == dispatch_id
        assert UUID(str(payload["task_attempt_id"])) == prepared.task_attempt_id
        assert payload["attempt_number"] == prepared.attempt_number
        assert payload["task_payload"] == prepared.task_parameters
        assert payload["references"] == {"object": "stable-reference"}
        assert payload["correlation_id"] == "correlation-1"
        assert payload["trace_context"] == {
            "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        }
        assert payload["required_capability"] == "current-workers"
        assert route == "capability.current-workers"


def test_scan_counts_locked_revalidation_skip_and_stops_on_no_candidate() -> None:
    skipped_id = uuid4()
    repository = FakeRepository([SkippedDueRetryCandidate(skipped_id), None])

    result = asyncio.run(
        DueRetryScanner(repository, registry()).scan_due_retries(batch_size=3)
    )

    assert result.examined == result.skipped == 1
    assert result.dispatched == 0
    assert result.dispatched_attempt_ids == ()


def test_invalid_predecessor_envelope_fails_closed_and_rolls_back() -> None:
    prepared = prepared_due()
    prepared.predecessor_payload["task_attempt_id"] = str(uuid4())
    repository = FakeRepository([prepared])

    with pytest.raises(DueRetryScanInvariantError):
        asyncio.run(
            DueRetryScanner(repository, registry()).scan_due_retries(batch_size=1)
        )

    assert repository.transactions[0].exited_with is DueRetryScanInvariantError
    assert repository.transactions[0].persisted == []


def test_missing_current_task_registration_fails_closed() -> None:
    repository = FakeRepository([prepared_due()])
    with pytest.raises(DueRetryScanInvariantError):
        asyncio.run(
            DueRetryScanner(repository, TaskTypeRegistry(())).scan_due_retries(
                batch_size=1
            )
        )
    assert repository.transactions[0].persisted == []


@pytest.mark.parametrize(
    ("failure", "expected"),
    (
        (DueRetryPersistenceInvariantViolation(), DueRetryScanInvariantError),
        (DueRetryPersistenceUnavailable(), DueRetryScanServiceUnavailable),
    ),
)
def test_persistence_errors_are_stably_translated(
    failure: Exception, expected: type[Exception]
) -> None:
    repository = FakeRepository([failure])
    with pytest.raises(expected):
        asyncio.run(
            DueRetryScanner(repository, registry()).scan_due_retries(batch_size=1)
        )
