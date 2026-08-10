"""SQL statement invariants for task dispatch and outbox publication."""

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace, TracebackType
from typing import cast
from uuid import uuid4

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.sql import ClauseElement

from taskforge.dispatch.publisher_ports import (
    DispatchAcknowledgementPersistenceFailure,
    DispatchPublicationInvariantConflict,
    PublicationAcknowledgement,
    StoredDispatch,
    UnpublishedDispatchCursor,
)
from taskforge.persistence.dispatch import (
    SQLAlchemyDispatchOutboxRepository,
    _dispatch_acknowledgement_snapshot_statement,
    _next_attempt_number_statement,
    _record_publication_acknowledgement_statement,
    _runnable_task_dispatch_snapshot_statement,
    _runnable_to_dispatched_statement,
    _unpublished_dispatch_page_statement,
    _workflow_run_dispatch_lock_statement,
)


def sql(statement: ClauseElement) -> str:
    return str(
        statement.compile(
            dialect=postgresql.dialect(),  # type: ignore[no-untyped-call]
            compile_kwargs={"literal_binds": True},
        )
    )


def test_outer_lock_targets_only_owning_workflow_run() -> None:
    statement = sql(_workflow_run_dispatch_lock_statement(uuid4(), uuid4()))

    assert "JOIN task_runs" in statement
    assert "task_runs.workflow_run_id = workflow_runs.id" in statement
    assert (
        "task_runs.workflow_version_id = workflow_runs.workflow_version_id" in statement
    )
    assert "FOR UPDATE OF workflow_runs" in statement


def test_snapshot_requires_owned_runnable_version_step() -> None:
    statement = sql(
        _runnable_task_dispatch_snapshot_statement(uuid4(), uuid4(), uuid4())
    )

    assert "workflow_version_steps" in statement
    assert "task_runs.workflow_run_id" in statement
    assert "task_runs.workflow_version_id" in statement
    assert "task_runs.status = 'runnable'" in statement


def test_attempt_allocation_is_scoped_to_task_run() -> None:
    statement = sql(_next_attempt_number_statement(uuid4()))

    assert "max(task_attempts.attempt_number)" in statement
    assert "task_attempts.task_run_id" in statement
    assert "+ 1" in statement


def test_final_transition_is_guarded_by_identity_ownership_and_state() -> None:
    statement = sql(_runnable_to_dispatched_statement(uuid4(), uuid4()))

    assert "task_runs.id" in statement
    assert "task_runs.workflow_run_id" in statement
    assert "task_runs.status = 'runnable'" in statement
    assert "status='dispatched'" in statement
    assert "RETURNING task_runs.id" in statement


def test_unpublished_scan_is_ordered_bounded_and_unlocked() -> None:
    initial = sql(_unpublished_dispatch_page_statement(None, 25))
    cursor = UnpublishedDispatchCursor(datetime.now(UTC), uuid4())
    subsequent = sql(_unpublished_dispatch_page_statement(cursor, 10))

    assert "published_at IS NULL" in initial
    assert (
        "ORDER BY task_dispatch_outbox.created_at, task_dispatch_outbox.id" in initial
    )
    assert "LIMIT 25" in initial
    assert "FOR UPDATE" not in initial
    assert "OFFSET" not in initial
    assert "(task_dispatch_outbox.created_at, task_dispatch_outbox.id) >" in subsequent
    assert "LIMIT 10" in subsequent


def test_acknowledgement_update_guards_complete_snapshot() -> None:
    stored = StoredDispatch(
        uuid4(),
        uuid4(),
        "capability.test",
        {"schema_version": 1},
        datetime.now(UTC),
    )
    statement = str(
        _record_publication_acknowledgement_statement(stored).compile(
            dialect=postgresql.dialect()  # type: ignore[no-untyped-call]
        )
    )

    assert "task_dispatch_outbox.id" in statement
    assert "task_dispatch_outbox.task_attempt_id" in statement
    assert "task_dispatch_outbox.route" in statement
    assert "task_dispatch_outbox.payload" in statement
    assert "task_dispatch_outbox.published_at IS NULL" in statement
    assert "CURRENT_TIMESTAMP" in statement
    assert "RETURNING task_dispatch_outbox.published_at" in statement


def test_acknowledgement_followup_reads_every_classification_field() -> None:
    statement = sql(_dispatch_acknowledgement_snapshot_statement(uuid4()))

    assert "task_attempt_id" in statement
    assert "route" in statement
    assert "payload" in statement
    assert "published_at" in statement


class FakeResult:
    def __init__(self, row: object) -> None:
        self._row = row

    def one_or_none(self) -> object:
        return self._row


class FakeAcknowledgementSession:
    def __init__(self, row: object) -> None:
        self.row = row

    async def scalar(self, statement: object) -> None:
        del statement
        return None

    async def execute(self, statement: object) -> FakeResult:
        del statement
        return FakeResult(self.row)


class FakeBeginContext:
    def __init__(self, session: FakeAcknowledgementSession) -> None:
        self.session = session

    async def __aenter__(self) -> FakeAcknowledgementSession:
        return self.session

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exception_type, exception, traceback


class FakeSessions:
    def __init__(self, row: object) -> None:
        self.session = FakeAcknowledgementSession(row)

    def begin(self) -> FakeBeginContext:
        return FakeBeginContext(self.session)


def acknowledgement_record() -> StoredDispatch:
    return StoredDispatch(
        uuid4(),
        uuid4(),
        "capability.test",
        {"schema_version": 1},
        datetime.now(UTC),
    )


def acknowledgement_repository(row: object) -> SQLAlchemyDispatchOutboxRepository:
    sessions = cast(async_sessionmaker[AsyncSession], FakeSessions(row))
    return SQLAlchemyDispatchOutboxRepository(sessions)


def test_zero_update_accepts_only_exact_already_published_snapshot() -> None:
    expected = acknowledgement_record()
    row = SimpleNamespace(
        task_attempt_id=expected.task_attempt_id,
        route=expected.route,
        payload=expected.payload,
        published_at=datetime.now(UTC),
    )

    outcome = asyncio.run(
        acknowledgement_repository(row).record_accepted_publication(expected)
    )

    assert outcome is PublicationAcknowledgement.ALREADY_RECORDED


@pytest.mark.parametrize("field", ("task_attempt_id", "route", "payload"))
@pytest.mark.parametrize("published", (False, True))
def test_zero_update_rejects_every_snapshot_mismatch(
    field: str, published: bool
) -> None:
    expected = acknowledgement_record()
    values = {
        "task_attempt_id": expected.task_attempt_id,
        "route": expected.route,
        "payload": expected.payload,
        "published_at": datetime.now(UTC) if published else None,
    }
    values[field] = {
        "task_attempt_id": uuid4(),
        "route": "capability.changed",
        "payload": {"schema_version": 2},
    }[field]

    with pytest.raises(DispatchPublicationInvariantConflict):
        asyncio.run(
            acknowledgement_repository(
                SimpleNamespace(**values)
            ).record_accepted_publication(expected)
        )


def test_zero_update_rejects_missing_row() -> None:
    expected = acknowledgement_record()

    with pytest.raises(DispatchPublicationInvariantConflict):
        asyncio.run(
            acknowledgement_repository(None).record_accepted_publication(expected)
        )


def test_zero_update_rejects_exact_still_unpublished_snapshot() -> None:
    expected = acknowledgement_record()
    row = SimpleNamespace(
        task_attempt_id=expected.task_attempt_id,
        route=expected.route,
        payload=expected.payload,
        published_at=None,
    )

    with pytest.raises(DispatchAcknowledgementPersistenceFailure):
        asyncio.run(
            acknowledgement_repository(row).record_accepted_publication(expected)
        )
