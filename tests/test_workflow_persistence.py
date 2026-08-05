"""Deterministic workflow SQLAlchemy adapter boundary tests."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import pytest

from taskforge.persistence.workflows import (
    SQLAlchemyWorkflowRepository,
    SQLAlchemyWorkflowUnitOfWork,
    _stored_draft,
    _workflow_list_statement,
    _workflow_page,
)
from taskforge.workflows.domain import (
    DraftWorkflowStep,
    WorkflowDefinitionStatus,
    WorkflowDraft,
)
from taskforge.workflows.persistence_ports import WorkflowPageCursor


class FakeSession:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def begin(self) -> None:
        self.calls.append("begin")

    async def rollback(self) -> None:
        self.calls.append("rollback")

    async def commit(self) -> None:
        self.calls.append("commit")

    async def close(self) -> None:
        self.calls.append("close")


def unit_of_work(session: FakeSession) -> SQLAlchemyWorkflowUnitOfWork:
    return SQLAlchemyWorkflowUnitOfWork(lambda: session)  # type: ignore[arg-type]


def test_repository_creates_a_fresh_explicit_unit_of_work() -> None:
    session = FakeSession()
    repository = SQLAlchemyWorkflowRepository(
        lambda: session  # type: ignore[arg-type]
    )

    first = repository.transaction()
    second = repository.transaction()

    assert isinstance(first, SQLAlchemyWorkflowUnitOfWork)
    assert isinstance(second, SQLAlchemyWorkflowUnitOfWork)
    assert first is not second


def test_uncommitted_unit_of_work_rolls_back_and_closes() -> None:
    session = FakeSession()

    async def exercise() -> None:
        async with unit_of_work(session):
            pass

    asyncio.run(exercise())

    assert session.calls == ["begin", "rollback", "close"]


def test_committed_unit_of_work_does_not_roll_back() -> None:
    session = FakeSession()

    async def exercise() -> None:
        async with unit_of_work(session) as transaction:
            await transaction.commit()

    asyncio.run(exercise())

    assert session.calls == ["begin", "commit", "close"]


def test_unit_of_work_rejects_operations_outside_its_context() -> None:
    with pytest.raises(RuntimeError, match="transaction is not active"):
        asyncio.run(unit_of_work(FakeSession()).commit())


def test_version_allocation_cannot_bypass_definition_lock() -> None:
    session = FakeSession()
    workflow_id = uuid4()

    async def exercise() -> None:
        async with unit_of_work(session) as transaction:
            with pytest.raises(RuntimeError, match="definition lock"):
                await transaction.next_version_number(workflow_id)

    asyncio.run(exercise())

    assert session.calls == ["begin", "rollback", "close"]


def test_version_insert_cannot_bypass_definition_lock() -> None:
    session = FakeSession()
    workflow = WorkflowDraft(
        id=uuid4(),
        owner_principal_id=uuid4(),
        name="Locked publication",
        description=None,
        status=WorkflowDefinitionStatus.DRAFT,
        steps=(DraftWorkflowStep(uuid4(), "first", "test.task", {}),),
        dependencies=(),
    )

    async def exercise() -> None:
        async with unit_of_work(session) as transaction:
            with pytest.raises(RuntimeError, match="definition lock"):
                await transaction.insert_version(uuid4(), 1, workflow)

    asyncio.run(exercise())

    assert session.calls == ["begin", "rollback", "close"]


def test_stored_draft_reconstructs_identifiers_and_ordinary_json_parameters() -> None:
    workflow_id, owner_id = uuid4(), uuid4()
    first_id, second_id = uuid4(), uuid4()
    now = datetime.now(UTC)
    definition = SimpleNamespace(
        id=workflow_id,
        owner_principal_id=owner_id,
        name="Stored workflow",
        description=None,
        status="draft",
        created_at=now,
        updated_at=now,
    )
    steps = [
        SimpleNamespace(
            id=first_id,
            step_identifier="first",
            task_type="test.task",
            parameters={"value": [1, 2]},
        ),
        SimpleNamespace(
            id=second_id,
            step_identifier="second",
            task_type="test.task",
            parameters={},
        ),
    ]
    dependency_id = uuid4()
    dependencies = [
        SimpleNamespace(
            id=dependency_id,
            predecessor_step_id=first_id,
            successor_step_id=second_id,
        )
    ]

    stored = _stored_draft(
        cast(Any, definition),
        cast(Any, steps),
        cast(Any, dependencies),
    )

    assert stored.draft.id == workflow_id
    assert stored.draft.owner_principal_id == owner_id
    assert stored.draft.status is WorkflowDefinitionStatus.DRAFT
    assert stored.draft.steps[0].parameters == {"value": [1, 2]}
    assert stored.draft.dependencies[0].id == dependency_id
    assert stored.draft.dependencies[0].predecessor_identifier == "first"
    assert stored.draft.dependencies[0].successor_identifier == "second"
    assert stored.created_at == stored.updated_at == now
    assert "value" not in repr(stored)


def test_list_statement_is_owner_scoped_stably_ordered_and_keyset_bounded() -> None:
    owner_id = uuid4()
    cursor = WorkflowPageCursor(datetime.now(UTC), uuid4())

    statement = _workflow_list_statement(owner_id, 2, cursor)
    sql = " ".join(str(statement).split())

    assert "workflow_definitions.owner_principal_id =" in sql
    assert "workflow_definitions.created_at <" in sql
    assert "workflow_definitions.created_at =" in sql
    assert "workflow_definitions.id <" in sql
    assert (
        "ORDER BY workflow_definitions.created_at DESC, workflow_definitions.id DESC"
        in sql
    )
    assert statement._limit_clause is not None
    assert statement._limit_clause.value == 3


def test_page_cursor_uses_last_returned_item_not_extra_fetched_row() -> None:
    owner_id = uuid4()
    timestamp = datetime.now(UTC)
    identifiers = (uuid4(), uuid4(), uuid4())
    rows = [
        SimpleNamespace(
            id=identifier,
            owner_principal_id=owner_id,
            name=f"workflow-{index}",
            description=None,
            status="draft",
            created_at=timestamp,
            updated_at=timestamp,
        )
        for index, identifier in enumerate(identifiers)
    ]

    page = _workflow_page(cast(Any, rows), 2)

    assert tuple(item.id for item in page.items) == identifiers[:2]
    assert page.next_cursor == WorkflowPageCursor(timestamp, identifiers[1])
    assert page.next_cursor.workflow_id != identifiers[2]


def test_page_has_no_cursor_without_an_extra_row() -> None:
    assert _workflow_page([], 10).next_cursor is None


def test_page_cursor_requires_timezone_and_normalizes_to_utc() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        WorkflowPageCursor(datetime(2026, 8, 4), uuid4())

    cursor = WorkflowPageCursor(
        datetime.fromisoformat("2026-08-04T12:00:00.123456-07:00"),
        uuid4(),
    )
    assert cursor.created_at.isoformat(timespec="microseconds") == (
        "2026-08-04T19:00:00.123456+00:00"
    )
