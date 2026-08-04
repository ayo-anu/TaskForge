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
)
from taskforge.workflows.domain import WorkflowDefinitionStatus


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
