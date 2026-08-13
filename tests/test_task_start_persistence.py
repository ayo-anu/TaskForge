"""Focused orchestration tests for PostgreSQL task start acknowledgement."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.sql.dml import Update

from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.persistence.task_start import SQLAlchemyTaskStartRepository
from taskforge.worker.start_persistence_ports import (
    TaskStartAuthorityRejected,
    TaskStartClaimStale,
    TaskStartInvariantViolation,
    TaskStartSessionRejected,
)


class FakeResult:
    def __init__(self, row: object) -> None:
        self.row = row

    def one_or_none(self) -> Any:
        return self.row


class FakeSession:
    def __init__(self, rows: list[object], scalars: list[object]) -> None:
        self.rows = rows
        self.scalars = scalars
        self.statements: list[object] = []

    async def execute(self, statement: object) -> FakeResult:
        self.statements.append(statement)
        return FakeResult(self.rows.pop(0))

    async def scalar(self, statement: object) -> Any:
        self.statements.append(statement)
        return self.scalars.pop(0)


class FakeBegin:
    def __init__(self, session: FakeSession) -> None:
        self.session = session

    async def __aenter__(self) -> FakeSession:
        return self.session

    async def __aexit__(self, *args: object) -> None:
        return None


class FakeSessions:
    def __init__(self, session: FakeSession) -> None:
        self.session = session

    def begin(self) -> FakeBegin:
        return FakeBegin(self.session)


def repository(session: FakeSession) -> SQLAlchemyTaskStartRepository:
    return SQLAlchemyTaskStartRepository(
        cast(async_sessionmaker[AsyncSession], FakeSessions(session))
    )


def facts(
    status: str = "claimed",
) -> tuple[AuthenticatedWorker, UUID, UUID, UUID, object, datetime]:
    worker = AuthenticatedWorker(uuid4(), uuid4())
    session_id, task_run_id, attempt_id = uuid4(), uuid4(), uuid4()
    expiry = datetime.now(UTC) + timedelta(seconds=60)
    task = SimpleNamespace(id=task_run_id, status=status, attempt_number=1)
    return worker, session_id, task_run_id, attempt_id, task, expiry


def test_repository_commits_guarded_claimed_to_running_transition() -> None:
    worker, session_id, task_run_id, attempt_id, task, expiry = facts()
    claim = SimpleNamespace(
        task_attempt_id=attempt_id,
        generation=2,
        worker_session_id=session_id,
        lease_expires_at=expiry,
    )
    session = FakeSession(
        [
            SimpleNamespace(id=worker.worker_identity_id),
            SimpleNamespace(id=worker.credential_id),
            SimpleNamespace(ended_at=None),
            task,
            claim,
            SimpleNamespace(id=task_run_id),
        ],
        [1, False],
    )

    started = asyncio.run(
        repository(session).start_task(worker, session_id, task_run_id, attempt_id, 2)
    )

    assert started
    updates = [value for value in session.statements if isinstance(value, Update)]
    assert len(updates) == 1
    sql = str(updates[0].compile(compile_kwargs={"literal_binds": True}))
    assert "task_runs.status = 'claimed'" in sql
    assert "status='running'" in sql


def test_repository_replays_running_without_mutation() -> None:
    worker, session_id, task_run_id, attempt_id, task, expiry = facts("running")
    claim = SimpleNamespace(
        generation=2, worker_session_id=session_id, lease_expires_at=expiry
    )
    session = FakeSession(
        [
            SimpleNamespace(id=worker.worker_identity_id),
            SimpleNamespace(id=worker.credential_id),
            SimpleNamespace(ended_at=None),
            task,
            claim,
        ],
        [1, False],
    )

    started = asyncio.run(
        repository(session).start_task(worker, session_id, task_run_id, attempt_id, 2)
    )

    assert not started
    assert not any(isinstance(value, Update) for value in session.statements)


@pytest.mark.parametrize(
    ("rows", "expected"),
    (
        ([None], TaskStartAuthorityRejected),
        (
            [SimpleNamespace(id=uuid4()), SimpleNamespace(id=uuid4()), None],
            TaskStartSessionRejected,
        ),
    ),
)
def test_repository_rejects_invalid_authority_or_session(
    rows: list[object], expected: type[Exception]
) -> None:
    worker, session_id, task_run_id, attempt_id, _, _ = facts()
    with pytest.raises(expected):
        asyncio.run(
            repository(FakeSession(rows, [])).start_task(
                worker, session_id, task_run_id, attempt_id, 1
            )
        )


@pytest.mark.parametrize(
    ("status", "generation", "owner_matches", "expired", "expected"),
    (
        ("claimed", 1, True, False, TaskStartClaimStale),
        ("claimed", 2, False, False, TaskStartClaimStale),
        ("claimed", 2, True, True, TaskStartClaimStale),
        ("dispatched", 2, True, False, TaskStartInvariantViolation),
    ),
)
def test_repository_fails_closed_for_stale_claim_or_invalid_state(
    status: str,
    generation: int,
    owner_matches: bool,
    expired: bool,
    expected: type[Exception],
) -> None:
    worker, session_id, task_run_id, attempt_id, task, expiry = facts(status)
    claim = SimpleNamespace(
        generation=generation,
        worker_session_id=session_id if owner_matches else uuid4(),
        lease_expires_at=expiry,
    )
    session = FakeSession(
        [
            SimpleNamespace(id=worker.worker_identity_id),
            SimpleNamespace(id=worker.credential_id),
            SimpleNamespace(ended_at=None),
            task,
            claim,
        ],
        [1, expired],
    )
    with pytest.raises(expected):
        asyncio.run(
            repository(session).start_task(
                worker, session_id, task_run_id, attempt_id, 2
            )
        )
