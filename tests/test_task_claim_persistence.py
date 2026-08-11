"""Repository orchestration tests complementing real PostgreSQL claim tests."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.claims.domain import TaskClaimOutcome
from taskforge.claims.persistence_ports import (
    TaskClaimAuthorityRejected,
    TaskClaimSessionInactive,
)
from taskforge.dispatch.envelope import (
    DispatchEnvelope,
    create_dispatch_envelope,
    dispatch_envelope_to_mapping,
)
from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.persistence.claims import SQLAlchemyTaskClaimRepository


class FakeResult:
    def __init__(self, row: object) -> None:
        self.row = row

    def one_or_none(self) -> Any:
        return self.row

    def one(self) -> Any:
        assert self.row is not None
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


def claim_fixture() -> tuple[AuthenticatedWorker, UUID, DispatchEnvelope]:
    worker = AuthenticatedWorker(uuid4(), uuid4())
    session_id = uuid4()
    envelope = create_dispatch_envelope(
        dispatch_id=uuid4(),
        task_attempt_id=uuid4(),
        task_run_id=uuid4(),
        workflow_run_id=uuid4(),
        attempt_number=1,
        task_type="test.task",
        required_capability="test-capability",
        task_payload={},
        references={},
    )
    return worker, session_id, envelope


def test_repository_orchestrates_new_acquisition_in_one_context() -> None:
    worker, session_id, envelope = claim_fixture()
    acquired_at = datetime.now(UTC)
    inserted = SimpleNamespace(
        task_attempt_id=envelope.task_attempt_id,
        generation=1,
        worker_session_id=session_id,
        acquired_at=acquired_at,
        lease_expires_at=acquired_at + timedelta(seconds=60),
    )
    durable = SimpleNamespace(
        route=envelope.route,
        payload=dispatch_envelope_to_mapping(envelope),
        attempt_number=1,
        task_type="test.task",
    )
    session = FakeSession(
        [
            SimpleNamespace(id=worker.worker_identity_id),
            SimpleNamespace(id=worker.credential_id),
            SimpleNamespace(ended_at=None),
            SimpleNamespace(
                id=envelope.task_run_id,
                workflow_run_id=envelope.workflow_run_id,
                workflow_version_id=uuid4(),
                step_identifier="step",
                status="dispatched",
            ),
            durable,
            None,
            SimpleNamespace(accepting_work=True, healthy=True),
            inserted,
            SimpleNamespace(id=envelope.task_run_id),
        ],
        [1, "test-capability", 1],
    )
    repository = SQLAlchemyTaskClaimRepository(
        cast(async_sessionmaker[AsyncSession], FakeSessions(session)),
        worker_stale_after_seconds=30,
    )

    result = asyncio.run(
        repository.acquire_claim(worker, session_id, envelope, lease_seconds=60)
    )

    assert result.outcome is TaskClaimOutcome.ACQUIRED_ACTIVE
    assert result.claim.generation == 1
    assert session.rows == []
    assert session.scalars == []


def test_repository_replays_without_new_assignment_reads_or_mutations() -> None:
    worker, session_id, envelope = claim_fixture()
    acquired_at = datetime.now(UTC)
    current = SimpleNamespace(
        task_attempt_id=envelope.task_attempt_id,
        generation=4,
        worker_session_id=session_id,
        acquired_at=acquired_at,
        lease_expires_at=acquired_at + timedelta(seconds=60),
    )
    durable = SimpleNamespace(
        route=envelope.route,
        payload=dispatch_envelope_to_mapping(envelope),
        attempt_number=1,
        task_type="test.task",
    )
    session = FakeSession(
        [
            SimpleNamespace(id=worker.worker_identity_id),
            SimpleNamespace(id=worker.credential_id),
            SimpleNamespace(ended_at=None),
            SimpleNamespace(status="claimed"),
            durable,
            current,
        ],
        [1, True],
    )
    repository = SQLAlchemyTaskClaimRepository(
        cast(async_sessionmaker[AsyncSession], FakeSessions(session)),
        worker_stale_after_seconds=30,
    )

    result = asyncio.run(
        repository.acquire_claim(worker, session_id, envelope, lease_seconds=60)
    )

    assert result.outcome is TaskClaimOutcome.REPLAYED_EXPIRED
    assert result.claim.generation == 4
    assert len(session.statements) == 8


def test_repository_rejects_invalid_policy_and_disabled_authority() -> None:
    sessions = cast(async_sessionmaker[AsyncSession], FakeSessions(FakeSession([], [])))
    with pytest.raises(ValueError, match="stale threshold"):
        SQLAlchemyTaskClaimRepository(sessions, worker_stale_after_seconds=0)

    worker, session_id, envelope = claim_fixture()
    repository = SQLAlchemyTaskClaimRepository(
        cast(
            async_sessionmaker[AsyncSession],
            FakeSessions(FakeSession([None], [])),
        ),
        worker_stale_after_seconds=30,
    )
    with pytest.raises(TaskClaimAuthorityRejected):
        asyncio.run(
            repository.acquire_claim(worker, session_id, envelope, lease_seconds=60)
        )


def test_repository_rejects_ended_authenticated_session() -> None:
    worker, session_id, envelope = claim_fixture()
    repository = SQLAlchemyTaskClaimRepository(
        cast(
            async_sessionmaker[AsyncSession],
            FakeSessions(
                FakeSession(
                    [
                        SimpleNamespace(id=worker.worker_identity_id),
                        SimpleNamespace(id=worker.credential_id),
                        SimpleNamespace(ended_at=datetime.now(UTC)),
                    ],
                    [],
                )
            ),
        ),
        worker_stale_after_seconds=30,
    )
    with pytest.raises(TaskClaimSessionInactive):
        asyncio.run(
            repository.acquire_claim(worker, session_id, envelope, lease_seconds=60)
        )
