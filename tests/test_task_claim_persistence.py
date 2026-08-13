"""Repository orchestration tests complementing real PostgreSQL claim tests."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.sql.dml import Insert, Update

from taskforge.claims.domain import (
    TaskClaimOutcome,
    TaskClaimRenewalOutcome,
    TaskClaimRenewalRequest,
)
from taskforge.claims.persistence_ports import (
    TaskClaimAuthorityRejected,
    TaskClaimSessionInactive,
)
from taskforge.dispatch.envelope import (
    DispatchEnvelope,
    create_dispatch_envelope,
    deserialize_dispatch_envelope,
    dispatch_envelope_to_mapping,
)
from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.persistence.claims import (
    SQLAlchemyTaskClaimRepository,
    _dispatch_matches,
)


def legacy(envelope: DispatchEnvelope) -> DispatchEnvelope:
    import json

    mapping = dispatch_envelope_to_mapping(envelope)
    mapping["schema_version"] = 1
    del mapping["deadline_at"]
    del mapping["execution_timeout_seconds"]
    return deserialize_dispatch_envelope(
        json.dumps(mapping, separators=(",", ":"), sort_keys=True).encode()
    )


def test_full_payload_match_fences_v3_policy_and_version() -> None:
    deadline = datetime(2026, 8, 13, 20, tzinfo=UTC)
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
        deadline_at=deadline,
        execution_timeout_seconds=30,
    )
    durable = SimpleNamespace(
        attempt_number=1,
        task_type="test.task",
        route=envelope.route,
        payload=dispatch_envelope_to_mapping(envelope),
    )
    changed = create_dispatch_envelope(
        dispatch_id=envelope.dispatch_id,
        task_attempt_id=envelope.task_attempt_id,
        task_run_id=envelope.task_run_id,
        workflow_run_id=envelope.workflow_run_id,
        attempt_number=1,
        task_type="test.task",
        required_capability="test-capability",
        task_payload={},
        references={},
        deadline_at=deadline + timedelta(seconds=1),
        execution_timeout_seconds=30,
    )
    assert _dispatch_matches(envelope, cast(Any, durable))
    assert not _dispatch_matches(changed, cast(Any, durable))
    assert not _dispatch_matches(legacy(envelope), cast(Any, durable))

    changed_timeout = create_dispatch_envelope(
        dispatch_id=envelope.dispatch_id,
        task_attempt_id=envelope.task_attempt_id,
        task_run_id=envelope.task_run_id,
        workflow_run_id=envelope.workflow_run_id,
        attempt_number=1,
        task_type="test.task",
        required_capability="test-capability",
        task_payload={},
        references={},
        deadline_at=deadline,
        execution_timeout_seconds=31,
    )
    assert not _dispatch_matches(changed_timeout, cast(Any, durable))


def test_full_payload_match_preserves_historical_v1() -> None:
    current = create_dispatch_envelope(
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
    envelope = legacy(current)
    durable = SimpleNamespace(
        attempt_number=1,
        task_type="test.task",
        route=envelope.route,
        payload=dispatch_envelope_to_mapping(envelope),
    )
    assert envelope.deadline_at is None
    assert envelope.execution_timeout_seconds is None
    assert _dispatch_matches(envelope, cast(Any, durable))


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
            None,
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
    event_inserts = [
        statement
        for statement in session.statements
        if isinstance(statement, Insert) and statement.table.name == "task_claim_events"
    ]
    assert len(event_inserts) == 1
    assert event_inserts[0].compile().params["event_type"] == "claim_acquired"
    assert event_inserts[0].compile().params["occurred_at"] == acquired_at


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
    assert not any(
        isinstance(statement, Insert) and statement.table.name == "task_claim_events"
        for statement in session.statements
    )


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


def renewal_rows(
    worker: AuthenticatedWorker,
    request: TaskClaimRenewalRequest,
    *,
    current_expiry: datetime,
    candidate_expiry: datetime,
    renewed_expiry: datetime | None,
) -> tuple[list[object], list[object]]:
    acquired_at = current_expiry - timedelta(seconds=30)
    claim = SimpleNamespace(
        task_attempt_id=request.task_attempt_id,
        generation=request.generation,
        worker_session_id=request.worker_session_id,
        acquired_at=acquired_at,
        lease_expires_at=current_expiry,
    )
    rows: list[object] = [
        SimpleNamespace(id=worker.worker_identity_id),
        SimpleNamespace(id=worker.credential_id),
        SimpleNamespace(ended_at=None),
        SimpleNamespace(id=uuid4(), status="claimed", attempt_number=1),
        claim,
        SimpleNamespace(
            reference_time=current_expiry - timedelta(seconds=10),
            candidate_expiry=candidate_expiry,
        ),
    ]
    if renewed_expiry is not None:
        rows.append(
            SimpleNamespace(
                task_attempt_id=request.task_attempt_id,
                generation=request.generation,
                worker_session_id=request.worker_session_id,
                acquired_at=acquired_at,
                lease_expires_at=renewed_expiry,
            )
        )
        rows.append(None)
    return rows, [1]


def test_repository_renews_with_guarded_update_after_locking() -> None:
    worker = AuthenticatedWorker(uuid4(), uuid4())
    session_id, attempt_id = uuid4(), uuid4()
    current_expiry = datetime.now(UTC) + timedelta(seconds=10)
    request = TaskClaimRenewalRequest(attempt_id, 2, session_id, current_expiry)
    rows, scalars = renewal_rows(
        worker,
        request,
        current_expiry=current_expiry,
        candidate_expiry=current_expiry + timedelta(seconds=50),
        renewed_expiry=current_expiry + timedelta(seconds=51),
    )
    session = FakeSession(rows, scalars)
    repository = SQLAlchemyTaskClaimRepository(
        cast(async_sessionmaker[AsyncSession], FakeSessions(session)),
        worker_stale_after_seconds=30,
    )

    result = asyncio.run(repository.renew_claim(worker, request, lease_seconds=60))

    assert result.outcome is TaskClaimRenewalOutcome.RENEWED
    updates = [
        statement for statement in session.statements if isinstance(statement, Update)
    ]
    assert len(updates) == 1
    sql = str(updates[0].compile(compile_kwargs={"literal_binds": True}))
    assert "lease_expires_at=greatest" in sql
    assert "terminated_at IS NULL" in sql
    assert "lease_expires_at > statement_timestamp()" in sql
    assert "task_runs.status IN ('claimed', 'running')" in sql
    assert "worker_session_health" not in sql
    assert "worker_session_capabilities" not in sql
    event_inserts = [
        statement
        for statement in session.statements
        if isinstance(statement, Insert) and statement.table.name == "task_claim_events"
    ]
    assert len(event_inserts) == 1
    event_params = event_inserts[0].compile().params
    assert event_params["event_type"] == "lease_renewed"
    assert event_params["previous_lease_expires_at"] == current_expiry


def test_repository_active_unchanged_and_replay_are_genuine_no_write_paths() -> None:
    worker = AuthenticatedWorker(uuid4(), uuid4())
    session_id, attempt_id = uuid4(), uuid4()
    current_expiry = datetime.now(UTC) + timedelta(seconds=120)
    request = TaskClaimRenewalRequest(attempt_id, 2, session_id, current_expiry)
    rows, scalars = renewal_rows(
        worker,
        request,
        current_expiry=current_expiry,
        candidate_expiry=current_expiry - timedelta(seconds=30),
        renewed_expiry=None,
    )
    session = FakeSession(rows, scalars)
    repository = SQLAlchemyTaskClaimRepository(
        cast(async_sessionmaker[AsyncSession], FakeSessions(session)),
        worker_stale_after_seconds=30,
    )
    unchanged = asyncio.run(repository.renew_claim(worker, request, lease_seconds=60))
    assert unchanged.outcome is TaskClaimRenewalOutcome.ACTIVE_UNCHANGED
    assert not any(isinstance(statement, Update) for statement in session.statements)

    replay_request = TaskClaimRenewalRequest(
        attempt_id, 2, session_id, current_expiry - timedelta(seconds=1)
    )
    rows, scalars = renewal_rows(
        worker,
        replay_request,
        current_expiry=current_expiry,
        candidate_expiry=current_expiry + timedelta(seconds=30),
        renewed_expiry=None,
    )
    replay_session = FakeSession(rows, scalars)
    replay_repository = SQLAlchemyTaskClaimRepository(
        cast(async_sessionmaker[AsyncSession], FakeSessions(replay_session)),
        worker_stale_after_seconds=30,
    )
    replayed = asyncio.run(
        replay_repository.renew_claim(worker, replay_request, lease_seconds=60)
    )
    assert replayed.outcome is TaskClaimRenewalOutcome.REPLAYED
    assert not any(
        isinstance(statement, Update) for statement in replay_session.statements
    )
