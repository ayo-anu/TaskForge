"""Worker inspection domain, service, and cursor contract tests."""

from __future__ import annotations

import asyncio
import base64
import json
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from sqlalchemy.dialects import postgresql

from taskforge.api.workers import (
    _decode_inspection_cursor,
    _encode_inspection_cursor,
)
from taskforge.persistence.workers import (
    SQLAlchemyWorkerInspectionRepository,
    _inspected_session,
    _session_inspection_statement,
)
from taskforge.worker.domain import (
    InspectedWorkerHeartbeatPage,
    InspectedWorkerSessionPage,
    InspectedWorkerSessionResource,
    WorkerHealthThresholds,
    WorkerInspectionObservation,
    WorkerSessionHealthStatus,
    WorkerSessionPageCursor,
)
from taskforge.worker.persistence_ports import (
    WorkerInspectionInvariantViolation,
    WorkerInspectionNotFound,
    WorkerInspectionPersistenceUnavailable,
)
from taskforge.worker.service import (
    WorkerInspectionInvariantError,
    WorkerInspectionNotFoundError,
    WorkerInspectionService,
    WorkerInspectionServiceUnavailable,
)


class InspectionRepository:
    def __init__(self) -> None:
        self.error: Exception | None = None
        self.calls: list[tuple[object, ...]] = []
        self.thresholds = WorkerHealthThresholds(30, 120)
        self.observation = WorkerInspectionObservation(
            datetime(2026, 8, 10, tzinfo=UTC), self.thresholds
        )

    async def get_session(
        self, worker_session_id: UUID, thresholds: WorkerHealthThresholds
    ) -> InspectedWorkerSessionResource:
        self.calls.append(("get", worker_session_id, thresholds))
        if self.error:
            raise self.error
        raise AssertionError("success result is supplied only by PostgreSQL tests")

    async def list_sessions(
        self,
        *,
        worker_identity_id: UUID | None,
        health_status: WorkerSessionHealthStatus | None,
        thresholds: WorkerHealthThresholds,
        limit: int,
        cursor: WorkerSessionPageCursor | None,
    ) -> InspectedWorkerSessionPage:
        self.calls.append(
            ("list", worker_identity_id, health_status, thresholds, limit, cursor)
        )
        if self.error:
            raise self.error
        return InspectedWorkerSessionPage((), self.observation, None)

    async def list_heartbeats(
        self,
        worker_session_id: UUID,
        *,
        before_sequence: int | None,
        limit: int,
    ) -> InspectedWorkerHeartbeatPage:
        self.calls.append(("history", worker_session_id, before_sequence, limit))
        if self.error:
            raise self.error
        return InspectedWorkerHeartbeatPage((), None)


class FakeResult:
    def __init__(self, rows: list[object]) -> None:
        self.rows = rows

    def one_or_none(self) -> object | None:
        return self.rows[0] if self.rows else None

    def all(self) -> list[object]:
        return self.rows


class FakeSession:
    def __init__(self, rows: list[object], *, scalar: object = None) -> None:
        self.rows = rows
        self.scalar_value = scalar

    async def __aenter__(self) -> FakeSession:
        return self

    async def __aexit__(self, *args: object) -> None:
        del args

    async def execute(self, statement: object) -> FakeResult:
        del statement
        return FakeResult(self.rows)

    async def scalar(self, statement: object) -> object:
        del statement
        return self.scalar_value


class FakeSessionFactory:
    def __init__(self, session: FakeSession) -> None:
        self.session = session

    def __call__(self) -> FakeSession:
        return self.session


def test_thresholds_enforce_bounded_ordered_contract() -> None:
    assert WorkerHealthThresholds(30, 120) == WorkerHealthThresholds(30, 120)
    for stale, offline in ((0, 120), (3601, 4000), (30, 1), (30, 86401), (30, 30)):
        with pytest.raises(ValueError):
            WorkerHealthThresholds(stale, offline)


def test_cursor_round_trip_binds_reference_position_filters_and_thresholds() -> None:
    thresholds = WorkerHealthThresholds(30, 120)
    identity_id = uuid4()
    cursor = WorkerSessionPageCursor(
        datetime(2026, 8, 10, 1, 2, 3, 456789, tzinfo=UTC),
        datetime(2026, 8, 10, 1, 1, tzinfo=UTC),
        uuid4(),
        identity_id,
        WorkerSessionHealthStatus.STALE,
        thresholds,
    )
    encoded = _encode_inspection_cursor(cursor)

    assert (
        _decode_inspection_cursor(
            encoded,
            worker_identity_id=identity_id,
            health_status=WorkerSessionHealthStatus.STALE,
            thresholds=thresholds,
        )
        == cursor
    )


def test_cursor_rejects_changed_traversal_contract_and_malformed_values() -> None:
    thresholds = WorkerHealthThresholds(30, 120)
    cursor = WorkerSessionPageCursor(
        datetime(2026, 8, 10, tzinfo=UTC),
        datetime(2026, 8, 9, tzinfo=UTC),
        uuid4(),
        None,
        None,
        thresholds,
    )
    encoded = _encode_inspection_cursor(cursor)
    mismatches = (
        (uuid4(), None, thresholds),
        (None, WorkerSessionHealthStatus.HEALTHY, thresholds),
        (None, None, WorkerHealthThresholds(31, 120)),
    )
    for identity, health, changed_thresholds in mismatches:
        with pytest.raises(ValueError):
            _decode_inspection_cursor(
                encoded,
                worker_identity_id=identity,
                health_status=health,
                thresholds=changed_thresholds,
            )


def test_cursor_strictly_rejects_wrong_shape_version_and_field_types() -> None:
    thresholds = WorkerHealthThresholds(30, 120)
    valid = {
        "v": 1,
        "rt": "2026-08-10T00:00:00.000000Z",
        "ls": "2026-08-09T00:00:00.000000Z",
        "sid": str(uuid4()),
        "wid": None,
        "hs": None,
        "st": 30,
        "ot": 120,
    }
    invalid_payloads = (
        {**valid, "extra": True},
        {**valid, "v": 2},
        {**valid, "st": True},
        {**valid, "rt": 1},
        {**valid, "wid": 1},
        {**valid, "hs": 1},
        {**valid, "sid": "not-a-uuid"},
        {**valid, "rt": "2026-08-10T00:00:00"},
        {**valid, "hs": "unknown"},
    )
    for payload in invalid_payloads:
        encoded = (
            base64.urlsafe_b64encode(
                json.dumps(payload, separators=(",", ":")).encode()
            )
            .rstrip(b"=")
            .decode()
        )
        with pytest.raises(ValueError):
            _decode_inspection_cursor(
                encoded,
                worker_identity_id=None,
                health_status=None,
                thresholds=thresholds,
            )


def test_inspection_sql_reuses_one_reference_and_maps_projection() -> None:
    thresholds = WorkerHealthThresholds(30, 120)
    reference = datetime(2026, 8, 10, tzinfo=UTC)
    first_page_sql = str(
        _session_inspection_statement(thresholds, reference_time=None).compile(
            dialect=postgresql.dialect()  # type: ignore[no-untyped-call]
        )
    )
    later_page_sql = str(
        _session_inspection_statement(thresholds, reference_time=reference).compile(
            dialect=postgresql.dialect()  # type: ignore[no-untyped-call]
        )
    )
    assert first_page_sql.count("statement_timestamp()") == 1
    assert "inspection_context.reference_time" in first_page_sql
    assert "statement_timestamp()" not in later_page_sql

    session_id, identity_id = uuid4(), uuid4()
    row = SimpleNamespace(
        worker_session_id=session_id,
        worker_identity_id=identity_id,
        worker_identity_name="worker-one",
        disabled_at=None,
        registered_at=reference,
        ended_at=None,
        capabilities=["documents", "notifications.email"],
        health_status="healthy",
        last_sequence=2,
        last_seen_at=reference,
        accepting_work=True,
        availability_changed_at=reference,
    )
    inspected = _inspected_session(row)  # type: ignore[arg-type]
    assert inspected.id == session_id
    assert inspected.identity.id == identity_id
    assert inspected.identity.enabled is True
    assert inspected.capabilities == ("documents", "notifications.email")
    assert inspected.health.status is WorkerSessionHealthStatus.HEALTHY


def test_sqlalchemy_inspection_adapter_maps_session_list_and_history_pages() -> None:
    now = datetime(2026, 8, 10, tzinfo=UTC)
    session_id, identity_id = uuid4(), uuid4()
    row = SimpleNamespace(
        worker_session_id=session_id,
        worker_identity_id=identity_id,
        worker_identity_name="worker-one",
        disabled_at=None,
        registered_at=now,
        ended_at=None,
        capabilities=None,
        health_status="healthy",
        last_sequence=0,
        last_seen_at=now,
        accepting_work=False,
        availability_changed_at=now,
        reference_time=now,
    )
    thresholds = WorkerHealthThresholds(30, 120)

    get_repository = SQLAlchemyWorkerInspectionRepository(
        FakeSessionFactory(FakeSession([row]))  # type: ignore[arg-type]
    )
    resource = asyncio.run(get_repository.get_session(session_id, thresholds))
    assert resource.session.id == session_id
    assert resource.session.capabilities == ()
    assert resource.observation.reference_time == now

    list_repository = SQLAlchemyWorkerInspectionRepository(
        FakeSessionFactory(FakeSession([row, row]))  # type: ignore[arg-type]
    )
    page = asyncio.run(
        list_repository.list_sessions(
            worker_identity_id=identity_id,
            health_status=WorkerSessionHealthStatus.HEALTHY,
            thresholds=thresholds,
            limit=1,
            cursor=None,
        )
    )
    assert len(page.items) == 1
    assert page.next_cursor is not None
    assert page.next_cursor.reference_time == now

    history_rows = [
        SimpleNamespace(sequence=2, received_at=now, accepting_work=False),
        SimpleNamespace(sequence=1, received_at=now, accepting_work=True),
    ]
    history_repository = SQLAlchemyWorkerInspectionRepository(
        FakeSessionFactory(FakeSession(history_rows, scalar=session_id))  # type: ignore[arg-type]
    )
    history = asyncio.run(
        history_repository.list_heartbeats(session_id, before_sequence=3, limit=1)
    )
    assert [item.sequence for item in history.items] == [2]
    assert history.next_before_sequence == 2
    for malformed in ("", "!", "a" * 769):
        with pytest.raises(ValueError):
            _decode_inspection_cursor(
                malformed,
                worker_identity_id=None,
                health_status=None,
                thresholds=thresholds,
            )


def test_service_propagates_filters_thresholds_and_history_bounds() -> None:
    repository = InspectionRepository()
    service = WorkerInspectionService(repository, repository.thresholds)
    session_id = uuid4()
    identity_id = uuid4()

    page = asyncio.run(
        service.list_sessions(
            worker_identity_id=identity_id,
            health_status=WorkerSessionHealthStatus.OFFLINE,
            limit=25,
            cursor=None,
        )
    )
    history = asyncio.run(
        service.list_heartbeats(session_id, before_sequence=9, limit=5)
    )

    assert page.items == ()
    assert history.items == ()
    assert service.thresholds == repository.thresholds
    assert repository.calls == [
        (
            "list",
            identity_id,
            WorkerSessionHealthStatus.OFFLINE,
            repository.thresholds,
            25,
            None,
        ),
        ("history", session_id, 9, 5),
    ]


@pytest.mark.parametrize(
    ("repository_error", "service_error", "operation"),
    (
        (WorkerInspectionNotFound(), WorkerInspectionNotFoundError, "get"),
        (
            WorkerInspectionInvariantViolation(),
            WorkerInspectionInvariantError,
            "get",
        ),
        (
            WorkerInspectionPersistenceUnavailable(),
            WorkerInspectionServiceUnavailable,
            "get",
        ),
        (WorkerInspectionNotFound(), WorkerInspectionNotFoundError, "history"),
        (
            WorkerInspectionPersistenceUnavailable(),
            WorkerInspectionServiceUnavailable,
            "history",
        ),
        (
            WorkerInspectionInvariantViolation(),
            WorkerInspectionInvariantError,
            "list",
        ),
        (
            WorkerInspectionPersistenceUnavailable(),
            WorkerInspectionServiceUnavailable,
            "list",
        ),
    ),
)
def test_service_normalizes_declared_inspection_failures(
    repository_error: Exception,
    service_error: type[Exception],
    operation: str,
) -> None:
    repository = InspectionRepository()
    repository.error = repository_error
    service = WorkerInspectionService(repository, repository.thresholds)
    with pytest.raises(service_error):
        if operation == "get":
            asyncio.run(service.get_session(uuid4()))
        elif operation == "list":
            asyncio.run(
                service.list_sessions(
                    worker_identity_id=None,
                    health_status=None,
                    limit=10,
                    cursor=None,
                )
            )
        else:
            asyncio.run(
                service.list_heartbeats(uuid4(), before_sequence=None, limit=10)
            )
