"""Worker heartbeat domain and service contract tests."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.worker.domain import (
    MAX_HEARTBEAT_SEQUENCE,
    WorkerHealthProjection,
    WorkerHeartbeat,
)
from taskforge.worker.persistence_ports import (
    WorkerHeartbeatAuthorityRejected,
    WorkerHeartbeatPersistenceUnavailable,
    WorkerHeartbeatReplayConflict,
    WorkerHeartbeatSequenceGap,
    WorkerHeartbeatSessionInactive,
    WorkerHeartbeatSessionUnavailable,
    WorkerHeartbeatStale,
)
from taskforge.worker.service import (
    ConflictingWorkerHeartbeatReplay,
    StaleWorkerHeartbeat,
    WorkerHeartbeatGap,
    WorkerHeartbeatRejected,
    WorkerHeartbeatService,
    WorkerHeartbeatServiceUnavailable,
    WorkerSessionInactive,
    WorkerSessionUnavailable,
)


class HeartbeatRepository:
    def __init__(self) -> None:
        self.calls: list[tuple[AuthenticatedWorker, UUID, WorkerHeartbeat]] = []
        self.error: BaseException | None = None

    async def apply_heartbeat(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        heartbeat: WorkerHeartbeat,
    ) -> WorkerHealthProjection:
        self.calls.append((authenticated_worker, worker_session_id, heartbeat))
        if self.error is not None:
            raise self.error
        now = datetime.now(UTC)
        return WorkerHealthProjection(
            worker_session_id,
            heartbeat.sequence,
            now,
            heartbeat.accepting_work,
            now,
        )


def test_heartbeat_validates_positive_bigint_sequence_and_strict_boolean() -> None:
    assert WorkerHeartbeat(1, False) == WorkerHeartbeat(1, False)
    assert WorkerHeartbeat(MAX_HEARTBEAT_SEQUENCE, True).sequence == (
        MAX_HEARTBEAT_SEQUENCE
    )
    for sequence in (True, 0, -1, MAX_HEARTBEAT_SEQUENCE + 1):
        with pytest.raises(ValueError):
            WorkerHeartbeat(sequence, True)
    with pytest.raises(ValueError):
        WorkerHeartbeat(1, 1)  # type: ignore[arg-type]


def test_health_projection_rejects_invalid_durable_state() -> None:
    now = datetime.now(UTC)
    with pytest.raises(ValueError, match="nonnegative"):
        WorkerHealthProjection(uuid4(), -1, now, False, now)
    with pytest.raises(ValueError, match="timezone-aware"):
        WorkerHealthProjection(
            uuid4(),
            0,
            now.replace(tzinfo=None),
            False,
            now,
        )


def test_service_passes_authenticated_worker_session_and_command_once() -> None:
    repository = HeartbeatRepository()
    service = WorkerHeartbeatService(repository)
    authenticated = AuthenticatedWorker(uuid4(), uuid4())
    session_id = uuid4()

    health = asyncio.run(
        service.heartbeat(
            authenticated,
            session_id,
            sequence=1,
            accepting_work=True,
        )
    )

    assert health.worker_session_id == session_id
    assert repository.calls == [(authenticated, session_id, WorkerHeartbeat(1, True))]


@pytest.mark.parametrize(
    ("persistence_error", "service_error"),
    (
        (WorkerHeartbeatAuthorityRejected(), WorkerHeartbeatRejected),
        (WorkerHeartbeatSessionUnavailable(), WorkerSessionUnavailable),
        (WorkerHeartbeatSessionInactive(), WorkerSessionInactive),
        (WorkerHeartbeatStale(), StaleWorkerHeartbeat),
        (WorkerHeartbeatSequenceGap(), WorkerHeartbeatGap),
        (WorkerHeartbeatReplayConflict(), ConflictingWorkerHeartbeatReplay),
        (
            WorkerHeartbeatPersistenceUnavailable(),
            WorkerHeartbeatServiceUnavailable,
        ),
    ),
)
def test_service_normalizes_declared_heartbeat_failures(
    persistence_error: Exception,
    service_error: type[Exception],
) -> None:
    repository = HeartbeatRepository()
    repository.error = persistence_error
    service = WorkerHeartbeatService(repository)
    with pytest.raises(service_error):
        asyncio.run(
            service.heartbeat(
                AuthenticatedWorker(uuid4(), uuid4()),
                uuid4(),
                sequence=1,
                accepting_work=False,
            )
        )


def test_service_does_not_swallow_programming_or_cancellation_failures() -> None:
    for failure in (RuntimeError("defect"), asyncio.CancelledError()):
        repository = HeartbeatRepository()
        repository.error = failure
        service = WorkerHeartbeatService(repository)
        with pytest.raises(type(failure)):
            asyncio.run(
                service.heartbeat(
                    AuthenticatedWorker(uuid4(), uuid4()),
                    uuid4(),
                    sequence=1,
                    accepting_work=False,
                )
            )
