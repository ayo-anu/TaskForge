"""Tests for process-owned heartbeat sequencing and uncertainty handling."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest

from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.worker.domain import WorkerHealthProjection
from taskforge.worker.heartbeat import WorkerHeartbeatSupervisor
from taskforge.worker.runtime_errors import WorkerProcessFailure
from taskforge.worker.service import (
    WorkerHeartbeatRejected,
    WorkerHeartbeatServiceUnavailable,
)


class Service:
    def __init__(self, outcomes: list[object]) -> None:
        self.outcomes = outcomes
        self.calls: list[tuple[Any, ...]] = []

    async def heartbeat(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        *,
        sequence: int,
        accepting_work: bool,
        correlation_id: UUID | None = None,
    ) -> WorkerHealthProjection:
        kwargs = {
            "sequence": sequence,
            "accepting_work": accepting_work,
        }
        self.calls.append((authenticated_worker, worker_session_id, kwargs))
        del correlation_id
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        now = datetime.now(UTC)
        return WorkerHealthProjection(worker_session_id, sequence, now, True, now)


def test_initial_unknown_outcome_retries_exact_sequence_and_payload() -> None:
    async def scenario() -> None:
        worker = AuthenticatedWorker(uuid4(), uuid4())
        session_id = uuid4()
        service = Service([WorkerHeartbeatServiceUnavailable(), object(), object()])
        supervisor = WorkerHeartbeatSupervisor(
            service,
            worker,
            session_id,
            interval_seconds=0.01,
            operation_timeout_seconds=0.001,
            stale_after_seconds=1,
        )
        await supervisor.send_initial()
        assert supervisor.sequence == 2
        assert [call[2] for call in service.calls] == [
            {"sequence": 1, "accepting_work": True},
            {"sequence": 1, "accepting_work": True},
            {"sequence": 2, "accepting_work": True},
        ]
        supervisor.start()
        await supervisor.close()

    asyncio.run(scenario())


def test_periodic_authority_failure_reaches_parent_supervision() -> None:
    async def scenario() -> None:
        worker = AuthenticatedWorker(uuid4(), uuid4())
        service = Service([object(), WorkerHeartbeatRejected()])
        supervisor = WorkerHeartbeatSupervisor(
            service,
            worker,
            uuid4(),
            interval_seconds=0.001,
            operation_timeout_seconds=0.0001,
            stale_after_seconds=1,
        )
        await supervisor.send_initial()
        supervisor.start()
        with pytest.raises(WorkerProcessFailure):
            await supervisor.wait_failed()
        await supervisor.close()

    asyncio.run(scenario())
