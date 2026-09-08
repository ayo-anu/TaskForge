"""Application-owned worker heartbeat sequencing and supervision."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from time import monotonic
from typing import Protocol
from uuid import UUID

from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.worker.domain import WorkerHealthProjection
from taskforge.worker.runtime_errors import WorkerProcessFailure
from taskforge.worker.service import (
    ConflictingWorkerHeartbeatReplay,
    StaleWorkerHeartbeat,
    WorkerHeartbeatGap,
    WorkerHeartbeatRejected,
    WorkerHeartbeatServiceUnavailable,
    WorkerSessionInactive,
    WorkerSessionUnavailable,
)


class HeartbeatService(Protocol):
    async def heartbeat(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        *,
        sequence: int,
        accepting_work: bool,
        correlation_id: UUID | None = None,
    ) -> WorkerHealthProjection: ...


class WorkerHeartbeatSupervisor:
    """Retry uncertain heartbeats identically within the stale safety window."""

    def __init__(
        self,
        service: HeartbeatService,
        worker: AuthenticatedWorker,
        worker_session_id: UUID,
        *,
        interval_seconds: float,
        operation_timeout_seconds: float,
        stale_after_seconds: int,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        self._service = service
        self._worker = worker
        self._worker_session_id = worker_session_id
        self._interval_seconds = interval_seconds
        self._operation_timeout_seconds = operation_timeout_seconds
        self._stale_after_seconds = stale_after_seconds
        self._clock = clock
        self._sequence = 0
        self._last_confirmed_at = clock()
        self._task: asyncio.Task[None] | None = None
        self._closed = False
        self._failure_observed = False

    @property
    def sequence(self) -> int:
        return self._sequence

    async def send_initial(self) -> None:
        if self._sequence != 0 or self._task is not None or self._closed:
            raise RuntimeError("initial heartbeat cannot be sent")
        uncertain_replay = await self._send_confirmed(1)
        while uncertain_replay:
            uncertain_replay = await self._send_confirmed(self._sequence + 1)

    def start(self) -> None:
        # An uncertain sequence-1 outcome is replayed exactly and followed by a
        # newly confirmed sequence before admission opens. In that case the
        # first periodic sequence is legitimately greater than one.
        if self._sequence < 1 or self._task is not None or self._closed:
            raise RuntimeError("heartbeat supervisor cannot be started")
        self._task = asyncio.create_task(self._run(), name="taskforge-worker-heartbeat")

    async def wait_failed(self) -> None:
        task = self._task
        if task is None:
            raise RuntimeError("heartbeat supervisor is not started")
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            raise
        except BaseException:
            self._failure_observed = True
            raise
        self._failure_observed = True
        raise WorkerProcessFailure("heartbeat supervisor terminated unexpectedly")

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except WorkerProcessFailure:
                if not self._failure_observed:
                    raise

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self._interval_seconds)
            uncertain_replay = await self._send_confirmed(self._sequence + 1)
            while uncertain_replay:
                uncertain_replay = await self._send_confirmed(self._sequence + 1)

    async def _send_confirmed(self, sequence: int) -> bool:
        outcome_was_uncertain = False
        while True:
            try:
                async with asyncio.timeout(self._operation_timeout_seconds):
                    projection = await self._service.heartbeat(
                        self._worker,
                        self._worker_session_id,
                        sequence=sequence,
                        accepting_work=True,
                    )
            except asyncio.CancelledError:
                raise
            except (
                WorkerHeartbeatRejected,
                WorkerSessionUnavailable,
                WorkerSessionInactive,
                StaleWorkerHeartbeat,
                WorkerHeartbeatGap,
                ConflictingWorkerHeartbeatReplay,
            ) as error:
                raise WorkerProcessFailure(
                    "worker heartbeat authority failed"
                ) from error
            except (WorkerHeartbeatServiceUnavailable, TimeoutError) as error:
                outcome_was_uncertain = True
                conservative_deadline = (
                    self._last_confirmed_at
                    + self._stale_after_seconds
                    - self._operation_timeout_seconds
                )
                if self._clock() >= conservative_deadline:
                    raise WorkerProcessFailure(
                        "worker heartbeat could not be confirmed before stale threshold"
                    ) from error
                await asyncio.sleep(self._operation_timeout_seconds)
                continue
            if (
                projection.worker_session_id != self._worker_session_id
                or projection.last_sequence != sequence
                or not projection.accepting_work
            ):
                raise WorkerProcessFailure("worker heartbeat receipt is inconsistent")
            self._sequence = sequence
            if not outcome_was_uncertain:
                self._last_confirmed_at = self._clock()
            return outcome_was_uncertain
