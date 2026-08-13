"""Concurrency-safe lifecycle for broker dispatch consumption and draining."""

from __future__ import annotations

import asyncio
from enum import StrEnum

from taskforge.worker.consumer_ports import (
    DispatchConsumer,
    DispatchDeliveryControl,
    DispatchDeliveryHandler,
)


class WorkerDispatchRuntimeState(StrEnum):
    NEW = "new"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"


class WorkerDispatchRuntimeStopping(Exception):
    """The dispatch runtime no longer accepts a start request."""


class WorkerDispatchRuntimeInvariantError(Exception):
    """The broker adapter violated the runtime callback lifecycle."""


class WorkerDispatchRuntime:
    """Own at most one subscription and drain its admitted callbacks safely."""

    def __init__(
        self,
        consumer: DispatchConsumer,
        handler: DispatchDeliveryHandler,
    ) -> None:
        self._consumer = consumer
        self._handler = handler
        self._state = WorkerDispatchRuntimeState.NEW
        self._lock = asyncio.Lock()
        self._drained = asyncio.Condition(self._lock)
        self._start_operation: asyncio.Task[str] | None = None
        self._shutdown_operation: asyncio.Task[None] | None = None
        self._consumer_tag: str | None = None
        self._in_flight = 0
        self._admission_closed = False
        self._stopping = asyncio.Event()

    @property
    def state(self) -> WorkerDispatchRuntimeState:
        return self._state

    @property
    def in_flight(self) -> int:
        return self._in_flight

    async def start(self) -> str:
        async with self._lock:
            if self._state is WorkerDispatchRuntimeState.NEW:
                self._state = WorkerDispatchRuntimeState.STARTING
                self._start_operation = asyncio.create_task(
                    self._register(), name="taskforge-worker-dispatch-start"
                )
            elif self._state is WorkerDispatchRuntimeState.RUNNING:
                if self._consumer_tag is None:
                    raise WorkerDispatchRuntimeInvariantError
                return self._consumer_tag
            elif self._state in (
                WorkerDispatchRuntimeState.STOPPING,
                WorkerDispatchRuntimeState.STOPPED,
            ):
                raise WorkerDispatchRuntimeStopping
            operation = self._start_operation
            if operation is None:
                raise WorkerDispatchRuntimeInvariantError
        return await asyncio.shield(operation)

    async def shutdown(self) -> None:
        async with self._lock:
            if self._state is WorkerDispatchRuntimeState.STOPPED:
                return
            if self._state is WorkerDispatchRuntimeState.NEW:
                self._admission_closed = True
                self._state = WorkerDispatchRuntimeState.STOPPED
                return
            if self._state in (
                WorkerDispatchRuntimeState.STARTING,
                WorkerDispatchRuntimeState.RUNNING,
            ):
                self._state = WorkerDispatchRuntimeState.STOPPING
                self._stopping.set()
            if self._state is not WorkerDispatchRuntimeState.STOPPING:
                raise WorkerDispatchRuntimeInvariantError
            if self._shutdown_operation is None:
                self._shutdown_operation = asyncio.create_task(
                    self._shutdown(), name="taskforge-worker-dispatch-shutdown"
                )
            operation = self._shutdown_operation
        await asyncio.shield(operation)

    async def _register(self) -> str:
        try:
            consumer_tag = await self._consumer.consume(self._admit)
        except BaseException:
            async with self._lock:
                if self._state is WorkerDispatchRuntimeState.STARTING:
                    self._state = WorkerDispatchRuntimeState.NEW
                self._start_operation = None
            raise
        async with self._lock:
            if self._consumer_tag is not None:
                raise WorkerDispatchRuntimeInvariantError
            self._consumer_tag = consumer_tag
            if self._state is WorkerDispatchRuntimeState.STARTING:
                self._state = WorkerDispatchRuntimeState.RUNNING
            elif self._state is not WorkerDispatchRuntimeState.STOPPING:
                raise WorkerDispatchRuntimeInvariantError
        return consumer_tag

    async def _shutdown(self) -> None:
        current = asyncio.current_task()
        try:
            async with self._lock:
                start_operation = self._start_operation
            if start_operation is not None:
                try:
                    await asyncio.shield(start_operation)
                except asyncio.CancelledError:
                    shutdown_operation = asyncio.current_task()
                    if (
                        shutdown_operation is None
                        or shutdown_operation.cancelling()
                        or not start_operation.cancelled()
                    ):
                        raise
                except Exception:
                    pass
            async with self._lock:
                consumer_tag = self._consumer_tag
            if consumer_tag is not None:
                await self._consumer.cancel(consumer_tag)
            async with self._drained:
                self._admission_closed = True
                await self._drained.wait_for(lambda: self._in_flight == 0)
                self._state = WorkerDispatchRuntimeState.STOPPED
        finally:
            async with self._lock:
                if self._shutdown_operation is current:
                    self._shutdown_operation = None

    async def _admit(self, control: DispatchDeliveryControl) -> None:
        async with self._lock:
            if self._admission_closed:
                raise WorkerDispatchRuntimeInvariantError(
                    "delivery arrived after broker cancellation"
                )
            if self._state not in (
                WorkerDispatchRuntimeState.STARTING,
                WorkerDispatchRuntimeState.RUNNING,
                WorkerDispatchRuntimeState.STOPPING,
            ):
                raise WorkerDispatchRuntimeInvariantError
            self._in_flight += 1
        try:
            await self._handler(control)
        finally:
            async with self._drained:
                self._in_flight -= 1
                if self._in_flight == 0:
                    self._drained.notify_all()
