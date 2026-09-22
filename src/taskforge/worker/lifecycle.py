"""Concurrency-safe lifecycle for broker dispatch consumption and draining."""

from __future__ import annotations

import asyncio
from enum import StrEnum

from taskforge.metrics import add as add_metric
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
        *,
        consumer_cancel_timeout_seconds: float = 2.0,
    ) -> None:
        if consumer_cancel_timeout_seconds <= 0:
            raise ValueError("consumer cancellation timeout must be positive")
        self._consumer = consumer
        self._handler = handler
        self._consumer_cancel_timeout_seconds = consumer_cancel_timeout_seconds
        self._state = WorkerDispatchRuntimeState.NEW
        self._lock = asyncio.Lock()
        self._drained = asyncio.Condition(self._lock)
        self._start_operation: asyncio.Task[str] | None = None
        self._shutdown_operation: asyncio.Task[None] | None = None
        self._consumer_tag: str | None = None
        self._consumer_cancelled = False
        self._in_flight = 0
        self._in_flight_tasks: set[asyncio.Task[None]] = set()
        self._admission_closed = False
        self._activated = asyncio.Event()
        self._stopping = asyncio.Event()
        self._failure: asyncio.Future[BaseException] | None = None
        self._started_paused: bool | None = None

    @property
    def state(self) -> WorkerDispatchRuntimeState:
        return self._state

    @property
    def in_flight(self) -> int:
        return self._in_flight

    async def start(self, *, paused: bool = False) -> str:
        async with self._lock:
            if self._state is WorkerDispatchRuntimeState.NEW:
                self._started_paused = paused
                if paused:
                    self._activated.clear()
                else:
                    self._activated.set()
                self._failure = asyncio.get_running_loop().create_future()
                self._state = WorkerDispatchRuntimeState.STARTING
                self._start_operation = asyncio.create_task(
                    self._register(), name="taskforge-worker-dispatch-start"
                )
            elif self._state is WorkerDispatchRuntimeState.RUNNING:
                if paused != self._started_paused:
                    raise WorkerDispatchRuntimeInvariantError
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

    async def activate(self) -> None:
        """Open delivery admission after every process consumer is registered."""
        async with self._lock:
            if self._state is not WorkerDispatchRuntimeState.RUNNING:
                raise WorkerDispatchRuntimeInvariantError
            self._activated.set()

    async def wait_failed(self) -> None:
        """Wait until a delivery callback reports a process-fatal failure."""
        async with self._lock:
            failure = self._failure
            if failure is None:
                raise WorkerDispatchRuntimeInvariantError
        error = await asyncio.shield(failure)
        raise error

    async def shutdown(self) -> None:
        await self.begin_shutdown()
        await self.wait_drained()
        async with self._lock:
            if self._state is WorkerDispatchRuntimeState.STOPPING:
                self._state = WorkerDispatchRuntimeState.STOPPED

    async def begin_shutdown(self) -> None:
        """Close local admission and cancel the broker subscription once."""
        async with self._lock:
            if self._state is WorkerDispatchRuntimeState.STOPPED:
                return
            if self._state is WorkerDispatchRuntimeState.NEW:
                self._admission_closed = True
                self._stopping.set()
                self._state = WorkerDispatchRuntimeState.STOPPED
                return
            if self._state in (
                WorkerDispatchRuntimeState.STARTING,
                WorkerDispatchRuntimeState.RUNNING,
            ):
                self._state = WorkerDispatchRuntimeState.STOPPING
                self._stopping.set()
                self._admission_closed = True
            if self._state is not WorkerDispatchRuntimeState.STOPPING:
                raise WorkerDispatchRuntimeInvariantError
            if self._shutdown_operation is None:
                self._shutdown_operation = asyncio.create_task(
                    self._shutdown(), name="taskforge-worker-dispatch-shutdown"
                )
            operation = self._shutdown_operation
        await asyncio.shield(operation)

    async def wait_drained(self) -> None:
        """Wait until every callback admitted before the cutoff has exited."""
        async with self._drained:
            await self._drained.wait_for(lambda: self._in_flight == 0)

    async def cancel_in_flight(self) -> int:
        """Request cooperative cancellation without revoking callback authority."""
        async with self._lock:
            tasks = tuple(task for task in self._in_flight_tasks if not task.done())
        for task in tasks:
            task.cancel()
        return len(tasks)

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
                # This in-process barrier closes before broker cancellation is
                # confirmed. Deliveries racing after it remain unacknowledged.
                self._admission_closed = True
                consumer_tag = self._consumer_tag
                consumer_cancelled = self._consumer_cancelled
            cancellation_error: Exception | None = None
            if consumer_tag is not None and not consumer_cancelled:
                try:
                    async with asyncio.timeout(self._consumer_cancel_timeout_seconds):
                        await self._consumer.cancel(consumer_tag)
                except Exception as error:
                    cancellation_error = error
                else:
                    async with self._lock:
                        self._consumer_cancelled = True
            if cancellation_error is not None:
                raise cancellation_error
        finally:
            async with self._lock:
                if self._shutdown_operation is current:
                    self._shutdown_operation = None

    async def _admit(self, control: DispatchDeliveryControl) -> None:
        async with self._lock:
            if self._admission_closed:
                # Manual acknowledgement is intentionally omitted. Channel close
                # will make this race delivery available for redelivery.
                return
            if self._state not in (
                WorkerDispatchRuntimeState.STARTING,
                WorkerDispatchRuntimeState.RUNNING,
                WorkerDispatchRuntimeState.STOPPING,
            ):
                raise WorkerDispatchRuntimeInvariantError
            self._in_flight += 1
            current = asyncio.current_task()
            if current is None:
                raise WorkerDispatchRuntimeInvariantError
            self._in_flight_tasks.add(current)
            add_metric("taskforge.worker.running.deliveries", 1)
        try:
            if not self._activated.is_set():
                activated = asyncio.create_task(self._activated.wait())
                stopping = asyncio.create_task(self._stopping.wait())
                done, pending = await asyncio.wait(
                    (activated, stopping), return_when=asyncio.FIRST_COMPLETED
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                if stopping in done and self._stopping.is_set():
                    return
            await self._handler(control)
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            async with self._lock:
                failure = self._failure
                if failure is not None and not failure.done():
                    failure.set_result(error)
            raise
        finally:
            async with self._drained:
                self._in_flight -= 1
                current = asyncio.current_task()
                if current is not None:
                    self._in_flight_tasks.discard(current)
                add_metric("taskforge.worker.running.deliveries", -1)
                if self._in_flight == 0:
                    self._drained.notify_all()
