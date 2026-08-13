from __future__ import annotations

import asyncio
from typing import Any

import pytest

from taskforge.worker.consumer_ports import BrokerConsumerUnavailable
from taskforge.worker.lifecycle import (
    WorkerDispatchRuntime,
    WorkerDispatchRuntimeInvariantError,
    WorkerDispatchRuntimeState,
    WorkerDispatchRuntimeStopping,
)


class Consumer:
    def __init__(self) -> None:
        self.registration_started = asyncio.Event()
        self.release_registration = asyncio.Event()
        self.cancellation_started = asyncio.Event()
        self.release_cancellation = asyncio.Event()
        self.consume_calls = 0
        self.cancel_calls: list[str] = []
        self.handler: Any = None
        self.registration_error: BaseException | None = None
        self.cancellation_errors: list[Exception] = []

    async def consume(self, handler: Any) -> str:
        self.consume_calls += 1
        self.handler = handler
        self.registration_started.set()
        await self.release_registration.wait()
        if self.registration_error is not None:
            raise self.registration_error
        return "consumer-tag"

    async def cancel(self, consumer_tag: str) -> None:
        self.cancel_calls.append(consumer_tag)
        self.cancellation_started.set()
        if self.cancellation_errors:
            raise self.cancellation_errors.pop(0)
        await self.release_cancellation.wait()


async def start_runtime(
    consumer: Consumer, handler: Any | None = None
) -> WorkerDispatchRuntime:
    consumer.release_registration.set()
    runtime = WorkerDispatchRuntime(consumer, handler or _completed_handler)
    assert await runtime.start() == "consumer-tag"
    return runtime


async def _completed_handler(control: Any) -> None:
    del control


def assert_state(
    runtime: WorkerDispatchRuntime, expected: WorkerDispatchRuntimeState
) -> None:
    assert runtime.state is expected


def test_concurrent_starts_create_at_most_one_subscription() -> None:
    async def scenario() -> None:
        consumer = Consumer()
        runtime = WorkerDispatchRuntime(consumer, _completed_handler)
        starts = [asyncio.create_task(runtime.start()) for _ in range(3)]
        await consumer.registration_started.wait()
        assert consumer.consume_calls == 1

        consumer.release_registration.set()
        assert await asyncio.gather(*starts) == ["consumer-tag"] * 3
        assert await runtime.start() == "consumer-tag"
        assert consumer.consume_calls == 1

    asyncio.run(scenario())


def test_shutdown_before_start_permanently_prevents_subscription() -> None:
    async def scenario() -> None:
        consumer = Consumer()
        runtime = WorkerDispatchRuntime(consumer, _completed_handler)

        await runtime.shutdown()
        await runtime.shutdown()

        assert runtime.state is WorkerDispatchRuntimeState.STOPPED
        with pytest.raises(WorkerDispatchRuntimeStopping):
            await runtime.start()
        assert consumer.consume_calls == 0
        assert consumer.cancel_calls == []

    asyncio.run(scenario())


def test_start_shutdown_race_cancels_new_subscription_before_returning() -> None:
    async def scenario() -> None:
        consumer = Consumer()
        runtime = WorkerDispatchRuntime(consumer, _completed_handler)
        start = asyncio.create_task(runtime.start())
        await consumer.registration_started.wait()

        shutdown = asyncio.create_task(runtime.shutdown())
        await runtime._stopping.wait()
        assert_state(runtime, WorkerDispatchRuntimeState.STOPPING)
        with pytest.raises(WorkerDispatchRuntimeStopping):
            await runtime.start()
        assert consumer.cancel_calls == []

        consumer.release_registration.set()
        assert await start == "consumer-tag"
        await consumer.cancellation_started.wait()
        assert not shutdown.done()
        consumer.release_cancellation.set()
        await shutdown

        assert consumer.cancel_calls == ["consumer-tag"]
        assert_state(runtime, WorkerDispatchRuntimeState.STOPPED)

    asyncio.run(scenario())


def test_registration_failure_is_shared_and_can_be_retried() -> None:
    async def scenario() -> None:
        consumer = Consumer()
        consumer.registration_error = BrokerConsumerUnavailable()
        runtime = WorkerDispatchRuntime(consumer, _completed_handler)
        starts = [asyncio.create_task(runtime.start()) for _ in range(2)]
        await consumer.registration_started.wait()
        consumer.release_registration.set()

        outcomes = await asyncio.gather(*starts, return_exceptions=True)
        assert all(isinstance(item, BrokerConsumerUnavailable) for item in outcomes)
        assert consumer.consume_calls == 1
        assert runtime.state is WorkerDispatchRuntimeState.NEW

        consumer.registration_error = None
        assert await runtime.start() == "consumer-tag"
        assert consumer.consume_calls == 2

    asyncio.run(scenario())


def test_registration_failure_during_shutdown_leaves_no_subscription() -> None:
    async def scenario() -> None:
        consumer = Consumer()
        consumer.registration_error = BrokerConsumerUnavailable()
        runtime = WorkerDispatchRuntime(consumer, _completed_handler)
        start = asyncio.create_task(runtime.start())
        await consumer.registration_started.wait()
        shutdown = asyncio.create_task(runtime.shutdown())
        consumer.release_registration.set()

        with pytest.raises(BrokerConsumerUnavailable):
            await start
        await shutdown
        assert runtime.state is WorkerDispatchRuntimeState.STOPPED
        assert consumer.cancel_calls == []

    asyncio.run(scenario())


def test_cancelled_registration_during_shutdown_is_failed_no_subscription() -> None:
    async def scenario() -> None:
        consumer = Consumer()
        consumer.registration_error = asyncio.CancelledError()
        handler_calls = 0

        async def handler(control: Any) -> None:
            nonlocal handler_calls
            del control
            handler_calls += 1

        runtime = WorkerDispatchRuntime(consumer, handler)
        start = asyncio.create_task(runtime.start())
        await consumer.registration_started.wait()
        shutdown = asyncio.create_task(runtime.shutdown())
        await runtime._stopping.wait()

        consumer.release_registration.set()
        with pytest.raises(asyncio.CancelledError):
            await start
        await shutdown

        assert_state(runtime, WorkerDispatchRuntimeState.STOPPED)
        assert runtime._consumer_tag is None
        assert consumer.cancel_calls == []
        assert handler_calls == 0
        with pytest.raises(WorkerDispatchRuntimeStopping):
            await runtime.start()

    asyncio.run(scenario())


def test_concurrent_shutdowns_share_cancellation_and_drain_in_flight() -> None:
    async def scenario() -> None:
        consumer = Consumer()
        handler_started = asyncio.Event()
        release_handler = asyncio.Event()
        handler_cancelled = False

        async def handler(control: Any) -> None:
            nonlocal handler_cancelled
            del control
            handler_started.set()
            try:
                await release_handler.wait()
            except asyncio.CancelledError:
                handler_cancelled = True
                raise

        runtime = await start_runtime(consumer, handler)
        delivery = asyncio.create_task(consumer.handler(object()))
        await handler_started.wait()
        shutdowns = [asyncio.create_task(runtime.shutdown()) for _ in range(3)]
        await consumer.cancellation_started.wait()
        assert consumer.cancel_calls == ["consumer-tag"]

        consumer.release_cancellation.set()
        assert runtime.in_flight == 1
        assert not any(item.done() for item in shutdowns)
        release_handler.set()
        await delivery
        await asyncio.gather(*shutdowns)

        assert not handler_cancelled
        assert runtime.in_flight == 0
        assert runtime.state is WorkerDispatchRuntimeState.STOPPED
        await runtime.shutdown()
        assert consumer.cancel_calls == ["consumer-tag"]

    asyncio.run(scenario())


def test_broker_cancellation_failure_stays_stopping_and_can_retry() -> None:
    async def scenario() -> None:
        consumer = Consumer()
        runtime = await start_runtime(consumer)
        consumer.cancellation_errors.append(BrokerConsumerUnavailable())

        first = asyncio.create_task(runtime.shutdown())
        with pytest.raises(BrokerConsumerUnavailable):
            await first
        assert_state(runtime, WorkerDispatchRuntimeState.STOPPING)
        with pytest.raises(WorkerDispatchRuntimeStopping):
            await runtime.start()

        consumer.release_cancellation.set()
        await runtime.shutdown()
        assert consumer.cancel_calls == ["consumer-tag", "consumer-tag"]
        assert_state(runtime, WorkerDispatchRuntimeState.STOPPED)

    asyncio.run(scenario())


def test_cancelling_one_shutdown_waiter_does_not_cancel_shared_drain() -> None:
    async def scenario() -> None:
        consumer = Consumer()
        handler_started = asyncio.Event()
        release_handler = asyncio.Event()

        async def handler(control: Any) -> None:
            del control
            handler_started.set()
            await release_handler.wait()

        runtime = await start_runtime(consumer, handler)
        delivery = asyncio.create_task(consumer.handler(object()))
        await handler_started.wait()
        cancelled_waiter = asyncio.create_task(runtime.shutdown())
        surviving_waiter = asyncio.create_task(runtime.shutdown())
        await consumer.cancellation_started.wait()

        cancelled_waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled_waiter
        assert not delivery.cancelled()
        assert not surviving_waiter.done()

        consumer.release_cancellation.set()
        release_handler.set()
        await delivery
        await surviving_waiter
        assert runtime.state is WorkerDispatchRuntimeState.STOPPED

    asyncio.run(scenario())


def test_callback_failure_still_releases_drain_accounting() -> None:
    async def scenario() -> None:
        consumer = Consumer()

        async def handler(control: Any) -> None:
            del control
            raise RuntimeError("handler failed")

        runtime = await start_runtime(consumer, handler)
        with pytest.raises(RuntimeError, match="handler failed"):
            await consumer.handler(object())
        assert runtime.in_flight == 0
        consumer.release_cancellation.set()
        await runtime.shutdown()

    asyncio.run(scenario())


def test_callback_admitted_while_cancellation_is_unconfirmed_is_drained() -> None:
    async def scenario() -> None:
        consumer = Consumer()
        admitted = asyncio.Event()
        release_handler = asyncio.Event()

        async def handler(control: Any) -> None:
            del control
            admitted.set()
            await release_handler.wait()

        runtime = await start_runtime(consumer, handler)
        shutdown = asyncio.create_task(runtime.shutdown())
        await consumer.cancellation_started.wait()
        delivery = asyncio.create_task(consumer.handler(object()))
        await admitted.wait()
        assert runtime.in_flight == 1

        consumer.release_cancellation.set()
        assert not shutdown.done()
        release_handler.set()
        await delivery
        await shutdown

        with pytest.raises(WorkerDispatchRuntimeInvariantError):
            await consumer.handler(object())

    asyncio.run(scenario())
