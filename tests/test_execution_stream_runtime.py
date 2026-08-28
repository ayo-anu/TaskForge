"""Deterministic execution-stream runtime concurrency tests."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID, uuid4

from fastapi import WebSocket
from pydantic import SecretStr

from taskforge.api.execution_stream_runtime import (
    EXECUTION_EVENT_WAKEUP_CHANNEL,
    SERVICE_FAILURE,
    SLOW_CONSUMER,
    ExecutionStreamCapacityExceeded,
    ExecutionStreamPrincipalCapacityExceeded,
    ExecutionStreamRuntime,
    ExecutionStreamSubscription,
    ExecutionStreamUnavailable,
    SubscriptionState,
    TerminationKind,
)
from taskforge.runs.domain import StoredWorkflowRunExecutionEvent
from taskforge.settings import Settings


def event(run_id: UUID, cursor: int) -> StoredWorkflowRunExecutionEvent:
    return StoredWorkflowRunExecutionEvent(
        uuid4(),
        run_id,
        cursor,
        None,
        "workflow_run.status_changed",
        {"previous_status": "pending", "status": "running"},
        datetime.now(UTC),
    )


class Repository:
    def __init__(self, events: dict[UUID, tuple[StoredWorkflowRunExecutionEvent, ...]]):
        self.events = events
        self.calls: list[tuple[UUID, int, int]] = []

    async def list_after(
        self, workflow_run_id: UUID, after_cursor: int, limit: int
    ) -> tuple[StoredWorkflowRunExecutionEvent, ...]:
        self.calls.append((workflow_run_id, after_cursor, limit))
        return tuple(
            item
            for item in self.events.get(workflow_run_id, ())
            if item.cursor > after_cursor
        )[:limit]

    async def inspect_resume_cursor(
        self, workflow_run_id: UUID, cursor: int | None
    ) -> Any:
        raise AssertionError("not used")


class Listener:
    def __init__(self) -> None:
        self.callback: Any = None
        self.termination_callback: Any = None
        self.closed = False

    async def add_listener(self, channel: str, callback: Any) -> None:
        assert channel == EXECUTION_EVENT_WAKEUP_CHANNEL
        self.callback = callback

    def add_termination_listener(self, callback: Any) -> None:
        self.termination_callback = callback

    async def close(self) -> None:
        self.closed = True


class Socket:
    def __init__(
        self,
        *,
        block_close: asyncio.Event | None = None,
        block_send: asyncio.Event | None = None,
        send_error: Exception | None = None,
    ) -> None:
        self.sent: list[dict[str, Any]] = []
        self.close_started = asyncio.Event()
        self.block_close = block_close
        self.block_send = block_send
        self.send_error = send_error
        self.send_started = asyncio.Event()
        self.sent_event = asyncio.Event()
        self.receive_block = asyncio.Event()
        self.close_calls: list[tuple[int, str]] = []
        self.active_sends = 0
        self.maximum_active_sends = 0

    async def send_json(self, message: dict[str, Any]) -> None:
        self.active_sends += 1
        self.maximum_active_sends = max(self.maximum_active_sends, self.active_sends)
        self.send_started.set()
        try:
            if self.block_send is not None:
                await self.block_send.wait()
            if self.send_error is not None:
                raise self.send_error
            self.sent.append(message)
            self.sent_event.set()
            await asyncio.sleep(0)
        finally:
            self.active_sends -= 1

    async def receive(self) -> dict[str, Any]:
        await self.receive_block.wait()
        return {"type": "websocket.disconnect"}

    async def close(self, *, code: int, reason: str) -> None:
        self.close_calls.append((code, reason))
        self.close_started.set()
        if self.block_close is not None:
            await self.block_close.wait()


def settings(*, queue_size: int = 1, max_connections: int = 500) -> Settings:
    return Settings(
        postgres_password=SecretStr("postgres-test-secret"),
        rabbitmq_password=SecretStr("rabbitmq-test-secret"),
        execution_stream_queue_size=queue_size,
        execution_stream_max_connections=max_connections,
    )


def serialized(item: StoredWorkflowRunExecutionEvent) -> dict[str, Any]:
    return {"cursor": item.cursor}


async def started_runtime(
    repository: Repository, *, queue_size: int = 1, max_connections: int = 500
) -> tuple[ExecutionStreamRuntime, Listener]:
    listener = Listener()

    async def connect() -> Listener:
        return listener

    runtime = ExecutionStreamRuntime(
        settings(queue_size=queue_size, max_connections=max_connections),
        repository,
        serialized,
        listener_factory=connect,
    )
    await runtime.start()
    return runtime, listener


async def wait_until_listener_ready(runtime: ExecutionStreamRuntime) -> None:
    async with asyncio.timeout(2):
        while not runtime.listener_ready:
            await asyncio.sleep(0)


async def wait_until_delivered(
    subscription: ExecutionStreamSubscription, expected_cursor: int
) -> None:
    async with asyncio.timeout(2):
        while subscription.last_delivered_cursor != expected_cursor:
            await asyncio.sleep(0)


async def wait_until_supervised(subscription: ExecutionStreamSubscription) -> None:
    async with asyncio.timeout(2):
        while subscription.supervisor_task is None:
            await asyncio.sleep(0)


def test_shared_pass_cursor_stays_stable_when_lagging_client_overflows() -> None:
    async def exercise() -> None:
        run_id = uuid4()
        repository = Repository(
            {run_id: tuple(event(run_id, value) for value in range(3, 9))}
        )
        runtime, _ = await started_runtime(repository)
        try:
            a = await runtime.open_subscription(
                cast(WebSocket, Socket()), run_id, 2, None, principal_id=uuid4()
            )
            b = await runtime.open_subscription(
                cast(WebSocket, Socket()), run_id, 5, None, principal_id=uuid4()
            )
            a.state = SubscriptionState.LIVE
            b.state = SubscriptionState.LIVE
            a.queue.put_nowait(event(run_id, 2))
            b.queue = asyncio.Queue(maxsize=10)

            runtime._mark_dirty(run_id)
            task = runtime._groups[run_id].reconcile_task
            assert task is not None
            await task

            assert a.state is SubscriptionState.TERMINATING
            assert a.termination.result().kind is TerminationKind.SLOW_CONSUMER
            assert [repository_call[1] for repository_call in repository.calls] == [
                2,
                8,
            ]
            assert [b.queue.get_nowait().cursor for _ in range(3)] == [6, 7, 8]
            assert b.last_queued_cursor == 8
        finally:
            await runtime.close()

    asyncio.run(exercise())


def test_blocked_slow_socket_cleanup_never_blocks_healthy_fanout() -> None:
    async def exercise() -> None:
        run_a, run_b = uuid4(), uuid4()
        repository = Repository(
            {
                run_a: tuple(event(run_a, value) for value in range(1, 4)),
                run_b: (event(run_b, 1),),
            }
        )
        runtime, _ = await started_runtime(repository)
        release_close = asyncio.Event()
        release_send = asyncio.Event()
        slow_socket = Socket(block_close=release_close, block_send=release_send)
        slow = await runtime.open_subscription(
            cast(WebSocket, slow_socket), run_a, 0, None, principal_id=uuid4()
        )
        healthy = await runtime.open_subscription(
            cast(WebSocket, Socket()), run_a, 0, None, principal_id=uuid4()
        )
        other = await runtime.open_subscription(
            cast(WebSocket, Socket()), run_b, 0, None, principal_id=uuid4()
        )
        slow.state = healthy.state = other.state = SubscriptionState.LIVE
        slow.queue.put_nowait(event(run_a, 99))
        healthy.queue = asyncio.Queue(maxsize=10)
        other.queue = asyncio.Queue(maxsize=10)
        supervisor = asyncio.create_task(runtime.serve(slow))
        await slow_socket.send_started.wait()
        slow.queue.put_nowait(event(run_a, 98))

        runtime._mark_dirty(run_a)
        runtime._mark_dirty(run_b)
        run_a_task = runtime._groups[run_a].reconcile_task
        run_b_task = runtime._groups[run_b].reconcile_task
        assert run_a_task is not None and run_b_task is not None
        await asyncio.gather(run_a_task, run_b_task)
        await slow_socket.close_started.wait()

        assert not supervisor.done()
        assert [healthy.queue.get_nowait().cursor for _ in range(3)] == [1, 2, 3]
        assert other.queue.get_nowait().cursor == 1
        assert [(run_id, cursor) for run_id, cursor, _ in repository.calls] == [
            (run_a, 0),
            (run_a, 3),
            (run_b, 0),
            (run_b, 1),
        ]
        release_close.set()
        await supervisor
        assert slow_socket.close_calls == [(1008, "slow consumer")]
        await runtime.close()

    asyncio.run(exercise())


def test_generation_change_at_empty_catchup_read_cannot_create_handoff_gap() -> None:
    class HandoffRepository(Repository):
        def __init__(self, run_id: UUID) -> None:
            super().__init__({run_id: ()})
            self.reaching_empty = asyncio.Event()
            self.release_empty = asyncio.Event()
            self.first_read = True

        async def list_after(
            self, workflow_run_id: UUID, after_cursor: int, limit: int
        ) -> tuple[StoredWorkflowRunExecutionEvent, ...]:
            if self.first_read:
                self.first_read = False
                self.calls.append((workflow_run_id, after_cursor, limit))
                snapshot = self.events[workflow_run_id]
                self.reaching_empty.set()
                await self.release_empty.wait()
                return snapshot
            return await super().list_after(workflow_run_id, after_cursor, limit)

    async def exercise() -> None:
        run_id = uuid4()
        repository = HandoffRepository(run_id)
        runtime, _ = await started_runtime(repository, queue_size=4)
        socket = Socket()
        subscription = await runtime.open_subscription(
            cast(WebSocket, socket), run_id, 0, None, principal_id=uuid4()
        )
        supervisor = asyncio.create_task(runtime.serve(subscription))

        await repository.reaching_empty.wait()
        repository.events[run_id] = (event(run_id, 1),)
        runtime._mark_dirty(run_id)
        repository.release_empty.set()
        await socket.sent_event.wait()
        await wait_until_delivered(subscription, 1)

        assert [item["cursor"] for item in socket.sent] == [1]
        assert subscription.last_delivered_cursor == 1
        assert subscription.state is SubscriptionState.LIVE
        socket.receive_block.set()
        await supervisor
        await runtime.close()

    asyncio.run(exercise())


def test_notification_is_run_scoped_and_capacity_is_bounded() -> None:
    async def exercise() -> None:
        run_id, unrelated = uuid4(), uuid4()
        repository = Repository({})
        runtime, listener = await started_runtime(repository, max_connections=1)
        try:
            subscription = await runtime.open_subscription(
                cast(WebSocket, Socket()), run_id, 0, None, principal_id=uuid4()
            )
            assert listener.callback is not None
            listener.callback(
                listener,
                1,
                EXECUTION_EVENT_WAKEUP_CHANNEL,
                '{"workflow_run_id":"' + str(unrelated) + '"}',
            )
            assert runtime._groups[run_id].generation == 0
            listener.callback(
                listener,
                1,
                EXECUTION_EVENT_WAKEUP_CHANNEL,
                '{"workflow_run_id":"' + str(run_id) + '"}',
            )
            assert runtime._groups[run_id].generation == 1
            try:
                await runtime.open_subscription(
                    cast(WebSocket, Socket()), run_id, 0, None, principal_id=uuid4()
                )
            except ExecutionStreamCapacityExceeded:
                pass
            else:
                raise AssertionError("capacity must be bounded")
            await runtime.abort_subscription(subscription)
        finally:
            await runtime.close()

    asyncio.run(exercise())


def test_sender_is_sequential_and_expiry_uses_supervisor_cleanup() -> None:
    async def exercise() -> None:
        run_id = uuid4()
        repository = Repository({})
        runtime, _ = await started_runtime(repository, queue_size=4)
        socket = Socket()
        deadline = asyncio.get_running_loop().time()
        subscription = await runtime.open_subscription(
            cast(WebSocket, socket), run_id, 0, deadline, principal_id=uuid4()
        )
        subscription.queue.put_nowait(event(run_id, 1))
        subscription.queue.put_nowait(event(run_id, 2))
        await runtime.serve(subscription)

        assert socket.maximum_active_sends == 1
        assert socket.close_calls == [(1008, "session expired")]
        assert runtime.active_connections == 0
        await runtime.close()

    asyncio.run(exercise())


def test_send_failure_uses_one_service_failure_cleanup() -> None:
    async def exercise() -> None:
        run_id = uuid4()
        runtime, _ = await started_runtime(Repository({}), queue_size=2)
        socket = Socket(send_error=RuntimeError("transport failed"))
        subscription = await runtime.open_subscription(
            cast(WebSocket, socket), run_id, 0, None, principal_id=uuid4()
        )
        subscription.queue.put_nowait(event(run_id, 1))

        await runtime.serve(subscription)

        assert socket.close_calls == [(1011, "service unavailable")]
        assert subscription.last_delivered_cursor == 0
        assert runtime.active_connections == 0
        await runtime.close()

    asyncio.run(exercise())


def test_normal_disconnect_cleans_up_without_server_close() -> None:
    async def exercise() -> None:
        run_id = uuid4()
        runtime, _ = await started_runtime(Repository({}))
        socket = Socket()
        socket.receive_block.set()
        subscription = await runtime.open_subscription(
            cast(WebSocket, socket), run_id, 0, None, principal_id=uuid4()
        )

        await runtime.serve(subscription)

        assert socket.close_calls == []
        assert runtime.active_connections == 0
        assert subscription.cleanup_complete is True
        await runtime.close()

    asyncio.run(exercise())


def test_shutdown_closes_active_subscription_once_for_service_restart() -> None:
    async def exercise() -> None:
        run_id = uuid4()
        runtime, _ = await started_runtime(Repository({}))
        socket = Socket()
        subscription = await runtime.open_subscription(
            cast(WebSocket, socket), run_id, 0, None, principal_id=uuid4()
        )
        supervisor = asyncio.create_task(runtime.serve(subscription))
        await wait_until_supervised(subscription)

        await runtime.close()
        await supervisor

        assert socket.close_calls == [(1012, "service restart")]
        assert runtime.active_connections == 0
        assert subscription.cleanup_complete is True

    asyncio.run(exercise())


def test_competing_termination_signals_release_capacity_and_close_once() -> None:
    async def exercise() -> None:
        run_id = uuid4()
        runtime, _ = await started_runtime(Repository({}), max_connections=1)
        socket = Socket()
        subscription = await runtime.open_subscription(
            cast(WebSocket, socket), run_id, 0, None, principal_id=uuid4()
        )
        runtime._signal_termination(subscription, SLOW_CONSUMER)
        runtime._signal_termination(subscription, SERVICE_FAILURE)

        await runtime.serve(subscription)
        await runtime.abort_subscription(subscription)

        assert subscription.termination.result().kind is TerminationKind.SLOW_CONSUMER
        assert socket.close_calls == [(1008, "slow consumer")]
        assert runtime.active_connections == 0
        replacement = await runtime.open_subscription(
            cast(WebSocket, Socket()), run_id, 0, None, principal_id=uuid4()
        )
        assert runtime.active_connections == 1
        await runtime.abort_subscription(replacement)
        await runtime.close()

    asyncio.run(exercise())


def test_initial_listener_failure_rejects_until_reconnect_is_usable() -> None:
    async def exercise() -> None:
        repository = Repository({})
        connected = asyncio.Event()
        listener = Listener()
        attempts = 0

        async def connect() -> Listener:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise OSError("listener unavailable")
            connected.set()
            return listener

        runtime = ExecutionStreamRuntime(
            settings(), repository, serialized, listener_factory=connect
        )
        await runtime.start()
        assert runtime.listener_ready is False
        try:
            await runtime.open_subscription(
                cast(WebSocket, Socket()), uuid4(), 0, None, principal_id=uuid4()
            )
        except ExecutionStreamUnavailable:
            pass
        else:
            raise AssertionError("registration must fail while LISTEN is unavailable")

        await connected.wait()
        await wait_until_listener_ready(runtime)
        subscription = await runtime.open_subscription(
            cast(WebSocket, Socket()), uuid4(), 0, None, principal_id=uuid4()
        )
        assert runtime.active_connections == 1
        await runtime.abort_subscription(subscription)
        await runtime.close()

    asyncio.run(exercise())


def test_listener_availability_callback_is_transition_only_and_failure_isolated() -> (
    None
):
    async def exercise() -> None:
        listener = Listener()
        states: list[bool] = []

        def observe(available: bool) -> None:
            states.append(available)
            if available:
                raise RuntimeError("telemetry sentinel secret")

        async def connect() -> Listener:
            return listener

        runtime = ExecutionStreamRuntime(
            settings(),
            Repository({}),
            serialized,
            listener_factory=connect,
            availability_changed=observe,
        )
        await runtime.start()
        assert runtime.listener_ready is True
        runtime._set_listener_ready(True)
        await runtime.close()
        runtime._set_listener_ready(False)
        assert states == [True, False]

    asyncio.run(exercise())


def test_same_run_clients_share_coalesced_duplicate_wakeup_reconciliation() -> None:
    async def exercise() -> None:
        run_id = uuid4()
        repository = Repository({run_id: (event(run_id, 1),)})
        runtime, listener = await started_runtime(repository, queue_size=4)
        first = await runtime.open_subscription(
            cast(WebSocket, Socket()), run_id, 0, None, principal_id=uuid4()
        )
        second = await runtime.open_subscription(
            cast(WebSocket, Socket()), run_id, 0, None, principal_id=uuid4()
        )
        first.state = second.state = SubscriptionState.LIVE
        assert listener.callback is not None
        payload = '{"workflow_run_id":"' + str(run_id) + '"}'
        listener.callback(listener, 1, EXECUTION_EVENT_WAKEUP_CHANNEL, payload)
        listener.callback(listener, 1, EXECUTION_EVENT_WAKEUP_CHANNEL, payload)
        task = runtime._groups[run_id].reconcile_task
        assert task is not None
        await task

        assert first.queue.get_nowait().cursor == 1
        assert second.queue.get_nowait().cursor == 1
        assert first.queue.empty() and second.queue.empty()
        assert [cursor for _, cursor, _ in repository.calls] == [0, 1]
        await runtime.abort_subscription(first)
        await runtime.abort_subscription(second)
        await runtime.close()

    asyncio.run(exercise())


def test_process_local_principal_capacity_is_independent_and_released() -> None:
    async def exercise() -> None:
        runtime, _ = await started_runtime(Repository({}), max_connections=20)
        principal_id = uuid4()
        subscriptions = [
            await runtime.open_subscription(
                cast(WebSocket, Socket()),
                uuid4(),
                0,
                None,
                principal_id=principal_id,
            )
            for _ in range(5)
        ]
        try:
            await runtime.open_subscription(
                cast(WebSocket, Socket()),
                uuid4(),
                0,
                None,
                principal_id=principal_id,
            )
        except ExecutionStreamPrincipalCapacityExceeded:
            pass
        else:
            raise AssertionError("principal connection cap was not enforced")
        independent = await runtime.open_subscription(
            cast(WebSocket, Socket()),
            uuid4(),
            0,
            None,
            principal_id=uuid4(),
        )
        await runtime.abort_subscription(subscriptions.pop())
        replacement = await runtime.open_subscription(
            cast(WebSocket, Socket()),
            uuid4(),
            0,
            None,
            principal_id=principal_id,
        )
        for subscription in (*subscriptions, independent, replacement):
            await runtime.abort_subscription(subscription)
        await runtime.close()

    asyncio.run(exercise())
