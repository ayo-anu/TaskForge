"""Process-local live delivery for durable workflow-run execution events."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol, cast
from uuid import UUID, uuid4

import asyncpg
from fastapi import WebSocket, WebSocketDisconnect, status

from taskforge.runs.domain import StoredWorkflowRunExecutionEvent
from taskforge.runs.persistence_ports import (
    WorkflowRunExecutionEventInvariantViolation,
    WorkflowRunExecutionEventPersistenceUnavailable,
    WorkflowRunExecutionEventRepository,
)
from taskforge.settings import Settings

EXECUTION_EVENT_WAKEUP_CHANNEL = "taskforge_workflow_run_execution_events"
EXECUTION_EVENT_PAGE_SIZE = 100
INITIAL_RECONNECT_DELAY_SECONDS = 0.25
SERVICE_FAILURE_REASON = "service unavailable"
SERVICE_RESTART_REASON = "service restart"
SLOW_CONSUMER_REASON = "slow consumer"
SESSION_EXPIRED_REASON = "session expired"

logger = logging.getLogger(__name__)


class ExecutionStreamUnavailable(Exception):
    """Live delivery is not currently available on this API process."""


class ExecutionStreamCapacityExceeded(Exception):
    """The bounded local subscription capacity has been reached."""


class SubscriptionState(StrEnum):
    CATCHING_UP = "catching_up"
    LIVE = "live"
    TERMINATING = "terminating"
    CLOSED = "closed"


class TerminationKind(StrEnum):
    SLOW_CONSUMER = "slow_consumer"
    SESSION_EXPIRED = "session_expired"
    SERVICE_FAILURE = "service_failure"
    SERVICE_RESTART = "service_restart"


@dataclass(frozen=True)
class TerminationDirective:
    kind: TerminationKind
    code: int
    reason: str


SLOW_CONSUMER = TerminationDirective(
    TerminationKind.SLOW_CONSUMER,
    status.WS_1008_POLICY_VIOLATION,
    SLOW_CONSUMER_REASON,
)
SESSION_EXPIRED = TerminationDirective(
    TerminationKind.SESSION_EXPIRED,
    status.WS_1008_POLICY_VIOLATION,
    SESSION_EXPIRED_REASON,
)
SERVICE_FAILURE = TerminationDirective(
    TerminationKind.SERVICE_FAILURE,
    status.WS_1011_INTERNAL_ERROR,
    SERVICE_FAILURE_REASON,
)
SERVICE_RESTART = TerminationDirective(
    TerminationKind.SERVICE_RESTART,
    status.WS_1012_SERVICE_RESTART,
    SERVICE_RESTART_REASON,
)


class ListenerConnection(Protocol):
    async def add_listener(
        self, channel: str, callback: Callable[..., None]
    ) -> None: ...

    def add_termination_listener(self, callback: Callable[..., None]) -> None: ...

    async def close(self) -> None: ...


ListenerFactory = Callable[[], Any]


@dataclass(eq=False)
class ExecutionStreamSubscription:
    id: UUID
    workflow_run_id: UUID
    websocket: WebSocket
    queue: asyncio.Queue[StoredWorkflowRunExecutionEvent]
    last_queued_cursor: int
    last_delivered_cursor: int
    expiry_deadline: float | None
    observed_generation: int
    termination: asyncio.Future[TerminationDirective]
    state: SubscriptionState = SubscriptionState.CATCHING_UP
    supervisor_task: asyncio.Task[None] | None = None
    cleanup_complete: bool = False


@dataclass
class _RunGroup:
    workflow_run_id: UUID
    subscriptions: dict[UUID, ExecutionStreamSubscription] = field(default_factory=dict)
    generation: int = 0
    reconcile_task: asyncio.Task[None] | None = None


class ExecutionStreamRuntime:
    """Own one listener and bounded local subscriptions for an API process."""

    def __init__(
        self,
        settings: Settings,
        repository: WorkflowRunExecutionEventRepository,
        serializer: Callable[[StoredWorkflowRunExecutionEvent], dict[str, Any]],
        *,
        listener_factory: ListenerFactory | None = None,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._serializer = serializer
        self._listener_factory = listener_factory or self._connect_listener
        self._groups: dict[UUID, _RunGroup] = {}
        self._active_connections = 0
        self._listener_ready = False
        self._stopping = False
        self._listener_task: asyncio.Task[None] | None = None
        self._listener_connection: ListenerConnection | None = None
        self._first_attempt_complete = asyncio.Event()

    @property
    def listener_ready(self) -> bool:
        return self._listener_ready

    @property
    def active_connections(self) -> int:
        return self._active_connections

    async def start(self) -> None:
        if self._listener_task is not None:
            return
        self._listener_task = asyncio.create_task(
            self._supervise_listener(), name="execution-event-listener"
        )
        await self._first_attempt_complete.wait()

    async def open_subscription(
        self,
        websocket: WebSocket,
        workflow_run_id: UUID,
        baseline_cursor: int,
        expiry_deadline: float | None,
    ) -> ExecutionStreamSubscription:
        if self._stopping or not self._listener_ready:
            raise ExecutionStreamUnavailable
        if self._active_connections >= self._settings.execution_stream_max_connections:
            raise ExecutionStreamCapacityExceeded
        group = self._groups.setdefault(workflow_run_id, _RunGroup(workflow_run_id))
        loop = asyncio.get_running_loop()
        subscription = ExecutionStreamSubscription(
            id=uuid4(),
            workflow_run_id=workflow_run_id,
            websocket=websocket,
            queue=asyncio.Queue(maxsize=self._settings.execution_stream_queue_size),
            last_queued_cursor=baseline_cursor,
            last_delivered_cursor=baseline_cursor,
            expiry_deadline=expiry_deadline,
            observed_generation=group.generation,
            termination=loop.create_future(),
        )
        group.subscriptions[subscription.id] = subscription
        self._active_connections += 1
        return subscription

    async def abort_subscription(
        self, subscription: ExecutionStreamSubscription
    ) -> None:
        self._finalize_subscription(subscription)

    async def serve(self, subscription: ExecutionStreamSubscription) -> None:
        """Supervise catch-up, sending, receiving, expiry, and final cleanup."""
        subscription.supervisor_task = asyncio.current_task()
        sender = asyncio.create_task(self._send_events(subscription))
        receiver = asyncio.create_task(self._receive_until_disconnect(subscription))
        catch_up = asyncio.create_task(self._catch_up_and_go_live(subscription))
        expiry = (
            asyncio.create_task(self._expire_at_deadline(subscription))
            if subscription.expiry_deadline is not None
            else None
        )
        tasks: set[asyncio.Future[Any]] = {sender, receiver, catch_up}
        if expiry is not None:
            tasks.add(expiry)
        termination_wait = asyncio.ensure_future(
            asyncio.shield(subscription.termination)
        )
        tasks.add(termination_wait)
        directive: TerminationDirective | None = None
        client_disconnected = False
        try:
            while True:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                if termination_wait in done:
                    directive = termination_wait.result()
                    break
                if receiver in done:
                    try:
                        receiver.result()
                    except WebSocketDisconnect:
                        pass
                    client_disconnected = True
                    break
                if sender in done:
                    try:
                        sender.result()
                    except WebSocketDisconnect:
                        client_disconnected = True
                    except (RuntimeError, TypeError, ValueError):
                        directive = SERVICE_FAILURE
                    break
                if expiry is not None and expiry in done:
                    directive = SESSION_EXPIRED
                    break
                if catch_up in done:
                    tasks.remove(catch_up)
                    try:
                        catch_up.result()
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        directive = SERVICE_FAILURE
                        break
        finally:
            for task in tasks:
                if task is not asyncio.current_task() and not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._discard_queue(subscription)
            self._finalize_subscription(subscription)
            if directive is not None and not client_disconnected:
                try:
                    await subscription.websocket.close(
                        code=directive.code, reason=directive.reason
                    )
                except (RuntimeError, WebSocketDisconnect):
                    pass

    async def close(self) -> None:
        if self._stopping:
            return
        self._stopping = True
        self._listener_ready = False
        subscriptions = [
            subscription
            for group in self._groups.values()
            for subscription in group.subscriptions.values()
        ]
        for subscription in subscriptions:
            self._signal_termination(subscription, SERVICE_RESTART)
        supervisors = [
            subscription.supervisor_task
            for subscription in subscriptions
            if subscription.supervisor_task is not None
        ]
        if supervisors:
            try:
                async with asyncio.timeout(2.0):
                    await asyncio.gather(*supervisors, return_exceptions=True)
            except TimeoutError:
                for supervisor in supervisors:
                    if not supervisor.done():
                        supervisor.cancel()
                await asyncio.gather(*supervisors, return_exceptions=True)
        for group in tuple(self._groups.values()):
            if group.reconcile_task is not None:
                group.reconcile_task.cancel()
        await asyncio.gather(
            *(
                group.reconcile_task
                for group in self._groups.values()
                if group.reconcile_task is not None
            ),
            return_exceptions=True,
        )
        if self._listener_task is not None:
            self._listener_task.cancel()
        if self._listener_connection is not None:
            try:
                await self._listener_connection.close()
            except Exception:
                pass
        if self._listener_task is not None:
            await asyncio.gather(self._listener_task, return_exceptions=True)

    async def _supervise_listener(self) -> None:
        delay = INITIAL_RECONNECT_DELAY_SECONDS
        while not self._stopping:
            terminated = asyncio.Event()
            connection: ListenerConnection | None = None
            try:
                connection = await self._listener_factory()
                await connection.add_listener(
                    EXECUTION_EVENT_WAKEUP_CHANNEL, self._notification_received
                )
                connection.add_termination_listener(
                    lambda _connection, signal=terminated: signal.set()
                )
                self._listener_connection = connection
                self._listener_ready = True
                self._first_attempt_complete.set()
                delay = INITIAL_RECONNECT_DELAY_SECONDS
                logger.info("execution event listener connected")
                for run_id in tuple(self._groups):
                    self._mark_dirty(run_id)
                await terminated.wait()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("execution event listener unavailable", exc_info=True)
            finally:
                self._listener_ready = False
                self._first_attempt_complete.set()
                if connection is not None:
                    try:
                        await connection.close()
                    except Exception:
                        pass
                if self._listener_connection is connection:
                    self._listener_connection = None
            if not self._stopping:
                await asyncio.sleep(delay)
                delay = min(
                    delay * 2,
                    self._settings.execution_stream_listener_reconnect_max_seconds,
                )

    async def _connect_listener(self) -> ListenerConnection:
        return cast(
            ListenerConnection,
            await asyncpg.connect(
                host=self._settings.postgres_host,
                port=self._settings.postgres_port,
                database=self._settings.postgres_database,
                user=self._settings.postgres_user,
                password=self._settings.postgres_password.get_secret_value(),
                timeout=self._settings.database_pool_timeout_seconds,
            ),
        )

    def _notification_received(
        self,
        _connection: object,
        _process_id: int,
        channel: str,
        payload: str,
    ) -> None:
        if self._stopping or channel != EXECUTION_EVENT_WAKEUP_CHANNEL:
            return
        try:
            value = json.loads(payload)
            if not isinstance(value, dict) or set(value) != {"workflow_run_id"}:
                return
            run_id = UUID(value["workflow_run_id"])
        except (TypeError, ValueError, json.JSONDecodeError):
            logger.warning("invalid execution event wake-up payload")
            return
        if run_id in self._groups:
            self._mark_dirty(run_id)

    def _mark_dirty(self, workflow_run_id: UUID) -> None:
        group = self._groups.get(workflow_run_id)
        if group is None or self._stopping:
            return
        group.generation += 1
        if group.reconcile_task is None or group.reconcile_task.done():
            group.reconcile_task = asyncio.create_task(
                self._reconcile_group(group),
                name=f"execution-event-reconcile-{workflow_run_id}",
            )

    async def _reconcile_group(self, group: _RunGroup) -> None:
        retry_delay = INITIAL_RECONNECT_DELAY_SECONDS
        while not self._stopping:
            relevant = tuple(
                subscription
                for subscription in group.subscriptions.values()
                if subscription.state is SubscriptionState.LIVE
            )
            if not relevant:
                return
            observed_generation = group.generation
            read_cursor = min(item.last_queued_cursor for item in relevant)
            try:
                while True:
                    page = await self._repository.list_after(
                        group.workflow_run_id,
                        read_cursor,
                        EXECUTION_EVENT_PAGE_SIZE,
                    )
                    if not page:
                        break
                    self._validate_page(group.workflow_run_id, read_cursor, page)
                    for event in page:
                        for subscription in tuple(group.subscriptions.values()):
                            if (
                                subscription.state is not SubscriptionState.LIVE
                                or subscription.last_queued_cursor >= event.cursor
                            ):
                                continue
                            try:
                                subscription.queue.put_nowait(event)
                            except asyncio.QueueFull:
                                self._signal_termination(subscription, SLOW_CONSUMER)
                                continue
                            subscription.last_queued_cursor = event.cursor
                    read_cursor = page[-1].cursor
                retry_delay = INITIAL_RECONNECT_DELAY_SECONDS
            except WorkflowRunExecutionEventPersistenceUnavailable:
                logger.warning("execution event reconciliation unavailable")
                await asyncio.sleep(retry_delay)
                retry_delay = min(
                    retry_delay * 2,
                    self._settings.execution_stream_listener_reconnect_max_seconds,
                )
                continue
            except (WorkflowRunExecutionEventInvariantViolation, TypeError, ValueError):
                logger.error("execution event reconciliation invariant failure")
                for subscription in tuple(group.subscriptions.values()):
                    self._signal_termination(subscription, SERVICE_FAILURE)
                return
            if group.generation == observed_generation:
                return

    async def _catch_up_and_go_live(
        self, subscription: ExecutionStreamSubscription
    ) -> None:
        group = self._groups[subscription.workflow_run_id]
        while subscription.state is SubscriptionState.CATCHING_UP:
            while True:
                page = await self._repository.list_after(
                    subscription.workflow_run_id,
                    subscription.last_queued_cursor,
                    EXECUTION_EVENT_PAGE_SIZE,
                )
                if not page:
                    break
                self._validate_page(
                    subscription.workflow_run_id,
                    subscription.last_queued_cursor,
                    page,
                )
                for event in page:
                    if not self._is_catching_up(subscription):
                        return
                    try:
                        subscription.queue.put_nowait(event)
                    except asyncio.QueueFull:
                        self._signal_termination(subscription, SLOW_CONSUMER)
                        return
                    subscription.last_queued_cursor = event.cursor
            # No await in this state check/transition: notification callbacks cannot
            # interleave halfway through the handoff.
            if group.generation != subscription.observed_generation:
                subscription.observed_generation = group.generation
                continue
            subscription.state = SubscriptionState.LIVE
            self._mark_dirty(subscription.workflow_run_id)
            return

    @staticmethod
    def _is_catching_up(subscription: ExecutionStreamSubscription) -> bool:
        return subscription.state is SubscriptionState.CATCHING_UP

    @staticmethod
    def _validate_page(
        workflow_run_id: UUID,
        after_cursor: int,
        page: tuple[StoredWorkflowRunExecutionEvent, ...],
    ) -> None:
        expected = after_cursor + 1
        for event in page:
            if event.workflow_run_id != workflow_run_id or event.cursor != expected:
                raise WorkflowRunExecutionEventInvariantViolation
            expected += 1

    async def _send_events(self, subscription: ExecutionStreamSubscription) -> None:
        while True:
            event = await subscription.queue.get()
            message = self._serializer(event)
            await subscription.websocket.send_json(message)
            subscription.last_delivered_cursor = event.cursor

    @staticmethod
    async def _receive_until_disconnect(
        subscription: ExecutionStreamSubscription,
    ) -> None:
        while True:
            message = await subscription.websocket.receive()
            if message["type"] == "websocket.disconnect":
                return

    @staticmethod
    async def _expire_at_deadline(
        subscription: ExecutionStreamSubscription,
    ) -> None:
        assert subscription.expiry_deadline is not None
        delay = max(
            0.0, subscription.expiry_deadline - asyncio.get_running_loop().time()
        )
        await asyncio.sleep(delay)

    def _signal_termination(
        self,
        subscription: ExecutionStreamSubscription,
        directive: TerminationDirective,
    ) -> None:
        if subscription.state in {
            SubscriptionState.TERMINATING,
            SubscriptionState.CLOSED,
        }:
            return
        subscription.state = SubscriptionState.TERMINATING
        if not subscription.termination.done():
            subscription.termination.set_result(directive)

    def _finalize_subscription(self, subscription: ExecutionStreamSubscription) -> None:
        if subscription.cleanup_complete:
            return
        subscription.cleanup_complete = True
        subscription.state = SubscriptionState.CLOSED
        group = self._groups.get(subscription.workflow_run_id)
        if group is not None:
            group.subscriptions.pop(subscription.id, None)
            if not group.subscriptions:
                if group.reconcile_task is not None:
                    group.reconcile_task.cancel()
                self._groups.pop(subscription.workflow_run_id, None)
        self._active_connections -= 1

    @staticmethod
    def _discard_queue(subscription: ExecutionStreamSubscription) -> None:
        while True:
            try:
                subscription.queue.get_nowait()
            except asyncio.QueueEmpty:
                return
