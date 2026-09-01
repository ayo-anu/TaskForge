"""Deterministic tests for bounded M21 Task 4 profiling infrastructure."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from fastapi import WebSocket
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from sqlalchemy import create_engine, event, text
from sqlalchemy.ext.asyncio import AsyncEngine

from taskforge.api.execution_stream_runtime import (
    ExecutionStreamRuntime,
    SubscriptionState,
)
from taskforge.runs.domain import StoredWorkflowRunExecutionEvent
from tests.performance.m21_profiling import (
    FANOUT_CARDINALITIES,
    FANOUT_EVENT_COUNT,
    MAX_QUERY_AGGREGATES,
    M21ProfilingObserver,
    ProfilingCardinalityExceeded,
    ProfilingLifecycleError,
    bounded_explain_plans,
    metric_delta,
    normalized_sql_shape,
    query_fingerprint,
    validate_websocket_fanout_results,
)
from tests.test_execution_stream_runtime import Listener, Repository, Socket, settings


def _adapter() -> tuple[Any, AsyncEngine]:
    sync_engine = create_engine("sqlite://")
    return sync_engine, cast(AsyncEngine, SimpleNamespace(sync_engine=sync_engine))


def test_normalized_sql_shape_is_parameter_only_bounded_and_stable() -> None:
    secret = "credential-secret-sentinel"
    statement = (
        "SELECT * FROM task_runs WHERE id = $1 AND status = 'running' "
        f"AND payload = '{secret}' AND attempt_number = 123"
    )
    shaped = normalized_sql_shape(statement)

    assert secret not in shaped
    assert "running" not in shaped
    assert "123" not in shaped
    assert shaped.count("?") == 4
    assert len(shaped) <= 512
    assert query_fingerprint("api", "phase", "select", ("task_runs",), shaped) == (
        query_fingerprint("api", "phase", "select", ("task_runs",), shaped)
    )


def test_balanced_exact_engine_lifecycle_closes_and_removes_listeners() -> None:
    async def exercise() -> None:
        observer = M21ProfilingObserver(InMemoryMetricReader())
        workload_sync, workload = _adapter()
        api_sync, api = _adapter()
        observer.attach_engine("workload", workload)
        observer.attach_engine("api", api)
        registrations = tuple(observer._registrations)
        assert registrations
        assert all(
            event.contains(item.target, item.identifier, item.callback)
            for item in registrations
        )

        with workload_sync.begin() as connection:
            connection.execute(text("SELECT 1"))
        observer.phase_changed("websocket_handshakes")
        with api_sync.begin() as connection:
            connection.execute(text("SELECT 2"))

        await observer.close()
        assert all(
            not event.contains(item.target, item.identifier, item.callback)
            for item in registrations
        )
        summary = observer.profile_summary()
        assert {item["engine_role"] for item in summary["top_by_call_count"]} == {
            "workload",
            "api",
        }
        assert summary["pool"]["workload"]["current"] == 0
        assert summary["pool"]["api"]["current"] == 0
        assert {item["outcome"] for item in summary["transactions"]} == {"commit"}
        workload_sync.dispose()
        api_sync.dispose()

    asyncio.run(exercise())


def test_incomplete_lifecycle_fails_after_cleanup_and_removes_listeners() -> None:
    async def exercise() -> None:
        observer = M21ProfilingObserver(InMemoryMetricReader())
        workload_sync, workload = _adapter()
        api_sync, api = _adapter()
        observer.attach_engine("workload", workload)
        observer.attach_engine("api", api)
        registrations = tuple(observer._registrations)
        connection = workload_sync.connect()
        transaction = connection.begin()
        connection.execute(text("SELECT 1"))
        observer._executions[1] = cast(Any, object())

        with pytest.raises(ProfilingLifecycleError, match="incomplete profiling"):
            await observer.close()

        assert all(
            not event.contains(item.target, item.identifier, item.callback)
            for item in registrations
        )
        assert observer.measurement._stop.is_set()
        summary = observer.profile_summary()
        assert summary["unfinished_cursor_executions"] == 1
        assert summary["unfinished_transactions"] == 1
        assert summary["checked_out_connection_records"] == 1
        assert summary["pool"]["workload"] == {
            "checkouts": 1,
            "checkins": 0,
            "current": 1,
            "maximum": 1,
        }

        transaction.rollback()
        connection.close()
        workload_sync.dispose()
        api_sync.dispose()

    asyncio.run(exercise())


def test_engine_attachment_rejects_duplicate_roles_and_engine_aliases() -> None:
    async def exercise() -> None:
        observer = M21ProfilingObserver(InMemoryMetricReader())
        first_sync, first = _adapter()
        second_sync, second = _adapter()
        observer.attach_engine("workload", first)
        with pytest.raises(ProfilingLifecycleError):
            observer.attach_engine("workload", second)
        with pytest.raises(ProfilingLifecycleError):
            observer.attach_engine("api", first)
        await observer.close()
        first_sync.dispose()
        second_sync.dispose()

    asyncio.run(exercise())


def test_query_cardinality_limit_fails_instead_of_discarding_shape() -> None:
    async def exercise() -> None:
        observer = M21ProfilingObserver(InMemoryMetricReader())
        sync_engine, engine = _adapter()
        observer.attach_engine("workload", engine)
        for ordinal in range(MAX_QUERY_AGGREGATES):
            observer._queries[("workload", "filled", str(ordinal))] = cast(
                Any, object()
            )
        with pytest.raises(ProfilingCardinalityExceeded):
            with sync_engine.connect() as connection:
                connection.execute(text("SELECT 1"))
        await observer.close()
        sync_engine.dispose()

    asyncio.run(exercise())


def test_metric_delta_is_phase_scoped_and_rejects_resets() -> None:
    before = {
        "publish": [
            {
                "attributes": {"outcome": "accepted"},
                "count": 2,
                "sum": 0.4,
                "bucket_counts": [1, 1],
                "min": 0.1,
                "max": 0.3,
            }
        ]
    }
    after = {
        "publish": [
            {
                "attributes": {"outcome": "accepted"},
                "count": 5,
                "sum": 1.0,
                "bucket_counts": [2, 3],
                "min": 0.1,
                "max": 0.3,
            }
        ]
    }

    result = metric_delta(before, after, ("publish",))
    assert result == {
        "publish": [
            {
                "attributes": {"outcome": "accepted"},
                "count": 3,
                "sum": 0.6,
                "bucket_counts": [1, 2],
            }
        ]
    }
    with pytest.raises(ProfilingLifecycleError):
        metric_delta(after, before, ("publish",))


def test_explain_plan_output_is_count_and_size_bounded() -> None:
    assert bounded_explain_plans([{"node": "Index Scan"}]) == [{"node": "Index Scan"}]
    with pytest.raises(ValueError):
        bounded_explain_plans([{} for _ in range(4)])
    with pytest.raises(ValueError):
        bounded_explain_plans([{"plan": "x" * (33 * 1024)}])


def _event(run_id: UUID, cursor: int) -> StoredWorkflowRunExecutionEvent:
    return StoredWorkflowRunExecutionEvent(
        uuid4(),
        run_id,
        cursor,
        None,
        "workflow_run.status_changed",
        {"previous_status": "pending", "status": "running"},
        datetime.now(UTC),
    )


def test_same_run_fanout_work_scales_structurally_at_1_5_10() -> None:
    async def exercise(cardinality: int) -> dict[str, Any]:
        run_id = uuid4()
        repository = Repository(
            {
                run_id: tuple(
                    _event(run_id, cursor)
                    for cursor in range(1, FANOUT_EVENT_COUNT + 1)
                )
            }
        )
        listener = Listener()
        serialization_count = 0

        async def connect() -> Listener:
            return listener

        def serialize(item: StoredWorkflowRunExecutionEvent) -> dict[str, Any]:
            nonlocal serialization_count
            serialization_count += 1
            return {"cursor": item.cursor}

        runtime = ExecutionStreamRuntime(
            settings(queue_size=100, max_connections=100),
            repository,
            serialize,
            listener_factory=connect,
        )
        await runtime.start()
        sockets = [Socket() for _ in range(cardinality)]
        subscriptions = [
            await runtime.open_subscription(
                cast(WebSocket, socket),
                run_id,
                0,
                None,
                principal_id=uuid4(),
            )
            for socket in sockets
        ]
        for subscription in subscriptions:
            subscription.state = SubscriptionState.LIVE
        supervisors = [
            asyncio.create_task(runtime.serve(subscription))
            for subscription in subscriptions
        ]
        runtime._mark_dirty(run_id)
        reconcile = runtime._groups[run_id].reconcile_task
        assert reconcile is not None
        await reconcile
        async with asyncio.timeout(2):
            while any(len(socket.sent) != FANOUT_EVENT_COUNT for socket in sockets):
                await asyncio.sleep(0)
        for socket in sockets:
            socket.receive_block.set()
        await asyncio.gather(*supervisors)
        result = {
            "subscriber_count": cardinality,
            "persisted_event_count": FANOUT_EVENT_COUNT,
            "durable_list_after_calls": len(repository.calls),
            "serialization_count": serialization_count,
            "send_count": sum(len(socket.sent) for socket in sockets),
            "elapsed_seconds": 0.0,
            "process_cpu_seconds": 0.0,
            "queue_size_bound": 100,
            "maximum_queue_depth_observed": FANOUT_EVENT_COUNT,
            "backpressure_events": 0,
            "slow_consumer_terminations": 0,
            "last_delivered_cursor": min(
                subscription.last_delivered_cursor for subscription in subscriptions
            ),
        }
        await runtime.close()
        return result

    results = [asyncio.run(exercise(value)) for value in FANOUT_CARDINALITIES]
    validate_websocket_fanout_results(results)
    assert [item["durable_list_after_calls"] for item in results] == [2, 2, 2]
    assert [item["serialization_count"] for item in results] == [20, 100, 200]
    assert [item["send_count"] for item in results] == [20, 100, 200]
