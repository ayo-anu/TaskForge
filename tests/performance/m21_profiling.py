"""Bounded, exact-engine profiling for the canonical M21 workload."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict, dataclass, field
from time import perf_counter, perf_counter_ns, process_time_ns
from typing import Any, Literal, cast
from uuid import UUID, uuid4

import asyncpg
from fastapi import WebSocket
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.sql import visitors

from taskforge.api.execution_stream import serialize_execution_event
from taskforge.api.execution_stream_runtime import (
    ExecutionStreamRuntime,
    SubscriptionState,
    TerminationKind,
)
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.execution_events import (
    SQLAlchemyWorkflowRunExecutionEventRepository,
)
from taskforge.runs.domain import StoredWorkflowRunExecutionEvent
from taskforge.worker.lifecycle import WorkerDispatchRuntime
from tests.integration.postgresql import asyncpg_dsn
from tests.integration.test_authentication_persistence import settings_for
from tests.integration.test_execution_event_persistence import raw_append, seed_runs
from tests.performance.m21_measurement import (
    M21MeasurementObserverImpl,
    RepetitionMeasurements,
)
from tests.performance.m21_runner import OwnerFacts

EngineRole = Literal["workload", "api"]

MAX_QUERY_AGGREGATES = 512
TOP_QUERY_COUNT = 25
MAX_SQL_DIAGNOSTIC_CHARACTERS = 512
MAX_EXPLAIN_PLANS = 3
MAX_EXPLAIN_PLAN_BYTES = 32 * 1024
FANOUT_CARDINALITIES = (1, 5, 10)
FANOUT_EVENT_COUNT = 20

_SPACE = re.compile(r"\s+")
_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_COMMENT = re.compile(r"--[^\n]*")
_QUOTED_LITERAL = re.compile(r"'(?:''|[^'])*'")
_DOLLAR_PARAMETER = re.compile(r"\$\d+")
_NAMED_PARAMETER = re.compile(r"(?<!:):[A-Za-z_][A-Za-z0-9_]*")
_PYFORMAT_PARAMETER = re.compile(r"%\([A-Za-z_][A-Za-z0-9_]*\)s|%s")
_NUMERIC_LITERAL = re.compile(r"(?<![A-Za-z_$])[-+]?\d+(?:\.\d+)?(?![A-Za-z_])")


class ProfilingCardinalityExceeded(RuntimeError):
    """The bounded profiler observed more distinct query shapes than approved."""


class ProfilingLifecycleError(RuntimeError):
    """Exact-engine instrumentation did not obey its lifecycle invariants."""


@dataclass
class BoundedSummary:
    count: int = 0
    total_seconds: float = 0.0
    minimum_seconds: float | None = None
    maximum_seconds: float = 0.0

    def observe(self, duration_seconds: float) -> None:
        if not math.isfinite(duration_seconds) or duration_seconds < 0:
            raise ProfilingLifecycleError("profiling duration must be finite")
        self.count += 1
        self.total_seconds += duration_seconds
        self.minimum_seconds = (
            duration_seconds
            if self.minimum_seconds is None
            else min(self.minimum_seconds, duration_seconds)
        )
        self.maximum_seconds = max(self.maximum_seconds, duration_seconds)

    def to_mapping(self) -> dict[str, int | float | None]:
        return {
            "count": self.count,
            "total_seconds": self.total_seconds,
            "mean_seconds": self.total_seconds / self.count if self.count else None,
            "minimum_seconds": self.minimum_seconds,
            "maximum_seconds": self.maximum_seconds if self.count else None,
        }


@dataclass
class QueryAggregate:
    engine_role: EngineRole
    phase: str
    fingerprint: str
    operation: str
    tables: tuple[str, ...]
    sql_shape: str
    cursor_execution: BoundedSummary = field(default_factory=BoundedSummary)
    errors: int = 0

    def to_mapping(self) -> dict[str, Any]:
        return {
            "engine_role": self.engine_role,
            "phase": self.phase,
            "fingerprint": self.fingerprint,
            "operation": self.operation,
            "tables": list(self.tables),
            "sql_shape": self.sql_shape,
            "cursor_execution": self.cursor_execution.to_mapping(),
            "errors": self.errors,
        }


@dataclass(frozen=True)
class _ExecutionStart:
    started_ns: int
    aggregate_key: tuple[EngineRole, str, str]


@dataclass(frozen=True)
class _TransactionStart:
    started_ns: int
    phase: str


@dataclass
class PoolSummary:
    checkouts: int = 0
    checkins: int = 0
    current: int = 0
    maximum: int = 0

    def checkout(self) -> None:
        self.checkouts += 1
        self.current += 1
        self.maximum = max(self.maximum, self.current)

    def checkin(self) -> None:
        if self.current <= 0:
            raise ProfilingLifecycleError("pool checkin has no matching checkout")
        self.checkins += 1
        self.current -= 1

    def to_mapping(self) -> dict[str, int]:
        return asdict(self)


@dataclass(frozen=True)
class _ListenerRegistration:
    target: Any
    identifier: str
    callback: Callable[..., Any]


def normalized_sql_shape(statement: str) -> str:
    """Return a bounded parameter-only diagnostic shape, never literal-bound SQL."""
    shaped = _BLOCK_COMMENT.sub(" ", statement)
    shaped = _LINE_COMMENT.sub(" ", shaped)
    shaped = _QUOTED_LITERAL.sub("?", shaped)
    shaped = _DOLLAR_PARAMETER.sub("?", shaped)
    shaped = _NAMED_PARAMETER.sub("?", shaped)
    shaped = _PYFORMAT_PARAMETER.sub("?", shaped)
    shaped = _NUMERIC_LITERAL.sub("?", shaped)
    return _SPACE.sub(" ", shaped).strip()[:MAX_SQL_DIAGNOSTIC_CHARACTERS]


def _statement_metadata(context: Any, statement: str) -> tuple[str, tuple[str, ...]]:
    compiled = getattr(context, "compiled", None)
    clause = getattr(compiled, "statement", None)
    operation = str(getattr(clause, "__visit_name__", "driver_sql"))[:64]
    table_names: set[str] = set()
    if clause is not None:
        try:
            for element in visitors.iterate(clause):
                if getattr(element, "__visit_name__", None) != "table":
                    continue
                name = getattr(element, "name", None)
                if isinstance(name, str) and name:
                    table_names.add(name[:128])
        except (AttributeError, TypeError, ValueError):
            table_names.clear()
    if not table_names:
        lowered = normalized_sql_shape(statement).lower()
        for match in re.finditer(
            r"\b(?:from|join|update|into)\s+([a-z_][a-z0-9_]*)", lowered
        ):
            table_names.add(match.group(1)[:128])
    return operation, tuple(sorted(table_names)[:16])


def query_fingerprint(
    role: EngineRole,
    phase: str,
    operation: str,
    tables: tuple[str, ...],
    sql_shape: str,
) -> str:
    structural = json.dumps(
        [role, phase, operation, tables, sql_shape],
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(structural).hexdigest()[:24]


def metric_snapshot(reader: InMemoryMetricReader) -> dict[str, list[dict[str, Any]]]:
    collected = reader.get_metrics_data()
    if collected is None:
        return {}
    result: dict[str, list[dict[str, Any]]] = {}
    for resource_metrics in collected.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                points: list[dict[str, Any]] = []
                for point in metric.data.data_points:
                    item: dict[str, Any] = {
                        "attributes": dict(point.attributes or {}),
                    }
                    for name in ("value", "count", "sum", "min", "max"):
                        value = getattr(point, name, None)
                        if isinstance(value, (int, float)) and not isinstance(
                            value, bool
                        ):
                            item[name] = value
                    buckets = getattr(point, "bucket_counts", None)
                    if buckets is not None:
                        item["bucket_counts"] = list(buckets)
                    points.append(item)
                result[metric.name] = points
    return result


def _metric_point_key(point: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    attributes = point.get("attributes", {})
    if not isinstance(attributes, Mapping):
        raise ProfilingLifecycleError("metric attributes are invalid")
    return tuple(sorted((str(key), str(value)) for key, value in attributes.items()))


def metric_delta(
    before: Mapping[str, list[dict[str, Any]]],
    after: Mapping[str, list[dict[str, Any]]],
    names: Iterable[str],
) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for name in names:
        earlier = {_metric_point_key(point): point for point in before.get(name, [])}
        retained: list[dict[str, Any]] = []
        for point in after.get(name, []):
            key = _metric_point_key(point)
            previous = earlier.get(key, {})
            delta: dict[str, Any] = {"attributes": dict(key)}
            for field_name in ("value", "count", "sum"):
                current_value = point.get(field_name)
                if current_value is None:
                    continue
                difference = current_value - previous.get(field_name, 0)
                if difference < 0:
                    raise ProfilingLifecycleError("production metric decreased")
                delta[field_name] = difference
            current_buckets = point.get("bucket_counts")
            if current_buckets is not None:
                old_buckets = previous.get(
                    "bucket_counts", [0 for _ in current_buckets]
                )
                if len(old_buckets) != len(current_buckets):
                    raise ProfilingLifecycleError("histogram shape changed")
                differences = [
                    current - old
                    for current, old in zip(current_buckets, old_buckets, strict=True)
                ]
                if any(value < 0 for value in differences):
                    raise ProfilingLifecycleError("histogram bucket decreased")
                delta["bucket_counts"] = differences
            if previous.get("count", 0) == 0:
                for field_name in ("min", "max"):
                    if field_name in point:
                        delta[field_name] = point[field_name]
            retained.append(delta)
        if retained:
            result[name] = retained
    return result


class M21ProfilingObserver:
    """Profile only concrete per-repetition engines with bounded summaries."""

    _BROKER_METRICS = (
        "taskforge.dispatch.publications",
        "taskforge.dispatch.publication_records",
        "taskforge.dispatch.publish.duration",
        "taskforge.dispatch.outbox.duration",
    )

    def __init__(
        self,
        reader: InMemoryMetricReader,
        *,
        sample_interval_seconds: float = 0.1,
    ) -> None:
        self.measurement = M21MeasurementObserverImpl(sample_interval_seconds)
        self._reader = reader
        self._phase = "infrastructure_setup"
        self._phase_started_ns = perf_counter_ns()
        self._phase_durations: dict[str, BoundedSummary] = {}
        self._registrations: list[_ListenerRegistration] = []
        self._engines: dict[EngineRole, AsyncEngine] = {}
        self._queries: dict[tuple[EngineRole, str, str], QueryAggregate] = {}
        self._executions: dict[int, _ExecutionStart] = {}
        self._transactions: dict[tuple[EngineRole, int], _TransactionStart] = {}
        self._transaction_summaries: dict[
            tuple[EngineRole, str, str], BoundedSummary
        ] = {}
        self._pools = {role: PoolSummary() for role in ("workload", "api")}
        self._checked_out: set[tuple[EngineRole, int]] = set()
        self._handshakes = BoundedSummary()
        self._websocket_events = 0
        self._publication_before: dict[str, list[dict[str, Any]]] | None = None
        self._publication_delta: dict[str, list[dict[str, Any]]] = {}
        self._closed = False

    @property
    def result(self) -> RepetitionMeasurements:
        return self.measurement.result

    def attach_engine(self, role: str, engine: AsyncEngine) -> None:
        if role not in ("workload", "api"):
            raise ProfilingLifecycleError("unknown profiling engine role")
        typed_role = cast(EngineRole, role)
        if typed_role in self._engines:
            raise ProfilingLifecycleError("profiling engine role attached twice")
        if engine in self._engines.values():
            raise ProfilingLifecycleError("one engine cannot have two profiling roles")
        self._engines[typed_role] = engine
        sync_engine = engine.sync_engine
        pool = sync_engine.pool

        def before_cursor_execute(
            _connection: Any,
            _cursor: Any,
            statement: str,
            _parameters: Any,
            context: Any,
            _executemany: bool,
        ) -> None:
            shape = normalized_sql_shape(statement)
            operation, tables = _statement_metadata(context, statement)
            fingerprint = query_fingerprint(
                typed_role, self._phase, operation, tables, shape
            )
            key = (typed_role, self._phase, fingerprint)
            if key not in self._queries:
                if len(self._queries) >= MAX_QUERY_AGGREGATES:
                    raise ProfilingCardinalityExceeded(
                        f"query aggregate limit {MAX_QUERY_AGGREGATES} exceeded"
                    )
                self._queries[key] = QueryAggregate(
                    typed_role,
                    self._phase,
                    fingerprint,
                    operation,
                    tables,
                    shape,
                )
            context_key = id(context)
            if context_key in self._executions:
                raise ProfilingLifecycleError("cursor execution started twice")
            self._executions[context_key] = _ExecutionStart(perf_counter_ns(), key)

        def after_cursor_execute(
            _connection: Any,
            _cursor: Any,
            _statement: str,
            _parameters: Any,
            context: Any,
            _executemany: bool,
        ) -> None:
            self._complete_execution(context, error=False)

        def handle_error(exception_context: Any) -> None:
            context = getattr(exception_context, "execution_context", None)
            if context is not None:
                self._complete_execution(context, error=True)

        def begin(connection: Any) -> None:
            key = (typed_role, id(connection))
            if key in self._transactions:
                raise ProfilingLifecycleError("transaction began twice")
            self._transactions[key] = _TransactionStart(perf_counter_ns(), self._phase)

        def transaction_end(connection: Any, outcome: str) -> None:
            key = (typed_role, id(connection))
            started = self._transactions.pop(key, None)
            if started is None:
                raise ProfilingLifecycleError("transaction ended without begin")
            summary = self._transaction_summaries.setdefault(
                (typed_role, started.phase, outcome), BoundedSummary()
            )
            summary.observe((perf_counter_ns() - started.started_ns) / 1_000_000_000)

        def commit(connection: Any) -> None:
            transaction_end(connection, "commit")

        def rollback(connection: Any) -> None:
            transaction_end(connection, "rollback")

        def checkout(
            _dbapi_connection: Any,
            connection_record: Any,
            _connection_proxy: Any,
        ) -> None:
            key = (typed_role, id(connection_record))
            if key in self._checked_out:
                raise ProfilingLifecycleError("pool connection checked out twice")
            self._checked_out.add(key)
            self._pools[typed_role].checkout()

        def checkin(_dbapi_connection: Any, connection_record: Any) -> None:
            key = (typed_role, id(connection_record))
            if key not in self._checked_out:
                raise ProfilingLifecycleError("pool checkin has no checkout")
            self._checked_out.remove(key)
            self._pools[typed_role].checkin()

        for target, identifier, callback in (
            (sync_engine, "before_cursor_execute", before_cursor_execute),
            (sync_engine, "after_cursor_execute", after_cursor_execute),
            (sync_engine, "handle_error", handle_error),
            (sync_engine, "begin", begin),
            (sync_engine, "commit", commit),
            (sync_engine, "rollback", rollback),
            (pool, "checkout", checkout),
            (pool, "checkin", checkin),
        ):
            event.listen(target, identifier, callback)
            self._registrations.append(
                _ListenerRegistration(target, identifier, callback)
            )

    def _complete_execution(self, context: Any, *, error: bool) -> None:
        started = self._executions.pop(id(context), None)
        if started is None:
            return
        aggregate = self._queries[started.aggregate_key]
        aggregate.cursor_execution.observe(
            (perf_counter_ns() - started.started_ns) / 1_000_000_000
        )
        if error:
            aggregate.errors += 1

    def phase_changed(self, phase: str) -> None:
        now = perf_counter_ns()
        self._phase_durations.setdefault(self._phase, BoundedSummary()).observe(
            (now - self._phase_started_ns) / 1_000_000_000
        )
        if self._phase == "broker_publication":
            if self._publication_before is None:
                raise ProfilingLifecycleError("publication metric baseline is missing")
            self._publication_delta = metric_delta(
                self._publication_before,
                metric_snapshot(self._reader),
                self._BROKER_METRICS,
            )
        self._phase = phase[:64]
        self._phase_started_ns = now
        if phase == "broker_publication":
            self._publication_before = metric_snapshot(self._reader)

    def websocket_handshake_completed(self, duration_seconds: float) -> None:
        self._handshakes.observe(duration_seconds)

    def websocket_event_received(self) -> None:
        self._websocket_events += 1

    async def start(
        self,
        *,
        api_port: int,
        runs_with_owners: list[tuple[Any, OwnerFacts]],
        database_url: Any,
        broker_metadata: dict[str, str],
        setup: Any,
        runtimes: list[WorkerDispatchRuntime],
    ) -> None:
        if set(self._engines) != {"workload", "api"}:
            raise ProfilingLifecycleError("both concrete engines must be attached")
        await self.measurement.start(
            api_port=api_port,
            runs_with_owners=runs_with_owners,
            database_url=database_url,
            broker_metadata=broker_metadata,
            setup=setup,
            runtimes=runtimes,
        )

    async def finish(
        self,
        *,
        setup: Any,
        run_ids: list[UUID],
        workers: list[Any],
        runtimes: list[WorkerDispatchRuntime],
    ) -> None:
        await self.measurement.finish(
            setup=setup,
            run_ids=run_ids,
            workers=workers,
            runtimes=runtimes,
        )

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        finalization_error: BaseException | None = None
        try:
            now = perf_counter_ns()
            self._phase_durations.setdefault(self._phase, BoundedSummary()).observe(
                (now - self._phase_started_ns) / 1_000_000_000
            )
            if (
                self._phase == "broker_publication"
                and self._publication_before is not None
            ):
                self._publication_delta = metric_delta(
                    self._publication_before,
                    metric_snapshot(self._reader),
                    self._BROKER_METRICS,
                )
            self._assert_complete_lifecycle()
        except BaseException as error:
            finalization_error = error
        cleanup_errors: list[BaseException] = []
        for registration in reversed(self._registrations):
            try:
                event.remove(
                    registration.target,
                    registration.identifier,
                    registration.callback,
                )
            except BaseException as error:
                cleanup_errors.append(error)
        self._registrations.clear()
        try:
            await self.measurement.close()
        except BaseException as error:
            cleanup_errors.append(error)
        if finalization_error is not None:
            if cleanup_errors:
                finalization_error.add_note(
                    f"profiling cleanup also failed with {len(cleanup_errors)} error(s)"
                )
            if isinstance(finalization_error, ProfilingLifecycleError):
                raise finalization_error
            raise ProfilingLifecycleError("profiling finalization failed") from (
                finalization_error
            )
        if cleanup_errors:
            raise ProfilingLifecycleError(
                f"profiling cleanup failed with {len(cleanup_errors)} error(s)"
            ) from cleanup_errors[0]

    def _assert_complete_lifecycle(self) -> None:
        violations: list[str] = []
        if self._executions:
            violations.append(f"unfinished cursor executions={len(self._executions)}")
        if self._transactions:
            violations.append(f"unfinished transactions={len(self._transactions)}")
        if self._checked_out:
            violations.append(
                f"checked-out connection records={len(self._checked_out)}"
            )
        for role in ("workload", "api"):
            pool = self._pools[role]
            if pool.current != 0:
                violations.append(f"{role} pool current={pool.current}")
            if pool.checkouts != pool.checkins:
                violations.append(
                    f"{role} pool checkouts={pool.checkouts} checkins={pool.checkins}"
                )
        if violations:
            raise ProfilingLifecycleError(
                "incomplete profiling lifecycle: " + "; ".join(violations)
            )

    def profile_summary(self) -> dict[str, Any]:
        ordered_time = sorted(
            self._queries.values(),
            key=lambda item: (
                -item.cursor_execution.total_seconds,
                item.engine_role,
                item.phase,
                item.fingerprint,
            ),
        )
        ordered_calls = sorted(
            self._queries.values(),
            key=lambda item: (
                -item.cursor_execution.count,
                item.engine_role,
                item.phase,
                item.fingerprint,
            ),
        )
        engine_phase: dict[tuple[EngineRole, str], BoundedSummary] = {}
        for aggregate in self._queries.values():
            summary = engine_phase.setdefault(
                (aggregate.engine_role, aggregate.phase), BoundedSummary()
            )
            summary.count += aggregate.cursor_execution.count
            summary.total_seconds += aggregate.cursor_execution.total_seconds
            if aggregate.cursor_execution.minimum_seconds is not None:
                summary.minimum_seconds = (
                    aggregate.cursor_execution.minimum_seconds
                    if summary.minimum_seconds is None
                    else min(
                        summary.minimum_seconds,
                        aggregate.cursor_execution.minimum_seconds,
                    )
                )
            summary.maximum_seconds = max(
                summary.maximum_seconds,
                aggregate.cursor_execution.maximum_seconds,
            )
        transactions = [
            {
                "engine_role": role,
                "phase": phase,
                "outcome": outcome,
                "transaction_lifetime": summary.to_mapping(),
            }
            for (role, phase, outcome), summary in sorted(
                self._transaction_summaries.items()
            )
        ]
        return {
            "definitions": {
                "cursor_execute_duration": (
                    "SQLAlchemy cursor execution boundary only; excludes pool wait, "
                    "application work, and transaction completion."
                ),
                "transaction_lifetime": (
                    "SQLAlchemy begin until commit/rollback boundary event; excludes "
                    "a separately measurable driver commit round trip."
                ),
                "pool_occupancy": (
                    "Exact checkout/checkin occupancy; not checkout wait duration."
                ),
            },
            "query_aggregate_count": len(self._queries),
            "query_aggregate_limit": MAX_QUERY_AGGREGATES,
            "top_by_cursor_execution_time": [
                item.to_mapping() for item in ordered_time[:TOP_QUERY_COUNT]
            ],
            "top_by_call_count": [
                item.to_mapping() for item in ordered_calls[:TOP_QUERY_COUNT]
            ],
            "engine_phase": [
                {
                    "engine_role": role,
                    "phase": phase,
                    "cursor_execution": summary.to_mapping(),
                }
                for (role, phase), summary in sorted(engine_phase.items())
            ],
            "transactions": transactions,
            "pool": {
                role: summary.to_mapping() for role, summary in self._pools.items()
            },
            "application_phase_duration": {
                phase: summary.to_mapping()
                for phase, summary in sorted(self._phase_durations.items())
            },
            "broker_publication_metric_delta": self._publication_delta,
            "websocket": {
                "handshake_application_duration": self._handshakes.to_mapping(),
                "received_execution_events": self._websocket_events,
            },
            "unfinished_cursor_executions": len(self._executions),
            "unfinished_transactions": len(self._transactions),
            "checked_out_connection_records": len(self._checked_out),
        }


class _ProbeListener:
    async def add_listener(self, channel: str, callback: Any) -> None:
        del channel, callback

    def add_termination_listener(self, callback: Any) -> None:
        del callback

    async def close(self) -> None:
        pass


class _CountingExecutionEventRepository:
    def __init__(self, delegate: SQLAlchemyWorkflowRunExecutionEventRepository):
        self._delegate = delegate
        self.list_after_calls = 0

    async def list_after(
        self, workflow_run_id: UUID, after_cursor: int, limit: int
    ) -> tuple[StoredWorkflowRunExecutionEvent, ...]:
        self.list_after_calls += 1
        return await self._delegate.list_after(workflow_run_id, after_cursor, limit)

    async def inspect_resume_cursor(
        self, workflow_run_id: UUID, cursor: int | None
    ) -> Any:
        return await self._delegate.inspect_resume_cursor(workflow_run_id, cursor)


class _ProbeSerializer:
    def __init__(self) -> None:
        self.count = 0

    def __call__(self, item: StoredWorkflowRunExecutionEvent) -> dict[str, object]:
        self.count += 1
        return serialize_execution_event(item)


class _ProbeSocket:
    def __init__(self, expected: int) -> None:
        self.expected = expected
        self.sent = 0
        self.release_sends = asyncio.Event()
        self.all_sent = asyncio.Event()
        self.disconnect = asyncio.Event()

    async def send_json(self, message: dict[str, Any]) -> None:
        del message
        await self.release_sends.wait()
        self.sent += 1
        if self.sent == self.expected:
            self.all_sent.set()

    async def receive(self) -> dict[str, Any]:
        await self.disconnect.wait()
        return {"type": "websocket.disconnect"}

    async def close(self, *, code: int, reason: str) -> None:
        del code, reason


async def run_websocket_fanout_profile(
    database_url: Any,
    *,
    cardinalities: tuple[int, ...] = FANOUT_CARDINALITIES,
) -> list[dict[str, Any]]:
    """Exercise shared durable reconciliation at bounded same-run cardinalities."""
    if cardinalities != FANOUT_CARDINALITIES:
        raise ValueError("the Task 4 fan-out cardinalities are fixed")
    setup = await asyncpg.connect(asyncpg_dsn(database_url))
    engine = build_async_engine(
        settings_for(database_url).model_copy(
            update={
                "execution_stream_queue_size": 100,
                "execution_stream_max_connections": 100,
            }
        )
    )
    sessions = build_session_factory(engine)
    results: list[dict[str, Any]] = []
    try:
        for cardinality in cardinalities:
            run_id, _, _, _ = await seed_runs(setup)
            for _ in range(FANOUT_EVENT_COUNT):
                await raw_append(setup, run_id)
            repository = _CountingExecutionEventRepository(
                SQLAlchemyWorkflowRunExecutionEventRepository(sessions)
            )
            serializer = _ProbeSerializer()
            listener = _ProbeListener()

            async def connect(
                current_listener: _ProbeListener = listener,
            ) -> _ProbeListener:
                return current_listener

            runtime = ExecutionStreamRuntime(
                settings_for(database_url).model_copy(
                    update={
                        "execution_stream_queue_size": 100,
                        "execution_stream_max_connections": 100,
                    }
                ),
                repository,
                serializer,
                listener_factory=connect,
            )
            await runtime.start()
            sockets = [_ProbeSocket(FANOUT_EVENT_COUNT) for _ in range(cardinality)]
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
            started = perf_counter()
            cpu_started = process_time_ns()
            runtime._mark_dirty(run_id)
            reconcile = runtime._groups[run_id].reconcile_task
            assert reconcile is not None
            await reconcile
            maximum_queue_depth = max(
                subscription.queue.qsize() for subscription in subscriptions
            )
            slow_consumer_terminations = sum(
                subscription.termination.done()
                and subscription.termination.result().kind
                is TerminationKind.SLOW_CONSUMER
                for subscription in subscriptions
            )
            for socket in sockets:
                socket.release_sends.set()
            async with asyncio.timeout(5):
                await asyncio.gather(*(socket.all_sent.wait() for socket in sockets))
            cpu_seconds = (process_time_ns() - cpu_started) / 1_000_000_000
            elapsed_seconds = perf_counter() - started
            for socket in sockets:
                socket.disconnect.set()
            await asyncio.gather(*supervisors)
            results.append(
                {
                    "subscriber_count": cardinality,
                    "persisted_event_count": FANOUT_EVENT_COUNT,
                    "durable_list_after_calls": repository.list_after_calls,
                    "serialization_count": serializer.count,
                    "send_count": sum(socket.sent for socket in sockets),
                    "elapsed_seconds": elapsed_seconds,
                    "process_cpu_seconds": cpu_seconds,
                    "queue_size_bound": 100,
                    "maximum_queue_depth_observed": maximum_queue_depth,
                    "backpressure_events": slow_consumer_terminations,
                    "slow_consumer_terminations": slow_consumer_terminations,
                    "last_delivered_cursor": min(
                        subscription.last_delivered_cursor
                        for subscription in subscriptions
                    ),
                }
            )
            await runtime.close()
    finally:
        await engine.dispose()
        await setup.close()
    validate_websocket_fanout_results(results)
    return results


def validate_websocket_fanout_results(results: list[dict[str, Any]]) -> None:
    if tuple(item.get("subscriber_count") for item in results) != FANOUT_CARDINALITIES:
        raise AssertionError("fan-out results do not cover 1/5/10 subscribers")
    for item in results:
        subscribers = item["subscriber_count"]
        assert item["durable_list_after_calls"] == 2
        assert item["serialization_count"] == FANOUT_EVENT_COUNT * subscribers
        assert item["send_count"] == FANOUT_EVENT_COUNT * subscribers
        assert item["maximum_queue_depth_observed"] <= item["queue_size_bound"]
        assert item["last_delivered_cursor"] == FANOUT_EVENT_COUNT


def bounded_explain_plans(plans: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Enforce the reviewed EXPLAIN count and serialized-size limits."""
    if len(plans) > MAX_EXPLAIN_PLANS:
        raise ValueError("too many targeted EXPLAIN plans")
    for plan in plans:
        if len(json.dumps(plan, sort_keys=True).encode()) > MAX_EXPLAIN_PLAN_BYTES:
            raise ValueError("targeted EXPLAIN plan exceeds its bound")
    return plans
