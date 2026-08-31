"""Bounded, test-side measurements for the canonical M21 workload."""

from __future__ import annotations

import asyncio
import json
import math
import os
import platform
import resource
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from importlib.metadata import PackageNotFoundError, version
from itertools import pairwise
from pathlib import Path
from statistics import fmean
from time import perf_counter, perf_counter_ns
from typing import Any, cast
from uuid import UUID

import asyncpg
import httpx2

from taskforge.worker.lifecycle import WorkerDispatchRuntime
from tests.integration.postgresql import asyncpg_dsn
from tests.performance.m21_runner import OwnerFacts

PERCENTILES = (50, 95, 99)
SAMPLE_INTERVAL_SECONDS = 0.1
MAX_WAIT_CATEGORIES = 32
MAX_ARTIFACT_BYTES = 2 * 1024 * 1024


@dataclass(frozen=True)
class M21MeasurementConfiguration:
    warmup_repetitions: int = 1
    measured_repetitions: int = 5
    repetition_timeout_seconds: float = 120.0
    controlled_deadline_seconds: float = 840.0
    overall_timeout_seconds: float = 900.0
    sample_interval_seconds: float = SAMPLE_INTERVAL_SECONDS
    api_max_connections: int = 25

    def __post_init__(self) -> None:
        if self.warmup_repetitions != 1 or self.measured_repetitions != 5:
            raise ValueError("the canonical Task 2 repetition policy is fixed")
        if not 0 < self.repetition_timeout_seconds < self.controlled_deadline_seconds:
            raise ValueError("invalid repetition deadline")
        if not (self.controlled_deadline_seconds < self.overall_timeout_seconds <= 900):
            raise ValueError("invalid overall deadline")
        if self.sample_interval_seconds < 0.05:
            raise ValueError("sampling interval is too aggressive")


@dataclass
class RepetitionMeasurements:
    api_run_detail_latency_ms: list[float] = field(default_factory=list)
    initial_root_creation_to_dispatch_latency_ms: list[float] = field(
        default_factory=list
    )
    dispatch_publication_latency_ms: list[float] = field(default_factory=list)
    claim_latency_ms: list[float] = field(default_factory=list)
    claim_to_running_latency_ms: list[float] = field(default_factory=list)
    completion_latency_ms: list[float] = field(default_factory=list)
    initial_root_creation_to_completion_latency_ms: list[float] = field(
        default_factory=list
    )
    throughput: dict[str, float] = field(default_factory=dict)
    authoritative_throughput_boundary: dict[str, Any] = field(default_factory=dict)
    worker_occupancy: dict[str, Any] = field(default_factory=dict)
    postgres_waits: dict[str, Any] = field(default_factory=dict)
    resources: dict[str, Any] = field(default_factory=dict)
    infrastructure: dict[str, str] = field(default_factory=dict)
    interval_seconds: float = 0.0

    def latency_samples(self) -> dict[str, list[float]]:
        return {
            name: cast(list[float], value)
            for name, value in vars(self).items()
            if name.endswith("_latency_ms")
        }


def nearest_rank(samples: list[float], percentile: int) -> float:
    if not samples:
        raise ValueError("percentiles require samples")
    if percentile <= 0 or percentile > 100:
        raise ValueError("percentile must be in (0, 100]")
    if any(not math.isfinite(value) or value < 0 for value in samples):
        raise ValueError("samples must be finite and non-negative")
    ordered = sorted(samples)
    return ordered[math.ceil(percentile / 100 * len(ordered)) - 1]


def summarize_samples(samples: list[float]) -> dict[str, float | int]:
    if not samples:
        raise ValueError("sample summary requires samples")
    result: dict[str, float | int] = {
        "count": len(samples),
        "min": min(samples),
        "mean": fmean(samples),
        "max": max(samples),
    }
    result.update({f"p{item}": nearest_rank(samples, item) for item in PERCENTILES})
    return result


def union_duration_seconds(
    intervals: list[tuple[datetime, datetime]],
    lower: datetime,
    upper: datetime,
) -> float:
    clipped = sorted(
        (max(start, lower), min(end, upper))
        for start, end in intervals
        if min(end, upper) > max(start, lower)
    )
    if not clipped:
        return 0.0
    total = 0.0
    current_start, current_end = clipped[0]
    for start, end in clipped[1:]:
        if start <= current_end:
            current_end = max(current_end, end)
        else:
            total += (current_end - current_start).total_seconds()
            current_start, current_end = start, end
    return total + (current_end - current_start).total_seconds()


def summarize_wait_samples(samples: list[dict[str, Any]]) -> dict[str, Any]:
    if not samples:
        return {
            "sample_count": 0,
            "max_active_sessions": 0,
            "max_waiting_sessions": 0,
            "wait_session_seconds": {},
        }
    exposure: defaultdict[str, float] = defaultdict(float)
    max_active = max_waiting = 0
    for index, sample in enumerate(samples):
        next_at = (
            samples[index + 1]["observed_at"]
            if index + 1 < len(samples)
            else sample["observed_at"]
        )
        elapsed = max(0.0, next_at - sample["observed_at"])
        groups = sample["groups"]
        active = sum(groups.values())
        waiting = sum(
            count for category, count in groups.items() if category != "running"
        )
        max_active = max(max_active, active)
        max_waiting = max(max_waiting, waiting)
        for category, count in groups.items():
            if category != "running":
                exposure[category] += count * elapsed
    ordered = sorted(exposure.items(), key=lambda item: (-item[1], item[0]))
    retained = dict(ordered[:MAX_WAIT_CATEGORIES])
    if len(ordered) > MAX_WAIT_CATEGORIES:
        retained["other"] = sum(value for _, value in ordered[MAX_WAIT_CATEGORIES:])
    return {
        "sample_count": len(samples),
        "max_active_sessions": max_active,
        "max_waiting_sessions": max_waiting,
        "wait_session_seconds": retained,
    }


def _read_key_values(path: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        key, separator, value = line.partition(":")
        if separator:
            values[key] = value.strip()
    return values


def _resource_snapshot() -> dict[str, float | int]:
    process_status = _read_key_values("/proc/self/status")
    memory = _read_key_values("/proc/meminfo")
    stat = Path("/proc/stat").read_text(encoding="utf-8").splitlines()[0].split()
    cpu_values = [int(value) for value in stat[1:]]
    idle = cpu_values[3] + (cpu_values[4] if len(cpu_values) > 4 else 0)
    total = sum(cpu_values)
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        "observed_at": perf_counter(),
        "host_cpu_total_ticks": total,
        "host_cpu_idle_ticks": idle,
        "host_memory_total_bytes": int(memory["MemTotal"].split()[0]) * 1024,
        "host_memory_available_bytes": int(memory["MemAvailable"].split()[0]) * 1024,
        "process_user_cpu_seconds": usage.ru_utime,
        "process_system_cpu_seconds": usage.ru_stime,
        "process_rss_bytes": int(process_status["VmRSS"].split()[0]) * 1024,
        "process_peak_rss_bytes": int(process_status["VmHWM"].split()[0]) * 1024,
        "process_threads": int(process_status["Threads"]),
        "process_open_fds": len(tuple(Path("/proc/self/fd").iterdir())),
    }


def summarize_resources(samples: list[dict[str, float | int]]) -> dict[str, Any]:
    if len(samples) < 2:
        return {"sample_count": len(samples), "available": False}
    host_cpu: list[float] = []
    process_cpu: list[float] = []
    for previous, current in pairwise(samples):
        total_delta = current["host_cpu_total_ticks"] - previous["host_cpu_total_ticks"]
        idle_delta = current["host_cpu_idle_ticks"] - previous["host_cpu_idle_ticks"]
        if total_delta > 0:
            host_cpu.append(100 * (total_delta - idle_delta) / total_delta)
        wall = current["observed_at"] - previous["observed_at"]
        cpu = (
            current["process_user_cpu_seconds"]
            + current["process_system_cpu_seconds"]
            - previous["process_user_cpu_seconds"]
            - previous["process_system_cpu_seconds"]
        )
        if wall > 0:
            process_cpu.append(100 * cpu / wall)
    memory_used = [
        100
        * (item["host_memory_total_bytes"] - item["host_memory_available_bytes"])
        / item["host_memory_total_bytes"]
        for item in samples
    ]
    return {
        "sample_count": len(samples),
        "available": True,
        "host_cpu_percent": summarize_samples(host_cpu),
        "host_memory_used_percent": summarize_samples(memory_used),
        "process_cpu_percent": summarize_samples(process_cpu),
        "process_rss_bytes": summarize_samples(
            [float(item["process_rss_bytes"]) for item in samples]
        ),
        "process_peak_rss_bytes": max(
            int(item["process_peak_rss_bytes"]) for item in samples
        ),
        "process_threads_max": max(int(item["process_threads"]) for item in samples),
        "process_open_fds_max": max(int(item["process_open_fds"]) for item in samples),
    }


class M21MeasurementObserverImpl:
    def __init__(
        self, sample_interval_seconds: float = SAMPLE_INTERVAL_SECONDS
    ) -> None:
        self.result = RepetitionMeasurements()
        self._sample_interval = sample_interval_seconds
        self._stop = asyncio.Event()
        self._api_tasks: list[asyncio.Task[float]] = []
        self._samplers: list[asyncio.Task[None]] = []
        self._wait_samples: list[dict[str, Any]] = []
        self._resource_samples: list[dict[str, float | int]] = []
        self._sampling_errors: defaultdict[str, int] = defaultdict(int)
        self._database: Any = None
        self._api_client: httpx2.AsyncClient | None = None
        self._started_at = 0.0

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
        del setup, runtimes
        self._started_at = perf_counter()
        self._database = await asyncpg.connect(asyncpg_dsn(database_url))
        self._api_client = httpx2.AsyncClient(
            timeout=20.0,
            limits=httpx2.Limits(
                max_connections=25,
                max_keepalive_connections=25,
            ),
        )
        self.result.infrastructure = {
            "postgresql_server_version": await self._database.fetchval(
                "SELECT current_setting('server_version')"
            ),
            "rabbitmq_product": broker_metadata["product"],
            "rabbitmq_server_version": broker_metadata["version"],
        }
        self._samplers = [
            asyncio.create_task(self._sample_postgres()),
            asyncio.create_task(self._sample_resources()),
        ]
        self._api_tasks = [
            asyncio.create_task(self._measure_api(api_port, run.id, owner.credential))
            for run, owner in runs_with_owners
        ]

    async def finish(
        self,
        *,
        setup: Any,
        run_ids: list[UUID],
        workers: list[Any],
        runtimes: list[WorkerDispatchRuntime],
    ) -> None:
        del runtimes
        self.result.interval_seconds = perf_counter() - self._started_at
        self._stop.set()
        self.result.api_run_detail_latency_ms = list(
            await asyncio.gather(*self._api_tasks)
        )
        api_client = self._api_client
        assert api_client is not None
        await api_client.aclose()
        self._api_client = None
        await asyncio.gather(*self._samplers)
        await self._database.close()
        self._database = None
        self.result.postgres_waits = summarize_wait_samples(self._wait_samples)
        self.result.postgres_waits["sampling_errors"] = dict(self._sampling_errors)
        self.result.resources = summarize_resources(self._resource_samples)
        rows = await setup.fetch(_TIMING_QUERY, run_ids)
        self._derive_persisted_metrics(rows, workers)

    async def close(self) -> None:
        self._stop.set()
        for task in (*self._api_tasks, *self._samplers):
            if not task.done():
                task.cancel()
        await asyncio.gather(*self._api_tasks, *self._samplers, return_exceptions=True)
        if self._database is not None:
            await self._database.close()
            self._database = None
        if self._api_client is not None:
            await self._api_client.aclose()
            self._api_client = None

    async def _measure_api(self, port: int, run_id: UUID, credential: str) -> float:
        assert self._api_client is not None
        started = perf_counter_ns()
        response = await self._api_client.get(
            f"http://127.0.0.1:{port}/api/v1/workflow-runs/{run_id}",
            headers={"Authorization": f"Bearer {credential}"},
        )
        payload = response.json()
        elapsed = perf_counter_ns() - started
        if response.status_code != 200 or payload.get("id") != str(run_id):
            raise AssertionError("authenticated run-detail measurement failed")
        return elapsed / 1_000_000

    async def _sample_postgres(self) -> None:
        while not self._stop.is_set():
            observed_at = perf_counter()
            try:
                rows = await self._database.fetch(_WAIT_QUERY)
                groups = {(row["category"] or "running"): row["count"] for row in rows}
                self._wait_samples.append(
                    {"observed_at": observed_at, "groups": groups}
                )
            except Exception as error:
                self._sampling_errors[type(error).__name__] += 1
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._sample_interval)
            except TimeoutError:
                pass

    async def _sample_resources(self) -> None:
        while not self._stop.is_set():
            try:
                self._resource_samples.append(_resource_snapshot())
            except (OSError, KeyError, ValueError) as error:
                self._sampling_errors[f"resource_{type(error).__name__}"] += 1
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._sample_interval)
            except TimeoutError:
                pass

    def _derive_persisted_metrics(self, rows: list[Any], workers: list[Any]) -> None:
        if len(rows) != 100:
            raise AssertionError("measurement requires exactly 100 persisted task rows")
        worker_ordinals = {
            worker.session_id: ordinal for ordinal, worker in enumerate(workers)
        }
        if len(worker_ordinals) != 8:
            raise AssertionError("measurement requires eight distinct worker sessions")
        intervals: defaultdict[int, list[tuple[datetime, datetime]]] = defaultdict(list)
        first_dispatch = min(row["dispatched_at"] for row in rows)
        last_completion = max(row["completed_at"] for row in rows)
        measurement_seconds = (last_completion - first_dispatch).total_seconds()
        if measurement_seconds <= 0:
            raise AssertionError("invalid authoritative measurement interval")
        slot_seconds = 0.0
        for row in rows:
            if not row["step_identifier"].startswith("root-"):
                raise AssertionError("root-only timing received a non-root task")
            values = (
                row["task_created_at"],
                row["dispatched_at"],
                row["outbox_created_at"],
                row["published_at"],
                row["acquired_at"],
                row["running_at"],
                row["completed_at"],
            )
            if any(value is None for value in values):
                raise AssertionError("incomplete persisted timing row")
            self.result.initial_root_creation_to_dispatch_latency_ms.append(
                _milliseconds(row["task_created_at"], row["dispatched_at"])
            )
            self.result.dispatch_publication_latency_ms.append(
                _milliseconds(row["outbox_created_at"], row["published_at"])
            )
            self.result.claim_latency_ms.append(
                _milliseconds(row["dispatched_at"], row["acquired_at"])
            )
            self.result.claim_to_running_latency_ms.append(
                _milliseconds(row["acquired_at"], row["running_at"])
            )
            self.result.completion_latency_ms.append(
                _milliseconds(row["running_at"], row["completed_at"])
            )
            self.result.initial_root_creation_to_completion_latency_ms.append(
                _milliseconds(row["task_created_at"], row["completed_at"])
            )
            try:
                ordinal = worker_ordinals[row["worker_session_id"]]
            except KeyError as error:
                raise AssertionError(
                    "claim belongs to a non-workload worker"
                ) from error
            intervals[ordinal].append((row["acquired_at"], row["completed_at"]))
            slot_seconds += (row["completed_at"] - row["acquired_at"]).total_seconds()
        if set(intervals) - set(range(8)):
            raise AssertionError("invalid workload worker mapping")
        per_worker = {
            str(ordinal): 100
            * union_duration_seconds(
                intervals[ordinal], first_dispatch, last_completion
            )
            / measurement_seconds
            for ordinal in range(8)
        }
        self.result.worker_occupancy = {
            "definition": "authoritative claim-to-result interval union by worker_session_id",
            "per_worker_percent": per_worker,
            "fleet_percent": sum(per_worker.values()) / 8,
            "delivery_slot_percent": 100 * slot_seconds / (8 * 4 * measurement_seconds),
        }
        publication_seconds = (
            max(row["published_at"] for row in rows)
            - min(row["outbox_created_at"] for row in rows)
        ).total_seconds()
        dispatch_seconds = (
            max(row["dispatched_at"] for row in rows)
            - min(row["dispatched_at"] for row in rows)
        ).total_seconds()
        self.result.throughput = {
            "authoritative_task_throughput_per_second": 100 / measurement_seconds,
            "measurement_wall_throughput_per_second": 100
            / self.result.interval_seconds,
            "publication_throughput_per_second": 100 / publication_seconds,
            "dispatch_creation_throughput_per_second": 100 / dispatch_seconds,
        }
        self.result.authoritative_throughput_boundary = {
            "first_dispatch": first_dispatch.isoformat(),
            "first_dispatch_semantics": (
                "earliest persisted workflow_run_execution_events.occurred_at for "
                "a canonical root task runnable-to-dispatched status transition"
            ),
            "last_result": last_completion.isoformat(),
            "last_result_semantics": (
                "latest persisted task_attempt_results.completed_at for an "
                "authoritative canonical root-task result"
            ),
            "denominator_seconds": measurement_seconds,
            "authoritative_task_throughput_per_second": 100 / measurement_seconds,
        }


def _milliseconds(start: datetime, end: datetime) -> float:
    value = (end - start).total_seconds() * 1000
    if not math.isfinite(value) or value < 0:
        raise AssertionError("persisted duration is negative or non-finite")
    return value


def environment_metadata() -> dict[str, Any]:
    packages = {}
    for name in (
        "taskforge",
        "uvicorn",
        "fastapi",
        "sqlalchemy",
        "asyncpg",
        "aio-pika",
        "opentelemetry-sdk",
    ):
        try:
            packages[name] = version(name)
        except PackageNotFoundError:
            packages[name] = "unavailable"
    cpu_model = "unavailable"
    try:
        for line in Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("model name"):
                cpu_model = line.partition(":")[2].strip()
                break
    except OSError:
        pass
    memory = _read_key_values("/proc/meminfo")
    git_commit = "unavailable"
    git_dirty: bool | str = "unavailable"
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
        git_commit = commit.stdout.strip()
        git_dirty = bool(status.stdout)
    except (OSError, subprocess.SubprocessError):
        pass

    def cgroup_value(path: str) -> str:
        try:
            return Path(path).read_text(encoding="utf-8").strip()
        except OSError:
            return "unavailable"

    return {
        "os": platform.system(),
        "os_release": platform.release(),
        "architecture": platform.machine(),
        "python": sys.version.split()[0],
        "cpu_model": cpu_model,
        "logical_cpu_count": os.cpu_count(),
        "memory_total_bytes": int(memory.get("MemTotal", "0 kB").split()[0]) * 1024,
        "cgroup_cpu_max": cgroup_value("/sys/fs/cgroup/cpu.max"),
        "cgroup_memory_max": cgroup_value("/sys/fs/cgroup/memory.max"),
        "git_commit": git_commit,
        "git_dirty": git_dirty,
        "packages": packages,
    }


def aggregate_repetitions(
    repetitions: list[RepetitionMeasurements],
) -> dict[str, Any]:
    if len(repetitions) != 5:
        raise ValueError("only five complete measured repetitions may be aggregated")
    for repetition in repetitions:
        _validate_authoritative_throughput_boundary(repetition)
    latencies: dict[str, Any] = {}
    for name in repetitions[0].latency_samples():
        pooled = [
            value for item in repetitions for value in item.latency_samples()[name]
        ]
        expected = 125 if name == "api_run_detail_latency_ms" else 500
        if len(pooled) != expected:
            raise ValueError(f"unexpected sample count for {name}")
        latencies[name] = {"summary": summarize_samples(pooled), "raw": pooled}
    throughput_names = repetitions[0].throughput
    throughput = {
        name: {
            "values": [item.throughput[name] for item in repetitions],
            "summary": _throughput_summary(
                [item.throughput[name] for item in repetitions]
            ),
        }
        for name in throughput_names
    }
    return {"latencies": latencies, "throughput": throughput}


def _validate_authoritative_throughput_boundary(
    repetition: RepetitionMeasurements,
) -> None:
    boundary = repetition.authoritative_throughput_boundary
    try:
        first_dispatch = datetime.fromisoformat(boundary["first_dispatch"])
        last_result = datetime.fromisoformat(boundary["last_result"])
        denominator = boundary["denominator_seconds"]
        boundary_throughput = boundary["authoritative_task_throughput_per_second"]
        throughput = repetition.throughput["authoritative_task_throughput_per_second"]
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("incomplete authoritative throughput boundary") from error
    derived = (last_result - first_dispatch).total_seconds()
    if (
        denominator <= 0
        or not math.isclose(denominator, derived, rel_tol=0, abs_tol=1e-9)
        or not math.isclose(boundary_throughput, 100 / denominator, rel_tol=1e-12)
        or not math.isclose(throughput, boundary_throughput, rel_tol=1e-12)
    ):
        raise ValueError("inconsistent authoritative throughput boundary")


def _throughput_summary(samples: list[float]) -> dict[str, float | int]:
    ordered = sorted(samples)
    return {
        "count": len(samples),
        "min": ordered[0],
        "mean": fmean(ordered),
        "median": ordered[len(ordered) // 2],
        "max": ordered[-1],
    }


def write_bounded_artifact(path: Path, evidence: dict[str, Any]) -> None:
    _assert_sanitized(evidence)
    payload = json.dumps(evidence, indent=2, sort_keys=True).encode()
    if len(payload) > MAX_ARTIFACT_BYTES:
        raise ValueError("measurement artifact exceeds the 2 MiB bound")
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(payload)
    temporary.replace(path)


def _assert_sanitized(value: Any, path: tuple[str, ...] = ()) -> None:
    forbidden_keys = {"password", "secret", "token", "credential", "url", "dsn"}
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = str(key).lower()
            if normalized in forbidden_keys:
                raise ValueError(
                    f"forbidden evidence field: {'.'.join((*path, normalized))}"
                )
            _assert_sanitized(item, (*path, normalized))
    elif isinstance(value, (list, tuple)):
        for item in value:
            _assert_sanitized(item, path)
    elif isinstance(value, str) and ("://" in value or value.startswith("Bearer ")):
        raise ValueError("evidence contains a connection or authority value")


_WAIT_QUERY = """
SELECT CASE
         WHEN wait_event IS NULL THEN 'running'
         ELSE coalesce(wait_event_type, 'unknown') || ':' || wait_event
       END AS category,
       count(*)::int AS count
FROM pg_stat_activity
WHERE datname = current_database()
  AND pid <> pg_backend_pid()
  AND backend_type = 'client backend'
  AND state = 'active'
GROUP BY category
ORDER BY category
"""

_TIMING_QUERY = """
WITH status_events AS (
  SELECT task_run_id,
         min(occurred_at) FILTER (
           WHERE payload->>'previous_status' = 'runnable'
             AND payload->>'status' = 'dispatched'
         ) AS dispatched_at,
         min(occurred_at) FILTER (
           WHERE payload->>'status' = 'running'
         ) AS running_at
  FROM workflow_run_execution_events
  WHERE workflow_run_id = ANY($1::uuid[])
    AND event_type = 'task_run.status_changed'
  GROUP BY task_run_id
)
SELECT tr.id,
       tr.step_identifier,
       tr.created_at AS task_created_at,
       se.dispatched_at,
       outbox.created_at AS outbox_created_at,
       outbox.published_at,
       claim.acquired_at,
       claim.worker_session_id,
       se.running_at,
       result.completed_at
FROM task_runs tr
JOIN task_attempts attempt ON attempt.task_run_id = tr.id
JOIN task_dispatch_outbox outbox ON outbox.task_attempt_id = attempt.id
JOIN task_attempt_results result ON result.task_attempt_id = attempt.id
JOIN task_attempt_claims claim
  ON claim.task_attempt_id = result.task_attempt_id
 AND claim.generation = result.claim_generation
JOIN status_events se ON se.task_run_id = tr.id
WHERE tr.workflow_run_id = ANY($1::uuid[])
ORDER BY tr.workflow_run_id, tr.step_identifier
"""
