"""Deterministic contract and bounded evidence for Milestone 21 Task 1."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class M21WorkloadConfiguration:
    seed: int = 21
    owner_count: int = 5
    runs_per_owner: int = 5
    roots_per_run: int = 4
    worker_count: int = 8
    worker_prefetch: int = 4
    redelivery_run_stride: int = 5
    phase_timeout_seconds: float = 20.0
    overall_timeout_seconds: float = 120.0

    def __post_init__(self) -> None:
        if self.run_count < 25:
            raise ValueError("the M21 workload requires at least 25 active runs")
        if self.task_count < 100:
            raise ValueError("the M21 workload requires at least 100 runnable tasks")
        if self.worker_count < 2:
            raise ValueError("the M21 workload requires competing workers")
        if self.worker_prefetch < 1:
            raise ValueError("worker prefetch must be positive")
        if self.redelivery_run_stride < 1:
            raise ValueError("redelivery stride must be positive")
        if self.phase_timeout_seconds <= 0 or self.overall_timeout_seconds <= 0:
            raise ValueError("workload timeouts must be positive")
        if self.phase_timeout_seconds > self.overall_timeout_seconds:
            raise ValueError("phase timeout cannot exceed the overall timeout")

    @property
    def run_count(self) -> int:
        return self.owner_count * self.runs_per_owner

    @property
    def task_count(self) -> int:
        return self.run_count * self.roots_per_run

    @property
    def subscriber_count(self) -> int:
        return self.run_count

    @property
    def nominal_in_flight_limit(self) -> int:
        return self.worker_count * self.worker_prefetch

    @property
    def redelivery_run_ordinals(self) -> tuple[int, ...]:
        return tuple(range(0, self.run_count, self.redelivery_run_stride))


@dataclass
class WorkerDistribution:
    worker_ordinal: int
    deliveries: int = 0
    handler_invocations: int = 0
    redelivered: int = 0


@dataclass
class M21Evidence:
    schema_version: int = 1
    status: str = "running"
    checkpoints: dict[str, dict[str, int]] = field(default_factory=dict)
    worker_distribution: list[WorkerDistribution] = field(default_factory=list)
    redelivery_dispatches: list[dict[str, Any]] = field(default_factory=list)
    subscriber_summaries: list[dict[str, Any]] = field(default_factory=list)
    invariants: list[dict[str, Any]] = field(default_factory=list)
    failure: dict[str, Any] | None = None

    def to_mapping(self, configuration: M21WorkloadConfiguration) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "configuration": asdict(configuration),
            "checkpoints": self.checkpoints,
            "worker_distribution": [asdict(item) for item in self.worker_distribution],
            "redelivery_dispatches": self.redelivery_dispatches,
            "subscriber_summaries": self.subscriber_summaries,
            "invariants": self.invariants,
            "failure": self.failure,
        }
