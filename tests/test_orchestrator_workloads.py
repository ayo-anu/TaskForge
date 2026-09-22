"""Bounded orchestrator pass and process-local sweep tests."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from taskforge.dispatch.service import TaskDispatchNotEligible
from taskforge.orchestrator.domain import (
    ActiveWorkflowRunCandidate,
    DiscoveryCursor,
    DiscoveryHighWater,
    DiscoveryPage,
    LoopExit,
    OrchestratorWorkloadInvariantError,
    RetryPendingTaskCandidate,
    RunnableTaskCandidate,
    WorkloadPassResult,
)
from taskforge.orchestrator.workloads import (
    OutboxPublicationWorkload,
    ProgressionDispatchWorkload,
    RetryWorkload,
    run_workload_loop,
)

NOW = datetime(2026, 1, 1, tzinfo=UTC)


class CandidateRepository:
    def __init__(self) -> None:
        self.high = DiscoveryHighWater(NOW, UUID(int=100))
        self.active_pages: list[DiscoveryPage[ActiveWorkflowRunCandidate]] = []
        self.runnable_pages: list[DiscoveryPage[RunnableTaskCandidate]] = []
        self.retry_pages: list[DiscoveryPage[RetryPendingTaskCandidate]] = []
        self.capture_counts = {"active": 0, "runnable": 0, "retry": 0}
        self.received_cursors: list[DiscoveryCursor | None] = []

    async def capture_active_run_high_water(self) -> DiscoveryHighWater | None:
        self.capture_counts["active"] += 1
        return self.high

    async def capture_runnable_task_high_water(self) -> DiscoveryHighWater | None:
        self.capture_counts["runnable"] += 1
        return self.high

    async def capture_retry_pending_high_water(self) -> DiscoveryHighWater | None:
        self.capture_counts["retry"] += 1
        return self.high

    async def list_active_workflow_runs(
        self,
        *,
        high_water: DiscoveryHighWater,
        cursor: DiscoveryCursor | None,
        limit: int,
    ) -> DiscoveryPage[ActiveWorkflowRunCandidate]:
        assert high_water == self.high and limit == 2
        self.received_cursors.append(cursor)
        return self.active_pages.pop(0)

    async def list_runnable_tasks(
        self,
        *,
        high_water: DiscoveryHighWater,
        cursor: DiscoveryCursor | None,
        limit: int,
    ) -> DiscoveryPage[RunnableTaskCandidate]:
        assert high_water == self.high and limit == 2
        self.received_cursors.append(cursor)
        return self.runnable_pages.pop(0)

    async def list_retry_pending_tasks(
        self,
        *,
        high_water: DiscoveryHighWater,
        cursor: DiscoveryCursor | None,
        limit: int,
    ) -> DiscoveryPage[RetryPendingTaskCandidate]:
        assert high_water == self.high and limit == 2
        self.received_cursors.append(cursor)
        return self.retry_pages.pop(0)


def cursor(last_id: int) -> DiscoveryCursor:
    return DiscoveryCursor(NOW, UUID(int=100), NOW, UUID(int=last_id))


def test_progression_pass_preserves_cursor_then_resets_and_ignores_dispatch_race() -> (
    None
):
    async def scenario() -> None:
        repository = CandidateRepository()
        run_a, task_a, task_b = uuid4(), uuid4(), uuid4()
        repository.active_pages = [
            DiscoveryPage((ActiveWorkflowRunCandidate(run_a, NOW),), cursor(10)),
            DiscoveryPage((), None),
        ]
        repository.runnable_pages = [
            DiscoveryPage(
                (
                    RunnableTaskCandidate(run_a, task_a, NOW),
                    RunnableTaskCandidate(run_a, task_b, NOW),
                ),
                cursor(20),
            ),
            DiscoveryPage((), None),
        ]

        class Runs:
            async def reconcile_workflow_run(self, run_id: UUID) -> SimpleNamespace:
                assert run_id == run_a
                return SimpleNamespace(
                    runnable_transition_count=1,
                    skipped_transition_count=1,
                    workflow_transition_count=0,
                    cancelled_transition_count=0,
                )

        class Dispatch:
            async def dispatch_task(self, run_id: UUID, task_id: UUID) -> None:
                assert run_id == run_a
                if task_id == task_b:
                    raise TaskDispatchNotEligible

        workload = ProgressionDispatchWorkload(
            repository,
            Runs(),
            Dispatch(),
            batch_size=2,
        )
        first = await workload.run_once()
        second = await workload.run_once()

        assert first == WorkloadPassResult(3, 3, True)
        assert second == WorkloadPassResult(0, 0, False)
        assert repository.capture_counts == {"active": 1, "runnable": 1, "retry": 0}
        repository.active_pages.append(DiscoveryPage((), None))
        repository.runnable_pages.append(DiscoveryPage((), None))
        await workload.run_once()
        assert repository.capture_counts == {"active": 2, "runnable": 2, "retry": 0}

    asyncio.run(scenario())


def test_retry_pass_treats_expected_noops_as_candidate_local() -> None:
    async def scenario() -> None:
        repository = CandidateRepository()
        task_ids = (uuid4(), uuid4(), uuid4())
        repository.retry_pages = [
            DiscoveryPage(
                tuple(RetryPendingTaskCandidate(item, NOW) for item in task_ids[:2]),
                cursor(20),
            )
        ]

        class Transitions:
            calls = 0

            async def transition_retry(self, task_id: UUID) -> SimpleNamespace:
                del task_id
                self.calls += 1
                outcome = "not_eligible" if self.calls == 1 else "scheduled"
                return SimpleNamespace(outcome=SimpleNamespace(value=outcome))

        class Due:
            async def scan_due_retries(self, *, batch_size: int) -> SimpleNamespace:
                assert batch_size == 2
                return SimpleNamespace(examined=2, dispatched=1)

        result = await RetryWorkload(
            repository,
            Transitions(),
            Due(),
            batch_size=2,
        ).run_once()
        assert result == WorkloadPassResult(4, 2, True)

    asyncio.run(scenario())


def test_outbox_durable_invalid_is_structural_failure() -> None:
    async def scenario() -> None:
        class Publisher:
            async def reconcile_unpublished(
                self, *, page_size: int, pass_limit: int
            ) -> SimpleNamespace:
                assert (page_size, pass_limit) == (2, 2)
                return SimpleNamespace(
                    examined=1,
                    acknowledged=0,
                    durable_invalid=1,
                    pass_limit_reached=False,
                )

        with pytest.raises(OrchestratorWorkloadInvariantError):
            await OutboxPublicationWorkload(
                Publisher(),
                batch_size=2,
            ).run_once()

    asyncio.run(scenario())


def test_supervised_loop_is_bounded_and_stop_aware_without_hot_polling() -> None:
    async def scenario() -> None:
        stop = asyncio.Event()

        class Workload:
            calls = 0

            async def run_once(self) -> WorkloadPassResult:
                self.calls += 1
                if self.calls == 2:
                    stop.set()
                return WorkloadPassResult(1, 1, True)

        workload = Workload()
        result = await run_workload_loop(
            "retry", workload, stop, poll_interval_seconds=10
        )
        assert result is LoopExit.STOP_REQUESTED
        assert workload.calls == 2

    asyncio.run(scenario())


def test_progression_stop_barrier_prevents_next_candidate() -> None:
    async def scenario() -> None:
        repository = CandidateRepository()
        run_ids = (uuid4(), uuid4())
        repository.active_pages = [
            DiscoveryPage(
                tuple(ActiveWorkflowRunCandidate(item, NOW) for item in run_ids), None
            )
        ]
        repository.runnable_pages = [DiscoveryPage((), None)]
        stopped = False
        calls: list[UUID] = []

        class Runs:
            async def reconcile_workflow_run(self, run_id: UUID) -> SimpleNamespace:
                nonlocal stopped
                calls.append(run_id)
                stopped = True
                return SimpleNamespace(
                    runnable_transition_count=0,
                    skipped_transition_count=0,
                    workflow_transition_count=1,
                    cancelled_transition_count=0,
                )

        class Dispatch:
            async def dispatch_task(self, run_id: UUID, task_id: UUID) -> None:
                raise AssertionError((run_id, task_id))

        result = await ProgressionDispatchWorkload(
            repository,
            Runs(),
            Dispatch(),
            batch_size=2,
            should_stop=lambda: stopped,
        ).run_once()

        assert calls == [run_ids[0]]
        assert result.candidates == 1
        assert result.transitions == 1

    asyncio.run(scenario())
