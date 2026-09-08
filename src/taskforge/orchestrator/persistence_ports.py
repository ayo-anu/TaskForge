"""Read-only persistence contracts for orchestration candidate discovery."""

from __future__ import annotations

from typing import Protocol

from taskforge.orchestrator.domain import (
    ActiveWorkflowRunCandidate,
    DiscoveryCursor,
    DiscoveryHighWater,
    DiscoveryPage,
    RetryPendingTaskCandidate,
    RunnableTaskCandidate,
)


class OrchestratorCandidateRepository(Protocol):
    async def capture_active_run_high_water(self) -> DiscoveryHighWater | None: ...

    async def list_active_workflow_runs(
        self,
        *,
        high_water: DiscoveryHighWater,
        cursor: DiscoveryCursor | None,
        limit: int,
    ) -> DiscoveryPage[ActiveWorkflowRunCandidate]: ...

    async def capture_runnable_task_high_water(self) -> DiscoveryHighWater | None: ...

    async def list_runnable_tasks(
        self,
        *,
        high_water: DiscoveryHighWater,
        cursor: DiscoveryCursor | None,
        limit: int,
    ) -> DiscoveryPage[RunnableTaskCandidate]: ...

    async def capture_retry_pending_high_water(
        self,
    ) -> DiscoveryHighWater | None: ...

    async def list_retry_pending_tasks(
        self,
        *,
        high_water: DiscoveryHighWater,
        cursor: DiscoveryCursor | None,
        limit: int,
    ) -> DiscoveryPage[RetryPendingTaskCandidate]: ...
