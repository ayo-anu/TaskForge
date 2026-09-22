"""Bounded pass drivers for the production orchestrator process."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from time import perf_counter
from typing import Any, Protocol

from taskforge.dispatch.service import TaskDispatchNotEligible
from taskforge.logging import log_event
from taskforge.metrics import add as add_metric
from taskforge.metrics import record as record_metric
from taskforge.orchestrator.domain import (
    DiscoveryCursor,
    DiscoveryHighWater,
    DiscoveryPage,
    LoopExit,
    OrchestratorWorkloadInvariantError,
    WorkloadPassResult,
)
from taskforge.orchestrator.persistence_ports import OrchestratorCandidateRepository
from taskforge.recovery.domain import (
    ExpiredClaimScanCursor,
    StaleWorkerSessionScanCursor,
)
from taskforge.recovery.progression import ExpiredClaimRecoveryProgressionService
from taskforge.recovery.scanner import RecoveryCandidateScanner
from taskforge.recovery.service import StaleWorkerSessionRecoveryService

logger = logging.getLogger(__name__)


class BoundedWorkload(Protocol):
    async def run_once(self) -> WorkloadPassResult: ...


class WorkflowRunReconciler(Protocol):
    async def reconcile_workflow_run(self, workflow_run_id: Any) -> Any: ...


class RunnableTaskDispatcher(Protocol):
    async def dispatch_task(self, workflow_run_id: Any, task_run_id: Any) -> Any: ...


class RetryTransitioner(Protocol):
    async def transition_retry(self, task_run_id: Any) -> Any: ...


class DueRetryDispatcher(Protocol):
    async def scan_due_retries(
        self, *, batch_size: int, should_stop: Callable[[], bool] | None = None
    ) -> Any: ...


class OutboxPublisher(Protocol):
    async def reconcile_unpublished(
        self,
        *,
        page_size: int,
        pass_limit: int,
        should_stop: Callable[[], bool] | None = None,
    ) -> Any: ...


class _KeysetSweep[T]:
    def __init__(
        self,
        capture: Callable[[], Awaitable[DiscoveryHighWater | None]],
        page: Callable[
            [DiscoveryHighWater, DiscoveryCursor | None, int],
            Awaitable[DiscoveryPage[T]],
        ],
    ) -> None:
        self._capture = capture
        self._page = page
        self.high_water: DiscoveryHighWater | None = None
        self.cursor: DiscoveryCursor | None = None

    async def next_page(self, limit: int) -> DiscoveryPage[T]:
        if self.high_water is None:
            self.high_water = await self._capture()
            self.cursor = None
            if self.high_water is None:
                return DiscoveryPage((), None)
        page = await self._page(self.high_water, self.cursor, limit)
        if page.next_cursor is None:
            self.high_water = None
            self.cursor = None
        else:
            self.cursor = page.next_cursor
        return page


class ProgressionDispatchWorkload:
    """Reconcile active runs and dispatch one page of first attempts."""

    def __init__(
        self,
        candidates: OrchestratorCandidateRepository,
        runs: WorkflowRunReconciler,
        dispatch: RunnableTaskDispatcher,
        *,
        batch_size: int,
        should_stop: Callable[[], bool] | None = None,
    ) -> None:
        self._runs = runs
        self._dispatch = dispatch
        self._batch_size = batch_size
        self._should_stop = should_stop or (lambda: False)
        self._active = _KeysetSweep(
            candidates.capture_active_run_high_water,
            lambda high, cursor, limit: candidates.list_active_workflow_runs(
                high_water=high, cursor=cursor, limit=limit
            ),
        )
        self._runnable = _KeysetSweep(
            candidates.capture_runnable_task_high_water,
            lambda high, cursor, limit: candidates.list_runnable_tasks(
                high_water=high, cursor=cursor, limit=limit
            ),
        )

    async def run_once(self) -> WorkloadPassResult:
        active = await self._active.next_page(self._batch_size)
        runnable = await self._runnable.next_page(self._batch_size)
        transitions = examined = 0
        for active_candidate in active.items:
            if self._should_stop():
                break
            examined += 1
            result = await self._runs.reconcile_workflow_run(
                active_candidate.workflow_run_id
            )
            transitions += (
                result.runnable_transition_count
                + result.skipped_transition_count
                + result.workflow_transition_count
                + result.cancelled_transition_count
            )
        for runnable_candidate in runnable.items:
            if self._should_stop():
                break
            examined += 1
            try:
                await self._dispatch.dispatch_task(
                    runnable_candidate.workflow_run_id,
                    runnable_candidate.task_run_id,
                )
            except TaskDispatchNotEligible:
                continue
            transitions += 1
        return WorkloadPassResult(
            examined,
            transitions,
            active.next_cursor is not None or runnable.next_cursor is not None,
        )


class RetryWorkload:
    """Transition retry-pending tasks and dispatch database-due retries."""

    def __init__(
        self,
        candidates: OrchestratorCandidateRepository,
        transitions: RetryTransitioner,
        due: DueRetryDispatcher,
        *,
        batch_size: int,
        should_stop: Callable[[], bool] | None = None,
    ) -> None:
        self._transitions = transitions
        self._due = due
        self._batch_size = batch_size
        self._should_stop = should_stop or (lambda: False)
        self._stop_aware = should_stop is not None
        self._pending = _KeysetSweep(
            candidates.capture_retry_pending_high_water,
            lambda high, cursor, limit: candidates.list_retry_pending_tasks(
                high_water=high, cursor=cursor, limit=limit
            ),
        )

    async def run_once(self) -> WorkloadPassResult:
        pending = await self._pending.next_page(self._batch_size)
        changed = examined = 0
        for candidate in pending.items:
            if self._should_stop():
                break
            examined += 1
            receipt = await self._transitions.transition_retry(candidate.task_run_id)
            changed += int(
                receipt.outcome.value not in {"not_eligible", "already_scheduled"}
            )
        due = (
            await self._due.scan_due_retries(
                batch_size=self._batch_size, should_stop=self._should_stop
            )
            if self._stop_aware
            else await self._due.scan_due_retries(batch_size=self._batch_size)
        )
        return WorkloadPassResult(
            examined + due.examined,
            changed + due.dispatched,
            pending.next_cursor is not None or due.examined == self._batch_size,
        )


class RecoveryWorkload:
    """Recover one bounded expired-claim and stale-session page per turn."""

    def __init__(
        self,
        candidates: RecoveryCandidateScanner,
        expired: ExpiredClaimRecoveryProgressionService,
        stale: StaleWorkerSessionRecoveryService,
        *,
        batch_size: int,
        stale_after_seconds: int,
        should_stop: Callable[[], bool] | None = None,
    ) -> None:
        self._candidates = candidates
        self._expired = expired
        self._stale = stale
        self._batch_size = batch_size
        self._stale_after_seconds = stale_after_seconds
        self._should_stop = should_stop or (lambda: False)
        self._expired_cursor: ExpiredClaimScanCursor | None = None
        self._stale_cursor: StaleWorkerSessionScanCursor | None = None

    async def run_once(self) -> WorkloadPassResult:
        expired = await self._candidates.scan_expired_claims(
            limit=self._batch_size, cursor=self._expired_cursor
        )
        stale = await self._candidates.scan_stale_worker_sessions(
            limit=self._batch_size, cursor=self._stale_cursor
        )
        self._expired_cursor = expired.next_cursor
        self._stale_cursor = stale.next_cursor
        changed = examined = 0
        for expired_candidate in expired.items:
            if self._should_stop():
                break
            examined += 1
            expired_receipt = await self._expired.recover_and_progress(
                expired_candidate
            )
            changed += int(expired_receipt.recovery.recovered_at is not None)
        for stale_candidate in stale.items:
            if self._should_stop():
                break
            examined += 1
            stale_receipt = await self._stale.end_stale_session(
                stale_candidate, stale_after_seconds=self._stale_after_seconds
            )
            changed += int(stale_receipt.ended_at is not None)
        return WorkloadPassResult(
            examined,
            changed,
            self._expired_cursor is not None or self._stale_cursor is not None,
        )


class OutboxPublicationWorkload:
    def __init__(
        self,
        publisher: OutboxPublisher,
        *,
        batch_size: int,
        should_stop: Callable[[], bool] | None = None,
    ) -> None:
        self._publisher = publisher
        self._batch_size = batch_size
        self._should_stop = should_stop or (lambda: False)
        self._stop_aware = should_stop is not None

    async def run_once(self) -> WorkloadPassResult:
        result = (
            await self._publisher.reconcile_unpublished(
                page_size=self._batch_size,
                pass_limit=self._batch_size,
                should_stop=self._should_stop,
            )
            if self._stop_aware
            else await self._publisher.reconcile_unpublished(
                page_size=self._batch_size,
                pass_limit=self._batch_size,
            )
        )
        if result.durable_invalid:
            raise OrchestratorWorkloadInvariantError(
                "unpublished dispatch contains invalid durable data"
            )
        return WorkloadPassResult(
            result.examined,
            result.acknowledged,
            result.pass_limit_reached,
        )


async def run_workload_loop(
    name: str,
    workload: BoundedWorkload,
    stop_scheduling: asyncio.Event,
    *,
    poll_interval_seconds: float,
) -> LoopExit:
    """Run bounded passes until ordinary stop or process failure is requested."""
    while not stop_scheduling.is_set():
        started = perf_counter()
        outcome = "completed"
        try:
            result = await workload.run_once()
        except BaseException:
            outcome = "failed"
            _record_pass(name, outcome, started)
            raise
        add_metric(
            "taskforge.orchestrator.candidates",
            result.candidates,
            {"taskforge.workload": name},
        )
        add_metric(
            "taskforge.orchestrator.transitions",
            result.transitions,
            {"taskforge.workload": name},
        )
        _record_pass(name, outcome, started)
        log_event(
            logger,
            logging.INFO,
            "orchestrator.pass.completed",
            {
                "workload": name,
                "candidates": result.candidates,
                "transitions": result.transitions,
                "has_more": result.has_more,
            },
        )
        if stop_scheduling.is_set():
            break
        if result.has_more or result.transitions:
            await asyncio.sleep(0)
            continue
        try:
            await asyncio.wait_for(
                stop_scheduling.wait(), timeout=poll_interval_seconds
            )
        except TimeoutError:
            pass
    return LoopExit.STOP_REQUESTED


def _record_pass(name: str, outcome: str, started: float) -> None:
    attributes = {"taskforge.workload": name, "taskforge.outcome": outcome}
    add_metric("taskforge.orchestrator.passes", attributes=attributes)
    record_metric(
        "taskforge.orchestrator.pass.duration", perf_counter() - started, attributes
    )
