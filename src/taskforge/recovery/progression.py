"""Compose committed claim recovery with conditional run reconciliation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from taskforge.recovery.domain import ExpiredClaimCandidate
from taskforge.recovery.service import (
    ExpiredClaimRecoveryOutcome,
    ExpiredClaimRecoveryReceipt,
)
from taskforge.runs.domain import WorkflowRunReconciliationResult
from taskforge.runs.service import WorkflowRunServiceUnavailable


class WorkflowRunReconciler(Protocol):
    async def reconcile_workflow_run(
        self, workflow_run_id: UUID
    ) -> WorkflowRunReconciliationResult: ...


class ExpiredClaimRecoverer(Protocol):
    async def recover_expired_claim(
        self, candidate: ExpiredClaimCandidate
    ) -> ExpiredClaimRecoveryReceipt: ...


@dataclass(frozen=True)
class ExpiredClaimRecoveryProgressionReceipt:
    recovery: ExpiredClaimRecoveryReceipt
    reconciliation: WorkflowRunReconciliationResult | None


class ExpiredClaimRecoveryProgressionUnavailable(Exception):
    """Recovery committed, but subsequent run reconciliation was unavailable."""

    def __init__(self, recovery_receipt: ExpiredClaimRecoveryReceipt) -> None:
        self.recovery_receipt = recovery_receipt
        super().__init__("claim recovery committed but run progression failed")


class ExpiredClaimRecoveryProgressionService:
    _RECONCILE_OUTCOMES = frozenset(
        {
            ExpiredClaimRecoveryOutcome.FAILED_NO_POLICY,
            ExpiredClaimRecoveryOutcome.FAILED_EXHAUSTED,
            ExpiredClaimRecoveryOutcome.ALREADY_RECOVERED,
        }
    )

    def __init__(
        self,
        recovery: ExpiredClaimRecoverer,
        reconciler: WorkflowRunReconciler,
    ) -> None:
        self._recovery = recovery
        self._reconciler = reconciler

    async def recover_and_progress(
        self, candidate: ExpiredClaimCandidate
    ) -> ExpiredClaimRecoveryProgressionReceipt:
        recovery = await self._recovery.recover_expired_claim(candidate)
        if recovery.outcome not in self._RECONCILE_OUTCOMES:
            return ExpiredClaimRecoveryProgressionReceipt(recovery, None)
        try:
            reconciliation = await self._reconciler.reconcile_workflow_run(
                candidate.workflow_run_id
            )
        except WorkflowRunServiceUnavailable as error:
            raise ExpiredClaimRecoveryProgressionUnavailable(recovery) from error
        return ExpiredClaimRecoveryProgressionReceipt(recovery, reconciliation)
