"""Focused tests for committed recovery and conditional run progression."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from taskforge.recovery.domain import ExpiredClaimCandidate
from taskforge.recovery.progression import (
    ExpiredClaimRecoveryProgressionService,
    ExpiredClaimRecoveryProgressionUnavailable,
)
from taskforge.recovery.service import (
    ExpiredClaimRecoveryOutcome,
    ExpiredClaimRecoveryReceipt,
)
from taskforge.runs.domain import (
    WorkflowRunReconciliationResult,
    WorkflowRunStatus,
)
from taskforge.runs.service import WorkflowRunServiceUnavailable


def candidate() -> ExpiredClaimCandidate:
    observed_at = datetime(2026, 8, 14, 12, tzinfo=UTC)
    return ExpiredClaimCandidate(
        uuid4(),
        uuid4(),
        uuid4(),
        1,
        1,
        uuid4(),
        observed_at - timedelta(seconds=1),
        observed_at,
    )


@dataclass
class FakeRecoveryService:
    receipt: ExpiredClaimRecoveryReceipt
    calls: list[ExpiredClaimCandidate] = field(default_factory=list)

    async def recover_expired_claim(
        self, value: ExpiredClaimCandidate
    ) -> ExpiredClaimRecoveryReceipt:
        self.calls.append(value)
        return self.receipt


@dataclass
class FakeReconciler:
    result: WorkflowRunReconciliationResult | None = None
    failure: Exception | None = None
    calls: list[UUID] = field(default_factory=list)

    async def reconcile_workflow_run(
        self, workflow_run_id: UUID
    ) -> WorkflowRunReconciliationResult:
        self.calls.append(workflow_run_id)
        if self.failure is not None:
            raise self.failure
        assert self.result is not None
        return self.result


def recovery_receipt(
    value: ExpiredClaimCandidate, outcome: ExpiredClaimRecoveryOutcome
) -> ExpiredClaimRecoveryReceipt:
    return ExpiredClaimRecoveryReceipt(
        outcome, value.task_attempt_id, value.task_run_id
    )


def reconciliation(value: ExpiredClaimCandidate) -> WorkflowRunReconciliationResult:
    return WorkflowRunReconciliationResult(
        value.workflow_run_id,
        True,
        1,
        0,
        0,
        1,
        WorkflowRunStatus.FAILED,
        True,
        False,
    )


@pytest.mark.parametrize(
    "outcome",
    [
        ExpiredClaimRecoveryOutcome.FAILED_NO_POLICY,
        ExpiredClaimRecoveryOutcome.FAILED_EXHAUSTED,
        ExpiredClaimRecoveryOutcome.ALREADY_RECOVERED,
    ],
)
def test_terminal_or_replayed_recovery_reconciles_after_recovery(
    outcome: ExpiredClaimRecoveryOutcome,
) -> None:
    value = candidate()
    recovery = FakeRecoveryService(recovery_receipt(value, outcome))
    reconciler = FakeReconciler(reconciliation(value))

    receipt = asyncio.run(
        ExpiredClaimRecoveryProgressionService(
            recovery, reconciler
        ).recover_and_progress(value)
    )

    assert recovery.calls == [value]
    assert reconciler.calls == [value.workflow_run_id]
    assert receipt.recovery.outcome is outcome
    assert receipt.reconciliation == reconciler.result


@pytest.mark.parametrize(
    "outcome",
    [
        outcome
        for outcome in ExpiredClaimRecoveryOutcome
        if outcome
        not in {
            ExpiredClaimRecoveryOutcome.FAILED_NO_POLICY,
            ExpiredClaimRecoveryOutcome.FAILED_EXHAUSTED,
            ExpiredClaimRecoveryOutcome.ALREADY_RECOVERED,
        }
    ],
)
def test_nonterminal_and_unrelated_noops_skip_reconciliation(
    outcome: ExpiredClaimRecoveryOutcome,
) -> None:
    value = candidate()
    recovery = FakeRecoveryService(recovery_receipt(value, outcome))
    reconciler = FakeReconciler()

    receipt = asyncio.run(
        ExpiredClaimRecoveryProgressionService(
            recovery, reconciler
        ).recover_and_progress(value)
    )

    assert receipt.recovery.outcome is outcome
    assert receipt.reconciliation is None
    assert reconciler.calls == []


def test_progression_failure_exposes_committed_recovery_receipt() -> None:
    value = candidate()
    committed = recovery_receipt(value, ExpiredClaimRecoveryOutcome.FAILED_EXHAUSTED)
    recovery = FakeRecoveryService(committed)
    reconciler = FakeReconciler(failure=WorkflowRunServiceUnavailable())

    with pytest.raises(ExpiredClaimRecoveryProgressionUnavailable) as captured:
        asyncio.run(
            ExpiredClaimRecoveryProgressionService(
                recovery, reconciler
            ).recover_and_progress(value)
        )

    assert captured.value.recovery_receipt is committed
    assert reconciler.calls == [value.workflow_run_id]
