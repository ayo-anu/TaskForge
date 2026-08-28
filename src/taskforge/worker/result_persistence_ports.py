"""Persistence boundary for authoritative task result submission."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol
from uuid import UUID

from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.worker.results import (
    TaskExecutionFailureKind,
    TaskExecutionResultKind,
)
from taskforge.workflows.task_types import JSONValue


class PersistedTaskResultOutcome(StrEnum):
    ACCEPTED = "accepted"
    REPLAYED_IDENTICAL = "replayed_identical"
    CONFLICT_REJECTED = "conflict_rejected"
    STALE_REJECTED = "stale_rejected"


class TaskResultPersistenceNotFound(Exception): ...


class TaskResultPersistenceInvalidState(Exception): ...


class TaskResultPersistenceAuthorityRejected(Exception): ...


class TaskResultPersistenceInvariantViolation(Exception): ...


class TaskResultPersistenceUnavailable(Exception): ...


@dataclass(frozen=True, repr=False)
class PersistableTaskResult:
    dispatch_id: UUID
    task_run_id: UUID
    task_attempt_id: UUID
    claim_generation: int
    result_kind: TaskExecutionResultKind
    failure_kind: TaskExecutionFailureKind | None
    output: JSONValue
    result_fingerprint: str
    correlation_id: str | None = None

    def __repr__(self) -> str:
        return (
            "PersistableTaskResult("
            f"dispatch_id={self.dispatch_id!r}, task_run_id={self.task_run_id!r}, "
            f"task_attempt_id={self.task_attempt_id!r}, "
            f"claim_generation={self.claim_generation!r}, "
            f"result_kind={self.result_kind!r}, failure_kind={self.failure_kind!r}, "
            "output=<redacted>, result_fingerprint=<redacted>)"
        )


@dataclass(frozen=True)
class PersistedTaskResult:
    outcome: PersistedTaskResultOutcome
    task_attempt_id: UUID
    dead_letter_created: bool = False


class TaskResultRepository(Protocol):
    async def submit_result(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        result: PersistableTaskResult,
    ) -> PersistedTaskResult: ...
