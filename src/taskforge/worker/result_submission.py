"""Application service for claim-bound authoritative task results."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from enum import StrEnum
from typing import cast
from uuid import UUID, uuid4

from taskforge.audit.domain import (
    AuditActor,
    AuditActorKind,
    AuditOutcome,
    AuditRecord,
    AuditRejected,
)
from taskforge.claims.authority import TaskClaimResultAuthorityIssuer
from taskforge.claims.domain import TaskClaimResultAuthority
from taskforge.correlation import is_valid_correlation_id
from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.persistence.audit import RejectedAuditRecorder
from taskforge.worker.result_persistence_ports import (
    PersistableTaskResult,
    PersistedTaskResultOutcome,
    TaskResultPersistenceAuthorityRejected,
    TaskResultPersistenceInvalidState,
    TaskResultPersistenceInvariantViolation,
    TaskResultPersistenceNotFound,
    TaskResultPersistenceUnavailable,
    TaskResultRepository,
)
from taskforge.worker.results import (
    TaskExecutionResult,
    TaskExecutionResultKind,
    task_result_fingerprint,
)
from taskforge.workflows.task_types import (
    MAX_COLLECTION_ITEMS,
    MAX_PARAMETER_DEPTH,
    MAX_PARAMETER_KEY_LENGTH,
    MAX_PARAMETER_NODES,
    MAX_PARAMETER_STRING_LENGTH,
    JSONValue,
)

MAX_TASK_RESULT_OUTPUT_BYTES = 16 * 1024


class TaskResultSubmissionOutcome(StrEnum):
    ACCEPTED = "accepted"
    REPLAYED_IDENTICAL = "replayed_identical"


class TaskResultAuthorityRejected(Exception): ...


class TaskResultStale(Exception): ...


class TaskResultConflict(Exception): ...


class TaskResultNotFound(Exception): ...


class TaskResultInvalidState(Exception): ...


class TaskResultInvalidOutput(ValueError): ...


class TaskResultInvariantError(Exception): ...


class TaskResultServiceUnavailable(Exception): ...


_RESULT_AUDIT_REASONS: dict[type[Exception], str] = {
    TaskResultAuthorityRejected: "worker_authority_rejected",
    TaskResultNotFound: "result_target_not_found",
    TaskResultInvalidState: "invalid_task_state",
    TaskResultInvalidOutput: "result_invalid_output",
}


@dataclass(frozen=True, repr=False)
class TaskResultSubmissionRequest:
    dispatch_id: UUID
    task_run_id: UUID
    task_attempt_id: UUID
    claim_generation: int
    result_authority: TaskClaimResultAuthority
    result: TaskExecutionResult
    correlation_id: str | None = None

    def __post_init__(self) -> None:
        if type(self.claim_generation) is not int or self.claim_generation <= 0:
            raise ValueError("claim generation must be positive")
        if not is_valid_correlation_id(self.correlation_id):
            raise ValueError("result correlation ID is invalid")

    def __repr__(self) -> str:
        return (
            "TaskResultSubmissionRequest("
            f"dispatch_id={self.dispatch_id!r}, task_run_id={self.task_run_id!r}, "
            f"task_attempt_id={self.task_attempt_id!r}, "
            f"claim_generation={self.claim_generation!r}, "
            "result_authority=<redacted>, result=<redacted>)"
        )


@dataclass(frozen=True)
class TaskResultSubmissionReceipt:
    outcome: TaskResultSubmissionOutcome
    task_attempt_id: UUID


class TaskResultSubmissionService:
    def __init__(
        self,
        repository: TaskResultRepository,
        authority_issuer: TaskClaimResultAuthorityIssuer,
        rejected_audit: RejectedAuditRecorder | None = None,
    ) -> None:
        self._repository = repository
        self._authority_issuer = authority_issuer
        self._rejected_audit = rejected_audit

    async def submit_result(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        request: TaskResultSubmissionRequest,
    ) -> TaskResultSubmissionReceipt:
        if not self._authority_issuer.verify(
            request.result_authority,
            worker_identity_id=authenticated_worker.worker_identity_id,
            worker_session_id=worker_session_id,
            task_attempt_id=request.task_attempt_id,
            generation=request.claim_generation,
        ):
            error = TaskResultAuthorityRejected()
            await self._audit_rejection(
                authenticated_worker, worker_session_id, request, error
            )
            raise error
        try:
            persistable = prepare_task_result(request)
        except TaskResultInvalidOutput as error:
            await self._audit_rejection(
                authenticated_worker, worker_session_id, request, error
            )
            raise
        try:
            persisted = await self._repository.submit_result(
                authenticated_worker, worker_session_id, persistable
            )
        except TaskResultPersistenceNotFound as error:
            rejection: Exception = TaskResultNotFound()
            await self._audit_rejection(
                authenticated_worker, worker_session_id, request, rejection
            )
            raise rejection from error
        except TaskResultPersistenceInvalidState as error:
            rejection = TaskResultInvalidState()
            await self._audit_rejection(
                authenticated_worker, worker_session_id, request, rejection
            )
            raise rejection from error
        except TaskResultPersistenceAuthorityRejected as error:
            rejection = TaskResultAuthorityRejected()
            await self._audit_rejection(
                authenticated_worker, worker_session_id, request, rejection
            )
            raise rejection from error
        except TaskResultPersistenceInvariantViolation as error:
            raise TaskResultInvariantError from error
        except TaskResultPersistenceUnavailable as error:
            raise TaskResultServiceUnavailable from error
        if persisted.outcome is PersistedTaskResultOutcome.CONFLICT_REJECTED:
            raise TaskResultConflict
        if persisted.outcome is PersistedTaskResultOutcome.STALE_REJECTED:
            raise TaskResultStale
        return TaskResultSubmissionReceipt(
            TaskResultSubmissionOutcome(persisted.outcome.value),
            persisted.task_attempt_id,
        )

    async def _audit_rejection(
        self,
        worker: AuthenticatedWorker,
        worker_session_id: UUID,
        request: TaskResultSubmissionRequest,
        error: Exception,
    ) -> None:
        if self._rejected_audit is None:
            return
        try:
            await self._rejected_audit.record(
                AuditRecord(
                    uuid4(),
                    AuditActor(
                        AuditActorKind.WORKER,
                        worker_identity_id=worker.worker_identity_id,
                        worker_session_id=worker_session_id,
                    ),
                    "task_result.submit",
                    AuditOutcome.REJECTED,
                    "task_attempt",
                    request.task_attempt_id,
                    request.correlation_id,
                    {"claim_generation": request.claim_generation},
                    _RESULT_AUDIT_REASONS[type(error)],
                )
            )
        except AuditRejected as audit_error:
            raise TaskResultServiceUnavailable from audit_error


def prepare_task_result(
    request: TaskResultSubmissionRequest,
) -> PersistableTaskResult:
    output: JSONValue = None
    if request.result.kind is TaskExecutionResultKind.SUCCESS:
        output = _copy_json_value(request.result.value)
        output_bytes = _canonical_json(output)
        if len(output_bytes) > MAX_TASK_RESULT_OUTPUT_BYTES:
            raise TaskResultInvalidOutput("task result output is too large")
    fingerprint = task_result_fingerprint(
        result_kind=request.result.kind,
        failure_kind=request.result.failure_kind,
        output=output,
    )
    return PersistableTaskResult(
        request.dispatch_id,
        request.task_run_id,
        request.task_attempt_id,
        request.claim_generation,
        request.result.kind,
        request.result.failure_kind,
        output,
        fingerprint,
        request.correlation_id,
    )


def _copy_json_value(value: object) -> JSONValue:
    active: set[int] = set()
    node_count = 0

    def visit(item: object, depth: int) -> JSONValue:
        nonlocal node_count
        node_count += 1
        if node_count > MAX_PARAMETER_NODES:
            raise TaskResultInvalidOutput("task result output is too complex")
        if depth > MAX_PARAMETER_DEPTH:
            raise TaskResultInvalidOutput("task result output is too deep")
        if item is None or type(item) in (bool, int, str):
            if type(item) is str and len(item) > MAX_PARAMETER_STRING_LENGTH:
                raise TaskResultInvalidOutput("task result string is too large")
            return item  # type: ignore[return-value]
        if type(item) is float:
            if not math.isfinite(item):
                raise TaskResultInvalidOutput("task result number must be finite")
            return item
        if type(item) not in (dict, list, tuple):
            raise TaskResultInvalidOutput("task result is not JSON compatible")
        container = cast(dict[object, object] | list[object] | tuple[object, ...], item)
        identity = id(item)
        if identity in active:
            raise TaskResultInvalidOutput("task result output is recursive")
        if len(container) > MAX_COLLECTION_ITEMS:
            raise TaskResultInvalidOutput("task result output is too complex")
        active.add(identity)
        try:
            if type(item) is dict:
                copied: dict[str, JSONValue] = {}
                mapping = cast(dict[object, object], container)
                for key, child in mapping.items():
                    if type(key) is not str:
                        raise TaskResultInvalidOutput(
                            "task result object keys must be strings"
                        )
                    if len(key) > MAX_PARAMETER_KEY_LENGTH:
                        raise TaskResultInvalidOutput("task result key is too large")
                    copied[key] = visit(child, depth + 1)
                return copied
            return [visit(child, depth + 1) for child in container]
        finally:
            active.remove(identity)

    return visit(value, 0)


def _canonical_json(value: JSONValue) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError) as error:
        raise TaskResultInvalidOutput("task result is not canonical JSON") from error
