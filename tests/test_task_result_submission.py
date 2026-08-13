from __future__ import annotations

import asyncio
import math
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from taskforge.claims.authority import TaskClaimResultAuthorityIssuer
from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.worker.result_persistence_ports import (
    PersistableTaskResult,
    PersistedTaskResult,
    PersistedTaskResultOutcome,
)
from taskforge.worker.result_submission import (
    TaskResultAuthorityRejected,
    TaskResultConflict,
    TaskResultInvalidOutput,
    TaskResultStale,
    TaskResultSubmissionOutcome,
    TaskResultSubmissionRequest,
    TaskResultSubmissionService,
    prepare_task_result,
)
from taskforge.worker.results import TaskExecutionResult


class Repository:
    def __init__(self, outcome: PersistedTaskResultOutcome) -> None:
        self.outcome = outcome
        self.received: PersistableTaskResult | None = None

    async def submit_result(self, *args: Any) -> PersistedTaskResult:
        self.received = args[-1]
        return PersistedTaskResult(self.outcome, self.received.task_attempt_id)


def request(
    result: TaskExecutionResult,
) -> tuple[
    TaskClaimResultAuthorityIssuer,
    AuthenticatedWorker,
    Any,
    TaskResultSubmissionRequest,
]:
    issuer = TaskClaimResultAuthorityIssuer(b"r" * 32)
    worker = AuthenticatedWorker(uuid4(), uuid4())
    session_id = uuid4()
    attempt_id = uuid4()
    authority = issuer.issue(
        worker_identity_id=worker.worker_identity_id,
        worker_session_id=session_id,
        task_attempt_id=attempt_id,
        generation=3,
    )
    return (
        issuer,
        worker,
        session_id,
        TaskResultSubmissionRequest(uuid4(), uuid4(), attempt_id, 3, authority, result),
    )


@pytest.mark.parametrize("claim_generation", (True, False, 0, -1, 1.0))
def test_submission_request_requires_a_positive_non_boolean_generation(
    claim_generation: object,
) -> None:
    _, _, _, valid = request(TaskExecutionResult.success(None))

    with pytest.raises(ValueError, match="claim generation must be positive"):
        TaskResultSubmissionRequest(
            valid.dispatch_id,
            valid.task_run_id,
            valid.task_attempt_id,
            claim_generation,  # type: ignore[arg-type]
            valid.result_authority,
            valid.result,
        )


def test_service_verifies_authority_and_maps_committed_outcomes() -> None:
    issuer, worker, session_id, submission = request(TaskExecutionResult.success(None))
    repository = Repository(PersistedTaskResultOutcome.ACCEPTED)
    receipt = asyncio.run(
        TaskResultSubmissionService(repository, issuer).submit_result(
            worker, session_id, submission
        )
    )
    assert receipt.outcome is TaskResultSubmissionOutcome.ACCEPTED
    assert repository.received is not None

    with pytest.raises(TaskResultAuthorityRejected):
        asyncio.run(
            TaskResultSubmissionService(repository, issuer).submit_result(
                worker, uuid4(), submission
            )
        )


@pytest.mark.parametrize(
    ("outcome", "error"),
    (
        (PersistedTaskResultOutcome.CONFLICT_REJECTED, TaskResultConflict),
        (PersistedTaskResultOutcome.STALE_REJECTED, TaskResultStale),
    ),
)
def test_service_converts_only_committed_repository_rejections(
    outcome: PersistedTaskResultOutcome, error: type[Exception]
) -> None:
    issuer, worker, session_id, submission = request(TaskExecutionResult.success(None))
    with pytest.raises(error):
        asyncio.run(
            TaskResultSubmissionService(Repository(outcome), issuer).submit_result(
                worker, session_id, submission
            )
        )


def test_output_is_copied_and_mapping_order_has_canonical_fingerprint() -> None:
    original = {"b": [2, {"nested": True}], "a": 1}
    _, _, _, first_request = request(TaskExecutionResult.success(original))
    first = prepare_task_result(first_request)
    original["b"][1]["nested"] = False  # type: ignore[index]

    _, _, _, second_request = request(
        TaskExecutionResult.success({"a": 1, "b": [2, {"nested": True}]})
    )
    second_request = TaskResultSubmissionRequest(
        first_request.dispatch_id,
        first_request.task_run_id,
        first_request.task_attempt_id,
        first_request.claim_generation,
        first_request.result_authority,
        second_request.result,
    )
    second = prepare_task_result(second_request)

    assert first.output == {"a": 1, "b": [2, {"nested": True}]}
    assert first.result_fingerprint == second.result_fingerprint
    assert "nested" not in repr(first)


class HostileDict(dict[str, object]):
    def items(self) -> Any:
        raise AssertionError("custom mapping behavior executed")


@pytest.mark.parametrize(
    "value",
    (
        math.nan,
        math.inf,
        b"bytes",
        uuid4(),
        datetime.now(UTC),
        {"set"},
        (item for item in (1,)),
        HostileDict(value=1),
        {1: "non-string-key"},
    ),
)
def test_non_json_and_custom_values_are_rejected(value: object) -> None:
    _, _, _, submission = request(TaskExecutionResult.success(value))
    with pytest.raises(TaskResultInvalidOutput):
        prepare_task_result(submission)


def test_recursive_output_is_rejected() -> None:
    value: list[object] = []
    value.append(value)
    _, _, _, submission = request(TaskExecutionResult.success(value))
    with pytest.raises(TaskResultInvalidOutput, match="recursive"):
        prepare_task_result(submission)


def test_canonical_output_bound_is_exactly_sixteen_kibibytes() -> None:
    exact = ["x" * 4096, "x" * 4096, "x" * 4096, "x" * 4083]
    _, _, _, accepted = request(TaskExecutionResult.success(exact))
    assert prepare_task_result(accepted).output == exact

    over = ["x" * 4096, "x" * 4096, "x" * 4096, "x" * 4084]
    _, _, _, rejected = request(TaskExecutionResult.success(over))
    with pytest.raises(TaskResultInvalidOutput, match="too large"):
        prepare_task_result(rejected)


def test_success_none_and_distinct_output_content_are_fingerprinted() -> None:
    _, _, _, null_request = request(TaskExecutionResult.success(None))
    _, _, _, value_request = request(TaskExecutionResult.success(False))
    null_result = prepare_task_result(null_request)
    value_result = prepare_task_result(value_request)
    assert null_result.output is None
    assert null_result.result_fingerprint != value_result.result_fingerprint
