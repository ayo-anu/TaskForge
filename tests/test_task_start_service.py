from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

import pytest

from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.worker.start import (
    TaskStartInvariantError,
    TaskStartOutcome,
    TaskStartRejected,
    TaskStartRequest,
    TaskStartService,
    TaskStartServiceUnavailable,
)
from taskforge.worker.start_persistence_ports import (
    TaskStartAuthorityRejected,
    TaskStartClaimStale,
    TaskStartInvariantViolation,
    TaskStartPersistenceUnavailable,
    TaskStartSessionRejected,
)


class Repository:
    def __init__(self, result: bool = True, error: Exception | None = None) -> None:
        self.result = result
        self.error = error
        self.call: tuple[Any, ...] | None = None

    async def start_task(self, *args: Any) -> bool:
        self.call = args
        if self.error is not None:
            raise self.error
        return self.result


def test_start_service_forwards_claim_identity_and_reports_replay() -> None:
    worker = AuthenticatedWorker(uuid4(), uuid4())
    session_id = uuid4()
    request = TaskStartRequest(uuid4(), uuid4(), 3)
    repository = Repository(result=False)

    outcome = asyncio.run(
        TaskStartService(repository).start_task(worker, session_id, request)
    )

    assert outcome is TaskStartOutcome.REPLAYED_RUNNING
    assert repository.call == (
        worker,
        session_id,
        request.task_run_id,
        request.task_attempt_id,
        3,
    )


@pytest.mark.parametrize(
    ("persistence_error", "service_error"),
    (
        (TaskStartAuthorityRejected(), TaskStartRejected),
        (TaskStartSessionRejected(), TaskStartRejected),
        (TaskStartClaimStale(), TaskStartRejected),
        (TaskStartInvariantViolation(), TaskStartInvariantError),
        (TaskStartPersistenceUnavailable(), TaskStartServiceUnavailable),
    ),
)
def test_start_service_translates_typed_failures(
    persistence_error: Exception, service_error: type[Exception]
) -> None:
    service = TaskStartService(Repository(error=persistence_error))
    with pytest.raises(service_error) as raised:
        asyncio.run(
            service.start_task(
                AuthenticatedWorker(uuid4(), uuid4()),
                uuid4(),
                TaskStartRequest(uuid4(), uuid4(), 1),
            )
        )
    assert raised.value.__cause__ is persistence_error
