"""Persistence contracts for atomic task dispatch creation."""

from __future__ import annotations

from dataclasses import dataclass
from types import TracebackType
from typing import Protocol
from uuid import UUID

from taskforge.workflows.task_types import JSONMapping


class TaskDispatchPersistenceUnavailable(Exception):
    """Task dispatch persistence is operationally unavailable."""


class TaskDispatchPersistenceConflict(Exception):
    """A database invariant rejected task dispatch persistence."""


class TaskDispatchStateConflict(Exception):
    """The prepared runnable task lost its final guarded transition."""


@dataclass(frozen=True, repr=False)
class PreparedTaskDispatch:
    workflow_run_id: UUID
    task_run_id: UUID
    workflow_version_id: UUID
    step_identifier: str
    task_type: str
    task_parameters: JSONMapping
    attempt_number: int

    def __repr__(self) -> str:
        return (
            "PreparedTaskDispatch("
            f"workflow_run_id={self.workflow_run_id!r}, "
            f"task_run_id={self.task_run_id!r}, "
            f"workflow_version_id={self.workflow_version_id!r}, "
            f"step_identifier={self.step_identifier!r}, "
            f"task_type={self.task_type!r}, task_parameters=<redacted>, "
            f"attempt_number={self.attempt_number!r})"
        )


@dataclass(frozen=True)
class NewTaskAttempt:
    id: UUID
    task_run_id: UUID
    attempt_number: int


@dataclass(frozen=True, repr=False)
class NewTaskDispatchOutbox:
    id: UUID
    task_attempt_id: UUID
    route: str
    payload: dict[str, object]

    def __repr__(self) -> str:
        return (
            "NewTaskDispatchOutbox("
            f"id={self.id!r}, task_attempt_id={self.task_attempt_id!r}, "
            f"route={self.route!r}, payload=<redacted>)"
        )


class TaskDispatchTransaction(Protocol):
    async def prepare_dispatch(
        self, workflow_run_id: UUID, task_run_id: UUID
    ) -> PreparedTaskDispatch | None: ...

    async def persist_dispatch(
        self,
        prepared: PreparedTaskDispatch,
        attempt: NewTaskAttempt,
        outbox: NewTaskDispatchOutbox,
    ) -> None: ...

    async def commit(self) -> None: ...


class TaskDispatchTransactionContext(Protocol):
    async def __aenter__(self) -> TaskDispatchTransaction: ...

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...


class TaskDispatchRepository(Protocol):
    def dispatch_transaction(self) -> TaskDispatchTransactionContext: ...
