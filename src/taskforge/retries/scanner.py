"""Bounded horizontally safe dispatch of due retry attempts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from uuid import UUID, uuid4

from taskforge.dispatch.envelope import (
    DispatchEnvelopeValidationError,
    create_dispatch_envelope,
    deserialize_dispatch_envelope,
    dispatch_envelope_to_mapping,
)
from taskforge.retries.persistence_ports import (
    DueRetryDispatchRepository,
    DueRetryPersistenceInvariantViolation,
    DueRetryPersistenceUnavailable,
    PreparedDueRetryDispatch,
    SkippedDueRetryCandidate,
)
from taskforge.workflows.task_types import TaskTypeRegistry

MAX_DUE_RETRY_BATCH_SIZE = 100


class DueRetryScanInvariantError(Exception):
    """A due retry cannot be dispatched from its durable state."""


class DueRetryScanServiceUnavailable(Exception):
    """Due-retry persistence is operationally unavailable."""


@dataclass(frozen=True)
class DueRetryScanResult:
    examined: int
    dispatched_attempt_ids: tuple[UUID, ...]
    skipped: int

    def __post_init__(self) -> None:
        if self.examined != self.dispatched + self.skipped:
            raise ValueError("examined retries must equal dispatched plus skipped")

    @property
    def dispatched(self) -> int:
        return len(self.dispatched_attempt_ids)


class DueRetryScanner:
    def __init__(
        self,
        repository: DueRetryDispatchRepository,
        task_types: TaskTypeRegistry,
    ) -> None:
        self._repository = repository
        self._task_types = task_types

    async def scan_due_retries(self, *, batch_size: int) -> DueRetryScanResult:
        if (
            type(batch_size) is not int
            or not 1 <= batch_size <= MAX_DUE_RETRY_BATCH_SIZE
        ):
            raise ValueError("due retry batch size is outside the supported bounds")

        examined = skipped = 0
        dispatched_attempt_ids: list[UUID] = []
        try:
            for _ in range(batch_size):
                dispatched_attempt_id: UUID | None = None
                async with self._repository.due_dispatch_transaction() as transaction:
                    prepared = await transaction.prepare_next_due()
                    if prepared is None:
                        break
                    examined += 1
                    if isinstance(prepared, SkippedDueRetryCandidate):
                        skipped += 1
                        continue

                    outbox_id, route, payload = self._prepare_outbox(prepared)
                    await transaction.persist_dispatch(
                        prepared, outbox_id, route, payload
                    )
                    dispatched_attempt_id = prepared.task_attempt_id
                assert dispatched_attempt_id is not None
                dispatched_attempt_ids.append(dispatched_attempt_id)
        except DueRetryPersistenceInvariantViolation as error:
            raise DueRetryScanInvariantError from error
        except DueRetryPersistenceUnavailable as error:
            raise DueRetryScanServiceUnavailable from error

        return DueRetryScanResult(examined, tuple(dispatched_attempt_ids), skipped)

    def _prepare_outbox(
        self, prepared: PreparedDueRetryDispatch
    ) -> tuple[UUID, str, dict[str, object]]:
        definition = self._task_types.definition(prepared.task_type)
        if definition is None:
            raise DueRetryScanInvariantError
        try:
            encoded_predecessor = json.dumps(
                prepared.predecessor_payload,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            predecessor = deserialize_dispatch_envelope(encoded_predecessor)
            predecessor_mapping = dispatch_envelope_to_mapping(predecessor)
            if (
                predecessor.dispatch_id != prepared.predecessor_dispatch_id
                or predecessor.task_attempt_id != prepared.predecessor_attempt_id
                or predecessor.task_run_id != prepared.task_run_id
                or predecessor.workflow_run_id != prepared.workflow_run_id
                or predecessor.attempt_number != prepared.predecessor_attempt_number
                or predecessor.task_type != prepared.task_type
                or predecessor.route != prepared.predecessor_route
                or predecessor_mapping["task_payload"] != prepared.task_parameters
                or predecessor.deadline_at != prepared.deadline_at
                or predecessor.execution_timeout_seconds
                != prepared.execution_timeout_seconds
            ):
                raise DueRetryScanInvariantError
            dispatch_id = uuid4()
            envelope = create_dispatch_envelope(
                dispatch_id=dispatch_id,
                task_attempt_id=prepared.task_attempt_id,
                task_run_id=prepared.task_run_id,
                workflow_run_id=prepared.workflow_run_id,
                attempt_number=prepared.attempt_number,
                task_type=prepared.task_type,
                required_capability=definition.required_capability,
                task_payload=prepared.task_parameters,
                references=predecessor_mapping["references"],
                deadline_at=prepared.deadline_at,
                execution_timeout_seconds=prepared.execution_timeout_seconds,
                correlation_id=predecessor.correlation_id,
                trace_context=predecessor.trace_context,
            )
        except (DispatchEnvelopeValidationError, TypeError, ValueError) as error:
            raise DueRetryScanInvariantError from error
        return dispatch_id, envelope.route, dispatch_envelope_to_mapping(envelope)
