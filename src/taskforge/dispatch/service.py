"""Application service for state-guarded durable task dispatch creation."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID, uuid4

from taskforge.dispatch.envelope import (
    DispatchEnvelopeValidationError,
    create_dispatch_envelope,
    dispatch_envelope_to_mapping,
)
from taskforge.dispatch.persistence_ports import (
    NewTaskAttempt,
    NewTaskDispatchOutbox,
    TaskDispatchPersistenceConflict,
    TaskDispatchPersistenceUnavailable,
    TaskDispatchRepository,
    TaskDispatchStateConflict,
)
from taskforge.tracing import inject_trace_context, set_attributes, set_error, span
from taskforge.workflows.task_types import TaskTypeRegistry


class TaskDispatchNotEligible(Exception):
    """The scoped task is absent or no longer eligible for dispatch."""


class TaskDispatchConfigurationInvalid(Exception):
    """Trusted persisted or handler configuration cannot form a dispatch."""


class TaskDispatchServiceUnavailable(Exception):
    """Task dispatch persistence was operationally unavailable."""


class TaskDispatchConflict(Exception):
    """A durable invariant rejected dispatch creation."""


@dataclass(frozen=True)
class DispatchedTask:
    workflow_run_id: UUID
    task_run_id: UUID
    task_attempt_id: UUID
    attempt_number: int
    dispatch_id: UUID
    route: str


class TaskDispatchService:
    def __init__(
        self,
        repository: TaskDispatchRepository,
        task_types: TaskTypeRegistry,
    ) -> None:
        self._repository = repository
        self._task_types = task_types

    async def dispatch_task(
        self,
        workflow_run_id: UUID,
        task_run_id: UUID,
        *,
        correlation_id: object = None,
        trace_context: object = None,
    ) -> DispatchedTask:
        """Commit one attempt, intent, and runnable-to-dispatched transition."""
        with span("taskforge.dispatch.create") as active_span:
            try:
                async with self._repository.dispatch_transaction() as transaction:
                    prepared = await transaction.prepare_dispatch(
                        workflow_run_id, task_run_id
                    )
                    if prepared is None:
                        raise TaskDispatchNotEligible
                    definition = self._task_types.definition(prepared.task_type)
                    if definition is None:
                        raise TaskDispatchConfigurationInvalid

                    task_attempt_id, dispatch_id = uuid4(), uuid4()
                    try:
                        envelope = create_dispatch_envelope(
                            dispatch_id=dispatch_id,
                            task_attempt_id=task_attempt_id,
                            task_run_id=prepared.task_run_id,
                            workflow_run_id=prepared.workflow_run_id,
                            attempt_number=prepared.attempt_number,
                            task_type=prepared.task_type,
                            required_capability=definition.required_capability,
                            task_payload=prepared.task_parameters,
                            references={},
                            deadline_at=prepared.deadline_at,
                            execution_timeout_seconds=(
                                prepared.execution_timeout_seconds
                            ),
                            correlation_id=correlation_id,
                            trace_context=(
                                trace_context
                                if trace_context is not None
                                else inject_trace_context()
                            ),
                        )
                    except DispatchEnvelopeValidationError as error:
                        raise TaskDispatchConfigurationInvalid from error

                    attempt = NewTaskAttempt(
                        task_attempt_id,
                        prepared.task_run_id,
                        prepared.attempt_number,
                    )
                    outbox = NewTaskDispatchOutbox(
                        dispatch_id,
                        task_attempt_id,
                        envelope.route,
                        dispatch_envelope_to_mapping(envelope),
                    )
                    await transaction.persist_dispatch(prepared, attempt, outbox)
                    await transaction.commit()
            except TaskDispatchStateConflict as error:
                set_attributes(active_span, {"taskforge.outcome": "not_eligible"})
                raise TaskDispatchNotEligible from error
            except TaskDispatchPersistenceConflict as error:
                set_error(active_span, error, "persistence_conflict")
                raise TaskDispatchConflict from error
            except TaskDispatchPersistenceUnavailable as error:
                set_error(active_span, error, "persistence_unavailable")
                raise TaskDispatchServiceUnavailable from error
            except TaskDispatchConfigurationInvalid as error:
                set_error(active_span, error, "dispatch_configuration_invalid")
                raise
            set_attributes(
                active_span,
                {
                    "taskforge.workflow_run.id": str(workflow_run_id),
                    "taskforge.task_run.id": str(task_run_id),
                    "taskforge.task_attempt.id": str(task_attempt_id),
                    "taskforge.dispatch.id": str(dispatch_id),
                    "taskforge.outcome": "dispatched",
                },
            )

        return DispatchedTask(
            workflow_run_id,
            task_run_id,
            task_attempt_id,
            prepared.attempt_number,
            dispatch_id,
            envelope.route,
        )
