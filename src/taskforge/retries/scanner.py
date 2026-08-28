"""Bounded horizontally safe dispatch of due retry attempts."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid4

from opentelemetry.trace import Span

from taskforge.logging import bind_log_context, log_event
from taskforge.metrics import add as add_metric
from taskforge.metrics import record as record_metric
from taskforge.retries.persistence_ports import (
    DueRetryDispatchRepository,
    DueRetryPersistenceInvariantViolation,
    DueRetryPersistenceUnavailable,
    PreparedDueRetryDispatch,
    SkippedDueRetryCandidate,
)
from taskforge.tracing import (
    DeferredSpan,
    add_link,
    inject_trace_context,
    link_from_trace_context,
    set_error,
)
from taskforge.workflows.task_types import TaskTypeRegistry

MAX_DUE_RETRY_BATCH_SIZE = 100
logger = logging.getLogger(__name__)


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

        with bind_log_context(**{"operation.id": uuid4()}):
            return await self._scan_due_retries_bound(batch_size=batch_size)

    async def _scan_due_retries_bound(self, *, batch_size: int) -> DueRetryScanResult:
        examined = skipped = 0
        dispatched_attempt_ids: list[UUID] = []
        try:
            for _ in range(batch_size):
                deferred_span = DeferredSpan()
                prepared_due: PreparedDueRetryDispatch | None = None
                dispatched_attempt_id: UUID | None = None
                committed_log_fields: dict[str, object] | None = None
                committed_route: str | None = None
                try:
                    async with (
                        self._repository.due_dispatch_transaction() as transaction
                    ):
                        prepared = await transaction.prepare_next_due()
                        if prepared is None:
                            break
                        examined += 1
                        if isinstance(prepared, SkippedDueRetryCandidate):
                            skipped += 1
                            add_metric(
                                "taskforge.retry.dispatches",
                                attributes={"taskforge.outcome": "skipped"},
                            )
                            continue

                        prepared_due = prepared

                        active_span = deferred_span.start(
                            "taskforge.retry.dispatch",
                            root=True,
                            attributes={"db.system.name": "postgresql"},
                        )
                        outbox_id, route, payload, correlation_id = (
                            self._prepare_outbox(prepared, active_span)
                        )
                        identifiers: dict[str, object] = {
                            "dispatch.id": outbox_id,
                            "workflow.run.id": prepared.workflow_run_id,
                            "task.run.id": prepared.task_run_id,
                            "task.attempt.id": prepared.task_attempt_id,
                            "task.attempt.number": prepared.attempt_number,
                            "task.type": prepared.task_type,
                        }
                        if correlation_id is not None:
                            identifiers["correlation.id"] = correlation_id
                        with bind_log_context(**identifiers):
                            await transaction.persist_dispatch(
                                prepared, outbox_id, route, payload
                            )
                        committed_log_fields = identifiers
                        committed_route = route
                        dispatched_attempt_id = prepared.task_attempt_id
                except (
                    DueRetryScanInvariantError,
                    DueRetryPersistenceInvariantViolation,
                    DueRetryPersistenceUnavailable,
                ) as error:
                    set_error(deferred_span.active, error, "retry_dispatch_failure")
                    raise
                finally:
                    deferred_span.end()
                assert dispatched_attempt_id is not None
                assert committed_log_fields is not None and committed_route is not None
                with bind_log_context(**committed_log_fields):
                    log_event(
                        logger,
                        logging.INFO,
                        "scheduler.retry.dispatched",
                        {"broker.route": committed_route, "outcome": "dispatched"},
                    )
                add_metric(
                    "taskforge.retry.dispatches",
                    attributes={"taskforge.outcome": "dispatched"},
                )
                assert prepared_due is not None
                record_metric(
                    "taskforge.retry.due.age",
                    (datetime.now(UTC) - prepared_due.next_eligible_at).total_seconds(),
                )
                dispatched_attempt_ids.append(dispatched_attempt_id)
        except DueRetryScanInvariantError as error:
            add_metric(
                "taskforge.retry.dispatches",
                attributes={"taskforge.outcome": "invariant_failure"},
            )
            log_event(
                logger,
                logging.ERROR,
                "scheduler.retry_scan.failed",
                {"error.category": "scanner_invariant", "outcome": "failed"},
                error=error,
            )
            raise
        except DueRetryPersistenceInvariantViolation as error:
            add_metric(
                "taskforge.retry.dispatches",
                attributes={"taskforge.outcome": "invariant_failure"},
            )
            log_event(
                logger,
                logging.ERROR,
                "scheduler.retry_scan.failed",
                {"error.category": "persistence_invariant", "outcome": "failed"},
                error=error,
            )
            raise DueRetryScanInvariantError from error
        except DueRetryPersistenceUnavailable as error:
            add_metric(
                "taskforge.retry.dispatches",
                attributes={"taskforge.outcome": "persistence_failure"},
            )
            log_event(
                logger,
                logging.WARNING,
                "scheduler.retry_scan.failed",
                {"error.category": "persistence_unavailable", "outcome": "failed"},
                error=error,
            )
            raise DueRetryScanServiceUnavailable from error

        result = DueRetryScanResult(examined, tuple(dispatched_attempt_ids), skipped)
        log_event(
            logger,
            logging.INFO,
            "scheduler.retry_scan.completed",
            {
                "examined": result.examined,
                "dispatched": result.dispatched,
                "skipped": result.skipped,
            },
        )
        return result

    def _prepare_outbox(
        self, prepared: PreparedDueRetryDispatch, active_span: Span | None
    ) -> tuple[UUID, str, dict[str, object], str | None]:
        from taskforge.dispatch.envelope import (
            DispatchEnvelopeValidationError,
            create_dispatch_envelope,
            deserialize_dispatch_envelope,
            dispatch_envelope_to_mapping,
        )

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
            add_link(active_span, link_from_trace_context(predecessor.trace_context))
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
                trace_context=inject_trace_context(),
            )
        except (DispatchEnvelopeValidationError, TypeError, ValueError) as error:
            raise DueRetryScanInvariantError from error
        return (
            dispatch_id,
            envelope.route,
            dispatch_envelope_to_mapping(envelope),
            envelope.correlation_id,
        )
