"""Stateless bounded reconciliation of durable dispatch publication intents."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from uuid import uuid4

from taskforge.dispatch.envelope import (
    DispatchEnvelopeValidationError,
    deserialize_dispatch_envelope,
    serialize_dispatch_envelope,
)
from taskforge.dispatch.publisher_ports import (
    BrokerDispatchPublication,
    BrokerPublicationRejected,
    BrokerPublicationTimeout,
    BrokerUnavailable,
    DispatchBrokerPublisher,
    DispatchOutboxRepository,
    PublicationAcknowledgement,
    StoredDispatch,
    UnpublishedDispatchCursor,
)
from taskforge.logging import bind_log_context, log_event

MAX_PUBLICATION_PAGE_SIZE = 100
MAX_PUBLICATION_PASS_SIZE = 1_000
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PublicationPassResult:
    examined: int
    acknowledged: int
    already_acknowledged: int
    durable_invalid: int
    reached_end: bool
    pass_limit_reached: bool


class TaskDispatchPublisher:
    def __init__(
        self,
        repository: DispatchOutboxRepository,
        broker: DispatchBrokerPublisher,
    ) -> None:
        self._repository = repository
        self._broker = broker

    async def reconcile_unpublished(
        self,
        *,
        page_size: int,
        pass_limit: int,
    ) -> PublicationPassResult:
        """Run one independent bounded keyset reconciliation pass."""
        _validate_bounds(page_size, pass_limit)
        operation_id = uuid4()
        with bind_log_context(**{"operation.id": operation_id}):
            return await self._reconcile_unpublished_bound(
                page_size=page_size, pass_limit=pass_limit
            )

    async def _reconcile_unpublished_bound(
        self, *, page_size: int, pass_limit: int
    ) -> PublicationPassResult:
        examined = acknowledged = already_acknowledged = durable_invalid = 0
        after: UnpublishedDispatchCursor | None = None
        reached_end = False

        while examined < pass_limit:
            query_limit = min(page_size, pass_limit - examined)
            page = await self._repository.list_unpublished_page(
                after=after, limit=query_limit
            )
            if not page:
                reached_end = True
                break

            for stored in page:
                examined += 1
                validated = _validated_publication(stored)
                if validated is None:
                    durable_invalid += 1
                    with bind_log_context(
                        **{
                            "dispatch.id": stored.dispatch_id,
                            "task.attempt.id": stored.task_attempt_id,
                        }
                    ):
                        log_event(
                            logger,
                            logging.ERROR,
                            "dispatch.publish.durable_invalid",
                            {"reason.code": "invalid_durable_envelope"},
                        )
                    after = stored.cursor
                    continue

                publication, identifiers = validated
                with bind_log_context(**identifiers):
                    try:
                        await self._broker.publish(publication)
                    except (
                        BrokerUnavailable,
                        BrokerPublicationTimeout,
                        BrokerPublicationRejected,
                    ) as error:
                        log_event(
                            logger,
                            logging.ERROR,
                            "dispatch.publish.failed",
                            {
                                "error.category": "broker_publication_failure",
                                "outcome": "failed",
                            },
                            error=error,
                        )
                        raise
                    outcome = await self._repository.record_accepted_publication(stored)
                    if outcome is PublicationAcknowledgement.RECORDED:
                        acknowledged += 1
                    else:
                        already_acknowledged += 1
                    log_event(
                        logger,
                        logging.INFO,
                        "dispatch.publish.succeeded",
                        {"outcome": outcome.value, "broker.route": stored.route},
                    )
                after = stored.cursor

            if len(page) < query_limit:
                reached_end = True
                break

        result = PublicationPassResult(
            examined=examined,
            acknowledged=acknowledged,
            already_acknowledged=already_acknowledged,
            durable_invalid=durable_invalid,
            reached_end=reached_end,
            pass_limit_reached=examined == pass_limit and not reached_end,
        )
        log_event(
            logger,
            logging.INFO,
            "dispatch.publish_pass.completed",
            {
                "examined": result.examined,
                "acknowledged": result.acknowledged,
                "already_acknowledged": result.already_acknowledged,
                "durable_invalid": result.durable_invalid,
                "reached_end": result.reached_end,
                "pass_limit_reached": result.pass_limit_reached,
            },
        )
        return result


def _validate_bounds(page_size: int, pass_limit: int) -> None:
    if type(page_size) is not int or not 1 <= page_size <= MAX_PUBLICATION_PAGE_SIZE:
        raise ValueError("publication page size is outside the supported bounds")
    if type(pass_limit) is not int or not 1 <= pass_limit <= MAX_PUBLICATION_PASS_SIZE:
        raise ValueError("publication pass limit is outside the supported bounds")


def _validated_publication(
    stored: StoredDispatch,
) -> tuple[BrokerDispatchPublication, dict[str, object]] | None:
    try:
        encoded = json.dumps(
            stored.payload,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        envelope = deserialize_dispatch_envelope(encoded)
        if (
            envelope.dispatch_id != stored.dispatch_id
            or envelope.task_attempt_id != stored.task_attempt_id
            or envelope.route != stored.route
        ):
            return None
        body = serialize_dispatch_envelope(envelope)
    except (DispatchEnvelopeValidationError, TypeError, ValueError):
        return None
    identifiers: dict[str, object] = {
        "dispatch.id": envelope.dispatch_id,
        "workflow.run.id": envelope.workflow_run_id,
        "task.run.id": envelope.task_run_id,
        "task.attempt.id": envelope.task_attempt_id,
        "task.attempt.number": envelope.attempt_number,
        "task.type": envelope.task_type,
    }
    if envelope.correlation_id is not None:
        identifiers["correlation.id"] = envelope.correlation_id
    return BrokerDispatchPublication(
        stored.dispatch_id, stored.route, body
    ), identifiers
