"""Stateless bounded reconciliation of durable dispatch publication intents."""

from __future__ import annotations

import json
from dataclasses import dataclass

from taskforge.dispatch.envelope import (
    DispatchEnvelopeValidationError,
    deserialize_dispatch_envelope,
    serialize_dispatch_envelope,
)
from taskforge.dispatch.publisher_ports import (
    BrokerDispatchPublication,
    DispatchBrokerPublisher,
    DispatchOutboxRepository,
    PublicationAcknowledgement,
    StoredDispatch,
    UnpublishedDispatchCursor,
)

MAX_PUBLICATION_PAGE_SIZE = 100
MAX_PUBLICATION_PASS_SIZE = 1_000


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
                publication = _validated_publication(stored)
                if publication is None:
                    durable_invalid += 1
                    after = stored.cursor
                    continue

                await self._broker.publish(publication)
                outcome = await self._repository.record_accepted_publication(stored)
                if outcome is PublicationAcknowledgement.RECORDED:
                    acknowledged += 1
                else:
                    already_acknowledged += 1
                after = stored.cursor

            if len(page) < query_limit:
                reached_end = True
                break

        return PublicationPassResult(
            examined=examined,
            acknowledged=acknowledged,
            already_acknowledged=already_acknowledged,
            durable_invalid=durable_invalid,
            reached_end=reached_end,
            pass_limit_reached=examined == pass_limit and not reached_end,
        )


def _validate_bounds(page_size: int, pass_limit: int) -> None:
    if type(page_size) is not int or not 1 <= page_size <= MAX_PUBLICATION_PAGE_SIZE:
        raise ValueError("publication page size is outside the supported bounds")
    if type(pass_limit) is not int or not 1 <= pass_limit <= MAX_PUBLICATION_PASS_SIZE:
        raise ValueError("publication pass limit is outside the supported bounds")


def _validated_publication(
    stored: StoredDispatch,
) -> BrokerDispatchPublication | None:
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
    return BrokerDispatchPublication(stored.dispatch_id, stored.route, body)
