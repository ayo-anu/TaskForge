"""Stateless bounded dispatch publisher reconciliation tests."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from taskforge.dispatch.envelope import (
    create_dispatch_envelope,
    dispatch_envelope_to_mapping,
)
from taskforge.dispatch.publisher import TaskDispatchPublisher
from taskforge.dispatch.publisher_ports import (
    BrokerDispatchPublication,
    BrokerPublicationTimeout,
    PublicationAcknowledgement,
    StoredDispatch,
    UnpublishedDispatchCursor,
)


@dataclass
class FakeRepository:
    records: tuple[StoredDispatch, ...]
    acknowledgement: PublicationAcknowledgement = PublicationAcknowledgement.RECORDED
    calls: list[tuple[UnpublishedDispatchCursor | None, int]] = field(
        default_factory=list
    )
    acknowledged: list[UUID] = field(default_factory=list)

    async def list_unpublished_page(
        self, *, after: UnpublishedDispatchCursor | None, limit: int
    ) -> tuple[StoredDispatch, ...]:
        self.calls.append((after, limit))
        eligible = (
            record
            for record in self.records
            if after is None
            or (record.created_at, record.dispatch_id)
            > (after.created_at, after.dispatch_id)
        )
        return tuple(list(eligible)[:limit])

    async def record_accepted_publication(
        self, expected: StoredDispatch
    ) -> PublicationAcknowledgement:
        self.acknowledged.append(expected.dispatch_id)
        return self.acknowledgement


@dataclass
class FakeBroker:
    failure_for: UUID | None = None
    publications: list[BrokerDispatchPublication] = field(default_factory=list)

    async def publish(self, publication: BrokerDispatchPublication) -> None:
        self.publications.append(publication)
        if publication.dispatch_id == self.failure_for:
            raise BrokerPublicationTimeout


def stored_dispatch(
    sequence: int,
    *,
    correlation_id: str | None = None,
    mutate: Callable[[dict[str, object]], None] | None = None,
) -> StoredDispatch:
    dispatch_id, attempt_id = uuid4(), uuid4()
    envelope = create_dispatch_envelope(
        dispatch_id=dispatch_id,
        task_attempt_id=attempt_id,
        task_run_id=uuid4(),
        workflow_run_id=uuid4(),
        attempt_number=1,
        task_type="document.extract",
        required_capability="document-workers",
        task_payload={"sequence": sequence},
        references={},
        correlation_id=correlation_id,
    )
    payload = dispatch_envelope_to_mapping(envelope)
    if mutate is not None:
        mutate(payload)
    return StoredDispatch(
        dispatch_id,
        attempt_id,
        "capability.document-workers",
        payload,
        datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=sequence),
    )


def remove_schema_version(payload: dict[str, object]) -> None:
    payload.pop("schema_version")


def test_empty_pass_is_bounded_and_stateless_across_calls() -> None:
    repository, broker = FakeRepository(()), FakeBroker()
    publisher = TaskDispatchPublisher(repository, broker)

    first = asyncio.run(publisher.reconcile_unpublished(page_size=4, pass_limit=10))
    second = asyncio.run(publisher.reconcile_unpublished(page_size=4, pass_limit=10))

    assert first == second
    assert first.examined == 0
    assert first.reached_end
    assert repository.calls == [(None, 4), (None, 4)]
    assert broker.publications == []


def test_pass_uses_keyset_pages_and_reduces_final_query_limit() -> None:
    records = tuple(stored_dispatch(index) for index in range(1, 8))
    repository, broker = FakeRepository(records), FakeBroker()

    result = asyncio.run(
        TaskDispatchPublisher(repository, broker).reconcile_unpublished(
            page_size=3, pass_limit=5
        )
    )

    assert result.examined == result.acknowledged == 5
    assert result.pass_limit_reached
    assert [limit for _, limit in repository.calls] == [3, 2]
    assert repository.calls[0][0] is None
    assert repository.calls[1][0] == records[2].cursor
    assert [item.dispatch_id for item in broker.publications] == [
        record.dispatch_id for record in records[:5]
    ]


def test_durable_invalid_rows_advance_cursor_and_do_not_starve_later_page() -> None:
    corrupt = tuple(
        stored_dispatch(index, mutate=remove_schema_version) for index in range(1, 4)
    )
    valid = stored_dispatch(4)
    repository, broker = FakeRepository((*corrupt, valid)), FakeBroker()

    result = asyncio.run(
        TaskDispatchPublisher(repository, broker).reconcile_unpublished(
            page_size=2, pass_limit=4
        )
    )

    assert result.examined == 4
    assert result.durable_invalid == 3
    assert result.acknowledged == 1
    assert repository.calls[1][0] == corrupt[1].cursor
    assert [item.dispatch_id for item in broker.publications] == [valid.dispatch_id]


@pytest.mark.parametrize(
    "mutation",
    (
        lambda payload: payload.__setitem__("dispatch_id", str(uuid4())),
        lambda payload: payload.__setitem__("task_attempt_id", str(uuid4())),
        lambda payload: payload.__setitem__("required_capability", "other-workers"),
    ),
)
def test_relational_envelope_mismatches_are_durable_invalid(
    mutation: Callable[[dict[str, object]], None],
) -> None:
    record = stored_dispatch(1, mutate=mutation)
    repository, broker = FakeRepository((record,)), FakeBroker()

    result = asyncio.run(
        TaskDispatchPublisher(repository, broker).reconcile_unpublished(
            page_size=1, pass_limit=1
        )
    )

    assert result.durable_invalid == 1
    assert broker.publications == []
    assert repository.acknowledged == []


def test_publication_uses_exact_durable_route_identity_and_snapshot() -> None:
    record = stored_dispatch(1)
    detached_payload = deepcopy(record.payload)
    repository, broker = FakeRepository((record,)), FakeBroker()

    asyncio.run(
        TaskDispatchPublisher(repository, broker).reconcile_unpublished(
            page_size=1, pass_limit=2
        )
    )

    publication = broker.publications[0]
    assert publication.dispatch_id == record.dispatch_id
    assert publication.route == record.route
    assert record.payload == detached_payload
    assert str(record.dispatch_id).encode() in publication.body


def test_broker_failure_logs_safe_owned_event_before_propagation(
    caplog: pytest.LogCaptureFixture,
) -> None:
    correlation_id = str(uuid4())
    first, second = (
        stored_dispatch(1, correlation_id=correlation_id),
        stored_dispatch(2),
    )
    repository = FakeRepository((first, second))
    broker = FakeBroker(failure_for=first.dispatch_id)
    caplog.set_level(logging.INFO, logger="taskforge.dispatch.publisher")

    with pytest.raises(BrokerPublicationTimeout):
        asyncio.run(
            TaskDispatchPublisher(repository, broker).reconcile_unpublished(
                page_size=2, pass_limit=2
            )
        )

    assert [item.dispatch_id for item in broker.publications] == [first.dispatch_id]
    assert repository.acknowledged == []
    event = next(
        record
        for record in caplog.records
        if getattr(record, "_event_name", None) == "dispatch.publish.failed"
    )
    fields = event.__dict__["_event_fields"]
    assert fields["dispatch.id"] == str(first.dispatch_id)
    assert fields["correlation.id"] == correlation_id
    assert event.__dict__["_safe_error_type"] == "BrokerPublicationTimeout"
    assert "body" not in fields


def test_concurrent_acknowledgement_outcome_is_counted() -> None:
    record = stored_dispatch(1)
    repository = FakeRepository((record,), PublicationAcknowledgement.ALREADY_RECORDED)

    result = asyncio.run(
        TaskDispatchPublisher(repository, FakeBroker()).reconcile_unpublished(
            page_size=1, pass_limit=2
        )
    )

    assert result.acknowledged == 0
    assert result.already_acknowledged == 1


def test_publication_reconstructs_isolated_identifier_contexts(
    caplog: pytest.LogCaptureFixture,
) -> None:
    first_correlation, second_correlation = str(uuid4()), str(uuid4())
    first = stored_dispatch(1, correlation_id=first_correlation)
    second = stored_dispatch(2, correlation_id=second_correlation)
    caplog.set_level(logging.INFO, logger="taskforge.dispatch.publisher")

    asyncio.run(
        TaskDispatchPublisher(
            FakeRepository((first, second)), FakeBroker()
        ).reconcile_unpublished(page_size=2, pass_limit=2)
    )

    events = [
        record
        for record in caplog.records
        if getattr(record, "_event_name", None) == "dispatch.publish.succeeded"
    ]
    assert [record.__dict__["_event_fields"]["dispatch.id"] for record in events] == [
        str(first.dispatch_id),
        str(second.dispatch_id),
    ]
    assert [
        record.__dict__["_event_fields"]["correlation.id"] for record in events
    ] == [
        first_correlation,
        second_correlation,
    ]


@pytest.mark.parametrize(
    ("page_size", "pass_limit"),
    ((0, 1), (101, 101), (1, 0), (1, 1001), (True, 1), (1, True)),
)
def test_reconciliation_bounds_are_strict(page_size: int, pass_limit: int) -> None:
    with pytest.raises(ValueError, match="supported bounds"):
        asyncio.run(
            TaskDispatchPublisher(
                FakeRepository(()), FakeBroker()
            ).reconcile_unpublished(page_size=page_size, pass_limit=pass_limit)
        )


def test_safe_representations_redact_durable_content() -> None:
    original = stored_dispatch(1)
    payload = deepcopy(original.payload)
    payload["sensitive"] = "payload-secret"
    record = StoredDispatch(
        original.dispatch_id,
        original.task_attempt_id,
        "route-secret",
        payload,
        original.created_at,
    )
    publication = BrokerDispatchPublication(
        record.dispatch_id, "route-secret", b"payload-secret"
    )

    assert "route-secret" not in repr(record)
    assert "payload-secret" not in repr(record)
    assert "route-secret" not in repr(publication)
    assert "payload-secret" not in repr(publication)
