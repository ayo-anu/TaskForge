"""Broker-neutral dispatch transport validation tests."""

from __future__ import annotations

import json
from typing import cast
from uuid import uuid4

import pytest

from taskforge.dispatch.envelope import (
    create_dispatch_envelope,
    serialize_dispatch_envelope,
)
from taskforge.dispatch.transport import (
    DispatchTransportMetadata,
    MalformedDispatchTransport,
    ValidatedDispatchTransport,
    validate_dispatch_transport,
)


def valid_transport() -> tuple[bytes, DispatchTransportMetadata]:
    envelope = create_dispatch_envelope(
        dispatch_id=uuid4(),
        task_attempt_id=uuid4(),
        task_run_id=uuid4(),
        workflow_run_id=uuid4(),
        attempt_number=1,
        task_type="document.extract",
        required_capability="document-workers",
        task_payload={"source": "object-reference"},
        references={},
    )
    return serialize_dispatch_envelope(envelope), DispatchTransportMetadata(
        str(envelope.dispatch_id), envelope.route, "application/json", "utf-8"
    )


def test_current_topology_accepts_current_v3_envelope() -> None:
    body, metadata = valid_transport()

    result = validate_dispatch_transport(body, metadata)

    assert isinstance(result, ValidatedDispatchTransport)
    assert result.envelope.schema_version == 3
    assert "source" not in repr(result)
    assert "document-workers" not in repr(metadata)


def test_duplicate_transport_deliveries_are_not_suppressed() -> None:
    body, metadata = valid_transport()

    first = validate_dispatch_transport(body, metadata)
    duplicate = validate_dispatch_transport(body, metadata)

    assert isinstance(first, ValidatedDispatchTransport)
    assert isinstance(duplicate, ValidatedDispatchTransport)
    assert duplicate.envelope.dispatch_id == first.envelope.dispatch_id


@pytest.mark.parametrize(
    ("field", "value", "code"),
    (
        ("content_type", "text/plain", "unsupported_content_type"),
        ("content_encoding", None, "unsupported_content_encoding"),
        ("message_id", str(uuid4()), "dispatch_identity_mismatch"),
        ("routing_key", "capability.other", "dispatch_route_mismatch"),
    ),
)
def test_transport_metadata_mismatches_are_permanently_malformed(
    field: str, value: str | None, code: str
) -> None:
    body, metadata = valid_transport()
    changed = DispatchTransportMetadata(
        message_id=value if field == "message_id" else metadata.message_id,
        routing_key=(
            cast(str, value) if field == "routing_key" else metadata.routing_key
        ),
        content_type=value if field == "content_type" else metadata.content_type,
        content_encoding=(
            value if field == "content_encoding" else metadata.content_encoding
        ),
    )

    result = validate_dispatch_transport(body, changed)

    assert result == MalformedDispatchTransport(code)


def test_unsupported_envelope_version_is_permanently_malformed() -> None:
    body, metadata = valid_transport()
    value = json.loads(body)
    value["schema_version"] = 4

    result = validate_dispatch_transport(
        json.dumps(value, separators=(",", ":")).encode(), metadata
    )

    assert result == MalformedDispatchTransport("unsupported_schema_version")


@pytest.mark.parametrize("body", (b"not-json", b'{"value":NaN}'))
def test_task2_parser_remains_authoritative_for_malformed_bodies(body: bytes) -> None:
    _, metadata = valid_transport()

    result = validate_dispatch_transport(body, metadata)

    assert result == MalformedDispatchTransport("malformed_json")
