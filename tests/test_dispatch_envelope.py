"""Contract tests for the broker-neutral versioned dispatch envelope."""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

from taskforge.dispatch.envelope import (
    DISPATCH_ENVELOPE_VERSION,
    MAX_DISPATCH_ENVELOPE_BYTES,
    DispatchEnvelope,
    DispatchEnvelopeValidationError,
    TraceContext,
    create_dispatch_envelope,
    deserialize_dispatch_envelope,
    dispatch_envelope_to_mapping,
    dispatch_route,
    serialize_dispatch_envelope,
)

VALID_TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


def envelope_arguments(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "dispatch_id": uuid4(),
        "task_attempt_id": uuid4(),
        "task_run_id": uuid4(),
        "workflow_run_id": uuid4(),
        "attempt_number": 1,
        "task_type": "document.transform",
        "required_capability": "document-processing",
        "task_payload": {"document": {"format": "pdf"}},
        "references": {"source": {"kind": "object_reference", "id": "doc-1"}},
    }
    values.update(overrides)
    return values


def create_envelope(**overrides: object) -> DispatchEnvelope:
    return create_dispatch_envelope(**envelope_arguments(**overrides))


def legacy_envelope(**overrides: object) -> DispatchEnvelope:
    mapping = dispatch_envelope_to_mapping(create_envelope(**overrides))
    mapping["schema_version"] = 1
    del mapping["deadline_at"]
    return deserialize_dispatch_envelope(canonical_bytes(mapping))


def test_exact_v1_contract_round_trips_without_deadline() -> None:
    envelope = legacy_envelope()
    encoded = serialize_dispatch_envelope(envelope)
    assert deserialize_dispatch_envelope(encoded) == envelope
    assert json.loads(encoded)["schema_version"] == 1
    assert "deadline_at" not in json.loads(encoded)


def test_v1_rejects_deadline_field() -> None:
    mapping = dispatch_envelope_to_mapping(legacy_envelope())
    mapping["deadline_at"] = None
    with pytest.raises(DispatchEnvelopeValidationError) as caught:
        deserialize_dispatch_envelope(canonical_bytes(mapping))
    assert issue_codes(caught.value) == ("unknown_field",)


def test_v2_requires_nullable_or_canonical_deadline() -> None:
    without = dispatch_envelope_to_mapping(create_envelope())
    del without["deadline_at"]
    with pytest.raises(DispatchEnvelopeValidationError) as missing:
        deserialize_dispatch_envelope(canonical_bytes(without))
    assert issue_codes(missing.value) == ("missing_field",)

    assert dispatch_envelope_to_mapping(create_envelope())["deadline_at"] is None
    deadline = datetime(2026, 8, 13, 20, tzinfo=UTC)
    present = create_envelope(deadline_at=deadline)
    assert dispatch_envelope_to_mapping(present)["deadline_at"] == (
        "2026-08-13T20:00:00.000000Z"
    )


def test_v2_rejects_noncanonical_deadline_and_unknown_field() -> None:
    mapping = dispatch_envelope_to_mapping(create_envelope())
    mapping["deadline_at"] = "2026-08-13T20:00:00Z"
    with pytest.raises(DispatchEnvelopeValidationError) as deadline:
        deserialize_dispatch_envelope(canonical_bytes(mapping))
    assert issue_codes(deadline.value) == ("invalid_deadline_at",)

    mapping = dispatch_envelope_to_mapping(create_envelope())
    mapping["extra"] = True
    with pytest.raises(DispatchEnvelopeValidationError) as unknown:
        deserialize_dispatch_envelope(canonical_bytes(mapping))
    assert issue_codes(unknown.value) == ("unknown_field",)


def issue_codes(error: DispatchEnvelopeValidationError) -> tuple[str, ...]:
    return tuple(issue.code for issue in error.issues)


def test_minimal_envelope_has_distinct_task_and_capability_semantics() -> None:
    envelope = create_envelope()

    assert envelope.schema_version == DISPATCH_ENVELOPE_VERSION
    assert envelope.task_type == "document.transform"
    assert envelope.required_capability == "document-processing"
    assert envelope.route == "capability.document-processing"
    assert envelope.correlation_id is None
    assert envelope.trace_context is None


def test_route_is_derived_exclusively_from_validated_capability() -> None:
    assert dispatch_route("gpu.large") == "capability.gpu.large"
    assert create_envelope(task_type="other.handler").route == (
        "capability.document-processing"
    )
    assert create_envelope(required_capability="gpu.large").route == (
        "capability.gpu.large"
    )
    for value in ("", "GPU", "gpu/*", "queue:name", " padded", "x" * 129):
        with pytest.raises(DispatchEnvelopeValidationError) as caught:
            dispatch_route(value)
        assert issue_codes(caught.value) == ("invalid_required_capability",)


def test_task_type_and_capability_are_independently_validated() -> None:
    with pytest.raises(DispatchEnvelopeValidationError) as caught:
        create_envelope(task_type="Invalid", required_capability="queue/*")

    assert issue_codes(caught.value) == (
        "invalid_task_type",
        "invalid_required_capability",
    )


def test_payload_and_references_are_transitively_immutable() -> None:
    payload = {"nested": {"items": [1, {"value": "original"}]}}
    references = {"artifact": {"ids": ["one"]}}
    envelope = create_envelope(task_payload=payload, references=references)
    before = serialize_dispatch_envelope(envelope)

    cast(Any, payload)["nested"]["items"][1]["value"] = "changed"
    cast(Any, references)["artifact"]["ids"].append("two")

    assert serialize_dispatch_envelope(envelope) == before
    frozen_payload = cast(Any, envelope.task_payload)
    assert frozen_payload["nested"]["items"][1]["value"] == "original"
    with pytest.raises(TypeError):
        envelope.task_payload["other"] = "value"  # type: ignore[index]
    assert isinstance(frozen_payload["nested"]["items"], tuple)


def test_detached_mapping_cannot_mutate_the_envelope() -> None:
    envelope = create_envelope()
    mapping = dispatch_envelope_to_mapping(envelope)
    payload = mapping["task_payload"]
    assert isinstance(payload, dict)
    payload["changed"] = True

    assert "changed" not in envelope.task_payload


def test_direct_or_post_construction_mutation_is_rejected() -> None:
    with pytest.raises(ValueError, match="use create_dispatch_envelope"):
        DispatchEnvelope(
            schema_version=1,
            dispatch_id=uuid4(),
            task_attempt_id=uuid4(),
            task_run_id=uuid4(),
            workflow_run_id=uuid4(),
            attempt_number=1,
            task_type="test.task",
            required_capability="test",
            task_payload={},
            references={},
        )
    envelope = create_envelope()
    with pytest.raises(FrozenInstanceError):
        envelope.attempt_number = 2  # type: ignore[misc]


def test_serialization_is_canonical_and_round_trips() -> None:
    envelope = create_envelope(
        correlation_id="request-123",
        trace_context={
            "traceparent": VALID_TRACEPARENT,
            "tracestate": "vendor=value",
        },
    )
    first = serialize_dispatch_envelope(envelope)
    second = serialize_dispatch_envelope(envelope)

    assert first == second
    assert b" " not in first
    assert deserialize_dispatch_envelope(first) == envelope
    assert list(json.loads(first)) == sorted(json.loads(first))


def test_optional_fields_are_omitted_when_absent() -> None:
    mapping = dispatch_envelope_to_mapping(create_envelope())

    assert "correlation_id" not in mapping
    assert "trace_context" not in mapping


@pytest.mark.parametrize(
    "field",
    ("dispatch_id", "task_attempt_id", "task_run_id", "workflow_run_id"),
)
def test_factory_requires_uuid_objects(field: str) -> None:
    with pytest.raises(DispatchEnvelopeValidationError) as caught:
        create_envelope(**{field: str(uuid4())})
    assert issue_codes(caught.value) == ("invalid_identifier",)


@pytest.mark.parametrize("value", (True, 0, -1, 1.0, "1"))
def test_attempt_number_is_a_strict_positive_integer(value: object) -> None:
    with pytest.raises(DispatchEnvelopeValidationError) as caught:
        create_envelope(attempt_number=value)
    assert issue_codes(caught.value) == ("invalid_attempt_number",)


def test_deserialization_requires_canonical_uuid_strings() -> None:
    mapping = dispatch_envelope_to_mapping(create_envelope())
    identifier = UUID(mapping["dispatch_id"])  # type: ignore[arg-type]
    for value in (
        str(identifier).upper(),
        identifier.hex,
        f"{{{identifier}}}",
        identifier.urn,
    ):
        candidate = {**mapping, "dispatch_id": value}
        with pytest.raises(DispatchEnvelopeValidationError) as caught:
            deserialize_dispatch_envelope(canonical_bytes(candidate))
        assert issue_codes(caught.value) == ("invalid_identifier",)


@pytest.mark.parametrize("version", (None, True, 0, -1, 1.0, "1", 3))
def test_unknown_or_invalid_versions_fail_fast(version: object) -> None:
    mapping = dispatch_envelope_to_mapping(create_envelope())
    mapping["schema_version"] = version
    with pytest.raises(DispatchEnvelopeValidationError) as caught:
        deserialize_dispatch_envelope(canonical_bytes(mapping))
    assert issue_codes(caught.value) == ("unsupported_schema_version",)


def test_missing_and_unknown_fields_are_rejected_deterministically() -> None:
    mapping = dispatch_envelope_to_mapping(create_envelope())
    del mapping["task_type"]
    mapping["broker_queue"] = "unsafe"

    with pytest.raises(DispatchEnvelopeValidationError) as caught:
        deserialize_dispatch_envelope(canonical_bytes(mapping))

    assert [(issue.code, issue.path) for issue in caught.value.issues] == [
        ("missing_field", ("task_type",)),
        ("unknown_field", ("broker_queue",)),
    ]


@pytest.mark.parametrize("value", ([], "value", 1, None))
def test_payload_and_references_must_be_objects(value: object) -> None:
    with pytest.raises(DispatchEnvelopeValidationError) as caught:
        create_envelope(task_payload=value, references=value)
    assert issue_codes(caught.value) == (
        "invalid_task_payload",
        "invalid_references",
    )


def test_non_json_recursive_and_nonfinite_values_are_rejected() -> None:
    recursive: dict[str, object] = {}
    recursive["self"] = recursive
    with pytest.raises(DispatchEnvelopeValidationError) as recursive_error:
        create_envelope(task_payload=recursive)
    assert "recursive_json_value" in issue_codes(recursive_error.value)

    with pytest.raises(DispatchEnvelopeValidationError) as nonfinite_error:
        create_envelope(task_payload={"number": float("nan")})
    assert issue_codes(nonfinite_error.value) == ("invalid_json_value",)


def test_json_depth_nodes_collections_keys_and_strings_are_bounded() -> None:
    cases = (
        ({"value": nested_value(10)}, "json_too_deep"),
        ({str(index): index for index in range(129)}, "json_too_complex"),
        ({"x" * 129: True}, "json_key_too_large"),
        ({"value": "x" * 4097}, "json_string_too_large"),
    )
    for payload, code in cases:
        with pytest.raises(DispatchEnvelopeValidationError) as caught:
            create_envelope(task_payload=payload)
        assert code in issue_codes(caught.value)


def test_each_content_section_has_an_independent_16_kib_limit() -> None:
    oversized = {str(index): "x" * 4096 for index in range(5)}
    for field, code in (
        ("task_payload", "task_payload_too_large"),
        ("references", "references_too_large"),
    ):
        with pytest.raises(DispatchEnvelopeValidationError) as caught:
            create_envelope(**{field: oversized})
        assert issue_codes(caught.value) == (code,)


def test_individually_valid_sections_can_exceed_complete_envelope_limit() -> None:
    near_limit = {str(index): "x" * 4060 for index in range(4)}
    with pytest.raises(DispatchEnvelopeValidationError) as caught:
        create_envelope(task_payload=near_limit, references=near_limit)
    assert issue_codes(caught.value) == ("envelope_too_large",)


def test_oversized_raw_input_is_rejected_before_json_decoding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_decode(*args: object, **kwargs: object) -> object:
        raise AssertionError("JSON decoder must not run")

    monkeypatch.setattr(json, "loads", unexpected_decode)
    with pytest.raises(DispatchEnvelopeValidationError) as caught:
        deserialize_dispatch_envelope(b"{" + b"x" * MAX_DISPATCH_ENVELOPE_BYTES)
    assert issue_codes(caught.value) == ("envelope_too_large",)


@pytest.mark.parametrize(
    ("data", "code"),
    (
        (b"\xff", "invalid_utf8"),
        (b"{", "malformed_json"),
        (b"[]", "invalid_envelope"),
        (b'{"schema_version":1,"schema_version":1}', "duplicate_json_key"),
        (
            b'{"schema_version":1,"task_payload":{"x":1,"x":2}}',
            "duplicate_json_key",
        ),
    ),
)
def test_malformed_transport_data_fails_safely(data: bytes, code: str) -> None:
    with pytest.raises(DispatchEnvelopeValidationError) as caught:
        deserialize_dispatch_envelope(data)
    assert issue_codes(caught.value) == (code,)


@pytest.mark.parametrize("constant", (b"NaN", b"Infinity", b"-Infinity"))
def test_nonstandard_json_numeric_constants_are_malformed(constant: bytes) -> None:
    data = b'{"schema_version":1,"task_payload":{"value":' + constant + b"}}"

    with pytest.raises(DispatchEnvelopeValidationError) as caught:
        deserialize_dispatch_envelope(data)

    assert issue_codes(caught.value) == ("malformed_json",)
    assert constant.decode() not in str(caught.value)


@pytest.mark.parametrize(
    "value",
    ("", " padded", "padded ", "line\nbreak", "é", "x" * 129),
)
def test_correlation_id_is_bounded_safe_ascii(value: str) -> None:
    with pytest.raises(DispatchEnvelopeValidationError) as caught:
        create_envelope(correlation_id=value)
    assert issue_codes(caught.value) == ("invalid_correlation_id",)


def test_trace_context_is_narrow_and_redacted() -> None:
    envelope = create_envelope(
        trace_context={"traceparent": VALID_TRACEPARENT, "tracestate": "vendor=value"}
    )
    assert envelope.trace_context == TraceContext(VALID_TRACEPARENT, "vendor=value")
    assert VALID_TRACEPARENT not in repr(envelope)
    assert VALID_TRACEPARENT not in repr(envelope.trace_context)


@pytest.mark.parametrize(
    "value",
    (
        {},
        {"traceparent": VALID_TRACEPARENT, "baggage": "secret=value"},
        {"traceparent": "invalid"},
        {"traceparent": "00-" + "0" * 32 + "-00f067aa0ba902b7-01"},
        {"traceparent": VALID_TRACEPARENT, "tracestate": "line\nbreak"},
        {"traceparent": VALID_TRACEPARENT, "tracestate": "x" * 513},
    ),
)
def test_invalid_or_expanded_trace_context_is_rejected(value: object) -> None:
    with pytest.raises(DispatchEnvelopeValidationError) as caught:
        create_envelope(trace_context=value)
    assert issue_codes(caught.value) == ("invalid_trace_context",)


def test_repr_and_errors_do_not_expose_content() -> None:
    secret = "sentinel-do-not-expose"
    envelope = create_envelope(
        task_payload={"value": secret}, references={"reference": secret}
    )
    assert secret not in repr(envelope)

    with pytest.raises(DispatchEnvelopeValidationError) as caught:
        create_envelope(task_payload={"value": secret * 1000})
    assert secret not in str(caught.value)
    assert secret not in repr(caught.value.issues)


def canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def nested_value(depth: int) -> dict[str, Any]:
    value: dict[str, Any] = {}
    current = value
    for _ in range(depth):
        child: dict[str, Any] = {}
        current["nested"] = child
        current = child
    return value
