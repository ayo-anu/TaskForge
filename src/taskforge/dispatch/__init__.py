"""Broker-neutral task dispatch contracts."""

from taskforge.dispatch.envelope import (
    DISPATCH_ENVELOPE_VERSION,
    MAX_DISPATCH_ENVELOPE_BYTES,
    MAX_REFERENCE_BYTES,
    MAX_TASK_PAYLOAD_BYTES,
    DispatchEnvelope,
    DispatchEnvelopeIssue,
    DispatchEnvelopeValidationError,
    TraceContext,
    create_dispatch_envelope,
    deserialize_dispatch_envelope,
    dispatch_envelope_to_mapping,
    dispatch_route,
    serialize_dispatch_envelope,
)

__all__ = [
    "DISPATCH_ENVELOPE_VERSION",
    "MAX_DISPATCH_ENVELOPE_BYTES",
    "MAX_REFERENCE_BYTES",
    "MAX_TASK_PAYLOAD_BYTES",
    "DispatchEnvelope",
    "DispatchEnvelopeIssue",
    "DispatchEnvelopeValidationError",
    "TraceContext",
    "create_dispatch_envelope",
    "deserialize_dispatch_envelope",
    "dispatch_envelope_to_mapping",
    "dispatch_route",
    "serialize_dispatch_envelope",
]
