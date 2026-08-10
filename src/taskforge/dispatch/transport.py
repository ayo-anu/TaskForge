"""Pure validation for dispatch messages received from a transport."""

from __future__ import annotations

from dataclasses import dataclass

from taskforge.dispatch.envelope import (
    DispatchEnvelope,
    DispatchEnvelopeValidationError,
    deserialize_dispatch_envelope,
)


@dataclass(frozen=True, repr=False)
class DispatchTransportMetadata:
    """Narrow broker-neutral metadata needed to validate one delivery."""

    message_id: str | None
    routing_key: str
    content_type: str | None
    content_encoding: str | None

    def __repr__(self) -> str:
        return "DispatchTransportMetadata(<redacted>)"


@dataclass(frozen=True, repr=False)
class ValidatedDispatchTransport:
    """A permanently immutable, validated dispatch and no delivery ownership."""

    envelope: DispatchEnvelope

    def __repr__(self) -> str:
        return "ValidatedDispatchTransport(envelope=<redacted>)"


@dataclass(frozen=True)
class MalformedDispatchTransport:
    """Safe permanent-malformed classification with no untrusted values."""

    code: str


type DispatchTransportValidation = (
    ValidatedDispatchTransport | MalformedDispatchTransport
)


def validate_dispatch_transport(
    body: bytes, metadata: DispatchTransportMetadata
) -> DispatchTransportValidation:
    """Validate transport metadata and delegate body validation to Task 2."""
    if metadata.content_type != "application/json":
        return MalformedDispatchTransport("unsupported_content_type")
    if metadata.content_encoding != "utf-8":
        return MalformedDispatchTransport("unsupported_content_encoding")
    try:
        envelope = deserialize_dispatch_envelope(body)
    except DispatchEnvelopeValidationError as error:
        return MalformedDispatchTransport(error.issues[0].code)
    if metadata.message_id != str(envelope.dispatch_id):
        return MalformedDispatchTransport("dispatch_identity_mismatch")
    if metadata.routing_key != envelope.route:
        return MalformedDispatchTransport("dispatch_route_mismatch")
    return ValidatedDispatchTransport(envelope)
