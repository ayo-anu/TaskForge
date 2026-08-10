"""Versioned, broker-neutral task dispatch envelope contract."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping
from dataclasses import InitVar, dataclass
from types import MappingProxyType
from uuid import UUID

from taskforge.workflows.task_types import (
    MAX_COLLECTION_ITEMS,
    MAX_PARAMETER_DEPTH,
    MAX_PARAMETER_KEY_LENGTH,
    MAX_PARAMETER_NODES,
    MAX_PARAMETER_STRING_LENGTH,
)

DISPATCH_ENVELOPE_VERSION = 1
MAX_TASK_PAYLOAD_BYTES = 16 * 1024
MAX_REFERENCE_BYTES = 16 * 1024
MAX_DISPATCH_ENVELOPE_BYTES = 32 * 1024
MAX_CORRELATION_ID_LENGTH = 128
MAX_TRACE_STATE_LENGTH = 512

type FrozenJSONScalar = bool | int | float | str | None
type FrozenJSONValue = (
    FrozenJSONScalar | tuple[FrozenJSONValue, ...] | Mapping[str, FrozenJSONValue]
)
type FrozenJSONMapping = Mapping[str, FrozenJSONValue]
type ValidationPath = tuple[str | int, ...]

_TASK_OR_CAPABILITY = re.compile(r"\A[a-z][a-z0-9_.-]{0,127}\Z")
_TRACE_PARENT = re.compile(r"\A00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})\Z")
_ENVELOPE_FIELDS = frozenset(
    {
        "schema_version",
        "dispatch_id",
        "task_attempt_id",
        "task_run_id",
        "workflow_run_id",
        "attempt_number",
        "task_type",
        "required_capability",
        "task_payload",
        "references",
        "correlation_id",
        "trace_context",
    }
)
_REQUIRED_FIELDS = _ENVELOPE_FIELDS - {"correlation_id", "trace_context"}
_VALIDATED_CONSTRUCTION = object()


@dataclass(frozen=True)
class DispatchEnvelopeIssue:
    """One safe, deterministic envelope validation issue."""

    code: str
    path: ValidationPath
    message: str


class DispatchEnvelopeValidationError(ValueError):
    """A dispatch envelope failed safe structural validation."""

    def __init__(self, issues: tuple[DispatchEnvelopeIssue, ...]) -> None:
        if not issues:
            raise ValueError("at least one dispatch envelope issue is required")
        self.issues = issues
        super().__init__("dispatch envelope validation failed")


@dataclass(frozen=True, repr=False)
class TraceContext:
    traceparent: str
    tracestate: str | None = None

    def __repr__(self) -> str:
        return "TraceContext(traceparent=<redacted>, tracestate=<redacted>)"


@dataclass(frozen=True, repr=False)
class DispatchEnvelope:
    schema_version: int
    dispatch_id: UUID
    task_attempt_id: UUID
    task_run_id: UUID
    workflow_run_id: UUID
    attempt_number: int
    task_type: str
    required_capability: str
    task_payload: FrozenJSONMapping
    references: FrozenJSONMapping
    correlation_id: str | None = None
    trace_context: TraceContext | None = None
    _validated_construction: InitVar[object] = None

    def __post_init__(self, _validated_construction: object) -> None:
        if _validated_construction is not _VALIDATED_CONSTRUCTION:
            raise ValueError("use create_dispatch_envelope to construct an envelope")

    @property
    def route(self) -> str:
        return dispatch_route(self.required_capability)

    def __repr__(self) -> str:
        return (
            "DispatchEnvelope("
            f"schema_version={self.schema_version!r}, "
            f"dispatch_id={self.dispatch_id!r}, "
            f"task_attempt_id={self.task_attempt_id!r}, "
            f"task_run_id={self.task_run_id!r}, "
            f"workflow_run_id={self.workflow_run_id!r}, "
            f"attempt_number={self.attempt_number!r}, "
            f"task_type={self.task_type!r}, "
            f"required_capability={self.required_capability!r}, "
            "task_payload=<redacted>, references=<redacted>, "
            f"correlation_id={self.correlation_id!r}, trace_context=<redacted>)"
        )


def dispatch_route(required_capability: str) -> str:
    """Derive the sole broker-neutral logical route for a capability."""
    if (
        not isinstance(required_capability, str)
        or _TASK_OR_CAPABILITY.fullmatch(required_capability) is None
    ):
        raise DispatchEnvelopeValidationError(
            (
                DispatchEnvelopeIssue(
                    "invalid_required_capability",
                    ("required_capability",),
                    "Required capability is invalid.",
                ),
            )
        )
    return f"capability.{required_capability}"


def create_dispatch_envelope(
    *,
    dispatch_id: object,
    task_attempt_id: object,
    task_run_id: object,
    workflow_run_id: object,
    attempt_number: object,
    task_type: object,
    required_capability: object,
    task_payload: object,
    references: object,
    correlation_id: object = None,
    trace_context: object = None,
) -> DispatchEnvelope:
    """Validate and freeze one version 1 dispatch envelope."""
    return _create_from_mapping(
        {
            "schema_version": DISPATCH_ENVELOPE_VERSION,
            "dispatch_id": dispatch_id,
            "task_attempt_id": task_attempt_id,
            "task_run_id": task_run_id,
            "workflow_run_id": workflow_run_id,
            "attempt_number": attempt_number,
            "task_type": task_type,
            "required_capability": required_capability,
            "task_payload": task_payload,
            "references": references,
            "correlation_id": correlation_id,
            "trace_context": trace_context,
        },
        canonical_uuid_strings=False,
    )


def dispatch_envelope_to_mapping(envelope: DispatchEnvelope) -> dict[str, object]:
    """Return a detached JSON-compatible mapping for durable persistence."""
    mapping: dict[str, object] = {
        "schema_version": envelope.schema_version,
        "dispatch_id": str(envelope.dispatch_id),
        "task_attempt_id": str(envelope.task_attempt_id),
        "task_run_id": str(envelope.task_run_id),
        "workflow_run_id": str(envelope.workflow_run_id),
        "attempt_number": envelope.attempt_number,
        "task_type": envelope.task_type,
        "required_capability": envelope.required_capability,
        "task_payload": _thaw_json(envelope.task_payload),
        "references": _thaw_json(envelope.references),
    }
    if envelope.correlation_id is not None:
        mapping["correlation_id"] = envelope.correlation_id
    if envelope.trace_context is not None:
        context: dict[str, str] = {"traceparent": envelope.trace_context.traceparent}
        if envelope.trace_context.tracestate is not None:
            context["tracestate"] = envelope.trace_context.tracestate
        mapping["trace_context"] = context
    return mapping


def serialize_dispatch_envelope(envelope: DispatchEnvelope) -> bytes:
    """Serialize a validated envelope to deterministic canonical JSON bytes."""
    encoded = _canonical_json(dispatch_envelope_to_mapping(envelope))
    if len(encoded) > MAX_DISPATCH_ENVELOPE_BYTES:
        raise _single_issue("envelope_too_large", (), "Dispatch envelope is too large.")
    return encoded


def deserialize_dispatch_envelope(data: bytes) -> DispatchEnvelope:
    """Parse and validate untrusted canonical or non-canonical JSON bytes."""
    if not isinstance(data, bytes):
        raise _single_issue(
            "invalid_envelope", (), "Dispatch envelope must be UTF-8 JSON bytes."
        )
    if len(data) > MAX_DISPATCH_ENVELOPE_BYTES:
        raise _single_issue("envelope_too_large", (), "Dispatch envelope is too large.")
    try:
        decoded = data.decode("utf-8")
    except UnicodeDecodeError:
        raise _single_issue(
            "invalid_utf8", (), "Dispatch envelope is not valid UTF-8."
        ) from None
    try:
        value = json.loads(
            decoded,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_nonstandard_json_constant,
        )
    except _DuplicateJSONKey:
        raise _single_issue(
            "duplicate_json_key", (), "Dispatch envelope contains duplicate keys."
        ) from None
    except (json.JSONDecodeError, ValueError):
        raise _single_issue(
            "malformed_json", (), "Dispatch envelope is not valid JSON."
        ) from None
    if not isinstance(value, dict):
        raise _single_issue(
            "invalid_envelope", (), "Dispatch envelope must be a JSON object."
        )
    return _create_from_mapping(value, canonical_uuid_strings=True)


def _create_from_mapping(
    value: Mapping[str, object], *, canonical_uuid_strings: bool
) -> DispatchEnvelope:
    version = value.get("schema_version")
    if type(version) is not int or version != DISPATCH_ENVELOPE_VERSION:
        raise _single_issue(
            "unsupported_schema_version",
            ("schema_version",),
            "Dispatch envelope schema version is unsupported.",
        )

    issues: list[DispatchEnvelopeIssue] = []
    for field in sorted(_REQUIRED_FIELDS - value.keys()):
        issues.append(
            DispatchEnvelopeIssue(
                "missing_field", (field,), "Required envelope field is missing."
            )
        )
    for field in sorted(value.keys() - _ENVELOPE_FIELDS):
        issues.append(
            DispatchEnvelopeIssue(
                "unknown_field", (field,), "Envelope field is not supported."
            )
        )
    if issues:
        raise DispatchEnvelopeValidationError(tuple(issues))

    dispatch_id = _validate_uuid(
        value["dispatch_id"], "dispatch_id", canonical_uuid_strings, issues
    )
    task_attempt_id = _validate_uuid(
        value["task_attempt_id"], "task_attempt_id", canonical_uuid_strings, issues
    )
    task_run_id = _validate_uuid(
        value["task_run_id"], "task_run_id", canonical_uuid_strings, issues
    )
    workflow_run_id = _validate_uuid(
        value["workflow_run_id"], "workflow_run_id", canonical_uuid_strings, issues
    )
    attempt_number = value["attempt_number"]
    if type(attempt_number) is not int or attempt_number <= 0:
        issues.append(
            DispatchEnvelopeIssue(
                "invalid_attempt_number",
                ("attempt_number",),
                "Attempt number must be a positive integer.",
            )
        )
    task_type = _validate_name(value["task_type"], "task_type", issues)
    required_capability = _validate_name(
        value["required_capability"], "required_capability", issues
    )
    frozen_payload = _validate_and_freeze_mapping(
        value["task_payload"],
        "task_payload",
        MAX_TASK_PAYLOAD_BYTES,
        "task_payload_too_large",
        issues,
    )
    frozen_references = _validate_and_freeze_mapping(
        value["references"],
        "references",
        MAX_REFERENCE_BYTES,
        "references_too_large",
        issues,
    )
    correlation_id = _validate_correlation_id(value.get("correlation_id"), issues)
    trace_context = _validate_trace_context(value.get("trace_context"), issues)
    if issues:
        raise DispatchEnvelopeValidationError(tuple(issues))

    assert dispatch_id is not None
    assert task_attempt_id is not None
    assert task_run_id is not None
    assert workflow_run_id is not None
    assert type(attempt_number) is int
    assert task_type is not None
    assert required_capability is not None
    assert frozen_payload is not None
    assert frozen_references is not None
    envelope = DispatchEnvelope(
        schema_version=DISPATCH_ENVELOPE_VERSION,
        dispatch_id=dispatch_id,
        task_attempt_id=task_attempt_id,
        task_run_id=task_run_id,
        workflow_run_id=workflow_run_id,
        attempt_number=attempt_number,
        task_type=task_type,
        required_capability=required_capability,
        task_payload=frozen_payload,
        references=frozen_references,
        correlation_id=correlation_id,
        trace_context=trace_context,
        _validated_construction=_VALIDATED_CONSTRUCTION,
    )
    serialize_dispatch_envelope(envelope)
    return envelope


def _validate_uuid(
    value: object,
    field: str,
    canonical_string: bool,
    issues: list[DispatchEnvelopeIssue],
) -> UUID | None:
    if canonical_string:
        if not isinstance(value, str):
            parsed = None
        else:
            try:
                parsed = UUID(value)
            except ValueError:
                parsed = None
            if parsed is not None and str(parsed) != value:
                parsed = None
    else:
        parsed = value if isinstance(value, UUID) else None
    if parsed is None:
        issues.append(
            DispatchEnvelopeIssue(
                "invalid_identifier", (field,), "Envelope identifier is invalid."
            )
        )
    return parsed


def _validate_name(
    value: object, field: str, issues: list[DispatchEnvelopeIssue]
) -> str | None:
    if not isinstance(value, str) or _TASK_OR_CAPABILITY.fullmatch(value) is None:
        issues.append(
            DispatchEnvelopeIssue(
                f"invalid_{field}",
                (field,),
                f"{field.replace('_', ' ').title()} is invalid.",
            )
        )
        return None
    return value


def _validate_and_freeze_mapping(
    value: object,
    field: str,
    byte_limit: int,
    size_code: str,
    issues: list[DispatchEnvelopeIssue],
) -> FrozenJSONMapping | None:
    if not isinstance(value, dict):
        issues.append(
            DispatchEnvelopeIssue(
                f"invalid_{field}",
                (field,),
                f"{field.replace('_', ' ').title()} must be a JSON object.",
            )
        )
        return None
    structural_issues: list[DispatchEnvelopeIssue] = []
    node_count = [0]
    frozen = _freeze_json(value, (field,), 0, node_count, set(), structural_issues)
    issues.extend(structural_issues)
    if structural_issues:
        return None
    assert isinstance(frozen, Mapping)
    if len(_canonical_json(_thaw_json(frozen))) > byte_limit:
        issues.append(
            DispatchEnvelopeIssue(
                size_code, (field,), f"{field.replace('_', ' ').title()} is too large."
            )
        )
        return None
    return frozen


def _freeze_json(
    value: object,
    path: ValidationPath,
    depth: int,
    node_count: list[int],
    active: set[int],
    issues: list[DispatchEnvelopeIssue],
) -> FrozenJSONValue:
    node_count[0] += 1
    if node_count[0] > MAX_PARAMETER_NODES:
        if not any(issue.code == "json_too_complex" for issue in issues):
            issues.append(
                DispatchEnvelopeIssue(
                    "json_too_complex", path[:1], "JSON value contains too many nodes."
                )
            )
        return None
    if depth > MAX_PARAMETER_DEPTH:
        issues.append(
            DispatchEnvelopeIssue(
                "json_too_deep", path, "JSON value is nested too deeply."
            )
        )
        return None
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            issues.append(
                DispatchEnvelopeIssue(
                    "invalid_json_value", path, "JSON number must be finite."
                )
            )
            return None
        return value
    if isinstance(value, str):
        if len(value) > MAX_PARAMETER_STRING_LENGTH:
            issues.append(
                DispatchEnvelopeIssue(
                    "json_string_too_large", path, "JSON string is too large."
                )
            )
        return value
    if not isinstance(value, (dict, list)):
        issues.append(
            DispatchEnvelopeIssue(
                "invalid_json_value", path, "Value is not JSON compatible."
            )
        )
        return None
    identity = id(value)
    if identity in active:
        issues.append(
            DispatchEnvelopeIssue(
                "recursive_json_value", path, "JSON value is recursive."
            )
        )
        return None
    if len(value) > MAX_COLLECTION_ITEMS:
        issues.append(
            DispatchEnvelopeIssue(
                "json_too_complex", path, "JSON collection contains too many items."
            )
        )
    active.add(identity)
    try:
        if isinstance(value, dict):
            frozen_mapping: dict[str, FrozenJSONValue] = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    issues.append(
                        DispatchEnvelopeIssue(
                            "invalid_json_key",
                            path,
                            "JSON object key must be a string.",
                        )
                    )
                    continue
                child_path = (*path, key)
                if len(key) > MAX_PARAMETER_KEY_LENGTH:
                    issues.append(
                        DispatchEnvelopeIssue(
                            "json_key_too_large",
                            child_path,
                            "JSON object key is too large.",
                        )
                    )
                frozen_mapping[key] = _freeze_json(
                    item, child_path, depth + 1, node_count, active, issues
                )
            return MappingProxyType(frozen_mapping)
        return tuple(
            _freeze_json(item, (*path, index), depth + 1, node_count, active, issues)
            for index, item in enumerate(value)
        )
    finally:
        active.remove(identity)


def _validate_correlation_id(
    value: object, issues: list[DispatchEnvelopeIssue]
) -> str | None:
    if value is None:
        return None
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= MAX_CORRELATION_ID_LENGTH
        or not value.isascii()
        or value != value.strip()
        or any(ord(character) < 32 or ord(character) > 126 for character in value)
    ):
        issues.append(
            DispatchEnvelopeIssue(
                "invalid_correlation_id",
                ("correlation_id",),
                "Correlation identifier is invalid.",
            )
        )
        return None
    return value


def _validate_trace_context(
    value: object, issues: list[DispatchEnvelopeIssue]
) -> TraceContext | None:
    if value is None:
        return None
    if isinstance(value, TraceContext):
        traceparent, tracestate = value.traceparent, value.tracestate
    elif isinstance(value, dict):
        unknown = value.keys() - {"traceparent", "tracestate"}
        if unknown or "traceparent" not in value:
            issues.append(
                DispatchEnvelopeIssue(
                    "invalid_trace_context",
                    ("trace_context",),
                    "Trace context is invalid.",
                )
            )
            return None
        traceparent = value["traceparent"]
        tracestate = value.get("tracestate")
    else:
        issues.append(
            DispatchEnvelopeIssue(
                "invalid_trace_context", ("trace_context",), "Trace context is invalid."
            )
        )
        return None
    match = (
        _TRACE_PARENT.fullmatch(traceparent) if isinstance(traceparent, str) else None
    )
    if match is None or match.group(1) == "0" * 32 or match.group(2) == "0" * 16:
        issues.append(
            DispatchEnvelopeIssue(
                "invalid_trace_context",
                ("trace_context", "traceparent"),
                "Trace parent is invalid.",
            )
        )
        return None
    if tracestate is not None and (
        not isinstance(tracestate, str)
        or not 1 <= len(tracestate) <= MAX_TRACE_STATE_LENGTH
        or not tracestate.isascii()
        or any(ord(character) < 32 or ord(character) > 126 for character in tracestate)
    ):
        issues.append(
            DispatchEnvelopeIssue(
                "invalid_trace_context",
                ("trace_context", "tracestate"),
                "Trace state is invalid.",
            )
        )
        return None
    return TraceContext(traceparent, tracestate)


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _thaw_json(value: FrozenJSONValue) -> object:
    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


class _DuplicateJSONKey(ValueError):
    pass


def _reject_nonstandard_json_constant(_value: str) -> None:
    raise ValueError("non-standard JSON numeric constant")


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise _DuplicateJSONKey
        value[key] = item
    return value


def _single_issue(
    code: str, path: ValidationPath, message: str
) -> DispatchEnvelopeValidationError:
    return DispatchEnvelopeValidationError(
        (DispatchEnvelopeIssue(code, path, message),)
    )
