"""Safe, bounded contracts for durable audit records."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from uuid import UUID

from taskforge.correlation import is_valid_correlation_id

MAX_AUDIT_PROVENANCE_BYTES = 2048
_NAME = re.compile(r"\A[a-z][a-z0-9_.-]{0,127}\Z")


class AuditActorKind(StrEnum):
    API_PRINCIPAL = "api_principal"
    WORKER = "worker"
    SYSTEM = "system"


class AuditOutcome(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"


class AuditAction(StrEnum):
    AUDIT_EXPORT = "audit.export"
    WORKFLOW_CREATE = "workflow.create"
    WORKFLOW_PUBLISH = "workflow.publish"
    WORKFLOW_AVAILABILITY_CHANGE = "workflow.availability_change"
    WORKER_SESSION_REGISTER = "worker_session.register"
    WORKER_SESSION_CAPABILITIES_REPLACE = "worker_session.capabilities_replace"
    WORKER_SESSION_HEARTBEAT = "worker_session.heartbeat"
    WORKER_SESSION_ENDED_STALE = "worker_session.ended_stale"
    WORKFLOW_RUN_CREATE = "workflow_run.create"
    WORKFLOW_RUN_CANCEL = "workflow_run.cancel"
    WORKFLOW_RUN_REPLAY = "workflow_run.replay"
    WORKFLOW_RUN_HISTORY_EXPORT = "workflow_run.history_export"
    TASK_CLAIM_ACQUIRE = "task_claim.acquire"
    TASK_CLAIM_RENEW = "task_claim.renew"
    TASK_ATTEMPT_START = "task_attempt.start"
    TASK_RESULT_SUBMIT = "task_result.submit"
    DEAD_LETTER_ACKNOWLEDGE = "dead_letter.acknowledge"
    DEAD_LETTER_RESOLVE = "dead_letter.resolve"
    DEAD_LETTER_REDRIVE = "dead_letter.redrive"
    IDENTITY_API_PRINCIPAL_CREATED = "identity.api_principal_created"
    IDENTITY_API_ROLES_ASSIGNED = "identity.api_roles_assigned"
    IDENTITY_WORKER_CREATED = "identity.worker_created"
    IDENTITY_API_CREDENTIAL_ADDED = "identity.api_credential_added"
    IDENTITY_WORKER_CREDENTIAL_ADDED = "identity.worker_credential_added"
    IDENTITY_API_CREDENTIAL_REVOKED = "identity.api_credential_revoked"
    IDENTITY_WORKER_CREDENTIAL_REVOKED = "identity.worker_credential_revoked"


AUDIT_ACTION_ALIASES = MappingProxyType(
    {
        "workflow.created": AuditAction.WORKFLOW_CREATE,
        "workflow.version_published": AuditAction.WORKFLOW_PUBLISH,
        "workflow.availability_changed": AuditAction.WORKFLOW_AVAILABILITY_CHANGE,
        "worker_session.registered": AuditAction.WORKER_SESSION_REGISTER,
        "worker_session.capabilities_replaced": (
            AuditAction.WORKER_SESSION_CAPABILITIES_REPLACE
        ),
    }
)


def canonical_audit_action(value: str) -> AuditAction:
    """Resolve one stored canonical action or historical semantic alias."""
    alias = AUDIT_ACTION_ALIASES.get(value)
    return alias if alias is not None else AuditAction(value)


def stored_audit_actions(action: AuditAction) -> frozenset[str]:
    """Return every historical stored spelling for one canonical action."""
    return frozenset(
        {action.value}
        | {
            stored
            for stored, canonical in AUDIT_ACTION_ALIASES.items()
            if canonical is action
        }
    )


@dataclass(frozen=True)
class _FieldRule:
    shape: str
    required: bool = True


@dataclass(frozen=True)
class _OutcomeContract:
    provenance: Mapping[str, _FieldRule]
    reasons: frozenset[str] = frozenset()
    target_optional_reasons: frozenset[str] = frozenset()
    target_always_optional: bool = False


@dataclass(frozen=True)
class _AuditContract:
    actor_kinds: frozenset[AuditActorKind]
    resource_type: str
    outcomes: Mapping[AuditOutcome, _OutcomeContract]


_EMPTY: Mapping[str, _FieldRule] = MappingProxyType({})
_SUMMARY = _FieldRule("summary")
_POSITIVE = _FieldRule("positive_int")
_NONNEGATIVE = _FieldRule("nonnegative_int")
_EXPORT_PROVENANCE = MappingProxyType(
    {
        "export_schema_version": _FieldRule("export_schema_version"),
        "filter_fingerprint": _FieldRule("sha256"),
        "high_water_present": _FieldRule("boolean"),
    }
)
_OPTIONAL_COUNTS = MappingProxyType(
    {
        "step_count": _FieldRule("nonnegative_int", required=False),
        "dependency_count": _FieldRule("nonnegative_int", required=False),
    }
)


def _accepted(
    provenance: Mapping[str, _FieldRule] = _EMPTY,
) -> _OutcomeContract:
    return _OutcomeContract(provenance)


def _rejected(
    reasons: set[str],
    provenance: Mapping[str, _FieldRule] = _EMPTY,
    *,
    target_optional_reasons: set[str] | None = None,
    target_always_optional: bool = False,
) -> _OutcomeContract:
    return _OutcomeContract(
        provenance,
        frozenset(reasons),
        frozenset(target_optional_reasons or ()),
        target_always_optional,
    )


_API = frozenset({AuditActorKind.API_PRINCIPAL})
_WORKER = frozenset({AuditActorKind.WORKER})
_SYSTEM = frozenset({AuditActorKind.SYSTEM})

_AUDIT_CONTRACTS: Mapping[AuditAction, _AuditContract] = MappingProxyType(
    {
        AuditAction.AUDIT_EXPORT: _AuditContract(
            _API,
            "audit_records",
            MappingProxyType(
                {
                    AuditOutcome.ACCEPTED: _OutcomeContract(
                        _EXPORT_PROVENANCE, target_always_optional=True
                    )
                }
            ),
        ),
        AuditAction.WORKFLOW_CREATE: _AuditContract(
            _API,
            "workflow",
            MappingProxyType(
                {
                    AuditOutcome.ACCEPTED: _accepted(_OPTIONAL_COUNTS),
                    AuditOutcome.REJECTED: _rejected(
                        {
                            "workflow_invalid",
                            "owner_not_found",
                            "owner_disabled",
                            "persistence_conflict",
                        },
                        _OPTIONAL_COUNTS,
                    ),
                }
            ),
        ),
        AuditAction.WORKFLOW_PUBLISH: _AuditContract(
            _API,
            "workflow",
            MappingProxyType(
                {
                    AuditOutcome.ACCEPTED: _accepted(
                        MappingProxyType(
                            {
                                "workflow_version_id": _FieldRule("uuid"),
                                "version_number": _POSITIVE,
                                "steps": _SUMMARY,
                            }
                        )
                    ),
                    AuditOutcome.REJECTED: _rejected(
                        {
                            "workflow_invalid",
                            "workflow_not_visible",
                            "owner_not_found",
                            "owner_disabled",
                            "persistence_conflict",
                        }
                    ),
                }
            ),
        ),
        AuditAction.WORKFLOW_AVAILABILITY_CHANGE: _AuditContract(
            _API,
            "workflow",
            MappingProxyType(
                {
                    AuditOutcome.ACCEPTED: _accepted(
                        MappingProxyType({"new_status": _FieldRule("workflow_status")})
                    ),
                    AuditOutcome.REJECTED: _rejected(
                        {
                            "workflow_not_visible",
                            "owner_not_found",
                            "owner_disabled",
                            "persistence_conflict",
                            "availability_transition_rejected",
                        }
                    ),
                }
            ),
        ),
        AuditAction.WORKER_SESSION_REGISTER: _AuditContract(
            _WORKER,
            "worker_session",
            MappingProxyType(
                {
                    AuditOutcome.ACCEPTED: _accepted(
                        MappingProxyType({"capabilities": _SUMMARY})
                    ),
                    AuditOutcome.REJECTED: _rejected(
                        {
                            "capability_advertisement_invalid",
                            "worker_authority_rejected",
                            "registration_conflict",
                        },
                        MappingProxyType({"capabilities": _SUMMARY}),
                        target_always_optional=True,
                    ),
                }
            ),
        ),
        AuditAction.WORKER_SESSION_CAPABILITIES_REPLACE: _AuditContract(
            _WORKER,
            "worker_session",
            MappingProxyType(
                {
                    AuditOutcome.ACCEPTED: _accepted(
                        MappingProxyType({"added": _SUMMARY, "removed": _SUMMARY})
                    ),
                    AuditOutcome.REJECTED: _rejected(
                        {
                            "capability_advertisement_invalid",
                            "worker_authority_rejected",
                            "worker_session_unavailable",
                            "worker_session_inactive",
                        },
                        MappingProxyType({"capabilities": _SUMMARY}),
                        target_optional_reasons={
                            "capability_advertisement_invalid",
                            "worker_authority_rejected",
                            "worker_session_unavailable",
                        },
                    ),
                }
            ),
        ),
        AuditAction.WORKER_SESSION_HEARTBEAT: _AuditContract(
            _WORKER,
            "worker_session",
            MappingProxyType(
                {
                    AuditOutcome.REJECTED: _rejected(
                        {
                            "worker_authority_rejected",
                            "worker_session_unavailable",
                            "worker_session_inactive",
                            "stale_heartbeat",
                            "heartbeat_sequence_gap",
                            "heartbeat_replay_conflict",
                        },
                        MappingProxyType({"sequence": _POSITIVE}),
                        target_optional_reasons={
                            "worker_authority_rejected",
                            "worker_session_unavailable",
                        },
                    )
                }
            ),
        ),
        AuditAction.WORKER_SESSION_ENDED_STALE: _AuditContract(
            _SYSTEM,
            "worker_session",
            MappingProxyType(
                {
                    AuditOutcome.ACCEPTED: _accepted(
                        MappingProxyType({"last_sequence": _NONNEGATIVE})
                    )
                }
            ),
        ),
        AuditAction.WORKFLOW_RUN_CREATE: _AuditContract(
            _API,
            "workflow",
            MappingProxyType(
                {AuditOutcome.REJECTED: _rejected({"idempotency_conflict"})}
            ),
        ),
        AuditAction.WORKFLOW_RUN_CANCEL: _AuditContract(
            _API,
            "workflow_run",
            MappingProxyType(
                {
                    AuditOutcome.REJECTED: _rejected(
                        {
                            "invalid_idempotency_key",
                            "invalid_reason",
                            "workflow_run_not_visible",
                            "idempotency_conflict",
                        }
                    )
                }
            ),
        ),
        AuditAction.WORKFLOW_RUN_REPLAY: _AuditContract(
            _API,
            "workflow_run",
            MappingProxyType(
                {
                    AuditOutcome.REJECTED: _rejected(
                        {
                            "invalid_idempotency_key",
                            "source_not_visible",
                            "source_not_replayable",
                            "invalid_failed_subgraph_request",
                            "failed_subgraph_invalid",
                            "idempotency_conflict",
                            "persistence_conflict",
                        },
                        MappingProxyType(
                            {"failed_steps": _FieldRule("summary", required=False)}
                        ),
                    )
                }
            ),
        ),
        AuditAction.WORKFLOW_RUN_HISTORY_EXPORT: _AuditContract(
            _API,
            "workflow_run",
            MappingProxyType({AuditOutcome.ACCEPTED: _accepted(_EXPORT_PROVENANCE)}),
        ),
        AuditAction.TASK_CLAIM_ACQUIRE: _AuditContract(
            _WORKER,
            "task_attempt",
            MappingProxyType(
                {
                    AuditOutcome.REJECTED: _rejected(
                        {
                            "invalid_dispatch",
                            "stale_attempt",
                            "obsolete_task",
                            "worker_authority_rejected",
                            "worker_session_unavailable",
                            "worker_session_inactive",
                            "worker_unavailable",
                            "capability_mismatch",
                            "already_authoritative",
                        },
                        MappingProxyType({"attempt_number": _POSITIVE}),
                    )
                }
            ),
        ),
        AuditAction.TASK_CLAIM_RENEW: _AuditContract(
            _WORKER,
            "task_attempt",
            MappingProxyType(
                {
                    AuditOutcome.REJECTED: _rejected(
                        {
                            "claim_expired",
                            "claim_recovered",
                            "stale_claim",
                            "task_inactive",
                        },
                        MappingProxyType({"claim_generation": _POSITIVE}),
                    )
                }
            ),
        ),
        AuditAction.TASK_ATTEMPT_START: _AuditContract(
            _WORKER,
            "task_attempt",
            MappingProxyType(
                {
                    AuditOutcome.REJECTED: _rejected(
                        {
                            "worker_authority_rejected",
                            "worker_session_rejected",
                            "stale_claim",
                        },
                        MappingProxyType({"claim_generation": _POSITIVE}),
                    )
                }
            ),
        ),
        AuditAction.TASK_RESULT_SUBMIT: _AuditContract(
            _WORKER,
            "task_attempt",
            MappingProxyType(
                {
                    AuditOutcome.REJECTED: _rejected(
                        {
                            "worker_authority_rejected",
                            "result_target_not_found",
                            "invalid_task_state",
                            "result_invalid_output",
                        },
                        MappingProxyType({"claim_generation": _POSITIVE}),
                    )
                }
            ),
        ),
        AuditAction.DEAD_LETTER_ACKNOWLEDGE: _AuditContract(
            _API,
            "dead_letter",
            MappingProxyType(
                {
                    AuditOutcome.REJECTED: _rejected(
                        {"dead_letter_not_visible", "transition_conflict"}
                    )
                }
            ),
        ),
        AuditAction.DEAD_LETTER_RESOLVE: _AuditContract(
            _API,
            "dead_letter",
            MappingProxyType(
                {
                    AuditOutcome.REJECTED: _rejected(
                        {"dead_letter_not_visible", "transition_conflict"}
                    )
                }
            ),
        ),
        AuditAction.DEAD_LETTER_REDRIVE: _AuditContract(
            _API,
            "dead_letter",
            MappingProxyType(
                {
                    AuditOutcome.REJECTED: _rejected(
                        {
                            "dead_letter_not_visible",
                            "invalid_idempotency_key",
                            "redrive_not_eligible",
                            "redrive_limit_exceeded",
                            "idempotency_conflict",
                        }
                    )
                }
            ),
        ),
        AuditAction.IDENTITY_API_PRINCIPAL_CREATED: _AuditContract(
            _SYSTEM,
            "api_principal",
            MappingProxyType({AuditOutcome.ACCEPTED: _accepted()}),
        ),
        AuditAction.IDENTITY_API_ROLES_ASSIGNED: _AuditContract(
            _SYSTEM,
            "api_principal",
            MappingProxyType(
                {
                    AuditOutcome.ACCEPTED: _accepted(
                        MappingProxyType({"roles": _SUMMARY})
                    )
                }
            ),
        ),
        AuditAction.IDENTITY_WORKER_CREATED: _AuditContract(
            _SYSTEM,
            "worker_identity",
            MappingProxyType({AuditOutcome.ACCEPTED: _accepted()}),
        ),
        AuditAction.IDENTITY_API_CREDENTIAL_ADDED: _AuditContract(
            _SYSTEM,
            "api_credential",
            MappingProxyType({AuditOutcome.ACCEPTED: _accepted()}),
        ),
        AuditAction.IDENTITY_WORKER_CREDENTIAL_ADDED: _AuditContract(
            _SYSTEM,
            "worker_credential",
            MappingProxyType({AuditOutcome.ACCEPTED: _accepted()}),
        ),
        AuditAction.IDENTITY_API_CREDENTIAL_REVOKED: _AuditContract(
            _SYSTEM,
            "api_credential",
            MappingProxyType({AuditOutcome.ACCEPTED: _accepted()}),
        ),
        AuditAction.IDENTITY_WORKER_CREDENTIAL_REVOKED: _AuditContract(
            _SYSTEM,
            "worker_credential",
            MappingProxyType({AuditOutcome.ACCEPTED: _accepted()}),
        ),
    }
)


@dataclass(frozen=True)
class AuditActor:
    kind: AuditActorKind
    api_principal_id: UUID | None = None
    worker_identity_id: UUID | None = None
    worker_session_id: UUID | None = None
    system_component: str | None = None

    def __post_init__(self) -> None:
        api = self.api_principal_id is not None
        worker = self.worker_identity_id is not None
        system = self.system_component is not None
        if self.kind is AuditActorKind.API_PRINCIPAL and not (
            api and not worker and not system and self.worker_session_id is None
        ):
            raise ValueError("API audit actor shape is invalid")
        if self.kind is AuditActorKind.WORKER and not (
            worker and not api and not system
        ):
            raise ValueError("worker audit actor shape is invalid")
        if self.kind is AuditActorKind.SYSTEM and not (
            system and not api and not worker and self.worker_session_id is None
        ):
            raise ValueError("system audit actor shape is invalid")
        if (
            self.system_component is not None
            and _NAME.fullmatch(self.system_component) is None
        ):
            raise ValueError("system audit component is invalid")


@dataclass(frozen=True)
class AuditRecord:
    id: UUID
    actor: AuditActor
    action: str
    outcome: AuditOutcome
    resource_type: str
    resource_id: UUID | None
    correlation_id: str | None
    provenance: Mapping[str, object]
    reason_code: str | None = None

    def __post_init__(self) -> None:
        for value in (self.action, self.resource_type):
            if _NAME.fullmatch(value) is None:
                raise ValueError("audit name is invalid")
        if (self.outcome is AuditOutcome.REJECTED) is (self.reason_code is None):
            raise ValueError("audit rejection reason shape is invalid")
        if self.reason_code is not None and _NAME.fullmatch(self.reason_code) is None:
            raise ValueError("audit reason is invalid")
        if not is_valid_correlation_id(self.correlation_id):
            raise ValueError("audit correlation is invalid")
        copied = json.loads(
            json.dumps(dict(self.provenance), separators=(",", ":"), sort_keys=True)
        )
        if not isinstance(copied, dict) or len(copied) > 16 or _depth(copied) > 2:
            raise ValueError("audit provenance shape is invalid")
        if (
            len(json.dumps(copied, ensure_ascii=False, sort_keys=True).encode())
            > MAX_AUDIT_PROVENANCE_BYTES
        ):
            raise ValueError("audit provenance is too large")
        _validate_semantics(self, copied)
        object.__setattr__(self, "provenance", MappingProxyType(copied))


class AuditRejected(Exception):
    """Required rejected-operation audit could not be committed."""


def bounded_string_set(values: tuple[str, ...] | frozenset[str]) -> dict[str, object]:
    """Represent arbitrary valid domain lists without exceeding the audit envelope."""
    ordered = sorted(values)
    encoded = json.dumps(ordered, ensure_ascii=False, separators=(",", ":")).encode()
    return {"count": len(ordered), "sha256": hashlib.sha256(encoded).hexdigest()}


def _depth(value: object) -> int:
    if isinstance(value, dict):
        return 1 + max((_depth(item) for item in value.values()), default=0)
    if isinstance(value, list):
        return 1 + max((_depth(item) for item in value), default=0)
    return 0


def _validate_semantics(record: AuditRecord, provenance: dict[str, object]) -> None:
    try:
        action = AuditAction(record.action)
    except ValueError as error:
        raise ValueError("audit action is unsupported") from error
    contract = _AUDIT_CONTRACTS[action]
    if record.actor.kind not in contract.actor_kinds:
        raise ValueError("audit actor is invalid for action")
    if record.resource_type != contract.resource_type:
        raise ValueError("audit resource type is invalid for action")
    outcome = contract.outcomes.get(record.outcome)
    if outcome is None:
        raise ValueError("audit outcome is invalid for action")
    if (
        record.outcome is AuditOutcome.REJECTED
        and record.reason_code not in outcome.reasons
    ):
        raise ValueError("audit reason is invalid for action")
    target_optional = outcome.target_always_optional or (
        record.reason_code in outcome.target_optional_reasons
    )
    if record.resource_id is None and not target_optional:
        raise ValueError("audit target is required for action")
    if set(provenance) - set(outcome.provenance):
        raise ValueError("audit provenance key is invalid for action")
    missing = {key for key, rule in outcome.provenance.items() if rule.required} - set(
        provenance
    )
    if missing:
        raise ValueError("audit provenance key is required for action")
    for key, value in provenance.items():
        if not _valid_field(value, outcome.provenance[key].shape):
            raise ValueError("audit provenance value is invalid for action")


def _valid_field(value: object, shape: str) -> bool:
    if shape == "boolean":
        return type(value) is bool
    if shape == "sha256":
        return (
            isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None
        )
    if shape == "export_schema_version":
        return value == "taskforge.history-export.v1"
    if shape == "positive_int":
        return type(value) is int and value > 0
    if shape == "nonnegative_int":
        return type(value) is int and value >= 0
    if shape == "uuid":
        try:
            return isinstance(value, str) and str(UUID(value)) == value.lower()
        except ValueError:
            return False
    if shape == "workflow_status":
        return value in {"enabled", "disabled"}
    if shape == "summary":
        return (
            isinstance(value, dict)
            and set(value) == {"count", "sha256"}
            and type(value["count"]) is int
            and value["count"] >= 0
            and isinstance(value["sha256"], str)
            and re.fullmatch(r"[0-9a-f]{64}", value["sha256"]) is not None
        )
    raise AssertionError(f"unknown audit field shape: {shape}")
