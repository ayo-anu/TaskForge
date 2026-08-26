"""Audit envelope actor and bounded-provenance contracts."""

import json
from uuid import uuid4

import pytest

from taskforge.audit.domain import (
    AUDIT_ACTION_ALIASES,
    MAX_AUDIT_PROVENANCE_BYTES,
    AuditAction,
    AuditActor,
    AuditActorKind,
    AuditOutcome,
    AuditRecord,
    bounded_string_set,
    canonical_audit_action,
)


def test_worker_actor_allows_registration_without_synthetic_session() -> None:
    identity = uuid4()
    actor = AuditActor(AuditActorKind.WORKER, worker_identity_id=identity)
    assert actor.worker_identity_id == identity
    assert actor.worker_session_id is None
    with pytest.raises(ValueError):
        AuditActor(AuditActorKind.WORKER)


def test_valid_maximum_lists_use_bounded_deterministic_summary() -> None:
    values = tuple(f"capability.{index:03d}." + "x" * 100 for index in range(256))
    first = bounded_string_set(values)
    second = bounded_string_set(tuple(reversed(values)))
    assert first == second
    assert first["count"] == 256
    assert len(json.dumps(first).encode()) < MAX_AUDIT_PROVENANCE_BYTES


def test_provenance_limit_and_rejection_reason_are_enforced() -> None:
    actor = AuditActor(AuditActorKind.API_PRINCIPAL, api_principal_id=uuid4())
    with pytest.raises(ValueError):
        AuditRecord(
            uuid4(),
            actor,
            "workflow.create",
            AuditOutcome.ACCEPTED,
            "workflow",
            uuid4(),
            None,
            {"value": "x" * 2048},
        )
    with pytest.raises(ValueError):
        AuditRecord(
            uuid4(),
            actor,
            "workflow.create",
            AuditOutcome.REJECTED,
            "workflow",
            uuid4(),
            None,
            {},
        )


def _actor(kind: AuditActorKind) -> AuditActor:
    if kind is AuditActorKind.API_PRINCIPAL:
        return AuditActor(kind, api_principal_id=uuid4())
    if kind is AuditActorKind.WORKER:
        return AuditActor(kind, worker_identity_id=uuid4(), worker_session_id=uuid4())
    return AuditActor(kind, system_component="test_component")


_SUMMARY = {"count": 1, "sha256": "0" * 64}
_VALID_CONTRACT_CASES = (
    (
        AuditAction.WORKFLOW_CREATE,
        AuditOutcome.ACCEPTED,
        AuditActorKind.API_PRINCIPAL,
        "workflow",
        {"step_count": 1, "dependency_count": 0},
        None,
    ),
    (
        AuditAction.WORKFLOW_CREATE,
        AuditOutcome.REJECTED,
        AuditActorKind.API_PRINCIPAL,
        "workflow",
        {},
        "workflow_invalid",
    ),
    (
        AuditAction.WORKFLOW_PUBLISH,
        AuditOutcome.ACCEPTED,
        AuditActorKind.API_PRINCIPAL,
        "workflow",
        {
            "workflow_version_id": "00000000-0000-0000-0000-000000000001",
            "version_number": 1,
            "steps": _SUMMARY,
        },
        None,
    ),
    (
        AuditAction.WORKFLOW_PUBLISH,
        AuditOutcome.REJECTED,
        AuditActorKind.API_PRINCIPAL,
        "workflow",
        {},
        "workflow_not_visible",
    ),
    (
        AuditAction.WORKFLOW_AVAILABILITY_CHANGE,
        AuditOutcome.ACCEPTED,
        AuditActorKind.API_PRINCIPAL,
        "workflow",
        {"new_status": "enabled"},
        None,
    ),
    (
        AuditAction.WORKFLOW_AVAILABILITY_CHANGE,
        AuditOutcome.REJECTED,
        AuditActorKind.API_PRINCIPAL,
        "workflow",
        {},
        "availability_transition_rejected",
    ),
    (
        AuditAction.WORKER_SESSION_REGISTER,
        AuditOutcome.ACCEPTED,
        AuditActorKind.WORKER,
        "worker_session",
        {"capabilities": _SUMMARY},
        None,
    ),
    (
        AuditAction.WORKER_SESSION_REGISTER,
        AuditOutcome.REJECTED,
        AuditActorKind.WORKER,
        "worker_session",
        {"capabilities": _SUMMARY},
        "registration_conflict",
    ),
    (
        AuditAction.WORKER_SESSION_CAPABILITIES_REPLACE,
        AuditOutcome.ACCEPTED,
        AuditActorKind.WORKER,
        "worker_session",
        {"added": _SUMMARY, "removed": _SUMMARY},
        None,
    ),
    (
        AuditAction.WORKER_SESSION_CAPABILITIES_REPLACE,
        AuditOutcome.REJECTED,
        AuditActorKind.WORKER,
        "worker_session",
        {"capabilities": _SUMMARY},
        "worker_session_inactive",
    ),
    (
        AuditAction.WORKER_SESSION_HEARTBEAT,
        AuditOutcome.REJECTED,
        AuditActorKind.WORKER,
        "worker_session",
        {"sequence": 1},
        "stale_heartbeat",
    ),
    (
        AuditAction.WORKER_SESSION_ENDED_STALE,
        AuditOutcome.ACCEPTED,
        AuditActorKind.SYSTEM,
        "worker_session",
        {"last_sequence": 0},
        None,
    ),
    (
        AuditAction.WORKFLOW_RUN_CREATE,
        AuditOutcome.REJECTED,
        AuditActorKind.API_PRINCIPAL,
        "workflow",
        {},
        "idempotency_conflict",
    ),
    (
        AuditAction.WORKFLOW_RUN_CANCEL,
        AuditOutcome.REJECTED,
        AuditActorKind.API_PRINCIPAL,
        "workflow_run",
        {},
        "invalid_reason",
    ),
    (
        AuditAction.WORKFLOW_RUN_REPLAY,
        AuditOutcome.REJECTED,
        AuditActorKind.API_PRINCIPAL,
        "workflow_run",
        {"failed_steps": _SUMMARY},
        "failed_subgraph_invalid",
    ),
    (
        AuditAction.TASK_CLAIM_ACQUIRE,
        AuditOutcome.REJECTED,
        AuditActorKind.WORKER,
        "task_attempt",
        {"attempt_number": 1},
        "obsolete_task",
    ),
    (
        AuditAction.TASK_CLAIM_RENEW,
        AuditOutcome.REJECTED,
        AuditActorKind.WORKER,
        "task_attempt",
        {"claim_generation": 1},
        "stale_claim",
    ),
    (
        AuditAction.TASK_ATTEMPT_START,
        AuditOutcome.REJECTED,
        AuditActorKind.WORKER,
        "task_attempt",
        {"claim_generation": 1},
        "stale_claim",
    ),
    (
        AuditAction.TASK_RESULT_SUBMIT,
        AuditOutcome.REJECTED,
        AuditActorKind.WORKER,
        "task_attempt",
        {"claim_generation": 1},
        "invalid_task_state",
    ),
    (
        AuditAction.DEAD_LETTER_ACKNOWLEDGE,
        AuditOutcome.REJECTED,
        AuditActorKind.API_PRINCIPAL,
        "dead_letter",
        {},
        "transition_conflict",
    ),
    (
        AuditAction.DEAD_LETTER_RESOLVE,
        AuditOutcome.REJECTED,
        AuditActorKind.API_PRINCIPAL,
        "dead_letter",
        {},
        "dead_letter_not_visible",
    ),
    (
        AuditAction.DEAD_LETTER_REDRIVE,
        AuditOutcome.REJECTED,
        AuditActorKind.API_PRINCIPAL,
        "dead_letter",
        {},
        "redrive_not_eligible",
    ),
    (
        AuditAction.IDENTITY_API_PRINCIPAL_CREATED,
        AuditOutcome.ACCEPTED,
        AuditActorKind.SYSTEM,
        "api_principal",
        {},
        None,
    ),
    (
        AuditAction.IDENTITY_API_ROLES_ASSIGNED,
        AuditOutcome.ACCEPTED,
        AuditActorKind.SYSTEM,
        "api_principal",
        {"roles": _SUMMARY},
        None,
    ),
    (
        AuditAction.IDENTITY_WORKER_CREATED,
        AuditOutcome.ACCEPTED,
        AuditActorKind.SYSTEM,
        "worker_identity",
        {},
        None,
    ),
    (
        AuditAction.IDENTITY_API_CREDENTIAL_ADDED,
        AuditOutcome.ACCEPTED,
        AuditActorKind.SYSTEM,
        "api_credential",
        {},
        None,
    ),
    (
        AuditAction.IDENTITY_WORKER_CREDENTIAL_ADDED,
        AuditOutcome.ACCEPTED,
        AuditActorKind.SYSTEM,
        "worker_credential",
        {},
        None,
    ),
    (
        AuditAction.IDENTITY_API_CREDENTIAL_REVOKED,
        AuditOutcome.ACCEPTED,
        AuditActorKind.SYSTEM,
        "api_credential",
        {},
        None,
    ),
    (
        AuditAction.IDENTITY_WORKER_CREDENTIAL_REVOKED,
        AuditOutcome.ACCEPTED,
        AuditActorKind.SYSTEM,
        "worker_credential",
        {},
        None,
    ),
)


@pytest.mark.parametrize(
    ("action", "outcome", "actor_kind", "resource_type", "provenance", "reason"),
    _VALID_CONTRACT_CASES,
)
def test_every_supported_audit_action_outcome_contract(
    action: AuditAction,
    outcome: AuditOutcome,
    actor_kind: AuditActorKind,
    resource_type: str,
    provenance: dict[str, object],
    reason: str | None,
) -> None:
    target = (
        None
        if action is AuditAction.WORKER_SESSION_REGISTER
        and outcome is AuditOutcome.REJECTED
        else uuid4()
    )
    record = AuditRecord(
        uuid4(),
        _actor(actor_kind),
        action.value,
        outcome,
        resource_type,
        target,
        "opaque correlation",
        provenance,
        reason,
    )
    assert record.action == action.value


def test_contract_covers_every_canonical_action_and_legacy_alias() -> None:
    assert {case[0] for case in _VALID_CONTRACT_CASES} == set(AuditAction)
    assert {
        alias: canonical_audit_action(alias) for alias in AUDIT_ACTION_ALIASES
    } == dict(AUDIT_ACTION_ALIASES)


def test_semantically_impossible_audit_shapes_are_rejected() -> None:
    principal = _actor(AuditActorKind.API_PRINCIPAL)
    base = dict(
        id=uuid4(),
        actor=principal,
        action=AuditAction.WORKFLOW_PUBLISH.value,
        outcome=AuditOutcome.REJECTED,
        resource_type="workflow",
        resource_id=uuid4(),
        correlation_id=None,
        provenance={},
    )
    with pytest.raises(ValueError, match="reason is invalid"):
        AuditRecord(**base, reason_code="stale_claim")
    with pytest.raises(ValueError, match="actor is invalid"):
        AuditRecord(
            **{**base, "actor": _actor(AuditActorKind.WORKER)},
            reason_code="workflow_not_visible",
        )
    with pytest.raises(ValueError, match="resource type"):
        AuditRecord(
            **{**base, "resource_type": "task_attempt"},
            reason_code="workflow_not_visible",
        )
    with pytest.raises(ValueError, match="provenance key"):
        AuditRecord(
            **{**base, "provenance": {"raw_reason": "secret"}},
            reason_code="workflow_not_visible",
        )


def test_outcome_and_reason_control_worker_session_target_requirement() -> None:
    actor = _actor(AuditActorKind.WORKER)
    summary = {"capabilities": _SUMMARY}
    AuditRecord(
        uuid4(),
        actor,
        "worker_session.register",
        AuditOutcome.REJECTED,
        "worker_session",
        None,
        None,
        summary,
        "registration_conflict",
    )
    with pytest.raises(ValueError, match="target is required"):
        AuditRecord(
            uuid4(),
            actor,
            "worker_session.register",
            AuditOutcome.ACCEPTED,
            "worker_session",
            None,
            None,
            summary,
        )
    AuditRecord(
        uuid4(),
        actor,
        "worker_session.heartbeat",
        AuditOutcome.REJECTED,
        "worker_session",
        None,
        None,
        {"sequence": 1},
        "worker_session_unavailable",
    )
    with pytest.raises(ValueError, match="target is required"):
        AuditRecord(
            uuid4(),
            actor,
            "worker_session.heartbeat",
            AuditOutcome.REJECTED,
            "worker_session",
            None,
            None,
            {"sequence": 1},
            "stale_heartbeat",
        )
