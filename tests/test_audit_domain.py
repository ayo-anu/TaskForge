"""Audit envelope actor and bounded-provenance contracts."""

import json
from uuid import uuid4

import pytest

from taskforge.audit.domain import (
    MAX_AUDIT_PROVENANCE_BYTES,
    AuditActor,
    AuditActorKind,
    AuditOutcome,
    AuditRecord,
    bounded_string_set,
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
            "workflow.created",
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
            "workflow.created",
            AuditOutcome.REJECTED,
            "workflow",
            uuid4(),
            None,
            {},
        )
