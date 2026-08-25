"""Exact structural guarantees for task-attempt claim lifecycles."""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    DefaultClause,
    ForeignKeyConstraint,
)
from sqlalchemy.dialects.postgresql import UUID

from taskforge.persistence.schema import metadata
from taskforge.runs.schema import task_attempt_claims, task_claim_events


def test_shared_metadata_registers_only_the_approved_claim_table_shape() -> None:
    assert metadata.tables["task_attempt_claims"] is task_attempt_claims
    assert tuple(task_attempt_claims.c.keys()) == (
        "task_attempt_id",
        "generation",
        "worker_session_id",
        "acquired_at",
        "lease_expires_at",
        "terminated_at",
    )
    assert not {
        "id",
        "claim_id",
        "token",
        "token_digest",
        "created_at",
        "updated_at",
        "renewed_at",
    } & set(task_attempt_claims.c.keys())


def test_claim_columns_types_nullability_and_defaults_match_the_migration() -> None:
    assert isinstance(task_attempt_claims.c.task_attempt_id.type, UUID)
    assert isinstance(task_attempt_claims.c.generation.type, BigInteger)
    assert isinstance(task_attempt_claims.c.worker_session_id.type, UUID)
    for name in ("acquired_at", "lease_expires_at", "terminated_at"):
        column_type = task_attempt_claims.c[name].type
        assert isinstance(column_type, DateTime)
        assert column_type.timezone is True

    assert task_attempt_claims.c.task_attempt_id.nullable is False
    assert task_attempt_claims.c.generation.nullable is False
    assert task_attempt_claims.c.worker_session_id.nullable is False
    assert task_attempt_claims.c.acquired_at.nullable is False
    assert task_attempt_claims.c.lease_expires_at.nullable is False
    assert task_attempt_claims.c.terminated_at.nullable is True
    acquired_default = task_attempt_claims.c.acquired_at.server_default
    assert isinstance(acquired_default, DefaultClause)
    assert str(acquired_default.arg) == "statement_timestamp()"
    for name in (
        "task_attempt_id",
        "generation",
        "worker_session_id",
        "lease_expires_at",
        "terminated_at",
    ):
        assert task_attempt_claims.c[name].server_default is None
        assert task_attempt_claims.c[name].default is None
    assert task_attempt_claims.c.generation.identity is None


def test_claim_primary_key_foreign_keys_and_checks_match_the_migration() -> None:
    assert task_attempt_claims.primary_key.name == "pk_task_attempt_claims"
    assert tuple(task_attempt_claims.primary_key.columns.keys()) == (
        "task_attempt_id",
        "generation",
    )
    assert _foreign_key_shapes() == {
        (
            "fk_task_attempt_claims_task_attempt_id_task_attempts",
            ("task_attempt_id",),
            ("task_attempts.id",),
            "RESTRICT",
            "RESTRICT",
        ),
        (
            "fk_task_attempt_claims_worker_session_id_worker_sessions",
            ("worker_session_id",),
            ("worker_sessions.id",),
            "RESTRICT",
            "RESTRICT",
        ),
    }
    assert _check_shapes() == {
        ("ck_task_attempt_claims_generation_positive", "generation > 0"),
        (
            "ck_task_attempt_claims_lease_expires_after_acquisition",
            "lease_expires_at > acquired_at",
        ),
        (
            "ck_task_attempt_claims_terminated_not_before_acquisition",
            "terminated_at IS NULL OR terminated_at >= acquired_at",
        ),
    }


def test_claim_partial_indexes_match_the_migration_exactly() -> None:
    assert {
        (
            index.name,
            index.unique,
            tuple(column.name for column in index.columns),
            str(index.dialect_options["postgresql"]["where"]),
        )
        for index in task_attempt_claims.indexes
    } == {
        (
            "uq_task_attempt_claims_current_task_attempt_id",
            True,
            ("task_attempt_id",),
            "task_attempt_claims.terminated_at IS NULL",
        ),
        (
            "ix_task_attempt_claims_current_lease_expires_at",
            False,
            ("lease_expires_at",),
            "task_attempt_claims.terminated_at IS NULL",
        ),
        (
            "ix_task_attempt_claims_current_worker_session_id",
            False,
            ("worker_session_id",),
            "task_attempt_claims.terminated_at IS NULL",
        ),
    }


def _foreign_key_shapes() -> set[
    tuple[str | None, tuple[str, ...], tuple[str, ...], str | None, str | None]
]:
    return {
        (
            str(constraint.name),
            tuple(constraint.columns.keys()),
            tuple(element.target_fullname for element in constraint.elements),
            constraint.onupdate,
            constraint.ondelete,
        )
        for constraint in task_attempt_claims.foreign_key_constraints
        if isinstance(constraint, ForeignKeyConstraint)
    }


def _check_shapes() -> set[tuple[str | None, str]]:
    return {
        (str(constraint.name), str(constraint.sqltext))
        for constraint in task_attempt_claims.constraints
        if isinstance(constraint, CheckConstraint)
    }


def test_claim_event_metadata_matches_the_immutable_event_migration() -> None:
    assert metadata.tables["task_claim_events"] is task_claim_events
    assert tuple(task_claim_events.c.keys()) == (
        "id",
        "task_attempt_id",
        "generation",
        "worker_identity_id",
        "worker_session_id",
        "correlation_id",
        "event_type",
        "occurred_at",
        "previous_lease_expires_at",
        "lease_expires_at",
    )
    assert task_claim_events.primary_key.name == "pk_task_claim_events"
    assert tuple(task_claim_events.primary_key.columns.keys()) == ("id",)
    assert task_claim_events.c.occurred_at.server_default is None
    assert task_claim_events.c.occurred_at.default is None
    assert _event_foreign_key_shapes() == {
        (
            "fk_task_claim_events_claim_generation",
            ("task_attempt_id", "generation"),
            (
                "task_attempt_claims.task_attempt_id",
                "task_attempt_claims.generation",
            ),
            "RESTRICT",
            "RESTRICT",
        ),
        (
            "fk_task_claim_events_worker_session_identity",
            ("worker_session_id", "worker_identity_id"),
            ("worker_sessions.id", "worker_sessions.worker_identity_id"),
            "RESTRICT",
            "RESTRICT",
        ),
    }
    assert _event_check_shapes() == {
        (
            "ck_task_claim_events_actor_attribution",
            "worker_identity_id IS NOT NULL AND worker_session_id IS NOT NULL",
        ),
        (
            "ck_task_claim_events_event_type_valid",
            "event_type IN ('claim_acquired', 'lease_renewed')",
        ),
        (
            "ck_task_claim_events_event_shape_valid",
            "(event_type = 'claim_acquired' AND previous_lease_expires_at IS NULL "
            "AND lease_expires_at > occurred_at) OR (event_type = 'lease_renewed' "
            "AND previous_lease_expires_at IS NOT NULL AND lease_expires_at > "
            "previous_lease_expires_at)",
        ),
    }
    assert {
        (
            index.name,
            index.unique,
            tuple(column.name for column in index.columns),
            str(index.dialect_options["postgresql"]["where"]),
        )
        for index in task_claim_events.indexes
    } == {
        (
            "uq_task_claim_events_acquired_generation",
            True,
            ("task_attempt_id", "generation"),
            "task_claim_events.event_type = :event_type_1",
        ),
        (
            "uq_task_claim_events_renewal_transition",
            True,
            (
                "task_attempt_id",
                "generation",
                "previous_lease_expires_at",
                "lease_expires_at",
            ),
            "task_claim_events.event_type = :event_type_1",
        ),
    }
    assert not {"metadata", "payload", "result_authority", "credential"} & set(
        task_claim_events.c.keys()
    )


def _event_foreign_key_shapes() -> set[
    tuple[str | None, tuple[str, ...], tuple[str, ...], str | None, str | None]
]:
    return {
        (
            str(constraint.name),
            tuple(constraint.columns.keys()),
            tuple(element.target_fullname for element in constraint.elements),
            constraint.onupdate,
            constraint.ondelete,
        )
        for constraint in task_claim_events.foreign_key_constraints
        if isinstance(constraint, ForeignKeyConstraint)
    }


def _event_check_shapes() -> set[tuple[str | None, str]]:
    return {
        (str(constraint.name), str(constraint.sqltext))
        for constraint in task_claim_events.constraints
        if isinstance(constraint, CheckConstraint)
    }
