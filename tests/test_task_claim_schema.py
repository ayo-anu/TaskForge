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
from taskforge.runs.schema import task_attempt_claims


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
