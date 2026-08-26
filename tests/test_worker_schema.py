"""Structural guarantees for worker sessions, capabilities, and liveness."""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DefaultClause,
    ForeignKeyConstraint,
    Index,
    String,
    Table,
)
from sqlalchemy.dialects.postgresql import UUID

from taskforge.persistence.schema import metadata
from taskforge.worker.schema import (
    worker_heartbeats,
    worker_session_capabilities,
    worker_session_health,
    worker_sessions,
)


def test_shared_metadata_registers_worker_liveness_tables() -> None:
    assert {
        "worker_sessions",
        "worker_session_capabilities",
        "worker_session_health",
        "worker_heartbeats",
    } <= set(metadata.tables)


def test_worker_sessions_are_distinct_process_incarnations() -> None:
    assert tuple(worker_sessions.primary_key.columns.keys()) == ("id",)
    assert isinstance(worker_sessions.c.id.type, UUID)
    assert worker_sessions.c.worker_identity_id.nullable is False
    assert worker_sessions.c.registered_at.nullable is False
    assert _server_default(worker_sessions, "registered_at") == "statement_timestamp()"
    assert worker_sessions.c.ended_at.nullable is True
    assert _foreign_key_shapes(worker_sessions) == {
        (("worker_identity_id",), ("worker_identities.id",), "RESTRICT", "RESTRICT")
    }
    assert _check_texts(worker_sessions) == {
        "ended_at IS NULL OR ended_at >= registered_at"
    }
    assert any(
        constraint.__class__.__name__ == "UniqueConstraint"
        for constraint in worker_sessions.constraints
    )


def test_capabilities_are_session_owned_and_contract_validated() -> None:
    assert tuple(worker_session_capabilities.primary_key.columns.keys()) == (
        "worker_session_id",
        "capability",
    )
    assert isinstance(worker_session_capabilities.c.capability.type, String)
    assert worker_session_capabilities.c.capability.type.length == 128
    assert (
        _server_default(worker_session_capabilities, "advertised_at")
        == "statement_timestamp()"
    )
    assert _foreign_key_shapes(worker_session_capabilities) == {
        (("worker_session_id",), ("worker_sessions.id",), "RESTRICT", "RESTRICT")
    }
    assert _check_texts(worker_session_capabilities) == {
        "capability ~ '^[a-z][a-z0-9_.-]{0,127}$'"
    }


def test_health_is_a_fact_projection_without_a_health_enum() -> None:
    assert tuple(worker_session_health.primary_key.columns.keys()) == (
        "worker_session_id",
    )
    assert isinstance(worker_session_health.c.last_sequence.type, BigInteger)
    assert _server_default(worker_session_health, "last_sequence") == "0"
    assert isinstance(worker_session_health.c.accepting_work.type, Boolean)
    assert set(worker_session_health.c.keys()) == {
        "worker_session_id",
        "last_sequence",
        "last_seen_at",
        "accepting_work",
        "availability_changed_at",
    }
    assert _check_texts(worker_session_health) == {
        "last_sequence >= 0",
        "availability_changed_at <= last_seen_at",
    }
    assert _foreign_key_shapes(worker_session_health) == {
        (("worker_session_id",), ("worker_sessions.id",), "RESTRICT", "RESTRICT")
    }


def test_heartbeat_history_is_compact_and_session_scoped() -> None:
    assert tuple(worker_heartbeats.primary_key.columns.keys()) == (
        "worker_session_id",
        "sequence",
    )
    assert isinstance(worker_heartbeats.c.sequence.type, BigInteger)
    assert _server_default(worker_heartbeats, "received_at") == "statement_timestamp()"
    assert set(worker_heartbeats.c.keys()) == {
        "worker_session_id",
        "sequence",
        "received_at",
        "accepting_work",
        "worker_identity_id",
        "correlation_id",
    }
    assert _check_texts(worker_heartbeats) == {
        "correlation_id IS NULL OR (length(correlation_id) BETWEEN 1 AND 128 "
        "AND correlation_id !~ '[^ -~]')",
        "sequence > 0",
        "worker_identity_id IS NOT NULL",
    }
    assert _foreign_key_shapes(worker_heartbeats) == {
        (("worker_session_id",), ("worker_sessions.id",), "RESTRICT", "RESTRICT"),
        (
            ("worker_session_id", "worker_identity_id"),
            ("worker_sessions.id", "worker_sessions.worker_identity_id"),
            "RESTRICT",
            "RESTRICT",
        ),
    }
    assert worker_heartbeats.indexes == set()


def test_worker_indexes_match_known_lookup_and_scan_patterns() -> None:
    indexes = {
        str(index.name): index
        for table in (
            worker_sessions,
            worker_session_capabilities,
            worker_session_health,
        )
        for index in table.indexes
    }
    assert set(indexes) == {
        "ix_worker_sessions_worker_identity_id_registered_at_id",
        "ix_worker_sessions_open_registered_at_id",
        "ix_worker_session_capabilities_capability_worker_session_id",
        "ix_worker_session_health_last_seen_at_worker_session_id",
    }
    assert all(isinstance(index, Index) for index in indexes.values())
    open_predicate = indexes[
        "ix_worker_sessions_open_registered_at_id"
    ].dialect_options["postgresql"]["where"]
    assert str(open_predicate) == "worker_sessions.ended_at IS NULL"


def test_every_worker_constraint_and_index_is_named() -> None:
    for table in (
        worker_sessions,
        worker_session_capabilities,
        worker_session_health,
        worker_heartbeats,
    ):
        assert all(constraint.name for constraint in table.constraints)
        assert all(index.name for index in table.indexes)


def _foreign_key_shapes(
    table: Table,
) -> set[tuple[tuple[str, ...], tuple[str, ...], str | None, str | None]]:
    return {
        (
            tuple(constraint.columns.keys()),
            tuple(element.target_fullname for element in constraint.elements),
            constraint.onupdate,
            constraint.ondelete,
        )
        for constraint in table.foreign_key_constraints
        if isinstance(constraint, ForeignKeyConstraint)
    }


def _check_texts(table: Table) -> set[str]:
    return {
        str(constraint.sqltext)
        for constraint in table.constraints
        if isinstance(constraint, CheckConstraint)
    }


def _server_default(table: Table, column_name: str) -> str:
    default = table.c[column_name].server_default
    assert isinstance(default, DefaultClause)
    return str(default.arg)
