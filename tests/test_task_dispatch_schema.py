"""Structural guarantees for task attempts and durable dispatch intent."""

from __future__ import annotations

from sqlalchemy import (
    CheckConstraint,
    ForeignKeyConstraint,
    Index,
    Integer,
    Table,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

from taskforge.persistence.schema import metadata
from taskforge.runs.schema import task_attempts, task_dispatch_outbox


def test_shared_metadata_registers_attempt_and_dispatch_tables() -> None:
    assert {"task_attempts", "task_dispatch_outbox"} <= set(metadata.tables)


def test_task_attempt_identity_numbering_and_ownership_are_constrained() -> None:
    assert tuple(task_attempts.primary_key.columns.keys()) == ("id",)
    assert isinstance(task_attempts.c.id.type, UUID)
    assert isinstance(task_attempts.c.attempt_number.type, Integer)
    assert task_attempts.c.next_eligible_at.nullable is True
    assert task_attempts.c.next_eligible_at.server_default is None
    assert task_attempts.c.task_run_id.nullable is False
    assert task_attempts.c.attempt_number.nullable is False
    assert task_attempts.c.created_at.nullable is False
    assert task_attempts.c.created_at.server_default is not None
    assert _unique_column_sets(task_attempts) == {("task_run_id", "attempt_number")}
    assert _foreign_key_shapes(task_attempts) == {
        (("task_run_id",), ("task_runs.id",), "RESTRICT", "RESTRICT")
    }
    assert _check_texts(task_attempts) == {"attempt_number > 0"}


def test_attempt_eligibility_has_only_the_due_scan_index() -> None:
    assert len(task_attempts.indexes) == 1
    index = next(iter(task_attempts.indexes))
    assert isinstance(index, Index)
    assert index.name == "ix_task_attempts_scheduled_next_eligible_at_id"
    assert tuple(column.name for column in index.columns) == (
        "next_eligible_at",
        "id",
    )
    predicate = index.dialect_options["postgresql"]["where"]
    assert str(predicate) == "task_attempts.next_eligible_at IS NOT NULL"


def test_dispatch_identity_payload_and_attempt_ownership_are_constrained() -> None:
    assert tuple(task_dispatch_outbox.primary_key.columns.keys()) == ("id",)
    assert isinstance(task_dispatch_outbox.c.id.type, UUID)
    assert task_dispatch_outbox.c.task_attempt_id.nullable is False
    assert task_dispatch_outbox.c.route.nullable is False
    assert isinstance(task_dispatch_outbox.c.payload.type, JSONB)
    assert task_dispatch_outbox.c.payload.nullable is False
    assert task_dispatch_outbox.c.payload.server_default is None
    assert task_dispatch_outbox.c.created_at.nullable is False
    assert task_dispatch_outbox.c.created_at.server_default is not None
    assert task_dispatch_outbox.c.published_at.nullable is True
    assert task_dispatch_outbox.c.published_at.server_default is None
    assert _unique_column_sets(task_dispatch_outbox) == {("task_attempt_id",)}
    assert _foreign_key_shapes(task_dispatch_outbox) == {
        (("task_attempt_id",), ("task_attempts.id",), "RESTRICT", "RESTRICT")
    }
    assert _check_texts(task_dispatch_outbox) == {
        "length(btrim(route)) > 0",
        "jsonb_typeof(payload) = 'object'",
    }


def test_dispatch_outbox_has_only_the_unpublished_scan_index() -> None:
    assert len(task_dispatch_outbox.indexes) == 1
    index = next(iter(task_dispatch_outbox.indexes))
    assert isinstance(index, Index)
    assert index.name == "ix_task_dispatch_outbox_unpublished_created_at_id"
    assert tuple(column.name for column in index.columns) == ("created_at", "id")
    predicate = index.dialect_options["postgresql"]["where"]
    assert str(predicate) == "task_dispatch_outbox.published_at IS NULL"


def test_every_attempt_and_dispatch_constraint_and_index_is_named() -> None:
    for table in (task_attempts, task_dispatch_outbox):
        assert all(constraint.name for constraint in table.constraints)
        assert all(index.name for index in table.indexes)


def _unique_column_sets(table: Table) -> set[tuple[str, ...]]:
    return {
        tuple(constraint.columns.keys())
        for constraint in table.constraints
        if isinstance(constraint, UniqueConstraint)
    }


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
