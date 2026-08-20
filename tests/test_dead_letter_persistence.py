"""Focused SQL evidence for dead-letter redrive serialization."""

from __future__ import annotations

from uuid import uuid4

from sqlalchemy.dialects import postgresql

from taskforge.identity.authorization import OwnerFilter
from taskforge.persistence.dead_letter_operations import _redrive_source_lock


def test_redrive_locks_one_owner_scoped_status_row_before_idempotency_lookup() -> None:
    principal_id = uuid4()
    statement = _redrive_source_lock(uuid4(), OwnerFilter.only(principal_id))
    sql = " ".join(
        str(
            statement.compile(
                dialect=postgresql.dialect()  # type: ignore[no-untyped-call]
            )
        ).split()
    )

    assert "dead_letter_items.id =" in sql
    assert "workflow_definitions.owner_principal_id =" in sql
    assert "FOR UPDATE OF dead_letter_status" in sql


def test_admin_redrive_uses_the_same_per_item_status_row_lock() -> None:
    statement = _redrive_source_lock(uuid4(), OwnerFilter.all_owners())
    sql = " ".join(
        str(
            statement.compile(
                dialect=postgresql.dialect()  # type: ignore[no-untyped-call]
            )
        ).split()
    )

    assert "dead_letter_items.id =" in sql
    assert "FOR UPDATE OF dead_letter_status" in sql
    assert "workflow_definitions.owner_principal_id =" not in sql
