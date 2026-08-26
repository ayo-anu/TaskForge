"""Task-5 retention classifications authorize no deletion."""

from taskforge.history.retention import (
    RETENTION_CLASSIFICATIONS,
    TASK5_DELETE_ELIGIBLE_TABLES,
    RetentionClass,
)
from taskforge.persistence.metadata import metadata


def test_every_taskforge_table_has_a_retention_classification() -> None:
    assert set(RETENTION_CLASSIFICATIONS) == set(metadata.tables)


def test_task5_authorizes_no_table_deletion() -> None:
    assert TASK5_DELETE_ELIGIBLE_TABLES == frozenset()
    assert (
        RETENTION_CLASSIFICATIONS["audit_records"] is RetentionClass.PERMANENT_LINEAGE
    )
    assert (
        RETENTION_CLASSIFICATIONS["worker_heartbeats"]
        is RetentionClass.ARCHIVABLE_VERIFIED_COPY_REQUIRED
    )
    assert (
        RETENTION_CLASSIFICATIONS["workflow_run_idempotency"]
        is RetentionClass.FUTURE_BOUNDED_POLICY
    )
