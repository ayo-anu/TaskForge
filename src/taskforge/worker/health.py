"""Canonical PostgreSQL-time worker health predicates."""

from sqlalchemy import ColumnElement, func


def worker_session_is_healthy(
    last_seen_at: ColumnElement[object],
    *,
    stale_after_seconds: int,
    reference_time: ColumnElement[object] | None = None,
) -> ColumnElement[bool]:
    """Return the Milestone 9 strict healthy-boundary expression."""
    reference = func.statement_timestamp() if reference_time is None else reference_time
    return last_seen_at > reference - func.make_interval(
        0, 0, 0, 0, 0, 0, stale_after_seconds
    )
