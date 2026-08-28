"""Shared rate-limit counter schema."""

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    Index,
    Integer,
    LargeBinary,
    String,
    Table,
)

from taskforge.persistence.metadata import metadata

rate_limit_counters = Table(
    "rate_limit_counters",
    metadata,
    Column("policy", String(64), primary_key=True),
    Column("key_digest", LargeBinary(32), primary_key=True),
    Column("window_started_at", DateTime(timezone=True), nullable=False),
    Column("count", Integer, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    CheckConstraint("count > 0", name="count_positive"),
)

Index("ix_rate_limit_counters_updated_at", rate_limit_counters.c.updated_at)
