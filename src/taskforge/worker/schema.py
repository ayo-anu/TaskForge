"""Relational worker-session facts without registration or heartbeat behavior."""

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    String,
    Table,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import UUID

from taskforge.persistence.metadata import metadata

worker_sessions = Table(
    "worker_sessions",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column(
        "worker_identity_id",
        UUID(as_uuid=True),
        ForeignKey("worker_identities.id", onupdate="RESTRICT", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column(
        "registered_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("statement_timestamp()"),
    ),
    Column("ended_at", DateTime(timezone=True)),
    CheckConstraint(
        "ended_at IS NULL OR ended_at >= registered_at",
        name="ended_not_before_registration",
    ),
    UniqueConstraint(
        "id", "worker_identity_id", name="uq_worker_sessions_id_worker_identity_id"
    ),
)
Index(
    "ix_worker_sessions_worker_identity_id_registered_at_id",
    worker_sessions.c.worker_identity_id,
    worker_sessions.c.registered_at.desc(),
    worker_sessions.c.id.desc(),
)
Index(
    "ix_worker_sessions_open_registered_at_id",
    worker_sessions.c.registered_at,
    worker_sessions.c.id,
    postgresql_where=worker_sessions.c.ended_at.is_(None),
)

worker_session_capabilities = Table(
    "worker_session_capabilities",
    metadata,
    Column(
        "worker_session_id",
        UUID(as_uuid=True),
        ForeignKey(
            "worker_sessions.id",
            name="fk_worker_session_capabilities_worker_session",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        primary_key=True,
    ),
    Column("capability", String(128), primary_key=True),
    Column(
        "advertised_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("statement_timestamp()"),
    ),
    CheckConstraint(
        "capability ~ '^[a-z][a-z0-9_.-]{0,127}$'",
        name="capability_valid",
    ),
)
Index(
    "ix_worker_session_capabilities_capability_worker_session_id",
    worker_session_capabilities.c.capability,
    worker_session_capabilities.c.worker_session_id,
)

worker_session_health = Table(
    "worker_session_health",
    metadata,
    Column(
        "worker_session_id",
        UUID(as_uuid=True),
        ForeignKey("worker_sessions.id", onupdate="RESTRICT", ondelete="RESTRICT"),
        primary_key=True,
    ),
    Column("last_sequence", BigInteger, nullable=False, server_default=text("0")),
    Column("last_seen_at", DateTime(timezone=True), nullable=False),
    Column("accepting_work", Boolean, nullable=False),
    Column("availability_changed_at", DateTime(timezone=True), nullable=False),
    CheckConstraint("last_sequence >= 0", name="last_sequence_nonnegative"),
    CheckConstraint(
        "availability_changed_at <= last_seen_at",
        name="availability_not_after_last_seen",
    ),
)
Index(
    "ix_worker_session_health_last_seen_at_worker_session_id",
    worker_session_health.c.last_seen_at,
    worker_session_health.c.worker_session_id,
)

worker_heartbeats = Table(
    "worker_heartbeats",
    metadata,
    Column(
        "worker_session_id",
        UUID(as_uuid=True),
        ForeignKey("worker_sessions.id", onupdate="RESTRICT", ondelete="RESTRICT"),
        primary_key=True,
    ),
    Column("sequence", BigInteger, primary_key=True),
    Column(
        "received_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("statement_timestamp()"),
    ),
    Column("accepting_work", Boolean, nullable=False),
    Column("worker_identity_id", UUID(as_uuid=True), nullable=True),
    Column("correlation_id", String(128), nullable=True),
    ForeignKeyConstraint(
        ("worker_session_id", "worker_identity_id"),
        ("worker_sessions.id", "worker_sessions.worker_identity_id"),
        name="fk_worker_heartbeats_worker_session_identity",
        onupdate="RESTRICT",
        ondelete="RESTRICT",
    ),
    CheckConstraint("sequence > 0", name="sequence_positive"),
    CheckConstraint("worker_identity_id IS NOT NULL", name="actor_attribution"),
)
