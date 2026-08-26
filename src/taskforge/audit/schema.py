"""Relational schema for generic append-only audit gaps."""

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    String,
    Table,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

from taskforge.persistence.metadata import metadata

audit_records = Table(
    "audit_records",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("actor_kind", String(24), nullable=False),
    Column(
        "api_principal_id",
        UUID(as_uuid=True),
        ForeignKey("api_principals.id", onupdate="RESTRICT", ondelete="RESTRICT"),
    ),
    Column(
        "worker_identity_id",
        UUID(as_uuid=True),
        ForeignKey("worker_identities.id", onupdate="RESTRICT", ondelete="RESTRICT"),
    ),
    Column("worker_session_id", UUID(as_uuid=True)),
    Column("system_component", String(32)),
    Column("action", String(128), nullable=False),
    Column("outcome", String(16), nullable=False),
    Column("reason_code", String(128)),
    Column("resource_type", String(64), nullable=False),
    Column("resource_id", UUID(as_uuid=True)),
    Column("correlation_id", String(128)),
    Column(
        "diagnostic_provenance",
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
    ),
    Column(
        "occurred_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("statement_timestamp()"),
    ),
    CheckConstraint(
        "actor_kind IN ('api_principal','worker','system')", name="actor_kind_valid"
    ),
    CheckConstraint(
        "action ~ '^[a-z][a-z0-9_-]*(\\.[a-z][a-z0-9_-]*)+$'",
        name="action_namespaced",
    ),
    CheckConstraint("outcome IN ('accepted','rejected')", name="outcome_valid"),
    CheckConstraint(
        "action ~ '^[a-z][a-z0-9_.-]{0,127}$' AND "
        "resource_type ~ '^[a-z][a-z0-9_.-]{0,63}$' AND "
        "(reason_code IS NULL OR reason_code ~ '^[a-z][a-z0-9_.-]{0,127}$') AND "
        "(system_component IS NULL OR system_component ~ '^[a-z][a-z0-9_.-]{0,31}$')",
        name="names_valid",
    ),
    CheckConstraint(
        "(outcome='accepted' AND reason_code IS NULL) OR (outcome='rejected' AND reason_code IS NOT NULL)",
        name="outcome_reason_valid",
    ),
    CheckConstraint(
        "jsonb_typeof(diagnostic_provenance)='object' AND octet_length(convert_to(diagnostic_provenance::text, 'UTF8')) <= 2048",
        name="provenance_valid",
    ),
    CheckConstraint(
        "correlation_id IS NULL OR (length(correlation_id) BETWEEN 1 AND 128 AND correlation_id !~ '[^ -~]')",
        name="correlation_valid",
    ),
    CheckConstraint(
        "(actor_kind='api_principal' AND api_principal_id IS NOT NULL AND worker_identity_id IS NULL AND worker_session_id IS NULL AND system_component IS NULL) OR (actor_kind='worker' AND api_principal_id IS NULL AND worker_identity_id IS NOT NULL AND system_component IS NULL) OR (actor_kind='system' AND api_principal_id IS NULL AND worker_identity_id IS NULL AND worker_session_id IS NULL AND system_component IS NOT NULL)",
        name="actor_shape_valid",
    ),
    ForeignKeyConstraint(
        ("worker_session_id", "worker_identity_id"),
        ("worker_sessions.id", "worker_sessions.worker_identity_id"),
        onupdate="RESTRICT",
        ondelete="RESTRICT",
    ),
)
