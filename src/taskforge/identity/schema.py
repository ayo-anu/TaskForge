"""Relational identity schema without authentication policy or behavior."""

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    String,
    Table,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import UUID

from taskforge.persistence.metadata import metadata

API_ROLES = ("viewer", "workflow_operator", "administrator")

api_principals = Table(
    "api_principals",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("name", String(128), nullable=False),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.current_timestamp(),
    ),
    Column("disabled_at", DateTime(timezone=True)),
    UniqueConstraint("name"),
    CheckConstraint("length(name) > 0", name="name_not_empty"),
    CheckConstraint(
        "disabled_at IS NULL OR disabled_at >= created_at",
        name="disabled_not_before_creation",
    ),
)

api_principal_roles = Table(
    "api_principal_roles",
    metadata,
    Column(
        "principal_id",
        UUID(as_uuid=True),
        ForeignKey("api_principals.id"),
        primary_key=True,
    ),
    Column("role", String(32), primary_key=True),
    Column(
        "assigned_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.current_timestamp(),
    ),
    CheckConstraint(
        "role IN ('viewer', 'workflow_operator', 'administrator')",
        name="role_allowed",
    ),
)

api_credentials = Table(
    "api_credentials",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column(
        "principal_id",
        UUID(as_uuid=True),
        ForeignKey("api_principals.id"),
        nullable=False,
        index=True,
    ),
    Column("credential_verifier", Text, nullable=False),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.current_timestamp(),
    ),
    Column("expires_at", DateTime(timezone=True)),
    Column("revoked_at", DateTime(timezone=True)),
    CheckConstraint(
        "expires_at IS NULL OR expires_at > created_at",
        name="expiry_after_creation",
    ),
    CheckConstraint(
        "revoked_at IS NULL OR revoked_at >= created_at",
        name="revocation_not_before_creation",
    ),
)

worker_identities = Table(
    "worker_identities",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("name", String(128), nullable=False),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.current_timestamp(),
    ),
    Column("disabled_at", DateTime(timezone=True)),
    UniqueConstraint("name"),
    CheckConstraint("length(name) > 0", name="name_not_empty"),
    CheckConstraint(
        "disabled_at IS NULL OR disabled_at >= created_at",
        name="disabled_not_before_creation",
    ),
)

worker_credentials = Table(
    "worker_credentials",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column(
        "worker_identity_id",
        UUID(as_uuid=True),
        ForeignKey("worker_identities.id"),
        nullable=False,
        index=True,
    ),
    Column("credential_verifier", Text, nullable=False),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.current_timestamp(),
    ),
    Column("expires_at", DateTime(timezone=True)),
    Column("revoked_at", DateTime(timezone=True)),
    CheckConstraint(
        "expires_at IS NULL OR expires_at > created_at",
        name="expiry_after_creation",
    ),
    CheckConstraint(
        "revoked_at IS NULL OR revoked_at >= created_at",
        name="revocation_not_before_creation",
    ),
)
