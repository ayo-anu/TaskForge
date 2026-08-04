"""Create the API and worker identity foundation.

Revision ID: 0001_identity
Revises:
Create Date: 2026-08-04
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_identity"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create identity, role, and separately scoped credential tables."""
    op.create_table(
        "api_principals",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("disabled_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "disabled_at IS NULL OR disabled_at >= created_at",
            name=op.f("ck_api_principals_disabled_not_before_creation"),
        ),
        sa.CheckConstraint(
            "length(name) > 0",
            name=op.f("ck_api_principals_name_not_empty"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_api_principals")),
        sa.UniqueConstraint("name", name=op.f("uq_api_principals_name")),
    )
    op.create_table(
        "worker_identities",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("disabled_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "disabled_at IS NULL OR disabled_at >= created_at",
            name=op.f("ck_worker_identities_disabled_not_before_creation"),
        ),
        sa.CheckConstraint(
            "length(name) > 0",
            name=op.f("ck_worker_identities_name_not_empty"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_worker_identities")),
        sa.UniqueConstraint("name", name=op.f("uq_worker_identities_name")),
    )
    op.create_table(
        "api_principal_roles",
        sa.Column("principal_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("role", sa.String(length=32), nullable=False),
        sa.Column(
            "assigned_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "role IN ('viewer', 'workflow_operator', 'administrator')",
            name=op.f("ck_api_principal_roles_role_allowed"),
        ),
        sa.ForeignKeyConstraint(
            ["principal_id"],
            ["api_principals.id"],
            name=op.f("fk_api_principal_roles_principal_id_api_principals"),
        ),
        sa.PrimaryKeyConstraint(
            "principal_id",
            "role",
            name=op.f("pk_api_principal_roles"),
        ),
    )
    op.create_table(
        "api_credentials",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("principal_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("credential_verifier", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "expires_at IS NULL OR expires_at > created_at",
            name=op.f("ck_api_credentials_expiry_after_creation"),
        ),
        sa.CheckConstraint(
            "revoked_at IS NULL OR revoked_at >= created_at",
            name=op.f("ck_api_credentials_revocation_not_before_creation"),
        ),
        sa.ForeignKeyConstraint(
            ["principal_id"],
            ["api_principals.id"],
            name=op.f("fk_api_credentials_principal_id_api_principals"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_api_credentials")),
    )
    op.create_index(
        op.f("ix_api_credentials_principal_id"),
        "api_credentials",
        ["principal_id"],
        unique=False,
    )
    op.create_table(
        "worker_credentials",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("worker_identity_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("credential_verifier", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "expires_at IS NULL OR expires_at > created_at",
            name=op.f("ck_worker_credentials_expiry_after_creation"),
        ),
        sa.CheckConstraint(
            "revoked_at IS NULL OR revoked_at >= created_at",
            name=op.f("ck_worker_credentials_revocation_not_before_creation"),
        ),
        sa.ForeignKeyConstraint(
            ["worker_identity_id"],
            ["worker_identities.id"],
            name=op.f("fk_worker_credentials_worker_identity_id_worker_identities"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_worker_credentials")),
    )
    op.create_index(
        op.f("ix_worker_credentials_worker_identity_id"),
        "worker_credentials",
        ["worker_identity_id"],
        unique=False,
    )


def downgrade() -> None:
    """Remove identity tables in reverse dependency order."""
    op.drop_index(
        op.f("ix_worker_credentials_worker_identity_id"),
        table_name="worker_credentials",
    )
    op.drop_table("worker_credentials")
    op.drop_index(
        op.f("ix_api_credentials_principal_id"),
        table_name="api_credentials",
    )
    op.drop_table("api_credentials")
    op.drop_table("api_principal_roles")
    op.drop_table("worker_identities")
    op.drop_table("api_principals")
