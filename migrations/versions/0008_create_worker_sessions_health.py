"""Create worker session, capability, health, and heartbeat storage.

Revision ID: 0008_worker_sessions_health
Revises: 0007_attempt_dispatch_outbox
Create Date: 2026-08-10
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0008_worker_sessions_health"
down_revision: str | None = "0007_attempt_dispatch_outbox"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create process-session facts and compact liveness persistence."""
    op.create_table(
        "worker_sessions",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("worker_identity_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "registered_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("statement_timestamp()"),
            nullable=False,
        ),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "ended_at IS NULL OR ended_at >= registered_at",
            name=op.f("ck_worker_sessions_ended_not_before_registration"),
        ),
        sa.ForeignKeyConstraint(
            ["worker_identity_id"],
            ["worker_identities.id"],
            name="fk_worker_sessions_worker_identity_id_worker_identities",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_worker_sessions"),
    )
    op.create_index(
        "ix_worker_sessions_worker_identity_id_registered_at_id",
        "worker_sessions",
        ["worker_identity_id", sa.text("registered_at DESC"), sa.text("id DESC")],
        unique=False,
    )
    op.create_index(
        "ix_worker_sessions_open_registered_at_id",
        "worker_sessions",
        ["registered_at", "id"],
        unique=False,
        postgresql_where=sa.text("ended_at IS NULL"),
    )

    op.create_table(
        "worker_session_capabilities",
        sa.Column("worker_session_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("capability", sa.String(length=128), nullable=False),
        sa.Column(
            "advertised_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("statement_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "capability ~ '^[a-z][a-z0-9_.-]{0,127}$'",
            name=op.f("ck_worker_session_capabilities_capability_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["worker_session_id"],
            ["worker_sessions.id"],
            name="fk_worker_session_capabilities_worker_session",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "worker_session_id",
            "capability",
            name="pk_worker_session_capabilities",
        ),
    )
    op.create_index(
        "ix_worker_session_capabilities_capability_worker_session_id",
        "worker_session_capabilities",
        ["capability", "worker_session_id"],
        unique=False,
    )

    op.create_table(
        "worker_session_health",
        sa.Column("worker_session_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "last_sequence",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("accepting_work", sa.Boolean(), nullable=False),
        sa.Column(
            "availability_changed_at", sa.DateTime(timezone=True), nullable=False
        ),
        sa.CheckConstraint(
            "last_sequence >= 0",
            name=op.f("ck_worker_session_health_last_sequence_nonnegative"),
        ),
        sa.CheckConstraint(
            "availability_changed_at <= last_seen_at",
            name=op.f("ck_worker_session_health_availability_not_after_last_seen"),
        ),
        sa.ForeignKeyConstraint(
            ["worker_session_id"],
            ["worker_sessions.id"],
            name="fk_worker_session_health_worker_session_id_worker_sessions",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("worker_session_id", name="pk_worker_session_health"),
    )
    op.create_index(
        "ix_worker_session_health_last_seen_at_worker_session_id",
        "worker_session_health",
        ["last_seen_at", "worker_session_id"],
        unique=False,
    )

    op.create_table(
        "worker_heartbeats",
        sa.Column("worker_session_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("statement_timestamp()"),
            nullable=False,
        ),
        sa.Column("accepting_work", sa.Boolean(), nullable=False),
        sa.CheckConstraint(
            "sequence > 0",
            name=op.f("ck_worker_heartbeats_sequence_positive"),
        ),
        sa.ForeignKeyConstraint(
            ["worker_session_id"],
            ["worker_sessions.id"],
            name="fk_worker_heartbeats_worker_session_id_worker_sessions",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "worker_session_id", "sequence", name="pk_worker_heartbeats"
        ),
    )


def downgrade() -> None:
    """Remove worker liveness storage in reverse dependency order."""
    op.drop_table("worker_heartbeats")
    op.drop_index(
        "ix_worker_session_health_last_seen_at_worker_session_id",
        table_name="worker_session_health",
    )
    op.drop_table("worker_session_health")
    op.drop_index(
        "ix_worker_session_capabilities_capability_worker_session_id",
        table_name="worker_session_capabilities",
    )
    op.drop_table("worker_session_capabilities")
    op.drop_index(
        "ix_worker_sessions_open_registered_at_id", table_name="worker_sessions"
    )
    op.drop_index(
        "ix_worker_sessions_worker_identity_id_registered_at_id",
        table_name="worker_sessions",
    )
    op.drop_table("worker_sessions")
