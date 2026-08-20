"""Relational schema for dead-letter facts and operational state."""

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    String,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import UUID

from taskforge.persistence.metadata import metadata

dead_letter_items = Table(
    "dead_letter_items",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("task_run_id", UUID(as_uuid=True), nullable=False),
    Column("source_task_attempt_id", UUID(as_uuid=True), nullable=False),
    Column("reason", String(32), nullable=False),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("statement_timestamp()"),
    ),
    ForeignKeyConstraint(
        ("task_run_id", "source_task_attempt_id"),
        ("task_attempts.task_run_id", "task_attempts.id"),
        name="fk_dead_letter_items_source_attempt",
        onupdate="RESTRICT",
        ondelete="RESTRICT",
    ),
    ForeignKeyConstraint(
        ("source_task_attempt_id",),
        ("task_attempt_results.task_attempt_id",),
        name="fk_dead_letter_items_source_result",
        onupdate="RESTRICT",
        ondelete="RESTRICT",
    ),
    UniqueConstraint(
        "source_task_attempt_id",
        name="uq_dead_letter_items_source_task_attempt_id",
    ),
    CheckConstraint(
        "reason IN ('permanent_failure', 'retry_exhausted')",
        name="reason_valid",
    ),
)
Index(
    "ix_dead_letter_items_task_run_id_created_at_id",
    dead_letter_items.c.task_run_id,
    dead_letter_items.c.created_at.desc(),
    dead_letter_items.c.id.desc(),
)
Index(
    "ix_dead_letter_items_created_at_id",
    dead_letter_items.c.created_at.desc(),
    dead_letter_items.c.id.desc(),
)

dead_letter_status = Table(
    "dead_letter_status",
    metadata,
    Column(
        "dead_letter_item_id",
        UUID(as_uuid=True),
        ForeignKey(
            "dead_letter_items.id",
            name="fk_dead_letter_status_dead_letter_item_id_dead_letter_items",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        primary_key=True,
    ),
    Column("status", String(32), nullable=False, server_default="open"),
    Column(
        "updated_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("statement_timestamp()"),
    ),
    CheckConstraint(
        "status IN ('open', 'acknowledged', 'resolved')",
        name="status_valid",
    ),
)
Index(
    "ix_dead_letter_status_status_updated_at_item_id",
    dead_letter_status.c.status,
    dead_letter_status.c.updated_at.desc(),
    dead_letter_status.c.dead_letter_item_id.desc(),
)

dead_letter_operator_actions = Table(
    "dead_letter_operator_actions",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column(
        "dead_letter_item_id",
        UUID(as_uuid=True),
        ForeignKey(
            "dead_letter_items.id",
            name="fk_dead_letter_operator_actions_item",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        nullable=False,
    ),
    Column(
        "operator_principal_id",
        UUID(as_uuid=True),
        ForeignKey(
            "api_principals.id",
            name="fk_dead_letter_operator_actions_operator",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        nullable=False,
    ),
    Column("action_type", String(32), nullable=False),
    Column("previous_status", String(32), nullable=False),
    Column("new_status", String(32), nullable=False),
    Column("reason", Text, nullable=True),
    Column("correlation_id", UUID(as_uuid=True), nullable=True),
    Column(
        "occurred_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("statement_timestamp()"),
    ),
    CheckConstraint(
        "action_type IN ('acknowledged', 'resolved')",
        name="action_type_valid",
    ),
    CheckConstraint(
        "(action_type = 'acknowledged' AND previous_status = 'open' "
        "AND new_status = 'acknowledged') OR "
        "(action_type = 'resolved' "
        "AND previous_status IN ('open', 'acknowledged') "
        "AND new_status = 'resolved' AND reason IS NOT NULL)",
        name="action_shape_valid",
    ),
    CheckConstraint(
        "reason IS NULL OR length(btrim(reason)) BETWEEN 1 AND 2000",
        name="reason_valid",
    ),
)
Index(
    "ix_dead_letter_operator_actions_item_occurred_at_id",
    dead_letter_operator_actions.c.dead_letter_item_id,
    dead_letter_operator_actions.c.occurred_at,
    dead_letter_operator_actions.c.id,
)
Index(
    "ix_dead_letter_operator_actions_operator_occurred_at_id",
    dead_letter_operator_actions.c.operator_principal_id,
    dead_letter_operator_actions.c.occurred_at.desc(),
    dead_letter_operator_actions.c.id.desc(),
)

dead_letter_redrive_requests = Table(
    "dead_letter_redrive_requests",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column(
        "dead_letter_item_id",
        UUID(as_uuid=True),
        ForeignKey(
            "dead_letter_items.id",
            name="fk_dead_letter_redrive_requests_item",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        nullable=False,
    ),
    Column(
        "requested_by_principal_id",
        UUID(as_uuid=True),
        ForeignKey(
            "api_principals.id",
            name="fk_dead_letter_redrive_requests_requester",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        nullable=False,
    ),
    Column("idempotency_key_digest", String(64), nullable=False),
    Column("request_fingerprint", String(64), nullable=False),
    Column(
        "target_workflow_run_id",
        UUID(as_uuid=True),
        ForeignKey(
            "workflow_runs.id",
            name="fk_dead_letter_redrive_requests_target_run",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        nullable=False,
    ),
    Column("reason", Text, nullable=True),
    Column("correlation_id", UUID(as_uuid=True), nullable=True),
    Column(
        "requested_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("statement_timestamp()"),
    ),
    UniqueConstraint(
        "dead_letter_item_id",
        "requested_by_principal_id",
        "idempotency_key_digest",
        name="uq_dead_letter_redrive_requests_item_requester_key",
    ),
    UniqueConstraint(
        "dead_letter_item_id",
        name="uq_dead_letter_redrive_requests_item",
    ),
    UniqueConstraint(
        "target_workflow_run_id",
        name="uq_dead_letter_redrive_requests_target_run",
    ),
    CheckConstraint(
        "idempotency_key_digest ~ '^[0-9a-f]{64}$'",
        name="key_digest_valid",
    ),
    CheckConstraint(
        "request_fingerprint ~ '^[0-9a-f]{64}$'",
        name="fingerprint_valid",
    ),
    CheckConstraint(
        "reason IS NULL OR length(btrim(reason)) BETWEEN 1 AND 2000",
        name="reason_valid",
    ),
)
Index(
    "ix_dead_letter_redrive_requests_item_requested_at_id",
    dead_letter_redrive_requests.c.dead_letter_item_id,
    dead_letter_redrive_requests.c.requested_at.desc(),
    dead_letter_redrive_requests.c.id.desc(),
)
