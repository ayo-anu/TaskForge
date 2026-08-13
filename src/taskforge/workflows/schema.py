"""Relational schema for mutable drafts and immutable version snapshots."""

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    String,
    Table,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

from taskforge.persistence.metadata import metadata

WORKFLOW_DEFINITION_STATUSES = ("draft", "enabled", "disabled", "archived")

workflow_definition_status = Enum(
    *WORKFLOW_DEFINITION_STATUSES,
    name="workflow_definition_status",
    native_enum=True,
)

workflow_definitions = Table(
    "workflow_definitions",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column(
        "owner_principal_id",
        UUID(as_uuid=True),
        ForeignKey("api_principals.id"),
        nullable=False,
    ),
    Column("name", String(128), nullable=False),
    Column("description", Text),
    Column("execution_policy", JSONB(none_as_null=True)),
    Column(
        "status",
        workflow_definition_status,
        nullable=False,
        server_default="draft",
    ),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.current_timestamp(),
    ),
    Column(
        "updated_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.current_timestamp(),
    ),
    CheckConstraint("length(name) > 0", name="name_not_empty"),
    CheckConstraint(
        "execution_policy IS NULL OR jsonb_typeof(execution_policy) = 'object'",
        name="execution_policy_object",
    ),
    CheckConstraint(
        "execution_policy IS NULL OR NOT (execution_policy ? 'deadline_seconds') "
        "OR (jsonb_typeof(execution_policy -> 'deadline_seconds') = 'number' "
        "AND (execution_policy ->> 'deadline_seconds') ~ '^[0-9]+$' "
        "AND (execution_policy ->> 'deadline_seconds')::numeric "
        "BETWEEN 1 AND 31536000)",
        name="deadline_seconds_valid",
    ),
    CheckConstraint(
        "execution_policy IS NULL "
        "OR NOT (execution_policy ? 'execution_timeout_seconds') "
        "OR (jsonb_typeof(execution_policy -> 'execution_timeout_seconds') = 'number' "
        "AND (execution_policy ->> 'execution_timeout_seconds') ~ '^[0-9]+$' "
        "AND (execution_policy ->> 'execution_timeout_seconds')::numeric "
        "BETWEEN 1 AND 31536000)",
        name="execution_timeout_seconds_valid",
    ),
)

Index(
    "ix_workflow_definitions_owner_created_id",
    workflow_definitions.c.owner_principal_id,
    workflow_definitions.c.created_at.desc(),
    workflow_definitions.c.id.desc(),
)

workflow_draft_steps = Table(
    "workflow_draft_steps",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column(
        "workflow_definition_id",
        UUID(as_uuid=True),
        ForeignKey("workflow_definitions.id"),
        nullable=False,
        index=True,
    ),
    Column("step_identifier", String(128), nullable=False),
    Column("task_type", String(128), nullable=False),
    Column("parameters", JSONB, nullable=False),
    Column("execution_policy", JSONB(none_as_null=True)),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.current_timestamp(),
    ),
    Column(
        "updated_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.current_timestamp(),
    ),
    UniqueConstraint("workflow_definition_id", "step_identifier"),
    UniqueConstraint("workflow_definition_id", "id"),
    CheckConstraint("length(step_identifier) > 0", name="identifier_not_empty"),
    CheckConstraint("length(task_type) > 0", name="task_type_not_empty"),
    CheckConstraint("jsonb_typeof(parameters) = 'object'", name="parameters_object"),
    CheckConstraint(
        "execution_policy IS NULL OR jsonb_typeof(execution_policy) = 'object'",
        name="execution_policy_object",
    ),
    CheckConstraint(
        "execution_policy IS NULL OR NOT (execution_policy ? 'deadline_seconds') "
        "OR (jsonb_typeof(execution_policy -> 'deadline_seconds') = 'number' "
        "AND (execution_policy ->> 'deadline_seconds') ~ '^[0-9]+$' "
        "AND (execution_policy ->> 'deadline_seconds')::numeric "
        "BETWEEN 1 AND 31536000)",
        name="deadline_seconds_valid",
    ),
    CheckConstraint(
        "execution_policy IS NULL "
        "OR NOT (execution_policy ? 'execution_timeout_seconds') "
        "OR (jsonb_typeof(execution_policy -> 'execution_timeout_seconds') = 'number' "
        "AND (execution_policy ->> 'execution_timeout_seconds') ~ '^[0-9]+$' "
        "AND (execution_policy ->> 'execution_timeout_seconds')::numeric "
        "BETWEEN 1 AND 31536000)",
        name="execution_timeout_seconds_valid",
    ),
)

workflow_draft_dependencies = Table(
    "workflow_draft_dependencies",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column(
        "workflow_definition_id",
        UUID(as_uuid=True),
        ForeignKey("workflow_definitions.id"),
        nullable=False,
    ),
    Column("predecessor_step_id", UUID(as_uuid=True), nullable=False),
    Column("successor_step_id", UUID(as_uuid=True), nullable=False),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.current_timestamp(),
    ),
    ForeignKeyConstraint(
        ("workflow_definition_id", "predecessor_step_id"),
        ("workflow_draft_steps.workflow_definition_id", "workflow_draft_steps.id"),
    ),
    ForeignKeyConstraint(
        ("workflow_definition_id", "successor_step_id"),
        ("workflow_draft_steps.workflow_definition_id", "workflow_draft_steps.id"),
    ),
    UniqueConstraint(
        "workflow_definition_id",
        "predecessor_step_id",
        "successor_step_id",
    ),
    CheckConstraint(
        "predecessor_step_id <> successor_step_id",
        name="steps_distinct",
    ),
    Index(None, "workflow_definition_id", "predecessor_step_id"),
    Index(None, "workflow_definition_id", "successor_step_id"),
)

workflow_versions = Table(
    "workflow_versions",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column(
        "workflow_definition_id",
        UUID(as_uuid=True),
        ForeignKey(
            "workflow_definitions.id",
            ondelete="RESTRICT",
            onupdate="RESTRICT",
        ),
        nullable=False,
    ),
    Column("version_number", BigInteger, nullable=False),
    Column("name", String(128), nullable=False),
    Column("description", Text),
    Column("execution_policy", JSONB(none_as_null=True)),
    Column(
        "published_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.current_timestamp(),
    ),
    UniqueConstraint("workflow_definition_id", "version_number"),
    UniqueConstraint(
        "workflow_definition_id",
        "id",
        name="workflow_definition_id_id",
    ),
    CheckConstraint("version_number > 0", name="version_number_positive"),
    CheckConstraint("length(name) > 0", name="name_not_empty"),
    CheckConstraint(
        "execution_policy IS NULL OR jsonb_typeof(execution_policy) = 'object'",
        name="execution_policy_object",
    ),
    CheckConstraint(
        "execution_policy IS NULL OR NOT (execution_policy ? 'deadline_seconds') "
        "OR (jsonb_typeof(execution_policy -> 'deadline_seconds') = 'number' "
        "AND (execution_policy ->> 'deadline_seconds') ~ '^[0-9]+$' "
        "AND (execution_policy ->> 'deadline_seconds')::numeric "
        "BETWEEN 1 AND 31536000)",
        name="deadline_seconds_valid",
        postgresql_not_valid=True,
    ),
    CheckConstraint(
        "execution_policy IS NULL "
        "OR NOT (execution_policy ? 'execution_timeout_seconds') "
        "OR (jsonb_typeof(execution_policy -> 'execution_timeout_seconds') = 'number' "
        "AND (execution_policy ->> 'execution_timeout_seconds') ~ '^[0-9]+$' "
        "AND (execution_policy ->> 'execution_timeout_seconds')::numeric "
        "BETWEEN 1 AND 31536000)",
        name="execution_timeout_seconds_valid",
        postgresql_not_valid=True,
    ),
)

workflow_version_steps = Table(
    "workflow_version_steps",
    metadata,
    Column(
        "workflow_version_id",
        UUID(as_uuid=True),
        ForeignKey(
            "workflow_versions.id",
            ondelete="RESTRICT",
            onupdate="RESTRICT",
        ),
        primary_key=True,
    ),
    Column("step_identifier", String(128), primary_key=True),
    Column("task_type", String(128), nullable=False),
    Column("parameters", JSONB, nullable=False),
    Column("execution_policy", JSONB(none_as_null=True)),
    CheckConstraint("length(step_identifier) > 0", name="identifier_not_empty"),
    CheckConstraint("length(task_type) > 0", name="task_type_not_empty"),
    CheckConstraint("jsonb_typeof(parameters) = 'object'", name="parameters_object"),
    CheckConstraint(
        "execution_policy IS NULL OR jsonb_typeof(execution_policy) = 'object'",
        name="execution_policy_object",
    ),
    CheckConstraint(
        "execution_policy IS NULL OR NOT (execution_policy ? 'deadline_seconds') "
        "OR (jsonb_typeof(execution_policy -> 'deadline_seconds') = 'number' "
        "AND (execution_policy ->> 'deadline_seconds') ~ '^[0-9]+$' "
        "AND (execution_policy ->> 'deadline_seconds')::numeric "
        "BETWEEN 1 AND 31536000)",
        name="deadline_seconds_valid",
        postgresql_not_valid=True,
    ),
    CheckConstraint(
        "execution_policy IS NULL "
        "OR NOT (execution_policy ? 'execution_timeout_seconds') "
        "OR (jsonb_typeof(execution_policy -> 'execution_timeout_seconds') = 'number' "
        "AND (execution_policy ->> 'execution_timeout_seconds') ~ '^[0-9]+$' "
        "AND (execution_policy ->> 'execution_timeout_seconds')::numeric "
        "BETWEEN 1 AND 31536000)",
        name="execution_timeout_seconds_valid",
        postgresql_not_valid=True,
    ),
)

workflow_version_dependencies = Table(
    "workflow_version_dependencies",
    metadata,
    Column(
        "workflow_version_id",
        UUID(as_uuid=True),
        ForeignKey(
            "workflow_versions.id",
            ondelete="RESTRICT",
            onupdate="RESTRICT",
        ),
        primary_key=True,
    ),
    Column("predecessor_step_identifier", String(128), primary_key=True),
    Column("successor_step_identifier", String(128), primary_key=True),
    ForeignKeyConstraint(
        ("workflow_version_id", "predecessor_step_identifier"),
        (
            "workflow_version_steps.workflow_version_id",
            "workflow_version_steps.step_identifier",
        ),
        ondelete="RESTRICT",
        onupdate="RESTRICT",
    ),
    ForeignKeyConstraint(
        ("workflow_version_id", "successor_step_identifier"),
        (
            "workflow_version_steps.workflow_version_id",
            "workflow_version_steps.step_identifier",
        ),
        ondelete="RESTRICT",
        onupdate="RESTRICT",
    ),
    CheckConstraint(
        "predecessor_step_identifier <> successor_step_identifier",
        name="steps_distinct",
    ),
    Index(None, "workflow_version_id", "successor_step_identifier"),
)
