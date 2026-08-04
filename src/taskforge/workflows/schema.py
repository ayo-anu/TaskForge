"""Relational schema for mutable workflow definitions and their draft graph."""

from sqlalchemy import (
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
