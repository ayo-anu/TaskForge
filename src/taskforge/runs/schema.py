"""Relational schema for workflow runs and accepted creation snapshots."""

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Table,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

from taskforge.persistence.metadata import metadata

WORKFLOW_RUN_STATUSES = (
    "pending",
    "running",
    "cancelling",
    "succeeded",
    "failed",
    "cancelled",
)
TASK_RUN_STATUSES = (
    "blocked",
    "runnable",
    "dispatched",
    "claimed",
    "running",
    "retry_pending",
    "retry_scheduled",
    "succeeded",
    "failed",
    "skipped",
    "cancelled",
)

workflow_run_status = Enum(
    *WORKFLOW_RUN_STATUSES,
    name="workflow_run_status",
    native_enum=True,
)
task_run_status = Enum(
    *TASK_RUN_STATUSES,
    name="task_run_status",
    native_enum=True,
)

workflow_runs = Table(
    "workflow_runs",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("workflow_definition_id", UUID(as_uuid=True), nullable=False),
    Column("workflow_version_id", UUID(as_uuid=True), nullable=False),
    Column(
        "requested_by_principal_id",
        UUID(as_uuid=True),
        ForeignKey(
            "api_principals.id",
            name="fk_workflow_runs_requested_by_principal_id_api_principals",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        nullable=False,
    ),
    Column("status", workflow_run_status, nullable=False),
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
    ForeignKeyConstraint(
        ("workflow_definition_id", "workflow_version_id"),
        ("workflow_versions.workflow_definition_id", "workflow_versions.id"),
        name="fk_workflow_runs_definition_version",
        onupdate="RESTRICT",
        ondelete="RESTRICT",
    ),
    UniqueConstraint(
        "id",
        "workflow_version_id",
        name="uq_workflow_runs_id_workflow_version_id",
    ),
    UniqueConstraint(
        "id",
        "requested_by_principal_id",
        "workflow_definition_id",
        name="uq_workflow_runs_id_requester_definition",
    ),
)

Index(
    "ix_workflow_runs_workflow_definition_id_created_at_id",
    workflow_runs.c.workflow_definition_id,
    workflow_runs.c.created_at.desc(),
    workflow_runs.c.id.desc(),
)
Index(
    "ix_workflow_runs_workflow_definition_id_workflow_version_id",
    workflow_runs.c.workflow_definition_id,
    workflow_runs.c.workflow_version_id,
)

workflow_run_inputs = Table(
    "workflow_run_inputs",
    metadata,
    Column(
        "workflow_run_id",
        UUID(as_uuid=True),
        ForeignKey(
            "workflow_runs.id",
            name="fk_workflow_run_inputs_workflow_run_id_workflow_runs",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        primary_key=True,
    ),
    Column("payload", JSONB, nullable=False),
    Column("input_references", JSONB, nullable=False),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.current_timestamp(),
    ),
    CheckConstraint(
        "jsonb_typeof(payload) = 'object'",
        name="payload_object",
    ),
    CheckConstraint(
        "jsonb_typeof(input_references) = 'object'",
        name="input_references_object",
    ),
)

task_runs = Table(
    "task_runs",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("workflow_run_id", UUID(as_uuid=True), nullable=False),
    Column("workflow_version_id", UUID(as_uuid=True), nullable=False),
    Column("step_identifier", String(128), nullable=False),
    Column("status", task_run_status, nullable=False),
    Column("deadline_at", DateTime(timezone=True), nullable=True),
    Column("execution_timeout_seconds", Integer, nullable=True),
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
    ForeignKeyConstraint(
        ("workflow_run_id", "workflow_version_id"),
        ("workflow_runs.id", "workflow_runs.workflow_version_id"),
        name="fk_task_runs_run_version",
        onupdate="RESTRICT",
        ondelete="RESTRICT",
    ),
    ForeignKeyConstraint(
        ("workflow_version_id", "step_identifier"),
        (
            "workflow_version_steps.workflow_version_id",
            "workflow_version_steps.step_identifier",
        ),
        name="fk_task_runs_version_step",
        onupdate="RESTRICT",
        ondelete="RESTRICT",
    ),
    UniqueConstraint(
        "workflow_run_id",
        "step_identifier",
        name="uq_task_runs_workflow_run_id_step_identifier",
    ),
    CheckConstraint(
        "length(btrim(step_identifier)) > 0",
        name="step_identifier_not_blank",
    ),
    CheckConstraint(
        "execution_timeout_seconds IS NULL "
        "OR execution_timeout_seconds BETWEEN 1 AND 31536000",
        name="execution_timeout_seconds_valid",
    ),
)

Index(
    "ix_task_runs_workflow_version_id_step_identifier",
    task_runs.c.workflow_version_id,
    task_runs.c.step_identifier,
)

task_attempts = Table(
    "task_attempts",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column(
        "task_run_id",
        UUID(as_uuid=True),
        ForeignKey(
            "task_runs.id",
            name="fk_task_attempts_task_run_id_task_runs",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        nullable=False,
    ),
    Column("attempt_number", Integer, nullable=False),
    Column("next_eligible_at", DateTime(timezone=True), nullable=True),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.current_timestamp(),
    ),
    UniqueConstraint(
        "task_run_id",
        "attempt_number",
        name="uq_task_attempts_task_run_id_attempt_number",
    ),
    CheckConstraint(
        "attempt_number > 0",
        name="attempt_number_positive",
    ),
)

Index(
    "ix_task_attempts_scheduled_next_eligible_at_id",
    task_attempts.c.next_eligible_at,
    task_attempts.c.id,
    postgresql_where=task_attempts.c.next_eligible_at.is_not(None),
)

task_attempt_results = Table(
    "task_attempt_results",
    metadata,
    Column(
        "task_attempt_id",
        UUID(as_uuid=True),
        ForeignKey(
            "task_attempts.id",
            name="fk_task_attempt_results_task_attempt_id_task_attempts",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        primary_key=True,
    ),
    Column("claim_generation", BigInteger, nullable=False),
    Column(
        "dispatch_id",
        UUID(as_uuid=True),
        ForeignKey(
            "task_dispatch_outbox.id",
            name="fk_task_attempt_results_dispatch_id_task_dispatch_outbox",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        nullable=False,
        unique=True,
    ),
    Column("result_kind", String(32), nullable=False),
    Column("failure_kind", String(32), nullable=True),
    Column("output", JSONB, nullable=True),
    Column("result_fingerprint", String(64), nullable=False),
    Column(
        "completed_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("statement_timestamp()"),
    ),
    ForeignKeyConstraint(
        ("task_attempt_id", "claim_generation"),
        (
            "task_attempt_claims.task_attempt_id",
            "task_attempt_claims.generation",
        ),
        name="fk_task_attempt_results_claim_generation",
        onupdate="RESTRICT",
        ondelete="RESTRICT",
    ),
    CheckConstraint("claim_generation > 0", name="claim_generation_positive"),
    CheckConstraint(
        "result_fingerprint ~ '^[0-9a-f]{64}$'",
        name="result_fingerprint_valid",
    ),
    CheckConstraint(
        "(result_kind = 'success' AND failure_kind IS NULL) OR "
        "(result_kind = 'retryable_failure' AND failure_kind IN "
        "('handler_reported', 'handler_exception', 'execution_timeout')) OR "
        "(result_kind = 'permanent_failure' AND failure_kind = "
        "'handler_reported') OR "
        "(result_kind = 'cancellation' AND failure_kind IS NULL)",
        name="result_shape_valid",
    ),
    CheckConstraint(
        "(result_kind = 'success' AND output IS NOT NULL) OR "
        "(result_kind <> 'success' AND output IS NULL)",
        name="output_presence_valid",
    ),
)

task_attempt_claims = Table(
    "task_attempt_claims",
    metadata,
    Column(
        "task_attempt_id",
        UUID(as_uuid=True),
        ForeignKey(
            "task_attempts.id",
            name="fk_task_attempt_claims_task_attempt_id_task_attempts",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        primary_key=True,
    ),
    Column("generation", BigInteger, primary_key=True),
    Column(
        "worker_session_id",
        UUID(as_uuid=True),
        ForeignKey(
            "worker_sessions.id",
            name="fk_task_attempt_claims_worker_session_id_worker_sessions",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        nullable=False,
    ),
    Column(
        "acquired_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("statement_timestamp()"),
    ),
    Column("lease_expires_at", DateTime(timezone=True), nullable=False),
    Column("terminated_at", DateTime(timezone=True), nullable=True),
    CheckConstraint("generation > 0", name="generation_positive"),
    CheckConstraint(
        "lease_expires_at > acquired_at",
        name="lease_expires_after_acquisition",
    ),
    CheckConstraint(
        "terminated_at IS NULL OR terminated_at >= acquired_at",
        name="terminated_not_before_acquisition",
    ),
)

task_claim_events = Table(
    "task_claim_events",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("task_attempt_id", UUID(as_uuid=True), nullable=False),
    Column("generation", BigInteger, nullable=False),
    Column("event_type", String(32), nullable=False),
    Column("occurred_at", DateTime(timezone=True), nullable=False),
    Column("previous_lease_expires_at", DateTime(timezone=True), nullable=True),
    Column("lease_expires_at", DateTime(timezone=True), nullable=False),
    CheckConstraint(
        "event_type IN ('claim_acquired', 'lease_renewed')",
        name="event_type_valid",
    ),
    CheckConstraint(
        "(event_type = 'claim_acquired' "
        "AND previous_lease_expires_at IS NULL "
        "AND lease_expires_at > occurred_at) OR "
        "(event_type = 'lease_renewed' "
        "AND previous_lease_expires_at IS NOT NULL "
        "AND lease_expires_at > previous_lease_expires_at)",
        name="event_shape_valid",
    ),
    ForeignKeyConstraint(
        ("task_attempt_id", "generation"),
        (
            "task_attempt_claims.task_attempt_id",
            "task_attempt_claims.generation",
        ),
        name="fk_task_claim_events_claim_generation",
        onupdate="RESTRICT",
        ondelete="RESTRICT",
    ),
)

task_result_events = Table(
    "task_result_events",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("task_attempt_id", UUID(as_uuid=True), nullable=False),
    Column("claim_generation", BigInteger, nullable=False),
    Column(
        "worker_session_id",
        UUID(as_uuid=True),
        ForeignKey(
            "worker_sessions.id",
            name="fk_task_result_events_worker_session_id_worker_sessions",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        nullable=False,
    ),
    Column(
        "dispatch_id",
        UUID(as_uuid=True),
        ForeignKey(
            "task_dispatch_outbox.id",
            name="fk_task_result_events_dispatch_id_task_dispatch_outbox",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        nullable=False,
    ),
    Column("event_type", String(32), nullable=False),
    Column("result_kind", String(32), nullable=False),
    Column("failure_kind", String(32), nullable=True),
    Column("result_fingerprint", String(64), nullable=False),
    Column(
        "occurred_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("statement_timestamp()"),
    ),
    ForeignKeyConstraint(
        ("task_attempt_id", "claim_generation"),
        (
            "task_attempt_claims.task_attempt_id",
            "task_attempt_claims.generation",
        ),
        name="fk_task_result_events_claim_generation",
        onupdate="RESTRICT",
        ondelete="RESTRICT",
    ),
    CheckConstraint("claim_generation > 0", name="claim_generation_positive"),
    CheckConstraint(
        "event_type IN ('result_accepted', 'result_replayed', "
        "'result_conflict_rejected', 'result_stale_rejected')",
        name="event_type_valid",
    ),
    CheckConstraint(
        "result_fingerprint ~ '^[0-9a-f]{64}$'",
        name="result_fingerprint_valid",
    ),
    CheckConstraint(
        "(result_kind = 'success' AND failure_kind IS NULL) OR "
        "(result_kind = 'retryable_failure' AND failure_kind IN "
        "('handler_reported', 'handler_exception', 'execution_timeout')) OR "
        "(result_kind = 'permanent_failure' AND failure_kind = "
        "'handler_reported') OR "
        "(result_kind = 'cancellation' AND failure_kind IS NULL)",
        name="result_shape_valid",
    ),
)

Index(
    "ix_task_result_events_task_attempt_id_occurred_at_id",
    task_result_events.c.task_attempt_id,
    task_result_events.c.occurred_at,
    task_result_events.c.id,
)

Index(
    "uq_task_claim_events_acquired_generation",
    task_claim_events.c.task_attempt_id,
    task_claim_events.c.generation,
    unique=True,
    postgresql_where=task_claim_events.c.event_type == "claim_acquired",
)
Index(
    "uq_task_claim_events_renewal_transition",
    task_claim_events.c.task_attempt_id,
    task_claim_events.c.generation,
    task_claim_events.c.previous_lease_expires_at,
    task_claim_events.c.lease_expires_at,
    unique=True,
    postgresql_where=task_claim_events.c.event_type == "lease_renewed",
)

Index(
    "uq_task_attempt_claims_current_task_attempt_id",
    task_attempt_claims.c.task_attempt_id,
    unique=True,
    postgresql_where=task_attempt_claims.c.terminated_at.is_(None),
)
Index(
    "ix_task_attempt_claims_current_lease_expires_at",
    task_attempt_claims.c.lease_expires_at,
    postgresql_where=task_attempt_claims.c.terminated_at.is_(None),
)
Index(
    "ix_task_attempt_claims_current_worker_session_id",
    task_attempt_claims.c.worker_session_id,
    postgresql_where=task_attempt_claims.c.terminated_at.is_(None),
)

task_dispatch_outbox = Table(
    "task_dispatch_outbox",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column(
        "task_attempt_id",
        UUID(as_uuid=True),
        ForeignKey(
            "task_attempts.id",
            name="fk_task_dispatch_outbox_task_attempt_id_task_attempts",
            onupdate="RESTRICT",
            ondelete="RESTRICT",
        ),
        nullable=False,
    ),
    Column("route", String(255), nullable=False),
    Column("payload", JSONB, nullable=False),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.current_timestamp(),
    ),
    Column("published_at", DateTime(timezone=True), nullable=True),
    UniqueConstraint(
        "task_attempt_id",
        name="uq_task_dispatch_outbox_task_attempt_id",
    ),
    CheckConstraint(
        "length(btrim(route)) > 0",
        name="route_not_blank",
    ),
    CheckConstraint(
        "jsonb_typeof(payload) = 'object'",
        name="payload_object",
    ),
)

Index(
    "ix_task_dispatch_outbox_unpublished_created_at_id",
    task_dispatch_outbox.c.created_at,
    task_dispatch_outbox.c.id,
    postgresql_where=task_dispatch_outbox.c.published_at.is_(None),
)

workflow_run_idempotency = Table(
    "workflow_run_idempotency",
    metadata,
    Column("principal_id", UUID(as_uuid=True), primary_key=True),
    Column("workflow_definition_id", UUID(as_uuid=True), primary_key=True),
    Column("idempotency_key_digest", String(256), primary_key=True),
    Column("request_fingerprint", String(256), nullable=False),
    Column("workflow_run_id", UUID(as_uuid=True), nullable=False),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.current_timestamp(),
    ),
    ForeignKeyConstraint(
        ("workflow_run_id", "principal_id", "workflow_definition_id"),
        (
            "workflow_runs.id",
            "workflow_runs.requested_by_principal_id",
            "workflow_runs.workflow_definition_id",
        ),
        name="fk_workflow_run_idempotency_run_scope",
        onupdate="RESTRICT",
        ondelete="RESTRICT",
    ),
    UniqueConstraint(
        "workflow_run_id",
        name="uq_workflow_run_idempotency_workflow_run_id",
    ),
    CheckConstraint(
        "length(btrim(idempotency_key_digest)) > 0",
        name="digest_not_blank",
    ),
    CheckConstraint(
        "length(btrim(request_fingerprint)) > 0",
        name="fingerprint_not_blank",
    ),
)
