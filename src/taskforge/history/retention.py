"""Closed semantic retention classifications; Task 5 authorizes no deletion."""

from enum import StrEnum
from types import MappingProxyType


class RetentionClass(StrEnum):
    PERMANENT_LINEAGE = "A"
    RETAIN_WHILE_REFERENCED = "B"
    ARCHIVABLE_VERIFIED_COPY_REQUIRED = "C"
    FUTURE_BOUNDED_POLICY = "D"
    CURRENT_STATE_OUTSIDE_RETENTION = "E"


RETENTION_CLASSIFICATIONS = MappingProxyType(
    {
        "audit_records": RetentionClass.PERMANENT_LINEAGE,
        "workflow_run_execution_events": RetentionClass.PERMANENT_LINEAGE,
        "task_claim_events": RetentionClass.PERMANENT_LINEAGE,
        "task_result_events": RetentionClass.PERMANENT_LINEAGE,
        "task_retry_events": RetentionClass.PERMANENT_LINEAGE,
        "workflow_run_replays": RetentionClass.PERMANENT_LINEAGE,
        "workflow_run_cancellation_requests": RetentionClass.PERMANENT_LINEAGE,
        "dead_letter_items": RetentionClass.PERMANENT_LINEAGE,
        "dead_letter_operator_actions": RetentionClass.PERMANENT_LINEAGE,
        "dead_letter_redrive_requests": RetentionClass.PERMANENT_LINEAGE,
        "workflow_versions": RetentionClass.PERMANENT_LINEAGE,
        "workflow_version_steps": RetentionClass.PERMANENT_LINEAGE,
        "workflow_version_dependencies": RetentionClass.PERMANENT_LINEAGE,
        "workflow_run_inputs": RetentionClass.RETAIN_WHILE_REFERENCED,
        "task_attempt_results": RetentionClass.RETAIN_WHILE_REFERENCED,
        "workflow_runs": RetentionClass.RETAIN_WHILE_REFERENCED,
        "task_runs": RetentionClass.RETAIN_WHILE_REFERENCED,
        "task_attempts": RetentionClass.RETAIN_WHILE_REFERENCED,
        "task_attempt_claims": RetentionClass.RETAIN_WHILE_REFERENCED,
        "task_dispatch_outbox": RetentionClass.RETAIN_WHILE_REFERENCED,
        "worker_sessions": RetentionClass.RETAIN_WHILE_REFERENCED,
        "api_principals": RetentionClass.RETAIN_WHILE_REFERENCED,
        "worker_identities": RetentionClass.RETAIN_WHILE_REFERENCED,
        "workflow_definitions": RetentionClass.RETAIN_WHILE_REFERENCED,
        "worker_heartbeats": RetentionClass.ARCHIVABLE_VERIFIED_COPY_REQUIRED,
        "workflow_run_idempotency": RetentionClass.FUTURE_BOUNDED_POLICY,
        "worker_session_capabilities": RetentionClass.CURRENT_STATE_OUTSIDE_RETENTION,
        "worker_session_health": RetentionClass.CURRENT_STATE_OUTSIDE_RETENTION,
        "dead_letter_status": RetentionClass.CURRENT_STATE_OUTSIDE_RETENTION,
        "workflow_draft_steps": RetentionClass.CURRENT_STATE_OUTSIDE_RETENTION,
        "workflow_draft_dependencies": RetentionClass.CURRENT_STATE_OUTSIDE_RETENTION,
        "api_principal_roles": RetentionClass.CURRENT_STATE_OUTSIDE_RETENTION,
        "api_credentials": RetentionClass.CURRENT_STATE_OUTSIDE_RETENTION,
        "worker_credentials": RetentionClass.CURRENT_STATE_OUTSIDE_RETENTION,
        "rate_limit_counters": RetentionClass.CURRENT_STATE_OUTSIDE_RETENTION,
    }
)

# Task 5 deliberately supplies no production pruning surface.
TASK5_DELETE_ELIGIBLE_TABLES: frozenset[str] = frozenset()
