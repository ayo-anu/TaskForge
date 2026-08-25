"""Structural guarantees for workflow run persistence."""

from __future__ import annotations

from sqlalchemy import (
    CheckConstraint,
    DefaultClause,
    Enum,
    ForeignKeyConstraint,
    Index,
    Table,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

from taskforge.persistence.schema import metadata
from taskforge.runs.schema import (
    TASK_RUN_STATUSES,
    WORKFLOW_REPLAY_MODES,
    WORKFLOW_RUN_STATUSES,
    task_attempt_results,
    task_claim_events,
    task_result_events,
    task_retry_events,
    task_runs,
    workflow_run_cancellation_requests,
    workflow_run_execution_events,
    workflow_run_idempotency,
    workflow_run_inputs,
    workflow_run_replays,
    workflow_runs,
)


def unique_column_sets(table: Table) -> set[tuple[str, ...]]:
    constraints = table.constraints
    return {
        tuple(constraint.columns.keys())
        for constraint in constraints
        if isinstance(constraint, UniqueConstraint)
    }


def foreign_key_shapes(table: Table) -> set[tuple[tuple[str, ...], tuple[str, ...]]]:
    constraints = table.foreign_key_constraints
    return {
        (
            tuple(constraint.columns.keys()),
            tuple(element.target_fullname for element in constraint.elements),
        )
        for constraint in constraints
        if isinstance(constraint, ForeignKeyConstraint)
    }


def check_texts(table: Table) -> set[str]:
    constraints = table.constraints
    return {
        str(constraint.sqltext)
        for constraint in constraints
        if isinstance(constraint, CheckConstraint)
    }


def check_text_by_name(table: Table, name: str) -> str:
    return next(
        str(constraint.sqltext)
        for constraint in table.constraints
        if isinstance(constraint, CheckConstraint) and constraint.name == name
    )


def test_shared_metadata_registers_run_dispatch_and_claim_foundation_tables() -> None:
    assert {
        "workflow_runs",
        "workflow_run_inputs",
        "task_runs",
        "task_attempts",
        "task_attempt_claims",
        "task_claim_events",
        "task_attempt_results",
        "task_result_events",
        "task_retry_events",
        "task_dispatch_outbox",
        "workflow_run_idempotency",
        "workflow_run_execution_events",
        "workflow_run_replays",
    } <= set(metadata.tables)
    assert "audit_records" in metadata.tables


def test_run_statuses_use_native_enums_without_database_defaults() -> None:
    workflow_status = workflow_runs.c.status
    task_status = task_runs.c.status

    assert isinstance(workflow_status.type, Enum)
    assert workflow_status.type.native_enum is True
    assert workflow_status.type.name == "workflow_run_status"
    assert tuple(workflow_status.type.enums) == WORKFLOW_RUN_STATUSES
    assert workflow_status.server_default is None
    assert isinstance(task_status.type, Enum)
    assert task_status.type.native_enum is True
    assert task_status.type.name == "task_run_status"
    assert tuple(task_status.type.enums) == TASK_RUN_STATUSES
    assert task_status.server_default is None


def test_workflow_run_binds_definition_version_and_requester() -> None:
    assert isinstance(workflow_runs.c.id.type, UUID)
    assert tuple(workflow_runs.primary_key.columns.keys()) == ("id",)
    assert unique_column_sets(workflow_runs) == {
        ("id", "workflow_version_id"),
        ("id", "requested_by_principal_id", "workflow_definition_id"),
    }
    assert foreign_key_shapes(workflow_runs) == {
        (
            ("requested_by_principal_id",),
            ("api_principals.id",),
        ),
        (
            ("workflow_definition_id", "workflow_version_id"),
            ("workflow_versions.workflow_definition_id", "workflow_versions.id"),
        ),
    }
    assert workflow_runs.c.created_at.server_default is not None
    assert workflow_runs.c.updated_at.server_default is not None
    cursor_default = workflow_runs.c.last_execution_event_cursor.server_default
    assert isinstance(cursor_default, DefaultClause)
    assert str(cursor_default.arg) == "0"
    assert "last_execution_event_cursor >= 0" in check_texts(workflow_runs)


def test_workflow_replay_records_constrained_immediate_source_lineage() -> None:
    assert tuple(workflow_run_replays.c.keys()) == (
        "workflow_run_id",
        "source_workflow_run_id",
        "mode",
        "requested_scope",
        "created_at",
    )
    assert tuple(workflow_run_replays.primary_key.columns.keys()) == (
        "workflow_run_id",
    )
    assert isinstance(workflow_run_replays.c.workflow_run_id.type, UUID)
    assert isinstance(workflow_run_replays.c.source_workflow_run_id.type, UUID)
    assert isinstance(workflow_run_replays.c.mode.type, Enum)
    assert workflow_run_replays.c.mode.type.native_enum is True
    assert workflow_run_replays.c.mode.type.name == "workflow_replay_mode"
    assert tuple(workflow_run_replays.c.mode.type.enums) == WORKFLOW_REPLAY_MODES
    assert isinstance(workflow_run_replays.c.requested_scope.type, JSONB)
    assert all(column.nullable is False for column in workflow_run_replays.c)
    assert workflow_run_replays.c.mode.server_default is None
    assert workflow_run_replays.c.requested_scope.server_default is None
    assert workflow_run_replays.c.created_at.server_default is not None
    assert foreign_key_shapes(workflow_run_replays) == {
        (("workflow_run_id",), ("workflow_runs.id",)),
        (("source_workflow_run_id",), ("workflow_runs.id",)),
    }
    for foreign_key in workflow_run_replays.foreign_key_constraints:
        assert foreign_key.onupdate == "RESTRICT"
        assert foreign_key.ondelete == "RESTRICT"
    assert check_texts(workflow_run_replays) == {
        "workflow_run_id <> source_workflow_run_id",
        "jsonb_typeof(requested_scope) = 'object'",
    }
    assert unique_column_sets(workflow_run_replays) == set()
    assert "workflow_definition_id" not in workflow_run_replays.c
    assert "workflow_version_id" not in workflow_run_replays.c
    assert {
        (index.name, tuple(column.name for column in index.columns))
        for index in workflow_run_replays.indexes
    } == {
        (
            "ix_workflow_run_replays_source_workflow_run_id",
            ("source_workflow_run_id",),
        )
    }


def test_run_inputs_require_explicit_json_objects() -> None:
    assert tuple(workflow_run_inputs.primary_key.columns.keys()) == ("workflow_run_id",)
    assert isinstance(workflow_run_inputs.c.payload.type, JSONB)
    assert isinstance(workflow_run_inputs.c.input_references.type, JSONB)
    assert workflow_run_inputs.c.payload.nullable is False
    assert workflow_run_inputs.c.input_references.nullable is False
    assert workflow_run_inputs.c.payload.server_default is None
    assert workflow_run_inputs.c.input_references.server_default is None
    assert check_texts(workflow_run_inputs) == {
        "jsonb_typeof(payload) = 'object'",
        "jsonb_typeof(input_references) = 'object'",
    }


def test_task_run_identity_and_composite_relationships_are_constrained() -> None:
    assert tuple(task_runs.primary_key.columns.keys()) == ("id",)
    assert unique_column_sets(task_runs) == {
        ("workflow_run_id", "step_identifier"),
        ("workflow_run_id", "id"),
    }
    assert foreign_key_shapes(task_runs) == {
        (
            ("workflow_run_id", "workflow_version_id"),
            ("workflow_runs.id", "workflow_runs.workflow_version_id"),
        ),
        (
            ("workflow_version_id", "step_identifier"),
            (
                "workflow_version_steps.workflow_version_id",
                "workflow_version_steps.step_identifier",
            ),
        ),
    }
    assert "length(btrim(step_identifier)) > 0" in check_texts(task_runs)


def test_execution_events_are_run_ordered_and_task_ownership_constrained() -> None:
    assert tuple(workflow_run_execution_events.primary_key.columns.keys()) == ("id",)
    assert unique_column_sets(workflow_run_execution_events) == {
        ("workflow_run_id", "cursor")
    }
    assert foreign_key_shapes(workflow_run_execution_events) == {
        (("workflow_run_id",), ("workflow_runs.id",)),
        (
            ("workflow_run_id", "task_run_id"),
            ("task_runs.workflow_run_id", "task_runs.id"),
        ),
    }
    assert workflow_run_execution_events.c.cursor.server_default is None
    assert workflow_run_execution_events.c.payload.server_default is not None
    assert workflow_run_execution_events.c.occurred_at.server_default is not None
    assert check_texts(workflow_run_execution_events) == {
        "cursor > 0",
        "length(btrim(event_type)) BETWEEN 1 AND 128",
        "jsonb_typeof(payload) = 'object'",
    }
    for foreign_key in workflow_run_execution_events.foreign_key_constraints:
        assert foreign_key.onupdate == "RESTRICT"
        assert foreign_key.ondelete == "RESTRICT"


def test_task_run_indexes_cover_only_the_required_new_composite_path() -> None:
    indexes = {
        (index.name, tuple(column.name for column in index.columns))
        for index in task_runs.indexes
        if isinstance(index, Index)
    }
    assert indexes == {
        (
            "ix_task_runs_workflow_version_id_step_identifier",
            ("workflow_version_id", "step_identifier"),
        )
    }


def test_idempotency_scope_is_bound_to_the_same_run_principal_and_workflow() -> None:
    assert tuple(workflow_run_idempotency.primary_key.columns.keys()) == (
        "principal_id",
        "workflow_definition_id",
        "idempotency_key_digest",
    )
    assert unique_column_sets(workflow_run_idempotency) == {("workflow_run_id",)}
    assert foreign_key_shapes(workflow_run_idempotency) == {
        (
            ("workflow_run_id", "principal_id", "workflow_definition_id"),
            (
                "workflow_runs.id",
                "workflow_runs.requested_by_principal_id",
                "workflow_runs.workflow_definition_id",
            ),
        )
    }
    checks = check_texts(workflow_run_idempotency)
    assert "length(btrim(idempotency_key_digest)) > 0" in checks
    assert "length(btrim(request_fingerprint)) > 0" in checks
    assert workflow_run_idempotency.c.created_at.server_default is not None


def test_cancellation_request_is_one_immutable_intention_per_run() -> None:
    assert tuple(workflow_run_cancellation_requests.primary_key.columns.keys()) == (
        "workflow_run_id",
    )
    assert tuple(workflow_run_cancellation_requests.c.keys()) == (
        "workflow_run_id",
        "requested_by_principal_id",
        "idempotency_key_digest",
        "request_fingerprint",
        "reason",
        "correlation_id",
        "requested_at",
    )
    assert foreign_key_shapes(workflow_run_cancellation_requests) == {
        (("workflow_run_id",), ("workflow_runs.id",)),
        (("requested_by_principal_id",), ("api_principals.id",)),
    }
    assert unique_column_sets(workflow_run_cancellation_requests) == set()
    assert workflow_run_cancellation_requests.indexes == set()
    assert workflow_run_cancellation_requests.c.reason.nullable is True
    assert workflow_run_cancellation_requests.c.requested_at.server_default is not None
    assert check_texts(workflow_run_cancellation_requests) == {
        "idempotency_key_digest ~ '^[0-9a-f]{64}$'",
        "request_fingerprint ~ '^[0-9a-f]{64}$'",
        "reason IS NULL OR length(btrim(reason)) BETWEEN 1 AND 2000",
    }
    for foreign_key in workflow_run_cancellation_requests.foreign_key_constraints:
        assert foreign_key.onupdate == "RESTRICT"
        assert foreign_key.ondelete == "RESTRICT"


def test_retry_events_use_task_scoped_attempt_foreign_keys_and_server_time() -> None:
    assert tuple(task_retry_events.c.keys()) == (
        "id",
        "task_run_id",
        "event_type",
        "actor_component",
        "correlation_id",
        "failed_attempt_number",
        "retry_attempt_number",
        "next_eligible_at",
        "decision_reason",
        "occurred_at",
    )
    assert foreign_key_shapes(task_retry_events) == {
        (("task_run_id",), ("task_runs.id",)),
        (
            ("task_run_id", "failed_attempt_number"),
            ("task_attempts.task_run_id", "task_attempts.attempt_number"),
        ),
        (
            ("task_run_id", "retry_attempt_number"),
            ("task_attempts.task_run_id", "task_attempts.attempt_number"),
        ),
    }
    assert task_retry_events.c.occurred_at.server_default is not None
    assert "retry_attempt_number = failed_attempt_number + 1" in " ".join(
        check_texts(task_retry_events)
    )
    indexes = {index.name for index in task_retry_events.indexes}
    assert indexes == {
        "uq_task_retry_events_scheduled_attempt",
        "uq_task_retry_events_dispatched_attempt",
        "uq_task_retry_events_not_scheduled_attempt",
        "ix_task_retry_events_task_run_id_occurred_at_id",
    }


def test_result_and_retry_actor_component_constraints_match_migration_contract() -> (
    None
):
    assert check_text_by_name(
        task_result_events, "ck_task_result_events_actor_component_valid"
    ) == (
        "actor_component IS NULL OR actor_component IN "
        "('expired_claim_recovery','cancellation_recovery')"
    )
    assert check_text_by_name(
        task_retry_events, "ck_task_retry_events_actor_component_valid"
    ) == (
        "actor_component IS NOT NULL AND actor_component IN "
        "('retry_transition','retry_dispatch','expired_claim_recovery')"
    )


def test_every_new_constraint_and_index_has_a_deterministic_name() -> None:
    for table in (
        workflow_runs,
        workflow_run_inputs,
        task_runs,
        task_claim_events,
        task_attempt_results,
        task_result_events,
        task_retry_events,
        workflow_run_cancellation_requests,
        workflow_run_idempotency,
        workflow_run_execution_events,
    ):
        assert all(constraint.name for constraint in table.constraints)
        assert all(index.name for index in table.indexes)
