"""Structural guarantees for workflow run persistence."""

from __future__ import annotations

from sqlalchemy import (
    CheckConstraint,
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
    WORKFLOW_RUN_STATUSES,
    task_attempt_results,
    task_claim_events,
    task_result_events,
    task_runs,
    workflow_run_idempotency,
    workflow_run_inputs,
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
        "task_dispatch_outbox",
        "workflow_run_idempotency",
    } <= set(metadata.tables)
    assert not any(
        fragment in table_name
        for table_name in metadata.tables
        for fragment in ("replay", "audit")
    )


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
    assert unique_column_sets(task_runs) == {("workflow_run_id", "step_identifier")}
    assert ("workflow_run_id", "id") not in unique_column_sets(task_runs)
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


def test_every_new_constraint_and_index_has_a_deterministic_name() -> None:
    for table in (
        workflow_runs,
        workflow_run_inputs,
        task_runs,
        task_claim_events,
        task_attempt_results,
        task_result_events,
        workflow_run_idempotency,
    ):
        assert all(constraint.name for constraint in table.constraints)
        assert all(index.name for index in table.indexes)
