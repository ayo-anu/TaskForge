"""Structural guarantees for workflow definition persistence."""

from __future__ import annotations

from sqlalchemy import CheckConstraint, Enum, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.schema import DefaultClause

from taskforge.persistence.schema import metadata
from taskforge.workflows.schema import (
    WORKFLOW_DEFINITION_STATUSES,
    workflow_definitions,
    workflow_draft_dependencies,
    workflow_draft_steps,
)

WORKFLOW_TABLES = {
    "workflow_definitions",
    "workflow_draft_dependencies",
    "workflow_draft_steps",
}


def test_workflow_tables_are_registered_without_later_milestone_tables() -> None:
    assert WORKFLOW_TABLES <= set(metadata.tables)
    assert not any(
        name.startswith(("workflow_versions", "workflow_runs", "task_runs"))
        for name in metadata.tables
    )


def test_workflow_definition_has_stable_identity_and_api_principal_owner() -> None:
    assert isinstance(workflow_definitions.c.id.type, UUID)
    assert list(workflow_definitions.primary_key.columns) == [workflow_definitions.c.id]
    assert {
        foreign_key.target_fullname for foreign_key in workflow_definitions.foreign_keys
    } == {"api_principals.id"}
    assert workflow_definitions.c.owner_principal_id.nullable is False


def test_workflow_status_uses_the_approved_native_enum() -> None:
    status_type = workflow_definitions.c.status.type

    assert isinstance(status_type, Enum)
    assert status_type.native_enum is True
    assert status_type.name == "workflow_definition_status"
    assert tuple(status_type.enums) == WORKFLOW_DEFINITION_STATUSES
    assert workflow_definitions.c.status.nullable is False
    status_default = workflow_definitions.c.status.server_default
    assert isinstance(status_default, DefaultClause)
    assert str(status_default.arg) == "draft"


def test_workflow_and_steps_have_server_generated_timestamps_without_order_checks() -> (
    None
):
    for table in (workflow_definitions, workflow_draft_steps):
        assert table.c.created_at.nullable is False
        assert table.c.created_at.server_default is not None
        assert table.c.updated_at.nullable is False
        assert table.c.updated_at.server_default is not None
        check_text = " ".join(
            str(constraint.sqltext)
            for constraint in table.constraints
            if isinstance(constraint, CheckConstraint)
        )
        assert "updated_at >= created_at" not in check_text


def test_step_identifiers_are_unique_only_within_their_workflow() -> None:
    unique_columns = {
        tuple(constraint.columns.keys())
        for constraint in workflow_draft_steps.constraints
        if isinstance(constraint, UniqueConstraint)
    }

    assert ("workflow_definition_id", "step_identifier") in unique_columns
    assert ("workflow_definition_id", "id") in unique_columns
    assert ("step_identifier",) not in unique_columns


def test_parameters_are_required_json_objects_without_a_database_default() -> None:
    parameters = workflow_draft_steps.c.parameters
    checks = {
        str(constraint.sqltext)
        for constraint in workflow_draft_steps.constraints
        if isinstance(constraint, CheckConstraint)
    }

    assert isinstance(parameters.type, JSONB)
    assert parameters.nullable is False
    assert parameters.server_default is None
    assert any("jsonb_typeof(parameters) = 'object'" in check for check in checks)


def test_dependency_edges_are_same_workflow_unique_and_not_self_referential() -> None:
    foreign_key_targets = [
        tuple(element.target_fullname for element in constraint.elements)
        for constraint in workflow_draft_dependencies.foreign_key_constraints
    ]
    unique_columns = {
        tuple(constraint.columns.keys())
        for constraint in workflow_draft_dependencies.constraints
        if isinstance(constraint, UniqueConstraint)
    }
    checks = {
        str(constraint.sqltext)
        for constraint in workflow_draft_dependencies.constraints
        if isinstance(constraint, CheckConstraint)
    }

    step_target = (
        "workflow_draft_steps.workflow_definition_id",
        "workflow_draft_steps.id",
    )
    assert foreign_key_targets.count(step_target) == 2
    assert ("workflow_definitions.id",) in foreign_key_targets
    assert (
        "workflow_definition_id",
        "predecessor_step_id",
        "successor_step_id",
    ) in unique_columns
    assert any("predecessor_step_id <> successor_step_id" in check for check in checks)


def test_dependency_indexes_support_both_graph_directions() -> None:
    index_columns = {
        tuple(column.name for column in index.columns)
        for index in workflow_draft_dependencies.indexes
        if isinstance(index, Index)
    }

    assert ("workflow_definition_id", "predecessor_step_id") in index_columns
    assert ("workflow_definition_id", "successor_step_id") in index_columns
