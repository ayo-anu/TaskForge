"""Workflow draft domain type and invariant tests."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, dataclass
from uuid import uuid4

import pytest

from taskforge.workflows.domain import (
    MAX_WORKFLOW_DESCRIPTION_LENGTH,
    MAX_WORKFLOW_NAME_LENGTH,
    DraftWorkflowStep,
    WorkflowAvailabilityIntent,
    WorkflowDefinitionStatus,
    create_draft_dependency,
    create_draft_step,
    create_workflow_draft,
    replace_workflow_draft,
)
from taskforge.workflows.task_types import (
    JSONMapping,
    TaskTypeDefinition,
    TaskTypeRegistry,
    WorkflowValidationError,
    WorkflowValidationIssue,
)


@dataclass(frozen=True)
class AcceptParameters:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        del parameters
        return ()


def task_types() -> TaskTypeRegistry:
    return TaskTypeRegistry((TaskTypeDefinition("test.accepted", AcceptParameters()),))


def step(identifier: str = "first") -> DraftWorkflowStep:
    return create_draft_step(
        step_id=uuid4(),
        identifier=identifier,
        task_type="test.accepted",
        parameters={"value": 1},
        task_types=task_types(),
    )


def test_domain_status_and_intent_types_match_persistence_without_transition_policy() -> (
    None
):
    assert tuple(status.value for status in WorkflowDefinitionStatus) == (
        "draft",
        "enabled",
        "disabled",
        "archived",
    )
    assert tuple(intent.value for intent in WorkflowAvailabilityIntent) == (
        "enable",
        "disable",
    )


@pytest.mark.parametrize(
    "name",
    ("", " padded", "padded ", "\x00name", "x" * (MAX_WORKFLOW_NAME_LENGTH + 1)),
)
def test_invalid_workflow_names_are_rejected(name: str) -> None:
    with pytest.raises(WorkflowValidationError) as error:
        create_workflow_draft(
            workflow_id=uuid4(),
            owner_principal_id=uuid4(),
            name=name,
            description=None,
            status=WorkflowDefinitionStatus.DRAFT,
            steps=(),
        )

    assert [issue.code for issue in error.value.issues] == ["invalid_workflow_name"]


def test_description_is_optional_but_bounded() -> None:
    valid = create_workflow_draft(
        workflow_id=uuid4(),
        owner_principal_id=uuid4(),
        name="Valid workflow",
        description=None,
        status=WorkflowDefinitionStatus.DRAFT,
        steps=(step(),),
    )
    with pytest.raises(WorkflowValidationError) as error:
        create_workflow_draft(
            workflow_id=uuid4(),
            owner_principal_id=uuid4(),
            name="Valid workflow",
            description="x" * (MAX_WORKFLOW_DESCRIPTION_LENGTH + 1),
            status=WorkflowDefinitionStatus.DRAFT,
            steps=(),
        )

    assert valid.description is None
    assert [issue.code for issue in error.value.issues] == ["description_too_large"]


@pytest.mark.parametrize(
    "identifier",
    ("", "Uppercase", "starts.with-dot", "has space", "1starts-with-number"),
)
def test_invalid_step_identifiers_are_rejected(identifier: str) -> None:
    with pytest.raises(WorkflowValidationError) as error:
        create_draft_step(
            step_id=uuid4(),
            identifier=identifier,
            task_type="test.accepted",
            parameters={},
            task_types=task_types(),
        )

    assert "invalid_step_identifier" in {issue.code for issue in error.value.issues}


def test_registered_task_type_and_parameters_are_required() -> None:
    with pytest.raises(WorkflowValidationError) as error:
        create_draft_step(
            step_id=uuid4(),
            identifier="first",
            task_type="test.unknown",
            parameters={},
            task_types=task_types(),
        )

    assert [issue.code for issue in error.value.issues] == ["unsupported_task_type"]


def test_parameters_remain_ordinary_json_structures() -> None:
    parameters = {"items": [{"value": 1}]}
    created = create_draft_step(
        step_id=uuid4(),
        identifier="first",
        task_type="test.accepted",
        parameters=parameters,
        task_types=task_types(),
    )
    assert isinstance(created.parameters, dict)
    assert isinstance(created.parameters["items"], list)
    assert created.parameters == {"items": [{"value": 1}]}


def test_duplicate_step_identifiers_and_ids_are_reported_deterministically() -> None:
    first = step("duplicate")
    duplicate = create_draft_step(
        step_id=first.id,
        identifier="duplicate",
        task_type="test.accepted",
        parameters={},
        task_types=task_types(),
    )

    with pytest.raises(WorkflowValidationError) as error:
        create_workflow_draft(
            workflow_id=uuid4(),
            owner_principal_id=uuid4(),
            name="Duplicates",
            description=None,
            status=WorkflowDefinitionStatus.DRAFT,
            steps=(first, duplicate),
        )

    assert [(issue.code, issue.path) for issue in error.value.issues] == [
        ("duplicate_step_id", ("steps", 1, "id")),
        ("duplicate_step_identifier", ("steps", 1, "identifier")),
    ]


def test_draft_records_are_frozen_but_parameters_are_not_container_wrapped() -> None:
    created = step()

    with pytest.raises(FrozenInstanceError):
        created.identifier = "changed"  # type: ignore[misc]
    assert isinstance(created.parameters, dict)


def test_parameter_values_are_redacted_from_representations_and_errors() -> None:
    sensitive_marker = "synthetic-sensitive-marker"
    created = create_draft_step(
        step_id=uuid4(),
        identifier="first",
        task_type="test.accepted",
        parameters={"value": sensitive_marker},
        task_types=task_types(),
    )

    assert sensitive_marker not in repr(created)
    with pytest.raises(WorkflowValidationError) as error:
        create_draft_step(
            step_id=uuid4(),
            identifier="first",
            task_type="test.unknown",
            parameters={"value": sensitive_marker},
            task_types=task_types(),
        )
    assert sensitive_marker not in str(error.value)
    assert sensitive_marker not in repr(error.value.issues)


def test_dependencies_are_typed_without_performing_later_graph_validation() -> None:
    dependency = create_draft_dependency(
        dependency_id=uuid4(),
        predecessor_identifier="same",
        successor_identifier="same",
    )

    assert dependency.predecessor_identifier == dependency.successor_identifier


def test_workflow_factory_rejects_invalid_graph_through_existing_error_family() -> None:
    first = step("first")
    dependency = create_draft_dependency(
        dependency_id=uuid4(),
        predecessor_identifier="first",
        successor_identifier="missing",
    )

    with pytest.raises(WorkflowValidationError) as error:
        create_workflow_draft(
            workflow_id=uuid4(),
            owner_principal_id=uuid4(),
            name="Invalid graph",
            description=None,
            status=WorkflowDefinitionStatus.DRAFT,
            steps=(first,),
            dependencies=(dependency,),
        )

    assert error.value.issues == ()
    assert error.value.graph_result is not None
    assert error.value.graph_result.violations == ("missing_dependency_reference",)


def test_draft_replacement_preserves_identity_owner_and_status_and_validates_graph() -> (
    None
):
    original = create_workflow_draft(
        workflow_id=uuid4(),
        owner_principal_id=uuid4(),
        name="Original",
        description=None,
        status=WorkflowDefinitionStatus.DRAFT,
        steps=(step("first"),),
    )
    replacement_steps = (step("replacement"),)

    replaced = replace_workflow_draft(
        original,
        name="Replacement",
        description="Changed",
        steps=replacement_steps,
    )

    assert (replaced.id, replaced.owner_principal_id, replaced.status) == (
        original.id,
        original.owner_principal_id,
        original.status,
    )
    assert replaced.name == "Replacement"
    with pytest.raises(WorkflowValidationError) as error:
        replace_workflow_draft(
            original,
            name="Invalid replacement",
            description=None,
            steps=(),
        )
    assert error.value.graph_result is not None
