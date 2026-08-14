"""Workflow draft domain type and invariant tests."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, dataclass
from datetime import UTC, datetime
from typing import cast
from uuid import uuid4

import pytest

from taskforge.workflows.domain import (
    MAX_TASK_DEADLINE_SECONDS,
    MAX_TASK_EXECUTION_TIMEOUT_SECONDS,
    MAX_WORKFLOW_DESCRIPTION_LENGTH,
    MAX_WORKFLOW_NAME_LENGTH,
    DraftWorkflowStep,
    PublishedWorkflowVersion,
    WorkflowAvailabilityIntent,
    WorkflowAvailabilityTransitionRejected,
    WorkflowDefinitionStatus,
    WorkflowDraft,
    WorkflowVersionDependency,
    WorkflowVersionSnapshot,
    WorkflowVersionStep,
    _validate_retry_policy,
    availability_requires_published_version,
    change_workflow_availability,
    create_draft_dependency,
    create_draft_step,
    create_workflow_draft,
    replace_workflow_draft,
    resolve_deadline_seconds,
    resolve_execution_timeout_seconds,
    validate_execution_policy,
)
from taskforge.workflows.task_types import (
    JSONMapping,
    JSONValue,
    TaskTypeDefinition,
    TaskTypeRegistry,
    WorkflowValidationError,
    WorkflowValidationIssue,
)


@pytest.mark.parametrize("value", (True, False, 0, -1, 1.5, "1", None, 31_536_001))
def test_deadline_seconds_is_positive_bounded_non_boolean(value: object) -> None:
    _, issues = validate_execution_policy({"deadline_seconds": value})
    assert [issue.code for issue in issues] == ["invalid_deadline_seconds"]


def test_deadline_resolution_is_explicit_step_over_workflow() -> None:
    assert MAX_TASK_DEADLINE_SECONDS == 31_536_000
    assert resolve_deadline_seconds({"deadline_seconds": 100}, None) == 100
    assert resolve_deadline_seconds(None, {"deadline_seconds": 50}) == 50
    assert (
        resolve_deadline_seconds(
            {"deadline_seconds": 100, "future": "workflow"},
            {"deadline_seconds": 50, "other": "step"},
        )
        == 50
    )
    assert resolve_deadline_seconds({"future": True}, {"other": True}) is None


@pytest.mark.parametrize("value", (True, False, 0, -1, 1.5, "1", None, 31_536_001))
def test_execution_timeout_is_positive_bounded_non_boolean(value: object) -> None:
    _, issues = validate_execution_policy({"execution_timeout_seconds": value})
    assert [issue.code for issue in issues] == ["invalid_execution_timeout_seconds"]


def test_execution_timeout_resolution_is_independent_step_over_workflow() -> None:
    assert MAX_TASK_EXECUTION_TIMEOUT_SECONDS == 31_536_000
    assert (
        resolve_execution_timeout_seconds({"execution_timeout_seconds": 100}, None)
        == 100
    )
    assert (
        resolve_execution_timeout_seconds(
            {"execution_timeout_seconds": 100, "deadline_seconds": 500},
            {"execution_timeout_seconds": 50, "deadline_seconds": 200},
        )
        == 50
    )
    assert (
        resolve_deadline_seconds(
            {"execution_timeout_seconds": 100, "deadline_seconds": 500},
            {"execution_timeout_seconds": 50, "deadline_seconds": 200},
        )
        == 200
    )
    assert resolve_execution_timeout_seconds({"future": True}, None) is None


def test_policy_reports_deadline_and_timeout_issues_independently() -> None:
    _, issues = validate_execution_policy(
        {"deadline_seconds": False, "execution_timeout_seconds": True}
    )
    assert [issue.code for issue in issues] == [
        "invalid_deadline_seconds",
        "invalid_execution_timeout_seconds",
    ]


def retry_policy(**overrides: object) -> dict[str, object]:
    policy: dict[str, object] = {
        "maximum_attempts": 3,
        "initial_delay_seconds": 10,
        "multiplier": 2,
        "maximum_delay_seconds": 300,
    }
    policy.update(overrides)
    return policy


def test_retry_policy_accepts_the_complete_persisted_contract() -> None:
    policy = {"retry_policy": retry_policy(maximum_attempts=1, multiplier=1.5)}
    validated, issues = validate_execution_policy(policy)
    assert issues == ()
    assert validated == policy


@pytest.mark.parametrize("multiplier", (float("inf"), float("-inf"), float("nan")))
def test_retry_policy_rejects_non_finite_multipliers(multiplier: float) -> None:
    issues = _validate_retry_policy(
        cast(JSONValue, retry_policy(multiplier=multiplier)), ("retry_policy",)
    )
    assert [issue.code for issue in issues] == ["invalid_retry_multiplier"]


@pytest.mark.parametrize(
    ("overrides", "code"),
    (
        ({"maximum_attempts": 0}, "invalid_retry_maximum_attempts"),
        ({"maximum_attempts": True}, "invalid_retry_maximum_attempts"),
        ({"initial_delay_seconds": -1}, "invalid_retry_initial_delay_seconds"),
        ({"initial_delay_seconds": 1.5}, "invalid_retry_initial_delay_seconds"),
        ({"multiplier": 0.5}, "invalid_retry_multiplier"),
        ({"multiplier": True}, "invalid_retry_multiplier"),
        ({"maximum_delay_seconds": -1}, "invalid_retry_maximum_delay_seconds"),
        ({"maximum_delay_seconds": 1.5}, "invalid_retry_maximum_delay_seconds"),
        (
            {"initial_delay_seconds": 11, "maximum_delay_seconds": 10},
            "invalid_retry_delay_order",
        ),
    ),
)
def test_retry_policy_rejects_invalid_values(
    overrides: dict[str, object], code: str
) -> None:
    _, issues = validate_execution_policy({"retry_policy": retry_policy(**overrides)})
    assert code in [issue.code for issue in issues]


def test_retry_policy_requires_exactly_the_supported_fields() -> None:
    missing = retry_policy()
    del missing["maximum_attempts"]
    _, missing_issues = validate_execution_policy({"retry_policy": missing})
    _, extra_issues = validate_execution_policy(
        {"retry_policy": retry_policy(jitter=True)}
    )
    assert "invalid_retry_policy_fields" in [issue.code for issue in missing_issues]
    assert "invalid_retry_policy_fields" in [issue.code for issue in extra_issues]


def test_retry_policy_must_be_an_object() -> None:
    _, issues = validate_execution_policy({"retry_policy": []})
    assert [issue.code for issue in issues] == ["invalid_retry_policy"]


@dataclass(frozen=True)
class AcceptParameters:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        del parameters
        return ()


def task_types() -> TaskTypeRegistry:
    return TaskTypeRegistry(
        (TaskTypeDefinition("test.accepted", "test-workers", AcceptParameters()),)
    )


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
    ("current", "intent", "published", "expected", "changed"),
    (
        ("draft", "enable", True, "enabled", True),
        ("enabled", "enable", True, "enabled", False),
        ("enabled", "disable", True, "disabled", True),
        ("disabled", "disable", True, "disabled", False),
        ("disabled", "enable", True, "enabled", True),
    ),
)
def test_domain_owns_availability_transitions(
    current: str,
    intent: str,
    published: bool,
    expected: str,
    changed: bool,
) -> None:
    workflow_id = uuid4()
    result = change_workflow_availability(
        workflow_id=workflow_id,
        current_status=WorkflowDefinitionStatus(current),
        intent=WorkflowAvailabilityIntent(intent),
        has_published_version=published,
    )

    assert result.workflow_id == workflow_id
    assert result.status is WorkflowDefinitionStatus(expected)
    assert result.changed is changed


@pytest.mark.parametrize(
    ("intent", "published"),
    (
        (WorkflowAvailabilityIntent.ENABLE, False),
        (WorkflowAvailabilityIntent.DISABLE, True),
    ),
)
def test_draft_availability_rejects_unpublished_enable_and_disable(
    intent: WorkflowAvailabilityIntent,
    published: bool,
) -> None:
    with pytest.raises(WorkflowAvailabilityTransitionRejected):
        change_workflow_availability(
            workflow_id=uuid4(),
            current_status=WorkflowDefinitionStatus.DRAFT,
            intent=intent,
            has_published_version=published,
        )


def test_availability_domain_rejects_an_untyped_intent() -> None:
    with pytest.raises(WorkflowAvailabilityTransitionRejected):
        change_workflow_availability(
            workflow_id=uuid4(),
            current_status=WorkflowDefinitionStatus.ENABLED,
            intent="enable",  # type: ignore[arg-type]
            has_published_version=True,
        )


@pytest.mark.parametrize(
    ("status", "intent", "expected"),
    (
        (WorkflowDefinitionStatus.DRAFT, WorkflowAvailabilityIntent.ENABLE, True),
        (WorkflowDefinitionStatus.DRAFT, WorkflowAvailabilityIntent.DISABLE, False),
        (WorkflowDefinitionStatus.ENABLED, WorkflowAvailabilityIntent.ENABLE, False),
        (WorkflowDefinitionStatus.ENABLED, WorkflowAvailabilityIntent.DISABLE, False),
        (WorkflowDefinitionStatus.DISABLED, WorkflowAvailabilityIntent.ENABLE, False),
        (WorkflowDefinitionStatus.DISABLED, WorkflowAvailabilityIntent.DISABLE, False),
    ),
)
def test_domain_identifies_only_initial_enablement_as_requiring_a_version(
    status: WorkflowDefinitionStatus,
    intent: WorkflowAvailabilityIntent,
    expected: bool,
) -> None:
    assert availability_requires_published_version(status, intent) is expected


def test_publication_requirement_rejects_an_untyped_intent() -> None:
    with pytest.raises(WorkflowAvailabilityTransitionRejected):
        availability_requires_published_version(
            WorkflowDefinitionStatus.DRAFT,
            "enable",  # type: ignore[arg-type]
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


def test_public_workflow_factory_still_returns_only_a_workflow_draft() -> None:
    created = create_workflow_draft(
        workflow_id=uuid4(),
        owner_principal_id=uuid4(),
        name="Public contract",
        description=None,
        status=WorkflowDefinitionStatus.DRAFT,
        steps=(step(),),
    )

    assert type(created) is WorkflowDraft


def test_published_version_metadata_is_frozen_positive_and_utc_normalized() -> None:
    published = PublishedWorkflowVersion(
        id=uuid4(),
        workflow_definition_id=uuid4(),
        version_number=1,
        published_at=datetime.fromisoformat("2026-08-05T12:00:00-07:00"),
    )

    assert published.published_at == datetime(2026, 8, 5, 19, tzinfo=UTC)
    with pytest.raises(FrozenInstanceError):
        published.version_number = 2  # type: ignore[misc]
    with pytest.raises(ValueError, match="positive"):
        PublishedWorkflowVersion(uuid4(), uuid4(), 0, datetime.now(UTC))
    with pytest.raises(ValueError, match="timezone-aware"):
        PublishedWorkflowVersion(uuid4(), uuid4(), 1, datetime(2026, 8, 5))


def test_complete_version_snapshot_is_frozen_ordered_and_redacted() -> None:
    marker = "sensitive-marker"
    version = WorkflowVersionSnapshot(
        id=uuid4(),
        workflow_definition_id=uuid4(),
        version_number=2,
        name="Historical",
        description=None,
        execution_policy={"marker": marker},
        published_at=datetime.fromisoformat("2026-08-05T12:00:00-07:00"),
        steps=(
            WorkflowVersionStep("first", "test.accepted", {"marker": marker}, None),
        ),
        dependencies=(WorkflowVersionDependency("first", "second"),),
    )

    assert version.published_at == datetime(2026, 8, 5, 19, tzinfo=UTC)
    assert version.steps[0].parameters == {"marker": marker}
    assert marker not in repr(version)
    assert marker not in repr(version.steps[0])
    with pytest.raises(FrozenInstanceError):
        version.version_number = 3  # type: ignore[misc]
    with pytest.raises(ValueError, match="positive"):
        WorkflowVersionSnapshot(
            uuid4(), uuid4(), 0, "Invalid", None, None, datetime.now(UTC), (), ()
        )


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
