"""Workflow service transaction and owner-scoped query tests."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import TracebackType
from typing import cast
from uuid import UUID, uuid4

import pytest

from taskforge.workflows.domain import (
    DraftDependency,
    DraftWorkflowStep,
    WorkflowDefinitionStatus,
    WorkflowDraft,
)
from taskforge.workflows.persistence_ports import (
    ResolvedDependency,
    StoredWorkflowDraft,
    WorkflowOwnerRecordDisabled,
    WorkflowOwnerRecordNotFound,
    WorkflowPage,
    WorkflowPageCursor,
    WorkflowPersistenceUnavailable,
    WorkflowRecordConflict,
    WorkflowTimestamps,
    WorkflowTransactionContext,
)
from taskforge.workflows.service import (
    InvalidWorkflowListQuery,
    WorkflowNotFound,
    WorkflowOwnerDisabled,
    WorkflowOwnerNotFound,
    WorkflowPersistenceConflict,
    WorkflowService,
    WorkflowServiceUnavailable,
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


def registry() -> TaskTypeRegistry:
    return TaskTypeRegistry((TaskTypeDefinition("test.task", AcceptParameters()),))


def workflow_service(repository: FakeRepository) -> WorkflowService:
    return WorkflowService(repository, registry())


def draft(*, dependency_target: str = "second") -> WorkflowDraft:
    first = DraftWorkflowStep(uuid4(), "first", "test.task", {"value": 1})
    second = DraftWorkflowStep(uuid4(), "second", "test.task", {"value": 2})
    return WorkflowDraft(
        id=uuid4(),
        owner_principal_id=uuid4(),
        name="Stored workflow",
        description=None,
        status=WorkflowDefinitionStatus.DRAFT,
        steps=(first, second),
        dependencies=(DraftDependency(uuid4(), "first", dependency_target),),
    )


class FakeTransaction:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []
        self.failure_for: str | None = None
        self.failure: Exception | None = None
        now = datetime.now(UTC)
        self.timestamps = WorkflowTimestamps(now, now)
        self.draft_value: StoredWorkflowDraft | None = None
        self.version_number = 1
        self.published_at = now

    async def __aenter__(self) -> FakeTransaction:
        self.calls.append(("enter",))
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exception_type, exception, traceback
        self.calls.append(("exit",))

    async def require_enabled_owner(self, owner_principal_id: UUID) -> None:
        self._record("require_enabled_owner", owner_principal_id)

    async def insert_definition(self, workflow: WorkflowDraft) -> WorkflowTimestamps:
        self._record("insert_definition", workflow.id)
        return self.timestamps

    async def insert_steps(
        self,
        workflow_id: UUID,
        steps: tuple[DraftWorkflowStep, ...],
    ) -> None:
        self._record("insert_steps", workflow_id, steps)

    async def insert_dependencies(
        self,
        workflow_id: UUID,
        dependencies: tuple[ResolvedDependency, ...],
    ) -> None:
        self._record("insert_dependencies", workflow_id, dependencies)

    async def lock_draft_for_publication(
        self,
        workflow_id: UUID,
        owner_principal_id: UUID,
    ) -> StoredWorkflowDraft | None:
        self._record("lock_draft_for_publication", workflow_id, owner_principal_id)
        return self.draft_value

    async def next_version_number(self, workflow_id: UUID) -> int:
        self._record("next_version_number", workflow_id)
        return self.version_number

    async def insert_version(
        self,
        version_id: UUID,
        version_number: int,
        workflow: WorkflowDraft,
    ) -> datetime:
        self._record("insert_version", version_id, version_number, workflow)
        return self.published_at

    async def insert_version_steps(
        self,
        version_id: UUID,
        steps: tuple[DraftWorkflowStep, ...],
    ) -> None:
        self._record("insert_version_steps", version_id, steps)

    async def insert_version_dependencies(
        self,
        version_id: UUID,
        dependencies: tuple[DraftDependency, ...],
    ) -> None:
        self._record("insert_version_dependencies", version_id, dependencies)

    async def commit(self) -> None:
        self._record("commit")

    def _record(self, name: str, *values: object) -> None:
        self.calls.append((name, *values))
        if self.failure_for == name and self.failure is not None:
            raise self.failure


@dataclass
class FakeRepository:
    transaction_value: FakeTransaction
    stored: StoredWorkflowDraft | None = None
    page: WorkflowPage = field(default_factory=lambda: WorkflowPage((), None))
    query_failure: Exception | None = None

    def __post_init__(self) -> None:
        self.find_calls: list[tuple[UUID, UUID]] = []
        self.list_calls: list[tuple[UUID, int, WorkflowPageCursor | None]] = []

    def transaction(self) -> WorkflowTransactionContext:
        return self.transaction_value

    async def find_draft(
        self,
        workflow_id: UUID,
        owner_principal_id: UUID,
    ) -> StoredWorkflowDraft | None:
        self.find_calls.append((workflow_id, owner_principal_id))
        if self.query_failure is not None:
            raise self.query_failure
        return self.stored

    async def list_summaries(
        self,
        owner_principal_id: UUID,
        *,
        limit: int,
        cursor: WorkflowPageCursor | None,
    ) -> WorkflowPage:
        self.list_calls.append((owner_principal_id, limit, cursor))
        if self.query_failure is not None:
            raise self.query_failure
        return self.page


def test_create_persists_complete_aggregate_in_one_transaction() -> None:
    workflow = draft()
    transaction = FakeTransaction()
    service = workflow_service(FakeRepository(transaction))

    stored = asyncio.run(service.create(workflow))

    assert [call[0] for call in transaction.calls] == [
        "enter",
        "require_enabled_owner",
        "insert_definition",
        "insert_steps",
        "insert_dependencies",
        "commit",
        "exit",
    ]
    dependency_rows = transaction.calls[4][2]
    assert dependency_rows == (
        ResolvedDependency(
            workflow.dependencies[0].id,
            workflow.steps[0].id,
            workflow.steps[1].id,
        ),
    )
    assert stored.draft is workflow
    assert stored.created_at == transaction.timestamps.created_at


def test_invalid_graph_fails_before_opening_a_transaction() -> None:
    transaction = FakeTransaction()
    service = workflow_service(FakeRepository(transaction))

    with pytest.raises(WorkflowValidationError) as error:
        asyncio.run(service.create(draft(dependency_target="missing")))

    assert error.value.graph_result is not None
    assert transaction.calls == []


def test_service_revalidates_graph_even_for_directly_constructed_domain_record() -> (
    None
):
    invalid = draft()
    invalid = WorkflowDraft(
        id=invalid.id,
        owner_principal_id=invalid.owner_principal_id,
        name=invalid.name,
        description=invalid.description,
        status=invalid.status,
        steps=invalid.steps,
        dependencies=(
            DraftDependency(uuid4(), "first", "second"),
            DraftDependency(uuid4(), "second", "first"),
        ),
    )
    transaction = FakeTransaction()

    with pytest.raises(WorkflowValidationError) as error:
        asyncio.run(workflow_service(FakeRepository(transaction)).create(invalid))

    assert error.value.graph_result is not None
    assert "cycle" in error.value.graph_result.violations
    assert transaction.calls == []


@pytest.mark.parametrize(
    ("persistence_failure", "service_failure"),
    (
        (WorkflowOwnerRecordNotFound(), WorkflowOwnerNotFound),
        (WorkflowOwnerRecordDisabled(), WorkflowOwnerDisabled),
        (WorkflowRecordConflict(), WorkflowPersistenceConflict),
        (WorkflowPersistenceUnavailable(), WorkflowServiceUnavailable),
    ),
)
def test_expected_create_failures_are_normalized_without_commit(
    persistence_failure: Exception,
    service_failure: type[Exception],
) -> None:
    transaction = FakeTransaction()
    transaction.failure_for = "insert_definition"
    transaction.failure = persistence_failure

    with pytest.raises(service_failure):
        asyncio.run(workflow_service(FakeRepository(transaction)).create(draft()))

    assert "commit" not in [call[0] for call in transaction.calls]
    assert transaction.calls[-1] == ("exit",)


def test_unexpected_create_failure_propagates() -> None:
    transaction = FakeTransaction()
    transaction.failure_for = "insert_steps"
    transaction.failure = RuntimeError("programming failure")

    with pytest.raises(RuntimeError, match="programming failure"):
        asyncio.run(workflow_service(FakeRepository(transaction)).create(draft()))


def test_publish_locks_revalidates_allocates_and_copies_deterministically() -> None:
    original = draft()
    third = DraftWorkflowStep(uuid4(), "third", "test.task", {"value": 3})
    original = WorkflowDraft(
        id=original.id,
        owner_principal_id=original.owner_principal_id,
        name=original.name,
        description=original.description,
        status=original.status,
        steps=(third, original.steps[1], original.steps[0]),
        dependencies=(
            DraftDependency(uuid4(), "second", "third"),
            DraftDependency(uuid4(), "first", "third"),
            original.dependencies[0],
        ),
    )
    transaction = FakeTransaction()
    transaction.draft_value = StoredWorkflowDraft(
        original,
        transaction.timestamps.created_at,
        transaction.timestamps.updated_at,
    )
    transaction.version_number = 7

    published = asyncio.run(
        workflow_service(FakeRepository(transaction)).publish(
            original.id,
            owner_principal_id=original.owner_principal_id,
        )
    )

    assert [call[0] for call in transaction.calls] == [
        "enter",
        "require_enabled_owner",
        "lock_draft_for_publication",
        "next_version_number",
        "insert_version",
        "insert_version_steps",
        "insert_version_dependencies",
        "commit",
        "exit",
    ]
    validated = transaction.calls[4][3]
    assert isinstance(validated, WorkflowDraft)
    assert validated is not original
    assert validated.name == original.name
    inserted_steps = cast(tuple[DraftWorkflowStep, ...], transaction.calls[5][2])
    assert tuple(step.identifier for step in inserted_steps) == (
        "first",
        "second",
        "third",
    )
    inserted_dependencies = cast(tuple[DraftDependency, ...], transaction.calls[6][2])
    assert tuple(
        (
            dependency.predecessor_identifier,
            dependency.successor_identifier,
        )
        for dependency in inserted_dependencies
    ) == (
        ("first", "second"),
        ("first", "third"),
        ("second", "third"),
    )
    assert published.workflow_definition_id == original.id
    assert published.version_number == 7
    assert published.published_at == transaction.published_at


@pytest.mark.parametrize("invalid_kind", ("task_type", "cycle"))
def test_invalid_persisted_draft_rolls_back_before_allocation(
    invalid_kind: str,
) -> None:
    invalid = draft()
    if invalid_kind == "task_type":
        invalid = WorkflowDraft(
            id=invalid.id,
            owner_principal_id=invalid.owner_principal_id,
            name=invalid.name,
            description=invalid.description,
            status=invalid.status,
            steps=(
                DraftWorkflowStep(uuid4(), "first", "unknown.task", {}),
                invalid.steps[1],
            ),
            dependencies=invalid.dependencies,
        )
    else:
        invalid = WorkflowDraft(
            id=invalid.id,
            owner_principal_id=invalid.owner_principal_id,
            name=invalid.name,
            description=invalid.description,
            status=invalid.status,
            steps=invalid.steps,
            dependencies=(
                DraftDependency(uuid4(), "first", "second"),
                DraftDependency(uuid4(), "second", "first"),
            ),
        )
    transaction = FakeTransaction()
    transaction.draft_value = StoredWorkflowDraft(
        invalid,
        transaction.timestamps.created_at,
        transaction.timestamps.updated_at,
    )

    with pytest.raises(WorkflowValidationError):
        asyncio.run(
            workflow_service(FakeRepository(transaction)).publish(
                invalid.id,
                owner_principal_id=invalid.owner_principal_id,
            )
        )

    assert [call[0] for call in transaction.calls] == [
        "enter",
        "require_enabled_owner",
        "lock_draft_for_publication",
        "exit",
    ]


def test_missing_owner_scoped_publication_stops_after_definition_lock() -> None:
    transaction = FakeTransaction()

    with pytest.raises(WorkflowNotFound):
        asyncio.run(
            workflow_service(FakeRepository(transaction)).publish(
                uuid4(),
                owner_principal_id=uuid4(),
            )
        )

    assert [call[0] for call in transaction.calls] == [
        "enter",
        "require_enabled_owner",
        "lock_draft_for_publication",
        "exit",
    ]


@pytest.mark.parametrize(
    ("failure_for", "failure", "expected"),
    (
        ("insert_version", WorkflowRecordConflict(), WorkflowPersistenceConflict),
        (
            "insert_version_steps",
            WorkflowPersistenceUnavailable(),
            WorkflowServiceUnavailable,
        ),
        ("insert_version_dependencies", RuntimeError("bug"), RuntimeError),
        ("commit", WorkflowPersistenceUnavailable(), WorkflowServiceUnavailable),
    ),
)
def test_publication_failure_never_commits_partial_snapshot(
    failure_for: str,
    failure: Exception,
    expected: type[Exception],
) -> None:
    workflow = draft()
    transaction = FakeTransaction()
    transaction.draft_value = StoredWorkflowDraft(
        workflow,
        transaction.timestamps.created_at,
        transaction.timestamps.updated_at,
    )
    transaction.failure_for = failure_for
    transaction.failure = failure

    with pytest.raises(expected):
        asyncio.run(
            workflow_service(FakeRepository(transaction)).publish(
                workflow.id,
                owner_principal_id=workflow.owner_principal_id,
            )
        )

    assert transaction.calls[-1] == ("exit",)
    assert not any(
        call[0] == "commit" for call in transaction.calls
    ) or failure_for == ("commit")


def test_get_is_owner_scoped_and_non_enumerating() -> None:
    workflow, owner_id = draft(), uuid4()
    now = datetime.now(UTC)
    repository = FakeRepository(
        FakeTransaction(), StoredWorkflowDraft(workflow, now, now)
    )
    service = workflow_service(repository)

    found = asyncio.run(service.get(workflow.id, owner_principal_id=owner_id))
    repository.stored = None
    with pytest.raises(WorkflowNotFound):
        asyncio.run(service.get(workflow.id, owner_principal_id=uuid4()))

    assert found.draft is workflow
    assert repository.find_calls[0] == (workflow.id, owner_id)


@pytest.mark.parametrize("limit", (0, -1, True, 1.5, "10"))
def test_list_requires_an_explicit_positive_integer_limit(limit: object) -> None:
    repository = FakeRepository(FakeTransaction())

    with pytest.raises(InvalidWorkflowListQuery):
        asyncio.run(
            workflow_service(repository).list(
                owner_principal_id=uuid4(),
                limit=limit,  # type: ignore[arg-type]
            )
        )

    assert repository.list_calls == []


def test_list_passes_owner_and_unbounded_policy_free_limit() -> None:
    owner_id = uuid4()
    repository = FakeRepository(FakeTransaction())
    cursor = WorkflowPageCursor(datetime.now(UTC), uuid4())

    result = asyncio.run(
        workflow_service(repository).list(
            owner_principal_id=owner_id,
            limit=10_000,
            cursor=cursor,
        )
    )

    assert result == WorkflowPage((), None)
    assert repository.list_calls == [(owner_id, 10_000, cursor)]


@pytest.mark.parametrize("operation", ("get", "list"))
def test_expected_query_failure_is_normalized(operation: str) -> None:
    repository = FakeRepository(
        FakeTransaction(), query_failure=WorkflowPersistenceUnavailable()
    )
    service = workflow_service(repository)

    with pytest.raises(WorkflowServiceUnavailable):
        if operation == "get":
            asyncio.run(service.get(uuid4(), owner_principal_id=uuid4()))
        else:
            asyncio.run(service.list(owner_principal_id=uuid4(), limit=1))
