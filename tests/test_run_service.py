"""Workflow run target resolution service tests."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from uuid import UUID, uuid4

import pytest

from taskforge.runs.domain import (
    ExplicitWorkflowVersion,
    LatestWorkflowVersion,
    WorkflowRunTargetUnavailable,
    WorkflowVersionSelection,
)
from taskforge.runs.persistence_ports import (
    WorkflowRunPersistenceUnavailable,
    WorkflowVersionResolutionRecord,
)
from taskforge.runs.service import (
    WorkflowRunService,
    WorkflowRunServiceUnavailable,
    WorkflowRunTargetNotFound,
    WorkflowVersionUnavailable,
)
from taskforge.workflows.domain import WorkflowDefinitionStatus


@dataclass
class FakeRepository:
    result: WorkflowVersionResolutionRecord | None = None
    failure: BaseException | None = None

    def __post_init__(self) -> None:
        self.calls: list[tuple[UUID, UUID, WorkflowVersionSelection]] = []

    async def resolve_workflow_version(
        self,
        workflow_id: UUID,
        owner_principal_id: UUID,
        selection: WorkflowVersionSelection,
    ) -> WorkflowVersionResolutionRecord | None:
        self.calls.append((workflow_id, owner_principal_id, selection))
        if self.failure is not None:
            raise self.failure
        return self.result


def record(
    status: WorkflowDefinitionStatus = WorkflowDefinitionStatus.ENABLED,
    *,
    with_version: bool = True,
) -> WorkflowVersionResolutionRecord:
    return WorkflowVersionResolutionRecord(
        workflow_definition_id=uuid4(),
        status=status,
        workflow_version_id=uuid4() if with_version else None,
        version_number=3 if with_version else None,
    )


@pytest.mark.parametrize(
    "selection", (ExplicitWorkflowVersion(2), LatestWorkflowVersion())
)
def test_service_resolves_explicit_and_latest_owner_scoped_targets(
    selection: WorkflowVersionSelection,
) -> None:
    repository = FakeRepository(record())
    service = WorkflowRunService(repository)
    workflow_id, owner_id = uuid4(), uuid4()

    resolved = asyncio.run(
        service.resolve_version(
            workflow_id, owner_principal_id=owner_id, selection=selection
        )
    )

    assert repository.calls == [(workflow_id, owner_id, selection)]
    assert resolved.workflow_definition_id == repository.result.workflow_definition_id  # type: ignore[union-attr]
    assert resolved.workflow_version_id == repository.result.workflow_version_id  # type: ignore[union-attr]
    assert resolved.version_number == 3


def test_absent_or_cross_owner_target_is_concealed_as_not_found() -> None:
    service = WorkflowRunService(FakeRepository())

    with pytest.raises(WorkflowRunTargetNotFound):
        asyncio.run(
            service.resolve_version(
                uuid4(),
                owner_principal_id=uuid4(),
                selection=LatestWorkflowVersion(),
            )
        )


@pytest.mark.parametrize(
    "status",
    (
        WorkflowDefinitionStatus.DRAFT,
        WorkflowDefinitionStatus.DISABLED,
        WorkflowDefinitionStatus.ARCHIVED,
    ),
)
def test_unavailable_status_takes_precedence_over_absent_version(
    status: WorkflowDefinitionStatus,
) -> None:
    service = WorkflowRunService(FakeRepository(record(status, with_version=False)))

    with pytest.raises(WorkflowRunTargetUnavailable) as caught:
        asyncio.run(
            service.resolve_version(
                uuid4(),
                owner_principal_id=uuid4(),
                selection=LatestWorkflowVersion(),
            )
        )

    assert caught.value.status is status


def test_enabled_target_without_selected_version_is_unavailable() -> None:
    service = WorkflowRunService(FakeRepository(record(with_version=False)))

    with pytest.raises(WorkflowVersionUnavailable):
        asyncio.run(
            service.resolve_version(
                uuid4(),
                owner_principal_id=uuid4(),
                selection=ExplicitWorkflowVersion(9),
            )
        )


def test_persistence_unavailability_is_normalized() -> None:
    service = WorkflowRunService(
        FakeRepository(failure=WorkflowRunPersistenceUnavailable())
    )

    with pytest.raises(WorkflowRunServiceUnavailable):
        asyncio.run(
            service.resolve_version(
                uuid4(),
                owner_principal_id=uuid4(),
                selection=LatestWorkflowVersion(),
            )
        )


@pytest.mark.parametrize("failure", (RuntimeError("bug"), asyncio.CancelledError()))
def test_unexpected_and_cancellation_failures_are_not_normalized(
    failure: BaseException,
) -> None:
    service = WorkflowRunService(FakeRepository(failure=failure))

    with pytest.raises(type(failure)):
        asyncio.run(
            service.resolve_version(
                uuid4(),
                owner_principal_id=uuid4(),
                selection=LatestWorkflowVersion(),
            )
        )
