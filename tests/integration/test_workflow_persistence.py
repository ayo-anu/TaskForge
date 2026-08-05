"""Opt-in workflow transaction verification against isolated PostgreSQL."""

from __future__ import annotations

import asyncio
import os
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import func, insert, select, update
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.identity.schema import api_principals
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.workflows import SQLAlchemyWorkflowRepository
from taskforge.workflows.domain import (
    DraftDependency,
    DraftWorkflowStep,
    WorkflowAvailabilityIntent,
    WorkflowAvailabilityTransitionRejected,
    WorkflowDefinitionStatus,
    WorkflowDraft,
)
from taskforge.workflows.schema import (
    workflow_definitions,
    workflow_draft_dependencies,
    workflow_draft_steps,
    workflow_version_dependencies,
    workflow_version_steps,
    workflow_versions,
)
from taskforge.workflows.service import (
    WorkflowNotFound,
    WorkflowOwnerDisabled,
    WorkflowOwnerNotFound,
    WorkflowPersistenceConflict,
    WorkflowService,
)
from taskforge.workflows.task_types import (
    JSONMapping,
    TaskTypeDefinition,
    TaskTypeRegistry,
    WorkflowValidationError,
    WorkflowValidationIssue,
)
from tests.integration.postgresql import migration_database_url, temporary_database
from tests.integration.test_authentication_persistence import settings_for

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION=1 explicitly",
    ),
]


class AcceptParameters:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        del parameters
        return ()


def workflow(owner_id: UUID, *, workflow_id: UUID | None = None) -> WorkflowDraft:
    first = DraftWorkflowStep(uuid4(), "first", "test.task", {"value": 1})
    second = DraftWorkflowStep(
        uuid4(), "second", "test.task", {"nested": [True, None, "value"]}
    )
    return WorkflowDraft(
        id=workflow_id or uuid4(),
        owner_principal_id=owner_id,
        name="Persistent workflow",
        description="A transactional draft",
        status=WorkflowDefinitionStatus.DRAFT,
        steps=(first, second),
        dependencies=(DraftDependency(uuid4(), "first", "second"),),
    )


async def count_workflow_rows(
    sessions: async_sessionmaker[AsyncSession], workflow_id: UUID
) -> tuple[int, ...]:
    counts: list[int] = []
    async with sessions() as session:
        for table in (
            workflow_definitions,
            workflow_draft_steps,
            workflow_draft_dependencies,
        ):
            identifier = (
                table.c.workflow_definition_id
                if "workflow_definition_id" in table.c
                else table.c.id
            )
            count = await session.scalar(
                select(func.count()).select_from(table).where(identifier == workflow_id)
            )
            counts.append(int(count or 0))
    return tuple(counts)


async def count_version_rows(
    sessions: async_sessionmaker[AsyncSession], workflow_id: UUID
) -> tuple[int, int, int]:
    async with sessions() as session:
        version_ids = select(workflow_versions.c.id).where(
            workflow_versions.c.workflow_definition_id == workflow_id
        )
        version_count = await session.scalar(
            select(func.count())
            .select_from(workflow_versions)
            .where(workflow_versions.c.workflow_definition_id == workflow_id)
        )
        step_count = await session.scalar(
            select(func.count())
            .select_from(workflow_version_steps)
            .where(workflow_version_steps.c.workflow_version_id.in_(version_ids))
        )
        dependency_count = await session.scalar(
            select(func.count())
            .select_from(workflow_version_dependencies)
            .where(workflow_version_dependencies.c.workflow_version_id.in_(version_ids))
        )
    return (
        int(version_count or 0),
        int(step_count or 0),
        int(dependency_count or 0),
    )


async def verify_workflow_persistence(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    repository = SQLAlchemyWorkflowRepository(sessions)
    service = WorkflowService(
        repository,
        TaskTypeRegistry((TaskTypeDefinition("test.task", AcceptParameters()),)),
    )
    owner_id, other_owner_id, disabled_owner_id = uuid4(), uuid4(), uuid4()
    try:
        async with sessions.begin() as session:
            await session.execute(
                insert(api_principals),
                [
                    {
                        "id": owner_id,
                        "name": f"owner-{uuid4().hex}",
                        "disabled_at": None,
                    },
                    {
                        "id": other_owner_id,
                        "name": f"owner-{uuid4().hex}",
                        "disabled_at": None,
                    },
                    {
                        "id": disabled_owner_id,
                        "name": f"owner-{uuid4().hex}",
                        "disabled_at": None,
                    },
                ],
            )
            await session.execute(
                update(api_principals)
                .where(api_principals.c.id == disabled_owner_id)
                .values(disabled_at=func.current_timestamp())
            )

        created_input = workflow(owner_id)
        created = await service.create(created_input)
        found = await service.get(
            created_input.id,
            owner_principal_id=owner_id,
        )
        assert found == created
        assert found.draft.steps[0].identifier == "first"
        assert found.draft.steps[0].parameters == {"value": 1}
        assert found.draft.dependencies[0].predecessor_identifier == "first"
        assert found.created_at.tzinfo is not None
        assert found.updated_at.tzinfo is not None

        first_publication = await service.publish(
            created_input.id,
            owner_principal_id=owner_id,
        )
        assert first_publication.version_number == 1
        assert first_publication.published_at.tzinfo is not None
        async with sessions() as session:
            version_row = (
                await session.execute(
                    select(workflow_versions).where(
                        workflow_versions.c.id == first_publication.id
                    )
                )
            ).one()
            version_steps = (
                await session.execute(
                    select(workflow_version_steps)
                    .where(
                        workflow_version_steps.c.workflow_version_id
                        == first_publication.id
                    )
                    .order_by(workflow_version_steps.c.step_identifier)
                )
            ).all()
            version_dependencies = (
                await session.execute(
                    select(workflow_version_dependencies).where(
                        workflow_version_dependencies.c.workflow_version_id
                        == first_publication.id
                    )
                )
            ).all()
        assert version_row.name == created_input.name
        assert version_row.description == created_input.description
        assert version_row.execution_policy is None
        assert [row.step_identifier for row in version_steps] == ["first", "second"]
        assert version_steps[0].task_type == "test.task"
        assert version_steps[0].parameters == {"value": 1}
        assert all(row.execution_policy is None for row in version_steps)
        assert [
            (
                row.predecessor_step_identifier,
                row.successor_step_identifier,
            )
            for row in version_dependencies
        ] == [("first", "second")]

        enabled = await service.set_availability(
            created_input.id,
            owner_principal_id=owner_id,
            intent=WorkflowAvailabilityIntent.ENABLE,
        )
        assert enabled.status is WorkflowDefinitionStatus.ENABLED
        assert enabled.changed is True
        async with sessions() as session:
            enabled_updated_at = await session.scalar(
                select(workflow_definitions.c.updated_at).where(
                    workflow_definitions.c.id == created_input.id
                )
            )
            snapshot_before_availability = (
                await session.execute(
                    select(workflow_versions).where(
                        workflow_versions.c.id == first_publication.id
                    )
                )
            ).one()
        unchanged = await service.set_availability(
            created_input.id,
            owner_principal_id=owner_id,
            intent=WorkflowAvailabilityIntent.ENABLE,
        )
        assert unchanged.changed is False
        async with sessions() as session:
            assert (
                await session.scalar(
                    select(workflow_definitions.c.updated_at).where(
                        workflow_definitions.c.id == created_input.id
                    )
                )
                == enabled_updated_at
            )
        disabled = await service.set_availability(
            created_input.id,
            owner_principal_id=owner_id,
            intent=WorkflowAvailabilityIntent.DISABLE,
        )
        assert disabled.status is WorkflowDefinitionStatus.DISABLED
        assert disabled.changed is True
        await service.set_availability(
            created_input.id,
            owner_principal_id=owner_id,
            intent=WorkflowAvailabilityIntent.ENABLE,
        )
        async with sessions() as session:
            snapshot_after_availability = (
                await session.execute(
                    select(workflow_versions).where(
                        workflow_versions.c.id == first_publication.id
                    )
                )
            ).one()
        assert snapshot_after_availability == snapshot_before_availability

        unpublished = workflow(owner_id)
        await service.create(unpublished)
        with pytest.raises(WorkflowAvailabilityTransitionRejected):
            await service.set_availability(
                unpublished.id,
                owner_principal_id=owner_id,
                intent=WorkflowAvailabilityIntent.ENABLE,
            )
        with pytest.raises(WorkflowAvailabilityTransitionRejected):
            await service.set_availability(
                unpublished.id,
                owner_principal_id=owner_id,
                intent=WorkflowAvailabilityIntent.DISABLE,
            )
        assert (
            await service.get(unpublished.id, owner_principal_id=owner_id)
        ).draft.status is WorkflowDefinitionStatus.DRAFT
        with pytest.raises(WorkflowNotFound):
            await service.set_availability(
                created_input.id,
                owner_principal_id=other_owner_id,
                intent=WorkflowAvailabilityIntent.DISABLE,
            )

        concurrent_availability = await asyncio.gather(
            service.set_availability(
                created_input.id,
                owner_principal_id=owner_id,
                intent=WorkflowAvailabilityIntent.DISABLE,
            ),
            service.set_availability(
                created_input.id,
                owner_principal_id=owner_id,
                intent=WorkflowAvailabilityIntent.ENABLE,
            ),
        )
        assert {item.status for item in concurrent_availability} == {
            WorkflowDefinitionStatus.ENABLED,
            WorkflowDefinitionStatus.DISABLED,
        }

        async with sessions.begin() as session:
            await session.execute(
                update(workflow_definitions)
                .where(workflow_definitions.c.id == created_input.id)
                .values(name="Changed draft", description="Changed")
            )
            await session.execute(
                update(workflow_draft_steps)
                .where(
                    workflow_draft_steps.c.workflow_definition_id == created_input.id,
                    workflow_draft_steps.c.step_identifier == "first",
                )
                .values(task_type="test.task", parameters={"changed": True})
            )
            await session.execute(
                workflow_draft_dependencies.delete().where(
                    workflow_draft_dependencies.c.workflow_definition_id
                    == created_input.id
                )
            )
        async with sessions() as session:
            unchanged_name = await session.scalar(
                select(workflow_versions.c.name).where(
                    workflow_versions.c.id == first_publication.id
                )
            )
            unchanged_parameters = await session.scalar(
                select(workflow_version_steps.c.parameters).where(
                    workflow_version_steps.c.workflow_version_id
                    == first_publication.id,
                    workflow_version_steps.c.step_identifier == "first",
                )
            )
            unchanged_edges = await session.scalar(
                select(func.count())
                .select_from(workflow_version_dependencies)
                .where(
                    workflow_version_dependencies.c.workflow_version_id
                    == first_publication.id
                )
            )
        assert unchanged_name == created_input.name
        assert unchanged_parameters == {"value": 1}
        assert unchanged_edges == 1

        second_publication = await service.publish(
            created_input.id,
            owner_principal_id=owner_id,
        )
        concurrent = await asyncio.gather(
            service.publish(created_input.id, owner_principal_id=owner_id),
            service.publish(created_input.id, owner_principal_id=owner_id),
        )
        assert second_publication.version_number == 2
        assert sorted(item.version_number for item in concurrent) == [3, 4]
        assert await count_version_rows(sessions, created_input.id) == (4, 8, 1)

        version_page = await service.list_versions(
            created_input.id,
            owner_principal_id=owner_id,
            limit=2,
        )
        assert [item.version_number for item in version_page.items] == [4, 3]
        assert version_page.next_cursor is not None
        later_publication = await service.publish(
            created_input.id, owner_principal_id=owner_id
        )
        assert later_publication.version_number == 5
        older_page = await service.list_versions(
            created_input.id,
            owner_principal_id=owner_id,
            limit=2,
            cursor=version_page.next_cursor,
        )
        assert [item.version_number for item in older_page.items] == [2, 1]
        assert {item.version_number for item in version_page.items}.isdisjoint(
            item.version_number for item in older_page.items
        )
        historical = await service.get_version(
            created_input.id,
            1,
            owner_principal_id=owner_id,
        )
        assert historical.name == created_input.name
        assert [step.identifier for step in historical.steps] == ["first", "second"]
        assert historical.steps[0].parameters == {"value": 1}
        assert [
            (
                dependency.predecessor_identifier,
                dependency.successor_identifier,
            )
            for dependency in historical.dependencies
        ] == [("first", "second")]
        with pytest.raises(WorkflowNotFound):
            await service.get_version(
                created_input.id, 1, owner_principal_id=other_owner_id
            )
        with pytest.raises(WorkflowNotFound):
            await service.list_versions(
                created_input.id,
                owner_principal_id=other_owner_id,
                limit=10,
            )

        with pytest.raises(WorkflowNotFound):
            await service.get(
                created_input.id,
                owner_principal_id=other_owner_id,
            )
        with pytest.raises(WorkflowNotFound):
            await service.publish(
                created_input.id,
                owner_principal_id=other_owner_id,
            )
        owner_list = await service.list(owner_principal_id=owner_id, limit=10_000)
        other_list = await service.list(
            owner_principal_id=other_owner_id,
            limit=10_000,
        )
        assert {summary.id for summary in owner_list.items} == {
            created_input.id,
            unpublished.id,
        }
        assert other_list.items == ()

        invalid = workflow(owner_id)
        invalid = WorkflowDraft(
            id=invalid.id,
            owner_principal_id=invalid.owner_principal_id,
            name=invalid.name,
            description=invalid.description,
            status=invalid.status,
            steps=invalid.steps,
            dependencies=(DraftDependency(uuid4(), "first", "first"),),
        )
        with pytest.raises(WorkflowValidationError) as error:
            await service.create(invalid)
        assert error.value.graph_result is not None
        assert await count_workflow_rows(sessions, invalid.id) == (0, 0, 0)

        cyclic = workflow(owner_id)
        await service.create(cyclic)
        identifiers = {step.identifier: step.id for step in cyclic.steps}
        async with sessions.begin() as session:
            await session.execute(
                insert(workflow_draft_dependencies).values(
                    id=uuid4(),
                    workflow_definition_id=cyclic.id,
                    predecessor_step_id=identifiers["second"],
                    successor_step_id=identifiers["first"],
                )
            )
        with pytest.raises(WorkflowValidationError) as publication_error:
            await service.publish(cyclic.id, owner_principal_id=owner_id)
        assert publication_error.value.graph_result is not None
        assert await count_version_rows(sessions, cyclic.id) == (0, 0, 0)
        async with sessions.begin() as session:
            await session.execute(
                workflow_draft_dependencies.delete().where(
                    workflow_draft_dependencies.c.workflow_definition_id == cyclic.id
                )
            )
            await session.execute(
                workflow_draft_steps.delete().where(
                    workflow_draft_steps.c.workflow_definition_id == cyclic.id
                )
            )
            await session.execute(
                workflow_definitions.delete().where(
                    workflow_definitions.c.id == cyclic.id
                )
            )

        with pytest.raises(WorkflowOwnerNotFound):
            await service.create(workflow(uuid4()))
        with pytest.raises(WorkflowOwnerDisabled):
            await service.create(workflow(disabled_owner_id))
        disabled_workflow = workflow(disabled_owner_id)
        disabled_identifiers = {
            step.identifier: step.id for step in disabled_workflow.steps
        }
        async with sessions.begin() as session:
            await session.execute(
                insert(workflow_definitions).values(
                    id=disabled_workflow.id,
                    owner_principal_id=disabled_owner_id,
                    name=disabled_workflow.name,
                    description=disabled_workflow.description,
                    status=disabled_workflow.status.value,
                )
            )
            await session.execute(
                insert(workflow_draft_steps),
                [
                    {
                        "id": step.id,
                        "workflow_definition_id": disabled_workflow.id,
                        "step_identifier": step.identifier,
                        "task_type": step.task_type,
                        "parameters": step.parameters,
                    }
                    for step in disabled_workflow.steps
                ],
            )
            await session.execute(
                insert(workflow_draft_dependencies).values(
                    id=disabled_workflow.dependencies[0].id,
                    workflow_definition_id=disabled_workflow.id,
                    predecessor_step_id=disabled_identifiers["first"],
                    successor_step_id=disabled_identifiers["second"],
                )
            )
        with pytest.raises(WorkflowOwnerDisabled):
            await service.publish(
                disabled_workflow.id,
                owner_principal_id=disabled_owner_id,
            )
        with pytest.raises(WorkflowOwnerDisabled):
            await service.set_availability(
                disabled_workflow.id,
                owner_principal_id=disabled_owner_id,
                intent=WorkflowAvailabilityIntent.ENABLE,
            )
        assert await count_version_rows(sessions, disabled_workflow.id) == (0, 0, 0)

        concurrent_input = workflow(owner_id)
        outcomes = await asyncio.gather(
            service.create(concurrent_input),
            service.create(concurrent_input),
            return_exceptions=True,
        )
        assert sum(not isinstance(outcome, BaseException) for outcome in outcomes) == 1
        assert (
            sum(
                isinstance(outcome, WorkflowPersistenceConflict) for outcome in outcomes
            )
            == 1
        )
        assert await count_workflow_rows(sessions, concurrent_input.id) == (1, 2, 1)

        same_name = workflow(owner_id)
        await service.create(same_name)
        assert (
            len((await service.list(owner_principal_id=owner_id, limit=10_000)).items)
            == 4
        )

        first_page = await service.list(owner_principal_id=owner_id, limit=2)
        assert len(first_page.items) == 2
        assert first_page.next_cursor is not None
        initial_ids = {
            created_input.id,
            concurrent_input.id,
            same_name.id,
            unpublished.id,
        }
        inserted_between_pages = workflow(owner_id)
        await service.create(inserted_between_pages)
        second_page = await service.list(
            owner_principal_id=owner_id,
            limit=2,
            cursor=first_page.next_cursor,
        )
        traversed_ids = {
            *(item.id for item in first_page.items),
            *(item.id for item in second_page.items),
        }
        assert traversed_ids == initial_ids
        assert inserted_between_pages.id not in traversed_ids
        assert second_page.next_cursor is None

        independent_inputs = (workflow(owner_id), workflow(owner_id))
        for independent in independent_inputs:
            await service.create(independent)
        independent_versions = await asyncio.gather(
            *(
                service.publish(item.id, owner_principal_id=owner_id)
                for item in independent_inputs
            )
        )
        assert [item.version_number for item in independent_versions] == [1, 1]
        for independent in independent_inputs:
            assert await count_version_rows(sessions, independent.id) == (1, 2, 1)

    finally:
        await engine.dispose()


def test_real_workflow_create_read_list_rollback_and_concurrency() -> None:
    with temporary_database(
        "TASKFORGE_WORKFLOW_PERSISTENCE_TEST_DATABASE_URL",
        "taskforge_workflow_persistence",
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        with migration_database_url(alembic_url):
            command.upgrade(Config("alembic.ini"), "head")
        asyncio.run(verify_workflow_persistence(database_url))
