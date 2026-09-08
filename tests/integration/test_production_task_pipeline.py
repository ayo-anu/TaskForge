"""Real catalog-backed representative workflow publication against PostgreSQL."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import insert
from sqlalchemy.engine import URL

from taskforge.dispatch.envelope import create_dispatch_envelope
from taskforge.identity.authorization import OwnerFilter
from taskforge.identity.schema import api_principals
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.workflows import SQLAlchemyWorkflowRepository
from taskforge.runtime_provider import (
    load_installed_task_catalog,
    load_installed_worker_profile,
)
from taskforge.tasks.catalog import (
    INGEST_TASK_TYPE,
    NOTIFY_TASK_TYPE,
    TRANSFORM_TASK_TYPE,
    VALIDATE_TASK_TYPE,
)
from taskforge.worker.cancellation import TaskCancellationToken
from taskforge.worker.handlers import TaskContext, create_task_context
from taskforge.worker.result_submission import MAX_TASK_RESULT_OUTPUT_BYTES
from taskforge.worker.results import TaskCancellation
from taskforge.workflows.domain import (
    DraftDependency,
    DraftWorkflowStep,
    WorkflowDefinitionStatus,
    WorkflowDraft,
)
from taskforge.workflows.service import WorkflowService
from taskforge.workflows.task_types import JSONMapping, WorkflowValidationError
from tests.integration.postgresql import migration_database_url, temporary_database
from tests.integration.test_authentication_persistence import settings_for

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION=1 explicitly",
    ),
]

STEP_PARAMETERS: dict[str, JSONMapping] = {
    "ingest": {
        "document_id": "pipeline-document",
        "content": "Café\r\nLine\rTail\n",
    },
    "validate": {
        "document_id": "pipeline-document",
        "document": {"title": "Café", "count": 7, "ready": True},
        "required_fields": ["title", "count"],
    },
    "transform": {
        "document_id": "pipeline-document",
        "content": " \tAlpha \n  BETA\u00a0X \r",
        "operations": ["strip", "collapse_whitespace", "lowercase_ascii"],
    },
    "notify": {
        "notification_key": "notice-001",
        "topic": "pipeline.complete",
        "message": "Café ready",
    },
}


def handler_invocation(
    task_type: str,
    capability: str,
    parameters: JSONMapping,
    *,
    cancellation_token: TaskCancellationToken | None = None,
) -> TaskContext:
    token = cancellation_token or TaskCancellationToken()
    envelope = create_dispatch_envelope(
        dispatch_id=uuid4(),
        task_attempt_id=uuid4(),
        task_run_id=uuid4(),
        workflow_run_id=uuid4(),
        attempt_number=1,
        task_type=task_type,
        required_capability=capability,
        task_payload=parameters,
        references={},
    )
    return create_task_context(
        dispatch_id=envelope.dispatch_id,
        workflow_run_id=envelope.workflow_run_id,
        task_run_id=envelope.task_run_id,
        task_attempt_id=envelope.task_attempt_id,
        attempt_number=envelope.attempt_number,
        task_type=envelope.task_type,
        parameters=envelope.task_payload,
        references=envelope.references,
        correlation_id=None,
        trace_context=None,
        cancellation_requested_at_start=token.is_cancellation_requested,
        cancellation_token=token,
        deadline=None,
    )


def pipeline_workflow(owner_id: UUID, *, valid: bool = True) -> WorkflowDraft:
    parameters = {name: dict(value) for name, value in STEP_PARAMETERS.items()}
    if not valid:
        parameters["transform"] = {
            "document_id": "pipeline-document",
            "content": "value",
            "operations": ["import:os"],
        }
    return WorkflowDraft(
        id=uuid4(),
        owner_principal_id=owner_id,
        name="Production representative pipeline",
        description="Static inputs prove ordering without result interpolation.",
        status=WorkflowDefinitionStatus.DRAFT,
        steps=(
            DraftWorkflowStep(
                uuid4(), "ingest", INGEST_TASK_TYPE, parameters["ingest"]
            ),
            DraftWorkflowStep(
                uuid4(), "validate", VALIDATE_TASK_TYPE, parameters["validate"]
            ),
            DraftWorkflowStep(
                uuid4(), "transform", TRANSFORM_TASK_TYPE, parameters["transform"]
            ),
            DraftWorkflowStep(
                uuid4(), "notify", NOTIFY_TASK_TYPE, parameters["notify"]
            ),
        ),
        dependencies=(
            DraftDependency(uuid4(), "ingest", "validate"),
            DraftDependency(uuid4(), "validate", "transform"),
            DraftDependency(uuid4(), "transform", "notify"),
        ),
    )


async def verify_pipeline(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    catalog = load_installed_task_catalog()
    service = WorkflowService(SQLAlchemyWorkflowRepository(sessions), catalog)
    owner_id = uuid4()
    try:
        async with sessions.begin() as session:
            await session.execute(
                insert(api_principals).values(
                    id=owner_id, name=f"pipeline-owner-{uuid4().hex}"
                )
            )

        draft = pipeline_workflow(owner_id)
        await service.create(draft)
        publication = await service.publish(
            draft.id,
            owner_filter=OwnerFilter.only(owner_id),
            actor_principal_id=owner_id,
        )
        version = await service.get_version(
            draft.id,
            publication.version_number,
            owner_filter=OwnerFilter.only(owner_id),
        )

        assert [(step.identifier, step.task_type) for step in version.steps] == [
            ("ingest", INGEST_TASK_TYPE),
            ("notify", NOTIFY_TASK_TYPE),
            ("transform", TRANSFORM_TASK_TYPE),
            ("validate", VALIDATE_TASK_TYPE),
        ]
        assert {step.identifier: step.parameters for step in version.steps} == (
            STEP_PARAMETERS
        )
        assert {
            (edge.predecessor_identifier, edge.successor_identifier)
            for edge in version.dependencies
        } == {
            ("ingest", "validate"),
            ("validate", "transform"),
            ("transform", "notify"),
        }

        profile = load_installed_worker_profile("pipeline", catalog)
        assert profile.handlers.task_types == {
            INGEST_TASK_TYPE,
            VALIDATE_TASK_TYPE,
            TRANSFORM_TASK_TYPE,
            NOTIFY_TASK_TYPE,
        }
        assert profile.capabilities == (
            "pipeline.ingestion",
            "pipeline.notification",
            "pipeline.processing",
        )
        task_types = {
            "ingest": INGEST_TASK_TYPE,
            "validate": VALIDATE_TASK_TYPE,
            "transform": TRANSFORM_TASK_TYPE,
            "notify": NOTIFY_TASK_TYPE,
        }
        expected_digests = {
            "ingest": (
                "content_sha256",
                "2f401031c20cbb24d4b3f98fa0962f73d62e5bf6843282405a1294f94d312dd2",
            ),
            "validate": (
                "document_sha256",
                "4fde1a3e20e597df960deb3e5e336f4735ef98e5fa5a6e15d5dc08c4f9e66b7a",
            ),
            "transform": (
                "content_sha256",
                "1888d67c6c16d896ca54d1218c66fd5d3b6383c115ca08441c4027a044f2c0e3",
            ),
            "notify": (
                "receipt_id",
                "e467cad6d470b611e925dd2d40d6e094db82924d3dabc22d6b8155b8363786ae",
            ),
        }
        for identifier, task_type in task_types.items():
            definition = profile.handlers.definition(task_type)
            assert definition is not None
            task_definition = catalog.definition(task_type)
            assert task_definition is not None
            first_invocation = handler_invocation(
                task_type,
                task_definition.required_capability,
                STEP_PARAMETERS[identifier],
            )
            second_invocation = handler_invocation(
                task_type,
                task_definition.required_capability,
                STEP_PARAMETERS[identifier],
            )
            assert first_invocation.dispatch_id != second_invocation.dispatch_id
            assert first_invocation.task_attempt_id != second_invocation.task_attempt_id
            assert first_invocation.task_run_id != second_invocation.task_run_id
            assert first_invocation.workflow_run_id != second_invocation.workflow_run_id
            first = await definition.handler(first_invocation)
            second = await definition.handler(second_invocation)
            assert first == second
            assert isinstance(first, dict)
            digest_field, digest = expected_digests[identifier]
            assert first[digest_field] == digest
            assert (
                len(
                    json.dumps(
                        first,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                        allow_nan=False,
                    ).encode("utf-8")
                )
                < MAX_TASK_RESULT_OUTPUT_BYTES
            )

            cancellation = TaskCancellationToken()
            cancellation._request(datetime(2030, 1, 1, tzinfo=UTC))
            assert isinstance(
                await definition.handler(
                    handler_invocation(
                        task_type,
                        task_definition.required_capability,
                        STEP_PARAMETERS[identifier],
                        cancellation_token=cancellation,
                    )
                ),
                TaskCancellation,
            )

        invalid = pipeline_workflow(owner_id, valid=False)
        await service.create(invalid)
        with pytest.raises(WorkflowValidationError):
            await service.publish(
                invalid.id,
                owner_filter=OwnerFilter.only(owner_id),
                actor_principal_id=owner_id,
            )
    finally:
        await engine.dispose()


def test_real_catalog_publishes_representative_pipeline() -> None:
    with temporary_database(
        "TASKFORGE_WORKFLOW_TEST_DATABASE_URL", "taskforge_workflow_persistence"
    ) as database_url:
        config = Config("alembic.ini")
        with migration_database_url(database_url.render_as_string(hide_password=False)):
            command.upgrade(config, "head")
        asyncio.run(verify_pipeline(database_url))
