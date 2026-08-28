"""Opt-in workflow route validation against isolated PostgreSQL."""

from __future__ import annotations

import asyncio
import base64
import os
import secrets
from uuid import UUID, uuid4

import httpx2
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import select
from sqlalchemy.engine import URL

from taskforge.api.application import create_app
from taskforge.api.health import ReadinessCoordinator
from taskforge.audit.schema import audit_records
from taskforge.identity.authorization import Role
from taskforge.identity.credentials import (
    DEFAULT_VERIFIER_ALGORITHM,
    DEFAULT_VERIFIERS,
)
from taskforge.identity.schema import (
    api_credentials,
    api_principal_roles,
    api_principals,
)
from taskforge.persistence.database import build_async_engine
from taskforge.workflows.schema import workflow_definitions
from taskforge.workflows.task_types import (
    JSONMapping,
    TaskTypeDefinition,
    TaskTypeRegistry,
    WorkflowValidationIssue,
)
from tests.integration.postgresql import migration_database_url, temporary_database
from tests.integration.test_protected_principal_route import AlwaysReady, settings_for

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_WORKFLOW_ROUTE_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_WORKFLOW_ROUTE_INTEGRATION=1 explicitly",
    ),
]


class AcceptParameters:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        return ()


def credential_value(credential_id: UUID, secret: bytes) -> str:
    encoded = base64.urlsafe_b64encode(secret).rstrip(b"=").decode()
    return f"tf_api_v1.{credential_id}.{encoded}"


async def verify_workflow_routes(database_url: URL) -> None:
    settings = settings_for(database_url)
    engine = build_async_engine(settings)
    owner_id, other_id, administrator_id = uuid4(), uuid4(), uuid4()
    owner_credential_id, other_credential_id, administrator_credential_id = (
        uuid4(),
        uuid4(),
        uuid4(),
    )
    owner_secret, other_secret, administrator_secret = (
        secrets.token_bytes(32),
        secrets.token_bytes(32),
        secrets.token_bytes(32),
    )

    def verifier(secret: bytes) -> str:
        return DEFAULT_VERIFIERS.encode(secret, algorithm=DEFAULT_VERIFIER_ALGORITHM)

    try:
        async with engine.begin() as connection:
            await connection.execute(
                api_principals.insert(),
                [
                    {"id": owner_id, "name": "workflow-owner"},
                    {"id": other_id, "name": "other-operator"},
                    {"id": administrator_id, "name": "administrator"},
                ],
            )
            await connection.execute(
                api_principal_roles.insert(),
                [
                    {"principal_id": owner_id, "role": Role.WORKFLOW_OPERATOR.value},
                    {"principal_id": other_id, "role": Role.WORKFLOW_OPERATOR.value},
                    {
                        "principal_id": administrator_id,
                        "role": Role.ADMINISTRATOR.value,
                    },
                ],
            )
            await connection.execute(
                api_credentials.insert(),
                [
                    {
                        "id": owner_credential_id,
                        "principal_id": owner_id,
                        "credential_verifier": verifier(owner_secret),
                    },
                    {
                        "id": other_credential_id,
                        "principal_id": other_id,
                        "credential_verifier": verifier(other_secret),
                    },
                    {
                        "id": administrator_credential_id,
                        "principal_id": administrator_id,
                        "credential_verifier": verifier(administrator_secret),
                    },
                ],
            )
    finally:
        await engine.dispose()

    registry = TaskTypeRegistry(
        (TaskTypeDefinition("test.task", "test-workers", AcceptParameters()),)
    )
    app = create_app(
        settings,
        ReadinessCoordinator(AlwaysReady(), timeout_seconds=1),
        task_types=registry,
    )
    owner_token = credential_value(owner_credential_id, owner_secret)
    other_token = credential_value(other_credential_id, other_secret)
    administrator_token = credential_value(
        administrator_credential_id, administrator_secret
    )
    body = {
        "name": "Integrated draft",
        "steps": [
            {
                "identifier": "first",
                "task_type": "test.task",
                "parameters": {"value": 1},
            },
            {"identifier": "second", "task_type": "test.task", "parameters": {}},
        ],
        "dependencies": [{"predecessor": "first", "successor": "second"}],
    }
    transport = httpx2.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with httpx2.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            validated = await client.post(
                "/api/v1/workflows/validate",
                json=body,
                headers={"Authorization": f"Bearer {owner_token}"},
            )
            rejected_validation = await client.post(
                "/api/v1/workflows/validate",
                json={**body, "steps": []},
                headers={"Authorization": f"Bearer {owner_token}"},
            )
            empty_after_validation = await client.get(
                "/api/v1/workflows",
                headers={"Authorization": f"Bearer {owner_token}"},
            )
            created = await client.post(
                "/api/v1/workflows",
                json=body,
                headers={"Authorization": f"Bearer {owner_token}"},
            )
            invalid = await client.post(
                "/api/v1/workflows",
                json={
                    **body,
                    "name": "Invalid integrated draft",
                    "dependencies": [
                        {"predecessor": "first", "successor": "second"},
                        {"predecessor": "second", "successor": "first"},
                    ],
                },
                headers={"Authorization": f"Bearer {owner_token}"},
            )
            location = created.headers["Location"]
            found = await client.get(
                location, headers={"Authorization": f"Bearer {owner_token}"}
            )
            workflow_id = created.json()["id"]
            publish_path = f"/api/v1/workflows/{workflow_id}/versions"
            published = await client.post(
                publish_path,
                headers={"Authorization": f"Bearer {owner_token}"},
            )
            version_detail = await client.get(
                published.headers["Location"],
                headers={"Authorization": f"Bearer {owner_token}"},
            )
            second_version = await client.post(
                publish_path,
                headers={"Authorization": f"Bearer {owner_token}"},
            )
            third_version = await client.post(
                publish_path,
                headers={"Authorization": f"Bearer {owner_token}"},
            )
            version_first_page = await client.get(
                f"{publish_path}?limit=2",
                headers={"Authorization": f"Bearer {owner_token}"},
            )
            version_cursor = version_first_page.json()["page"]["next_cursor"]
            fourth_version = await client.post(
                publish_path,
                headers={"Authorization": f"Bearer {owner_token}"},
            )
            version_second_page = await client.get(
                f"{publish_path}?limit=2&cursor={version_cursor}",
                headers={"Authorization": f"Bearer {owner_token}"},
            )
            hidden_version = await client.get(
                published.headers["Location"],
                headers={"Authorization": f"Bearer {other_token}"},
            )
            missing_version = await client.get(
                f"/api/v1/workflows/{workflow_id}/versions/999",
                headers={"Authorization": f"Bearer {owner_token}"},
            )
            after_publication = await client.get(
                location, headers={"Authorization": f"Bearer {owner_token}"}
            )
            second_created = await client.post(
                "/api/v1/workflows",
                json={**body, "name": "Second integrated draft"},
                headers={"Authorization": f"Bearer {owner_token}"},
            )
            third_created = await client.post(
                "/api/v1/workflows",
                json={**body, "name": "Third integrated draft"},
                headers={"Authorization": f"Bearer {owner_token}"},
            )
            first_page = await client.get(
                "/api/v1/workflows?limit=2",
                headers={"Authorization": f"Bearer {owner_token}"},
            )
            cursor = first_page.json()["page"]["next_cursor"]
            inserted_between_pages = await client.post(
                "/api/v1/workflows",
                json={**body, "name": "Inserted between pages"},
                headers={"Authorization": f"Bearer {owner_token}"},
            )
            second_page = await client.get(
                f"/api/v1/workflows?limit=2&cursor={cursor}",
                headers={"Authorization": f"Bearer {owner_token}"},
            )
            other_owner_page = await client.get(
                "/api/v1/workflows",
                headers={"Authorization": f"Bearer {other_token}"},
            )
            hidden = await client.get(
                location, headers={"Authorization": f"Bearer {other_token}"}
            )
            missing = await client.get(
                f"/api/v1/workflows/{uuid4()}",
                headers={"Authorization": f"Bearer {other_token}"},
            )
            administrator_headers = {"Authorization": f"Bearer {administrator_token}"}
            administrator_page = await client.get(
                "/api/v1/workflows", headers=administrator_headers
            )
            administrator_detail = await client.get(
                location, headers=administrator_headers
            )
            administrator_version = await client.get(
                published.headers["Location"], headers=administrator_headers
            )
            administrator_publication = await client.post(
                publish_path, headers=administrator_headers
            )

    assert validated.status_code == 200
    assert validated.json() == {
        "valid": True,
        "topological_order": ["first", "second"],
    }
    assert rejected_validation.status_code == 422
    assert empty_after_validation.json()["items"] == []
    assert created.status_code == 201
    assert invalid.status_code == 422
    assert invalid.json()["error"]["details"] == [
        {
            "code": "cycle",
            "path": ["dependencies"],
            "message": "Workflow dependencies must not contain a cycle.",
        }
    ]
    assert created.json()["owner_principal_id"] == str(owner_id)
    assert found.status_code == 200
    assert published.status_code == 201
    assert published.json()["version_number"] == 1
    assert version_detail.status_code == 200
    assert version_detail.json()["name"] == body["name"]
    assert [step["identifier"] for step in version_detail.json()["steps"]] == [
        "first",
        "second",
    ]
    assert version_detail.json()["dependencies"] == [
        {"predecessor": "first", "successor": "second"}
    ]
    assert second_version.json()["version_number"] == 2
    assert third_version.json()["version_number"] == 3
    assert fourth_version.json()["version_number"] == 4
    assert [item["version_number"] for item in version_first_page.json()["items"]] == [
        3,
        2,
    ]
    assert [item["version_number"] for item in version_second_page.json()["items"]] == [
        1
    ]
    assert after_publication.json()["status"] == "draft"
    assert hidden_version.status_code == missing_version.status_code == 404
    assert second_created.status_code == third_created.status_code == 201
    assert first_page.status_code == second_page.status_code == 200
    assert len(first_page.json()["items"]) == 2
    assert len(second_page.json()["items"]) == 1
    assert second_page.json()["page"]["next_cursor"] is None
    initial_ids = {
        created.json()["id"],
        second_created.json()["id"],
        third_created.json()["id"],
    }
    traversed_ids = {
        item["id"]
        for page in (first_page, second_page)
        for item in page.json()["items"]
    }
    assert traversed_ids == initial_ids
    assert inserted_between_pages.json()["id"] not in traversed_ids
    assert other_owner_page.json()["items"] == []
    assert hidden.status_code == missing.status_code == 404
    assert {item["id"] for item in administrator_page.json()["items"]} >= initial_ids
    assert administrator_detail.json()["owner_principal_id"] == str(owner_id)
    assert administrator_version.status_code == 200
    assert administrator_publication.status_code == 201
    assert administrator_publication.json()["version_number"] == 5
    hidden_body, missing_body = hidden.json(), missing.json()
    hidden_body["error"].pop("request_id")
    missing_body["error"].pop("request_id")
    assert hidden_body == missing_body
    assert set(hidden.headers) == set(missing.headers)

    verification_engine = build_async_engine(settings)
    try:
        async with verification_engine.connect() as connection:
            persisted_owner = await connection.scalar(
                select(workflow_definitions.c.owner_principal_id).where(
                    workflow_definitions.c.id == UUID(workflow_id)
                )
            )
            publication_actor = await connection.scalar(
                select(audit_records.c.api_principal_id)
                .where(
                    audit_records.c.action == "workflow.publish",
                    audit_records.c.resource_id == UUID(workflow_id),
                )
                .order_by(audit_records.c.occurred_at.desc())
                .limit(1)
            )
    finally:
        await verification_engine.dispose()
    assert persisted_owner == owner_id
    assert publication_actor == administrator_id


def test_real_authorized_workflow_create_and_owner_scoped_read() -> None:
    with temporary_database(
        "TASKFORGE_WORKFLOW_ROUTE_TEST_DATABASE_URL",
        "taskforge_workflow_route",
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        with migration_database_url(alembic_url):
            command.upgrade(Config("alembic.ini"), "head")
        asyncio.run(verify_workflow_routes(database_url))
