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
from sqlalchemy.engine import URL

from taskforge.api.application import create_app
from taskforge.api.health import ReadinessCoordinator
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
    owner_id, other_id = uuid4(), uuid4()
    owner_credential_id, other_credential_id = uuid4(), uuid4()
    owner_secret, other_secret = secrets.token_bytes(32), secrets.token_bytes(32)

    def verifier(secret: bytes) -> str:
        return DEFAULT_VERIFIERS.encode(secret, algorithm=DEFAULT_VERIFIER_ALGORITHM)

    try:
        async with engine.begin() as connection:
            await connection.execute(
                api_principals.insert(),
                [
                    {"id": owner_id, "name": "workflow-owner"},
                    {"id": other_id, "name": "other-operator"},
                ],
            )
            await connection.execute(
                api_principal_roles.insert(),
                [
                    {"principal_id": owner_id, "role": Role.WORKFLOW_OPERATOR.value},
                    {"principal_id": other_id, "role": Role.WORKFLOW_OPERATOR.value},
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
                ],
            )
    finally:
        await engine.dispose()

    registry = TaskTypeRegistry((TaskTypeDefinition("test.task", AcceptParameters()),))
    app = create_app(
        settings,
        ReadinessCoordinator((AlwaysReady(),), timeout_seconds=1),
        task_types=registry,
    )
    owner_token = credential_value(owner_credential_id, owner_secret)
    other_token = credential_value(other_credential_id, other_secret)
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
    hidden_body, missing_body = hidden.json(), missing.json()
    hidden_body["error"].pop("request_id")
    missing_body["error"].pop("request_id")
    assert hidden_body == missing_body
    assert set(hidden.headers) == set(missing.headers)


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
