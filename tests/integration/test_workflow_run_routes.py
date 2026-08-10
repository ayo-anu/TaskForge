"""Opt-in workflow-run route verification against PostgreSQL."""

from __future__ import annotations

import asyncio
import os
import secrets
from uuid import uuid4

import httpx2
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import func, select, update
from sqlalchemy.engine import URL

from taskforge.api.application import create_app
from taskforge.api.health import ReadinessCoordinator
from taskforge.identity.authorization import Role
from taskforge.identity.credentials import DEFAULT_VERIFIER_ALGORITHM, DEFAULT_VERIFIERS
from taskforge.identity.schema import (
    api_credentials,
    api_principal_roles,
    api_principals,
)
from taskforge.persistence.database import build_async_engine
from taskforge.runs.schema import (
    task_runs,
    workflow_run_idempotency,
    workflow_run_inputs,
    workflow_runs,
)
from taskforge.workflows.schema import workflow_definitions
from taskforge.workflows.task_types import TaskTypeDefinition, TaskTypeRegistry
from tests.integration.postgresql import migration_database_url, temporary_database
from tests.integration.test_authentication_persistence import settings_for
from tests.integration.test_protected_principal_route import AlwaysReady
from tests.integration.test_workflow_routes import (
    AcceptParameters,
    credential_value,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_WORKFLOW_ROUTE_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_WORKFLOW_ROUTE_INTEGRATION=1 explicitly",
    ),
]


async def verify_workflow_run_routes(database_url: URL) -> None:
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
                    {"id": owner_id, "name": "run-owner"},
                    {"id": other_id, "name": "other-run-owner"},
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

    registry = TaskTypeRegistry(
        (TaskTypeDefinition("test.task", "test-workers", AcceptParameters()),)
    )
    app = create_app(
        settings,
        ReadinessCoordinator((AlwaysReady(),), timeout_seconds=1),
        task_types=registry,
    )
    owner_headers = {
        "Authorization": f"Bearer {credential_value(owner_credential_id, owner_secret)}"
    }
    other_headers = {
        "Authorization": f"Bearer {credential_value(other_credential_id, other_secret)}"
    }
    workflow_body = {
        "name": "Run route workflow",
        "steps": [
            {"identifier": "root", "task_type": "test.task", "parameters": {}},
            {"identifier": "leaf", "task_type": "test.task", "parameters": {}},
        ],
        "dependencies": [{"predecessor": "root", "successor": "leaf"}],
    }

    transport = httpx2.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with httpx2.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            workflow = await client.post(
                "/api/v1/workflows", json=workflow_body, headers=owner_headers
            )
            workflow_id = workflow.json()["id"]
            await client.post(
                f"/api/v1/workflows/{workflow_id}/versions", headers=owner_headers
            )
            enable_engine = build_async_engine(settings)
            try:
                async with enable_engine.begin() as connection:
                    await connection.execute(
                        update(workflow_definitions)
                        .where(workflow_definitions.c.id == workflow_id)
                        .values(status="enabled")
                    )
            finally:
                await enable_engine.dispose()
            keyless = await client.post(
                f"/api/v1/workflows/{workflow_id}/runs",
                json={"payload": {"secret": "not-returned"}},
                headers=owner_headers,
            )
            keyed_headers = {**owner_headers, "Idempotency-Key": "route-key-123456"}
            keyed = await client.post(
                f"/api/v1/workflows/{workflow_id}/runs",
                json={"version_number": 1},
                headers=keyed_headers,
            )
            replay = await client.post(
                f"/api/v1/workflows/{workflow_id}/runs",
                json={"version_number": 1},
                headers=keyed_headers,
            )
            conflict = await client.post(
                f"/api/v1/workflows/{workflow_id}/runs",
                json={"version_number": 1, "payload": {"different": True}},
                headers=keyed_headers,
            )
            assert keyed.status_code == 201, keyed.text
            run = await client.get(keyed.headers["Location"], headers=owner_headers)
            tasks = await client.get(
                f"{keyed.headers['Location']}/tasks", headers=owner_headers
            )
            task = await client.get(
                f"/api/v1/task-runs/{tasks.json()['items'][0]['id']}",
                headers=owner_headers,
            )
            assert run.json()["failure_reason"] is None
            assert all(item["failure_reason"] is None for item in tasks.json()["items"])

            status_engine = build_async_engine(settings)
            try:
                async with status_engine.begin() as connection:
                    await connection.execute(
                        update(workflow_runs)
                        .where(workflow_runs.c.id == keyed.json()["id"])
                        .values(status="failed")
                    )
                    task_rows = tasks.json()["items"]
                    await connection.execute(
                        update(task_runs)
                        .where(task_runs.c.id == task_rows[0]["id"])
                        .values(status="skipped")
                    )
                    await connection.execute(
                        update(task_runs)
                        .where(task_runs.c.id == task_rows[1]["id"])
                        .values(status="failed")
                    )
            finally:
                await status_engine.dispose()

            failed_run = await client.get(
                keyed.headers["Location"], headers=owner_headers
            )
            failed_tasks = await client.get(
                f"{keyed.headers['Location']}/tasks", headers=owner_headers
            )
            hidden_run = await client.get(
                keyed.headers["Location"], headers=other_headers
            )
            hidden_tasks = await client.get(
                f"{keyed.headers['Location']}/tasks", headers=other_headers
            )

    assert workflow.status_code == 201
    assert keyless.status_code == keyed.status_code == replay.status_code == 201
    assert keyless.json()["id"] != keyed.json()["id"]
    assert replay.json()["id"] == keyed.json()["id"]
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "idempotency_conflict"
    assert run.status_code == tasks.status_code == task.status_code == 200
    assert failed_run.json()["failure_reason"] == "task_failed"
    assert {
        item["status"]: item["failure_reason"] for item in failed_tasks.json()["items"]
    } == {"failed": "task_failed", "skipped": "dependency_failed"}
    assert "task_count" not in run.json()
    assert "secret" not in keyless.text
    assert [item["step_identifier"] for item in tasks.json()["items"]] == [
        "leaf",
        "root",
    ]
    assert hidden_run.status_code == hidden_tasks.status_code == 404

    verification_engine = build_async_engine(settings)
    try:
        async with verification_engine.connect() as connection:
            counts = [
                int(
                    await connection.scalar(select(func.count()).select_from(table))
                    or 0
                )
                for table in (
                    workflow_runs,
                    workflow_run_inputs,
                    task_runs,
                    workflow_run_idempotency,
                )
            ]
    finally:
        await verification_engine.dispose()
    assert counts == [2, 2, 4, 1]


def test_workflow_run_routes_are_atomic_idempotent_and_owner_scoped() -> None:
    with temporary_database(
        "TASKFORGE_WORKFLOW_ROUTE_TEST_DATABASE_URL",
        "taskforge_workflow_run_route",
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        configuration = Config("alembic.ini")
        with migration_database_url(alembic_url):
            command.upgrade(configuration, "head")
        asyncio.run(verify_workflow_run_routes(database_url))
