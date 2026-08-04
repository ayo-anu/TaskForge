"""API contract tests for authorized workflow draft routes."""

from __future__ import annotations

import asyncio
import base64
import secrets
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import httpx2
from fastapi import FastAPI
from pydantic import SecretStr

from taskforge.api.application import create_app
from taskforge.api.health import ReadinessCoordinator
from taskforge.identity.authentication import APIAuthenticator, WorkerAuthenticator
from taskforge.identity.authorization import AuthorizationService, Role
from taskforge.identity.credentials import (
    DEFAULT_VERIFIER_ALGORITHM,
    DEFAULT_VERIFIERS,
    CredentialScope,
)
from taskforge.identity.ports import CredentialRecord
from taskforge.settings import Settings
from taskforge.workflows.domain import WorkflowDraft
from taskforge.workflows.persistence_ports import StoredWorkflowDraft
from taskforge.workflows.service import (
    WorkflowNotFound,
    WorkflowPersistenceConflict,
    WorkflowServiceUnavailable,
)
from taskforge.workflows.task_types import (
    JSONMapping,
    TaskTypeDefinition,
    TaskTypeRegistry,
    WorkflowValidationIssue,
)


class AlwaysReady:
    async def start(self) -> None:
        pass

    async def is_ready(self) -> bool:
        return True

    async def close(self) -> None:
        pass


class CredentialRepository:
    def __init__(self, record: CredentialRecord) -> None:
        self.record = record

    async def find_api_credential(self, credential_id: UUID) -> CredentialRecord | None:
        return self.record if self.record.credential_id == credential_id else None

    async def find_worker_credential(
        self, credential_id: UUID
    ) -> CredentialRecord | None:
        return self.record if self.record.credential_id == credential_id else None


class RoleRepository:
    def __init__(self, roles: frozenset[str]) -> None:
        self.roles = roles

    async def find_role_names(self, principal_id: UUID) -> frozenset[str]:
        return self.roles


class Validator:
    def __init__(self) -> None:
        self.calls = 0

    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        self.calls += 1
        return ()


class WorkflowServiceStub:
    def __init__(self) -> None:
        self.created: list[WorkflowDraft] = []
        self.stored: dict[UUID, StoredWorkflowDraft] = {}
        self.create_error: Exception | None = None
        self.get_error: Exception | None = None

    async def create(self, workflow: WorkflowDraft) -> StoredWorkflowDraft:
        if self.create_error:
            raise self.create_error
        self.created.append(workflow)
        now = datetime.now(UTC)
        stored = StoredWorkflowDraft(workflow, now, now)
        self.stored[workflow.id] = stored
        return stored

    async def get(
        self, workflow_id: UUID, *, owner_principal_id: UUID
    ) -> StoredWorkflowDraft:
        if self.get_error:
            raise self.get_error
        stored = self.stored.get(workflow_id)
        if stored is None or stored.draft.owner_principal_id != owner_principal_id:
            raise WorkflowNotFound
        return stored


class Runtime:
    def __init__(
        self,
        caller_id: UUID,
        roles: frozenset[str],
        service: WorkflowServiceStub,
        registry: TaskTypeRegistry,
    ) -> None:
        api_value, api_record = make_credential(CredentialScope.API, caller_id)
        worker_value, worker_record = make_credential(CredentialScope.WORKER, uuid4())
        self.api_credential = api_value
        self.worker_credential = worker_value
        self.api_authenticator = APIAuthenticator(
            CredentialRepository(api_record), timeout_seconds=0.05
        )
        self.worker_authenticator = WorkerAuthenticator(
            CredentialRepository(worker_record), timeout_seconds=0.05
        )
        self.authorization_service = AuthorizationService(
            RoleRepository(roles), timeout_seconds=0.05
        )
        self.workflow_service: Any = service
        self.task_type_registry = registry

    async def close(self) -> None:
        pass


def make_credential(
    scope: CredentialScope, identity_id: UUID
) -> tuple[str, CredentialRecord]:
    credential_id = uuid4()
    secret = secrets.token_bytes(32)
    prefix = "tf_api_v1" if scope is CredentialScope.API else "tf_worker_v1"
    encoded = base64.urlsafe_b64encode(secret).rstrip(b"=").decode()
    value = f"{prefix}.{credential_id}.{encoded}"
    return value, CredentialRecord(
        credential_id,
        identity_id,
        DEFAULT_VERIFIERS.encode(secret, algorithm=DEFAULT_VERIFIER_ALGORITHM),
        False,
        False,
        False,
    )


def make_app(
    roles: frozenset[str],
) -> tuple[FastAPI, Runtime, WorkflowServiceStub, Validator]:
    service, validator = WorkflowServiceStub(), Validator()
    registry = TaskTypeRegistry((TaskTypeDefinition("test.task", validator),))
    runtime = Runtime(uuid4(), roles, service, registry)
    settings = Settings(
        postgres_password=SecretStr("test"), rabbitmq_password=SecretStr("test")
    )
    app = create_app(
        settings, ReadinessCoordinator((AlwaysReady(),), timeout_seconds=0.05), runtime
    )
    return app, runtime, service, validator


def request(
    app: FastAPI,
    method: str,
    path: str,
    credential: str | None,
    *,
    json: object | None = None,
) -> httpx2.Response:
    async def send() -> httpx2.Response:
        headers = {"Authorization": f"Bearer {credential}"} if credential else {}
        transport = httpx2.ASGITransport(app=app)
        async with app.router.lifespan_context(app):
            async with httpx2.AsyncClient(
                transport=transport, base_url="http://testserver"
            ) as client:
                return await client.request(method, path, headers=headers, json=json)

    return asyncio.run(send())


def valid_body() -> dict[str, object]:
    return {
        "name": "Example",
        "steps": [
            {
                "identifier": "first",
                "task_type": "test.task",
                "parameters": {"value": 1},
            }
        ],
    }


def test_operator_creates_and_reads_own_draft() -> None:
    app, runtime, service, _ = make_app(frozenset({Role.WORKFLOW_OPERATOR.value}))
    created = request(
        app, "POST", "/api/v1/workflows", runtime.api_credential, json=valid_body()
    )
    assert created.status_code == 201
    assert created.headers["Location"] == f"/api/v1/workflows/{created.json()['id']}"
    assert created.json()["owner_principal_id"] == str(
        service.created[0].owner_principal_id
    )
    assert created.json()["status"] == "draft"
    found = request(app, "GET", created.headers["Location"], runtime.api_credential)
    assert found.status_code == 200
    assert found.json() == created.json()


def test_authorization_precedes_domain_and_task_type_validation() -> None:
    app, runtime, service, validator = make_app(frozenset({Role.VIEWER.value}))
    response = request(
        app, "POST", "/api/v1/workflows", runtime.api_credential, json=valid_body()
    )
    assert response.status_code == 403
    assert validator.calls == 0
    assert service.created == []


def test_worker_credential_is_rejected() -> None:
    app, runtime, service, _ = make_app(frozenset({Role.WORKFLOW_OPERATOR.value}))
    response = request(
        app, "POST", "/api/v1/workflows", runtime.worker_credential, json=valid_body()
    )
    assert response.status_code == 401
    assert service.created == []


def test_transport_and_domain_validation_are_422() -> None:
    app, runtime, service, _ = make_app(frozenset({Role.WORKFLOW_OPERATOR.value}))
    transport = request(
        app,
        "POST",
        "/api/v1/workflows",
        runtime.api_credential,
        json={"name": "Example", "steps": [], "extra": "secret"},
    )
    assert transport.status_code == 422
    assert transport.json()["error"]["details"][0]["code"] == "unexpected_field"
    assert "secret" not in transport.text
    body = valid_body()
    body["steps"] = [body["steps"][0], body["steps"][0]]  # type: ignore[index]
    domain = request(
        app, "POST", "/api/v1/workflows", runtime.api_credential, json=body
    )
    assert domain.status_code == 422
    assert domain.json()["error"]["details"][0]["code"] == "duplicate_step_identifier"
    assert service.created == []


def test_persistence_conflict_and_unavailability_are_normalized() -> None:
    app, runtime, service, _ = make_app(frozenset({Role.WORKFLOW_OPERATOR.value}))
    service.create_error = WorkflowPersistenceConflict()
    conflict = request(
        app, "POST", "/api/v1/workflows", runtime.api_credential, json=valid_body()
    )
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "resource_conflict"
    service.create_error = WorkflowServiceUnavailable()
    unavailable = request(
        app, "POST", "/api/v1/workflows", runtime.api_credential, json=valid_body()
    )
    assert unavailable.status_code == 503


def test_hidden_and_missing_workflows_are_identical() -> None:
    app, runtime, service, _ = make_app(frozenset({Role.VIEWER.value}))
    service.get_error = WorkflowNotFound()
    first = request(app, "GET", f"/api/v1/workflows/{uuid4()}", runtime.api_credential)
    second = request(app, "GET", f"/api/v1/workflows/{uuid4()}", runtime.api_credential)
    assert first.status_code == second.status_code == 404
    first_body, second_body = first.json(), second.json()
    first_body["error"].pop("request_id")
    second_body["error"].pop("request_id")
    assert first_body == second_body
    assert set(first.headers) == set(second.headers)


def test_openapi_exposes_only_task_four_workflow_operations() -> None:
    app, _, _, _ = make_app(frozenset())
    schema = request(app, "GET", "/openapi.json", None).json()
    assert set(schema["paths"]["/api/v1/workflows"]) == {"post"}
    assert set(schema["paths"]["/api/v1/workflows/{workflow_id}"]) == {"get"}
