"""API contract tests for authorized workflow draft routes."""

from __future__ import annotations

import asyncio
import base64
import json
import secrets
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import httpx2
import pytest
from fastapi import FastAPI
from pydantic import SecretStr

from taskforge.api.application import create_app
from taskforge.api.health import ReadinessCoordinator
from taskforge.api.workflows import (
    MAX_CURSOR_LENGTH,
    MAX_DECODED_CURSOR_BYTES,
    _decode_cursor,
    _decode_version_cursor,
    _encode_cursor,
    _encode_version_cursor,
)
from taskforge.identity.authentication import APIAuthenticator, WorkerAuthenticator
from taskforge.identity.authorization import AuthorizationService, Role
from taskforge.identity.credentials import (
    DEFAULT_VERIFIER_ALGORITHM,
    DEFAULT_VERIFIERS,
    CredentialScope,
)
from taskforge.identity.ports import CredentialRecord
from taskforge.settings import Settings
from taskforge.workflows.dag_validation import (
    DAGEdge,
    DAGValidationResult,
    validate_dag,
)
from taskforge.workflows.domain import (
    PublishedWorkflowVersion,
    WorkflowDefinitionStatus,
    WorkflowDraft,
    WorkflowVersionDependency,
    WorkflowVersionSnapshot,
    WorkflowVersionStep,
)
from taskforge.workflows.persistence_ports import (
    StoredWorkflowDraft,
    WorkflowPage,
    WorkflowPageCursor,
    WorkflowSummary,
    WorkflowVersionPage,
    WorkflowVersionPageCursor,
    WorkflowVersionSummary,
)
from taskforge.workflows.service import (
    WorkflowNotFound,
    WorkflowOwnerDisabled,
    WorkflowPersistenceConflict,
    WorkflowServiceUnavailable,
)
from taskforge.workflows.task_types import (
    JSONMapping,
    TaskTypeDefinition,
    TaskTypeRegistry,
    WorkflowValidationError,
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
        self.list_error: Exception | None = None
        self.page = WorkflowPage((), None)
        self.version_page = WorkflowVersionPage((), None)
        self.version_value: WorkflowVersionSnapshot | None = None
        self.publish_error: Exception | None = None
        self.version_list_error: Exception | None = None
        self.version_get_error: Exception | None = None
        self.publish_calls: list[tuple[UUID, UUID]] = []
        self.version_list_calls: list[
            tuple[UUID, UUID, int, WorkflowVersionPageCursor | None]
        ] = []
        self.version_get_calls: list[tuple[UUID, int, UUID]] = []
        self.list_calls: list[tuple[UUID, int, WorkflowPageCursor | None]] = []

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

    async def list(
        self,
        *,
        owner_principal_id: UUID,
        limit: int,
        cursor: WorkflowPageCursor | None = None,
    ) -> WorkflowPage:
        self.list_calls.append((owner_principal_id, limit, cursor))
        if self.list_error:
            raise self.list_error
        return self.page

    async def publish(
        self, workflow_id: UUID, *, owner_principal_id: UUID
    ) -> PublishedWorkflowVersion:
        self.publish_calls.append((workflow_id, owner_principal_id))
        if self.publish_error:
            raise self.publish_error
        return PublishedWorkflowVersion(uuid4(), workflow_id, 1, datetime.now(UTC))

    async def list_versions(
        self,
        workflow_id: UUID,
        *,
        owner_principal_id: UUID,
        limit: int,
        cursor: WorkflowVersionPageCursor | None = None,
    ) -> WorkflowVersionPage:
        self.version_list_calls.append((workflow_id, owner_principal_id, limit, cursor))
        if self.version_list_error:
            raise self.version_list_error
        return self.version_page

    async def get_version(
        self,
        workflow_id: UUID,
        version_number: int,
        *,
        owner_principal_id: UUID,
    ) -> WorkflowVersionSnapshot:
        self.version_get_calls.append((workflow_id, version_number, owner_principal_id))
        if self.version_get_error:
            raise self.version_get_error
        if self.version_value is None:
            raise WorkflowNotFound
        return self.version_value


class Runtime:
    def __init__(
        self,
        caller_id: UUID,
        roles: frozenset[str],
        service: WorkflowServiceStub,
        registry: TaskTypeRegistry,
    ) -> None:
        self.caller_id = caller_id
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
    registry = TaskTypeRegistry(
        (TaskTypeDefinition("test.task", "test-workers", validator),)
    )
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


def summary(owner_id: UUID, *, created_at: datetime | None = None) -> WorkflowSummary:
    now = created_at or datetime.now(UTC)
    return WorkflowSummary(
        uuid4(),
        owner_id,
        "Summary",
        None,
        WorkflowDefinitionStatus.DRAFT,
        now,
        now,
    )


def version_snapshot(
    workflow_id: UUID, version_number: int = 1
) -> WorkflowVersionSnapshot:
    return WorkflowVersionSnapshot(
        uuid4(),
        workflow_id,
        version_number,
        "Published",
        "Historical",
        None,
        datetime.now(UTC),
        (WorkflowVersionStep("first", "test.task", {"value": 1}, None),),
        (WorkflowVersionDependency("first", "second"),),
    )


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


def test_invalid_graph_returns_deterministic_422_before_domain_or_service_work() -> (
    None
):
    app, runtime, service, validator = make_app(
        frozenset({Role.WORKFLOW_OPERATOR.value})
    )
    body = {
        "name": "Cyclic",
        "steps": [
            {"identifier": "first", "task_type": "test.task", "parameters": {}},
            {"identifier": "second", "task_type": "test.task", "parameters": {}},
        ],
        "dependencies": [
            {"predecessor": "first", "successor": "second"},
            {"predecessor": "second", "successor": "first"},
        ],
    }

    response = request(
        app, "POST", "/api/v1/workflows", runtime.api_credential, json=body
    )

    assert response.status_code == 422
    assert response.json()["error"]["details"] == [
        {
            "code": "cycle",
            "path": ["dependencies"],
            "message": "Workflow dependencies must not contain a cycle.",
        }
    ]
    assert validator.calls == 0
    assert service.created == []


def test_validation_endpoint_runs_dag_once_and_never_invokes_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, runtime, _, _ = make_app(frozenset({Role.WORKFLOW_OPERATOR.value}))
    runtime.workflow_service = object()
    calls = 0

    def counted_validate(
        step_identifiers: Sequence[str],
        dependencies: Sequence[DAGEdge],
    ) -> DAGValidationResult:
        nonlocal calls
        calls += 1
        return validate_dag(step_identifiers, dependencies)

    monkeypatch.setattr(
        "taskforge.workflows.domain.validate_dag",
        counted_validate,
    )

    response = request(
        app,
        "POST",
        "/api/v1/workflows/validate",
        runtime.api_credential,
        json=valid_body(),
    )

    assert response.status_code == 200
    assert response.json() == {"valid": True, "topological_order": ["first"]}
    assert calls == 1


def test_validation_endpoint_uses_common_graph_error_envelope() -> None:
    app, runtime, service, validator = make_app(
        frozenset({Role.WORKFLOW_OPERATOR.value})
    )
    body = valid_body()
    body["steps"] = []

    response = request(
        app,
        "POST",
        "/api/v1/workflows/validate",
        runtime.api_credential,
        json=body,
    )

    payload = response.json()
    assert response.status_code == 422
    assert payload["error"]["version"] == "1"
    assert payload["error"]["code"] == "validation_failed"
    assert payload["error"]["request_id"] == response.headers["X-Request-ID"]
    assert payload["error"]["details"] == [
        {
            "code": "empty_graph",
            "path": ["steps"],
            "message": "A workflow must contain at least one step.",
        }
    ]
    assert service.created == []
    assert service.list_calls == []
    assert validator.calls == 0


def test_validation_endpoint_returns_deterministic_branching_order() -> None:
    app, runtime, service, validator = make_app(
        frozenset({Role.WORKFLOW_OPERATOR.value})
    )
    body = {
        "name": "Branching",
        "steps": [
            {"identifier": "finish", "task_type": "test.task", "parameters": {}},
            {"identifier": "right", "task_type": "test.task", "parameters": {}},
            {"identifier": "start", "task_type": "test.task", "parameters": {}},
            {"identifier": "left", "task_type": "test.task", "parameters": {}},
        ],
        "dependencies": [
            {"predecessor": "right", "successor": "finish"},
            {"predecessor": "start", "successor": "right"},
            {"predecessor": "left", "successor": "finish"},
            {"predecessor": "start", "successor": "left"},
        ],
    }

    first = request(
        app,
        "POST",
        "/api/v1/workflows/validate",
        runtime.api_credential,
        json=body,
    )
    body["steps"] = list(reversed(body["steps"]))
    body["dependencies"] = list(reversed(body["dependencies"]))
    second = request(
        app,
        "POST",
        "/api/v1/workflows/validate",
        runtime.api_credential,
        json=body,
    )

    expected = {
        "valid": True,
        "topological_order": ["start", "left", "right", "finish"],
    }
    assert first.json() == second.json() == expected
    assert validator.calls == 8
    assert service.created == []


def test_validation_endpoint_runs_task_type_validation_for_admissible_graph() -> None:
    app, runtime, service, validator = make_app(
        frozenset({Role.WORKFLOW_OPERATOR.value})
    )
    body = valid_body()
    body["steps"][0]["task_type"] = "unknown.task"  # type: ignore[index]

    response = request(
        app,
        "POST",
        "/api/v1/workflows/validate",
        runtime.api_credential,
        json=body,
    )

    assert response.status_code == 422
    assert response.json()["error"]["details"][0]["code"] == "unsupported_task_type"
    assert validator.calls == 0
    assert service.created == []


def test_validation_endpoint_requires_author_permission_before_domain_validation() -> (
    None
):
    app, runtime, service, validator = make_app(frozenset({Role.VIEWER.value}))

    response = request(
        app,
        "POST",
        "/api/v1/workflows/validate",
        runtime.api_credential,
        json=valid_body(),
    )

    assert response.status_code == 403
    assert validator.calls == 0
    assert service.created == []


def test_validation_endpoint_rejects_worker_credentials() -> None:
    app, runtime, service, validator = make_app(
        frozenset({Role.WORKFLOW_OPERATOR.value})
    )

    response = request(
        app,
        "POST",
        "/api/v1/workflows/validate",
        runtime.worker_credential,
        json=valid_body(),
    )

    assert response.status_code == 401
    assert validator.calls == 0
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


def test_openapi_exposes_only_current_workflow_operations() -> None:
    app, _, _, _ = make_app(frozenset())
    schema = request(app, "GET", "/openapi.json", None).json()
    assert set(schema["paths"]["/api/v1/workflows"]) == {"get", "post"}
    assert set(schema["paths"]["/api/v1/workflows/{workflow_id}"]) == {"get"}
    assert set(schema["paths"]["/api/v1/workflows/validate"]) == {"post"}
    assert set(schema["paths"]["/api/v1/workflows/{workflow_id}/versions"]) == {
        "get",
        "post",
    }
    assert set(
        schema["paths"]["/api/v1/workflows/{workflow_id}/versions/{version_number}"]
    ) == {"get"}


def test_operator_publishes_using_existing_application_result() -> None:
    app, runtime, service, _ = make_app(frozenset({Role.WORKFLOW_OPERATOR.value}))
    workflow_id = uuid4()

    response = request(
        app,
        "POST",
        f"/api/v1/workflows/{workflow_id}/versions",
        runtime.api_credential,
    )

    assert response.status_code == 201
    assert response.json()["workflow_definition_id"] == str(workflow_id)
    assert response.headers["Location"].endswith("/versions/1")
    assert service.publish_calls == [(workflow_id, runtime.caller_id)]


def test_publish_authorization_and_errors_are_safe() -> None:
    app, runtime, service, _ = make_app(frozenset({Role.VIEWER.value}))
    path = f"/api/v1/workflows/{uuid4()}/versions"
    forbidden = request(app, "POST", path, runtime.api_credential)
    assert forbidden.status_code == 403
    assert service.publish_calls == []

    app, runtime, service, _ = make_app(frozenset({Role.WORKFLOW_OPERATOR.value}))
    service.publish_error = WorkflowNotFound()
    missing = request(app, "POST", path, runtime.api_credential)
    assert missing.status_code == 404
    service.publish_error = WorkflowPersistenceConflict()
    assert request(app, "POST", path, runtime.api_credential).status_code == 409
    service.publish_error = WorkflowServiceUnavailable("database-secret")
    unavailable = request(app, "POST", path, runtime.api_credential)
    assert unavailable.status_code == 503
    assert "database-secret" not in unavailable.text
    service.publish_error = WorkflowOwnerDisabled()
    assert request(app, "POST", path, runtime.api_credential).status_code == 403


def test_invalid_persisted_draft_publication_returns_structured_422() -> None:
    app, runtime, service, _ = make_app(frozenset({Role.WORKFLOW_OPERATOR.value}))
    service.publish_error = WorkflowValidationError.from_graph(validate_dag((), ()))

    response = request(
        app,
        "POST",
        f"/api/v1/workflows/{uuid4()}/versions",
        runtime.api_credential,
    )

    assert response.status_code == 422
    assert response.json()["error"]["details"] == [
        {
            "code": "empty_graph",
            "path": ["steps"],
            "message": "A workflow must contain at least one step.",
        }
    ]


def test_viewer_lists_versions_with_stable_cursor_contract() -> None:
    app, runtime, service, _ = make_app(frozenset({Role.VIEWER.value}))
    workflow_id = uuid4()
    now = datetime.now(UTC)
    service.version_page = WorkflowVersionPage(
        (WorkflowVersionSummary(uuid4(), 3, now),), WorkflowVersionPageCursor(3)
    )

    first = request(
        app,
        "GET",
        f"/api/v1/workflows/{workflow_id}/versions?limit=1",
        runtime.api_credential,
    )
    encoded = first.json()["page"]["next_cursor"]
    assert _decode_version_cursor(encoded) == WorkflowVersionPageCursor(3)
    service.version_page = WorkflowVersionPage((), None)
    second = request(
        app,
        "GET",
        f"/api/v1/workflows/{workflow_id}/versions?limit=1&cursor={encoded}",
        runtime.api_credential,
    )
    assert second.status_code == 200
    assert service.version_list_calls[-1] == (
        workflow_id,
        runtime.caller_id,
        1,
        WorkflowVersionPageCursor(3),
    )


def test_version_cursor_rejects_invalid_payload_before_service() -> None:
    app, runtime, service, _ = make_app(frozenset({Role.VIEWER.value}))
    workflow_id = uuid4()
    invalid = request(
        app,
        "GET",
        f"/api/v1/workflows/{workflow_id}/versions?cursor=not-base64!",
        runtime.api_credential,
    )
    assert invalid.status_code == 422
    assert service.version_list_calls == []
    assert _decode_version_cursor(
        _encode_version_cursor(WorkflowVersionPageCursor(7))
    ) == (WorkflowVersionPageCursor(7))
    invalid_payloads: tuple[object, ...] = (
        {"v": 2, "version_number": 7},
        {"v": 1, "version_number": 0},
        {"v": 1, "version_number": True},
        {"v": 1, "version_number": 7, "extra": 1},
    )
    for payload in invalid_payloads:
        encoded = (
            base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode()
        )
        with pytest.raises(ValueError, match="invalid cursor"):
            _decode_version_cursor(encoded)
    oversized = (
        base64.urlsafe_b64encode(b"x" * (MAX_DECODED_CURSOR_BYTES + 1))
        .rstrip(b"=")
        .decode()
    )
    with pytest.raises(ValueError, match="invalid cursor"):
        _decode_version_cursor(oversized)


def test_viewer_retrieves_complete_version_by_number_only() -> None:
    app, runtime, service, _ = make_app(frozenset({Role.VIEWER.value}))
    workflow_id = uuid4()
    service.version_value = version_snapshot(workflow_id, 2)

    response = request(
        app,
        "GET",
        f"/api/v1/workflows/{workflow_id}/versions/2",
        runtime.api_credential,
    )

    assert response.status_code == 200
    assert response.json()["steps"][0]["identifier"] == "first"
    assert response.json()["dependencies"] == [
        {"predecessor": "first", "successor": "second"}
    ]
    assert service.version_get_calls == [(workflow_id, 2, runtime.caller_id)]
    assert (
        request(
            app,
            "GET",
            f"/api/v1/workflows/{workflow_id}/versions/0",
            runtime.api_credential,
        ).status_code
        == 422
    )


def test_version_reads_normalize_missing_and_unavailable() -> None:
    app, runtime, service, _ = make_app(frozenset({Role.VIEWER.value}))
    workflow_id = uuid4()
    service.version_list_error = WorkflowNotFound()
    missing_list = request(
        app,
        "GET",
        f"/api/v1/workflows/{workflow_id}/versions",
        runtime.api_credential,
    )
    service.version_get_error = WorkflowNotFound()
    missing_detail = request(
        app,
        "GET",
        f"/api/v1/workflows/{workflow_id}/versions/1",
        runtime.api_credential,
    )
    assert missing_list.status_code == missing_detail.status_code == 404

    service.version_list_error = WorkflowServiceUnavailable("database-secret")
    unavailable = request(
        app,
        "GET",
        f"/api/v1/workflows/{workflow_id}/versions",
        runtime.api_credential,
    )
    assert unavailable.status_code == 503
    assert "database-secret" not in unavailable.text
    service.version_get_error = WorkflowServiceUnavailable("database-secret")
    unavailable_detail = request(
        app,
        "GET",
        f"/api/v1/workflows/{workflow_id}/versions/1",
        runtime.api_credential,
    )
    assert unavailable_detail.status_code == 503
    assert "database-secret" not in unavailable_detail.text


def test_viewer_lists_only_service_owner_scope_with_default_page_size() -> None:
    app, runtime, service, _ = make_app(frozenset({Role.VIEWER.value}))
    item = summary(runtime.caller_id)
    service.page = WorkflowPage((item,), None)

    response = request(app, "GET", "/api/v1/workflows", runtime.api_credential)

    assert response.status_code == 200
    assert response.json()["page"] == {"limit": 50, "next_cursor": None}
    assert response.json()["items"][0]["id"] == str(item.id)
    assert "owner_principal_id" not in response.json()["items"][0]
    assert "steps" not in response.json()["items"][0]
    assert service.list_calls == [(item.owner_principal_id, 50, None)]


def test_returned_cursor_round_trips_utc_microseconds_and_next_page() -> None:
    app, runtime, service, _ = make_app(frozenset({Role.VIEWER.value}))
    owner_id = runtime.caller_id
    timestamp = datetime.fromisoformat("2026-08-04T12:34:56.123456-07:00")
    item = summary(owner_id, created_at=timestamp)
    service.page = WorkflowPage((item,), WorkflowPageCursor(timestamp, item.id))

    first = request(app, "GET", "/api/v1/workflows?limit=1", runtime.api_credential)
    encoded = first.json()["page"]["next_cursor"]
    decoded = _decode_cursor(encoded)
    assert (
        decoded.created_at.isoformat(timespec="microseconds")
        == "2026-08-04T19:34:56.123456+00:00"
    )
    service.page = WorkflowPage((), None)
    second = request(
        app,
        "GET",
        f"/api/v1/workflows?limit=1&cursor={encoded}",
        runtime.api_credential,
    )
    assert second.status_code == 200
    assert service.list_calls[-1] == (owner_id, 1, decoded)


def test_invalid_and_oversized_cursors_fail_before_service_access() -> None:
    app, runtime, service, _ = make_app(frozenset({Role.VIEWER.value}))
    malformed = request(
        app, "GET", "/api/v1/workflows?cursor=not-base64!", runtime.api_credential
    )
    oversized = request(
        app,
        "GET",
        f"/api/v1/workflows?cursor={'a' * (MAX_CURSOR_LENGTH + 1)}",
        runtime.api_credential,
    )
    oversized_decoded_payload = (
        base64.urlsafe_b64encode(b"x" * (MAX_DECODED_CURSOR_BYTES + 1))
        .rstrip(b"=")
        .decode()
    )
    decoded_oversized = request(
        app,
        "GET",
        f"/api/v1/workflows?cursor={oversized_decoded_payload}",
        runtime.api_credential,
    )
    assert (
        malformed.status_code
        == oversized.status_code
        == decoded_oversized.status_code
        == 422
    )
    assert malformed.json()["error"]["details"][0]["code"] == "invalid_cursor"
    assert service.list_calls == []


def test_page_limits_are_bounded_and_list_outages_are_safe() -> None:
    app, runtime, service, _ = make_app(frozenset({Role.VIEWER.value}))
    assert (
        request(
            app, "GET", "/api/v1/workflows?limit=1", runtime.api_credential
        ).status_code
        == 200
    )
    assert (
        request(
            app, "GET", "/api/v1/workflows?limit=100", runtime.api_credential
        ).status_code
        == 200
    )
    assert (
        request(
            app, "GET", "/api/v1/workflows?limit=0", runtime.api_credential
        ).status_code
        == 422
    )
    assert (
        request(
            app, "GET", "/api/v1/workflows?limit=101", runtime.api_credential
        ).status_code
        == 422
    )
    service.list_error = WorkflowServiceUnavailable("database-secret")
    unavailable = request(app, "GET", "/api/v1/workflows", runtime.api_credential)
    assert unavailable.status_code == 503
    assert "database-secret" not in unavailable.text


def test_list_authorization_precedes_service_access() -> None:
    app, runtime, service, _ = make_app(frozenset())
    forbidden = request(app, "GET", "/api/v1/workflows", runtime.api_credential)
    worker = request(app, "GET", "/api/v1/workflows", runtime.worker_credential)
    assert forbidden.status_code == 403
    assert worker.status_code == 401
    assert service.list_calls == []


def test_cursor_codec_rejects_unsupported_and_naive_payloads() -> None:
    cursor = WorkflowPageCursor(datetime.now(UTC), uuid4())
    encoded = _encode_cursor(cursor)
    assert len(encoded) <= MAX_CURSOR_LENGTH
    for payload in (
        {"v": 2, "created_at": "2026-08-04T00:00:00.000000Z", "id": str(uuid4())},
        {"v": 1, "created_at": "2026-08-04T00:00:00.000000", "id": str(uuid4())},
    ):
        raw = (
            base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode()
        )
        with pytest.raises(ValueError, match="invalid cursor"):
            _decode_cursor(raw)
