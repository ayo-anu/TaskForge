"""API contract tests for workflow-run start and inspection routes."""

from __future__ import annotations

import base64
import secrets
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import httpx2
import pytest
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
from taskforge.runs.domain import (
    CreatedWorkflowRun,
    ExplicitWorkflowVersion,
    InspectedTaskRun,
    InspectedWorkflowRun,
    InvalidWorkflowRunIdempotencyKey,
    LatestWorkflowVersion,
    TaskRunStatus,
    WorkflowRunIdempotencyConflict,
    WorkflowRunInput,
    WorkflowRunStatus,
    WorkflowRunTargetUnavailable,
    WorkflowVersionSelection,
)
from taskforge.runs.service import (
    TaskRunNotFound,
    WorkflowRunNotFound,
    WorkflowRunPersistenceConflict,
    WorkflowRunServiceUnavailable,
    WorkflowRunTargetNotFound,
    WorkflowVersionUnavailable,
)
from taskforge.settings import Settings
from taskforge.workflows.domain import WorkflowDefinitionStatus
from taskforge.workflows.task_types import TaskTypeRegistry


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
        return self.record if credential_id == self.record.credential_id else None

    async def find_worker_credential(
        self, credential_id: UUID
    ) -> CredentialRecord | None:
        return self.record if credential_id == self.record.credential_id else None


class RoleRepository:
    def __init__(self, roles: frozenset[str]) -> None:
        self.roles = roles

    async def find_role_names(self, principal_id: UUID) -> frozenset[str]:
        del principal_id
        return self.roles


class WorkflowServiceUnused:
    pass


class RunServiceStub:
    def __init__(self, principal_id: UUID) -> None:
        self.principal_id = principal_id
        self.calls: list[tuple[Any, ...]] = []
        self.error: Exception | None = None
        self.run_id, self.workflow_id, self.version_id = uuid4(), uuid4(), uuid4()
        self.task_id = uuid4()
        now = datetime.now(UTC)
        self.created = CreatedWorkflowRun(
            self.run_id,
            self.workflow_id,
            self.version_id,
            2,
            principal_id,
            WorkflowRunStatus.PENDING,
            now,
            1,
            1,
            0,
        )
        self.inspected = InspectedWorkflowRun(
            self.run_id,
            self.workflow_id,
            self.version_id,
            2,
            principal_id,
            WorkflowRunStatus.PENDING,
            now,
            now,
        )
        self.task = InspectedTaskRun(
            self.task_id,
            self.run_id,
            self.version_id,
            "root",
            TaskRunStatus.RUNNABLE,
            now,
            now,
        )

    def _raise(self) -> None:
        if self.error is not None:
            raise self.error

    async def create_run(
        self,
        workflow_id: UUID,
        *,
        owner_principal_id: UUID,
        requested_by_principal_id: UUID,
        selection: WorkflowVersionSelection,
        input_snapshot: WorkflowRunInput,
    ) -> CreatedWorkflowRun:
        self.calls.append(
            (
                "create",
                workflow_id,
                owner_principal_id,
                requested_by_principal_id,
                selection,
                input_snapshot,
            )
        )
        self._raise()
        return self.created

    async def create_idempotent_run(
        self,
        workflow_id: UUID,
        *,
        owner_principal_id: UUID,
        requested_by_principal_id: UUID,
        selection: WorkflowVersionSelection,
        input_snapshot: WorkflowRunInput,
        idempotency_key: object,
    ) -> CreatedWorkflowRun:
        self.calls.append(
            (
                "create_idempotent",
                workflow_id,
                owner_principal_id,
                requested_by_principal_id,
                selection,
                input_snapshot,
                idempotency_key,
            )
        )
        self._raise()
        return self.created

    async def get_run(
        self, run_id: UUID, *, owner_principal_id: UUID
    ) -> InspectedWorkflowRun:
        self.calls.append(("get_run", run_id, owner_principal_id))
        self._raise()
        return self.inspected

    async def list_task_runs(
        self, run_id: UUID, *, owner_principal_id: UUID
    ) -> tuple[InspectedTaskRun, ...]:
        self.calls.append(("list_tasks", run_id, owner_principal_id))
        self._raise()
        return (self.task,)

    async def get_task_run(
        self, task_run_id: UUID, *, owner_principal_id: UUID
    ) -> InspectedTaskRun:
        self.calls.append(("get_task", task_run_id, owner_principal_id))
        self._raise()
        return self.task


class Runtime:
    def __init__(self, roles: frozenset[str]) -> None:
        self.principal_id = uuid4()
        api_value, api_record = make_credential(CredentialScope.API, self.principal_id)
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
        self.workflow_service = WorkflowServiceUnused()
        self.workflow_run_service: Any = RunServiceStub(self.principal_id)
        self.task_type_registry = TaskTypeRegistry(())

    async def close(self) -> None:
        pass


def make_credential(
    scope: CredentialScope, identity_id: UUID
) -> tuple[str, CredentialRecord]:
    credential_id = uuid4()
    secret = secrets.token_bytes(32)
    prefix = "tf_api_v1" if scope is CredentialScope.API else "tf_worker_v1"
    value = (
        f"{prefix}.{credential_id}."
        f"{base64.urlsafe_b64encode(secret).rstrip(b'=').decode()}"
    )
    return value, CredentialRecord(
        credential_id,
        identity_id,
        DEFAULT_VERIFIERS.encode(secret, algorithm=DEFAULT_VERIFIER_ALGORITHM),
        False,
        False,
        False,
    )


def make_app(roles: frozenset[str]) -> tuple[Any, Runtime, RunServiceStub]:
    runtime = Runtime(roles)
    settings = Settings(
        postgres_password=SecretStr("test"), rabbitmq_password=SecretStr("test")
    )
    app = create_app(
        settings,
        ReadinessCoordinator((AlwaysReady(),), timeout_seconds=0.05),
        runtime,
    )
    return app, runtime, runtime.workflow_run_service


def request(
    app: Any,
    method: str,
    path: str,
    token: str,
    **kwargs: Any,
) -> httpx2.Response:
    async def execute() -> httpx2.Response:
        transport = httpx2.ASGITransport(app=app)
        async with app.router.lifespan_context(app):
            async with httpx2.AsyncClient(
                transport=transport, base_url="http://testserver"
            ) as client:
                return await client.request(
                    method,
                    path,
                    headers={
                        "Authorization": f"Bearer {token}",
                        **kwargs.pop("headers", {}),
                    },
                    **kwargs,
                )

    import asyncio

    return asyncio.run(execute())


def test_keyless_latest_start_uses_non_idempotent_path_and_minimal_response() -> None:
    app, runtime, service = make_app(frozenset({Role.WORKFLOW_OPERATOR.value}))
    workflow_id = uuid4()

    response = request(
        app,
        "POST",
        f"/api/v1/workflows/{workflow_id}/runs",
        runtime.api_credential,
        json={"payload": {"value": 1}},
    )

    assert response.status_code == 201
    assert service.calls[0][0] == "create"
    assert isinstance(service.calls[0][4], LatestWorkflowVersion)
    assert response.headers["Location"].endswith(str(service.run_id))
    assert set(response.json()) == {
        "id",
        "workflow_definition_id",
        "workflow_version_id",
        "version_number",
        "requested_by_principal_id",
        "status",
        "created_at",
    }


def test_supplied_key_uses_idempotent_explicit_path_without_normalization() -> None:
    app, runtime, service = make_app(frozenset({Role.WORKFLOW_OPERATOR.value}))
    key = "Key!With.Punctuation-123"

    response = request(
        app,
        "POST",
        f"/api/v1/workflows/{uuid4()}/runs",
        runtime.api_credential,
        headers={"Idempotency-Key": key},
        json={"version_number": 4},
    )

    assert response.status_code == 201
    assert service.calls[0][0] == "create_idempotent"
    assert isinstance(service.calls[0][4], ExplicitWorkflowVersion)
    assert service.calls[0][6] == key


@pytest.mark.parametrize("value", (True, False, "2", 2.0, 0, -1))
def test_version_number_rejects_non_strict_or_non_positive_values_safely(
    value: object,
) -> None:
    app, runtime, service = make_app(frozenset({Role.WORKFLOW_OPERATOR.value}))

    response = request(
        app,
        "POST",
        f"/api/v1/workflows/{uuid4()}/runs",
        runtime.api_credential,
        json={"version_number": value},
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_failed"
    assert response.json()["error"]["details"] == [
        {
            "code": "invalid_request_value",
            "path": ["body", "version_number"],
            "message": "Field value is invalid.",
        }
    ]
    assert service.calls == []


def test_idempotency_conflict_and_invalid_key_are_safe() -> None:
    app, runtime, service = make_app(frozenset({Role.WORKFLOW_OPERATOR.value}))
    service.error = WorkflowRunIdempotencyConflict()
    path = f"/api/v1/workflows/{uuid4()}/runs"

    conflict = request(
        app,
        "POST",
        path,
        runtime.api_credential,
        headers={"Idempotency-Key": "abcdefghijklmnop"},
        json={},
    )
    service.error = InvalidWorkflowRunIdempotencyKey()
    invalid = request(
        app,
        "POST",
        path,
        runtime.api_credential,
        headers={"Idempotency-Key": "secret too short"},
        json={},
    )

    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "idempotency_conflict"
    assert "abcdefghijklmnop" not in conflict.text
    assert invalid.status_code == 422
    assert "secret too short" not in invalid.text


def test_run_and_task_inspection_are_owner_scoped_and_minimal() -> None:
    app, runtime, service = make_app(frozenset({Role.VIEWER.value}))

    run = request(
        app,
        "GET",
        f"/api/v1/workflow-runs/{service.run_id}",
        runtime.api_credential,
    )
    tasks = request(
        app,
        "GET",
        f"/api/v1/workflow-runs/{service.run_id}/tasks",
        runtime.api_credential,
    )
    task = request(
        app,
        "GET",
        f"/api/v1/task-runs/{service.task_id}",
        runtime.api_credential,
    )

    assert run.status_code == tasks.status_code == task.status_code == 200
    assert "task_count" not in run.json()
    assert run.json()["failure_reason"] is None
    assert tasks.json()["items"][0]["failure_reason"] is None
    assert task.json()["failure_reason"] is None
    assert tasks.json()["items"][0]["step_identifier"] == "root"
    assert service.calls == [
        ("get_run", service.run_id, runtime.principal_id),
        ("list_tasks", service.run_id, runtime.principal_id),
        ("get_task", service.task_id, runtime.principal_id),
    ]


def test_unknown_and_cross_owner_inspection_share_not_found_contract() -> None:
    app, runtime, service = make_app(frozenset({Role.VIEWER.value}))
    service.error = WorkflowRunNotFound()
    missing = request(
        app,
        "GET",
        f"/api/v1/workflow-runs/{uuid4()}",
        runtime.api_credential,
    )
    hidden = request(
        app,
        "GET",
        f"/api/v1/workflow-runs/{uuid4()}",
        runtime.api_credential,
    )
    service.error = TaskRunNotFound()
    task = request(
        app,
        "GET",
        f"/api/v1/task-runs/{uuid4()}",
        runtime.api_credential,
    )

    assert missing.status_code == hidden.status_code == task.status_code == 404
    assert missing.json()["error"]["code"] == "resource_not_found"
    assert missing.json()["error"]["message"] == hidden.json()["error"]["message"]


def test_viewer_cannot_start_and_worker_credential_is_rejected() -> None:
    app, runtime, service = make_app(frozenset({Role.VIEWER.value}))
    path = f"/api/v1/workflows/{uuid4()}/runs"

    forbidden = request(app, "POST", path, runtime.api_credential, json={})
    worker = request(app, "POST", path, runtime.worker_credential, json={})

    assert forbidden.status_code == 403
    assert worker.status_code == 401
    assert service.calls == []


def test_start_failures_use_safe_stable_statuses() -> None:
    app, runtime, service = make_app(frozenset({Role.WORKFLOW_OPERATOR.value}))
    path = f"/api/v1/workflows/{uuid4()}/runs"

    for error, expected_status in (
        (WorkflowRunTargetNotFound(), 404),
        (WorkflowVersionUnavailable(), 404),
        (
            WorkflowRunTargetUnavailable(WorkflowDefinitionStatus.DISABLED),
            409,
        ),
        (WorkflowRunPersistenceConflict(), 409),
        (WorkflowRunServiceUnavailable(), 503),
    ):
        service.error = error
        response = request(
            app,
            "POST",
            path,
            runtime.api_credential,
            json={},
        )
        assert response.status_code == expected_status


def test_inspection_unavailability_is_normalized_without_metadata() -> None:
    app, runtime, service = make_app(frozenset({Role.VIEWER.value}))
    service.error = WorkflowRunServiceUnavailable()

    responses = (
        request(
            app,
            "GET",
            f"/api/v1/workflow-runs/{uuid4()}",
            runtime.api_credential,
        ),
        request(
            app,
            "GET",
            f"/api/v1/workflow-runs/{uuid4()}/tasks",
            runtime.api_credential,
        ),
        request(
            app,
            "GET",
            f"/api/v1/task-runs/{uuid4()}",
            runtime.api_credential,
        ),
    )

    assert all(response.status_code == 503 for response in responses)
    assert all(
        response.json()["error"]["code"] == "service_unavailable"
        for response in responses
    )
