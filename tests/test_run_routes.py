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
from taskforge.identity.authorization import AuthorizationService, OwnerFilter, Role
from taskforge.identity.credentials import (
    DEFAULT_VERIFIER_ALGORITHM,
    DEFAULT_VERIFIERS,
    CredentialScope,
)
from taskforge.identity.ports import CredentialRecord
from taskforge.retries.domain import (
    InspectedRetryEvent,
    InspectedRetryEventPage,
    RetryEventCursor,
    RetryEventType,
)
from taskforge.runs.domain import (
    AcceptedWorkflowRunCancellation,
    CreatedFailedSubgraphWorkflowReplay,
    CreatedFullWorkflowReplay,
    CreatedWorkflowRun,
    ExplicitWorkflowVersion,
    FailedSubgraphReplaySelectionInvalid,
    InspectedTaskRun,
    InspectedWorkflowRun,
    InspectedWorkflowRunCancellation,
    InvalidFailedSubgraphReplayRequest,
    InvalidWorkflowRunCancellationIdempotencyKey,
    InvalidWorkflowRunIdempotencyKey,
    LatestWorkflowVersion,
    TaskRunStatus,
    WorkflowReplayIdempotencyConflict,
    WorkflowReplayMode,
    WorkflowRunCancellationCaveat,
    WorkflowRunCancellationIdempotencyConflict,
    WorkflowRunCancellationOutcome,
    WorkflowRunCancellationResult,
    WorkflowRunIdempotencyConflict,
    WorkflowRunInput,
    WorkflowRunReplayNotEligible,
    WorkflowRunStatus,
    WorkflowRunTargetUnavailable,
    WorkflowVersionSelection,
)
from taskforge.runs.service import (
    TaskRunNotFound,
    WorkflowRunCancellationInvariantError,
    WorkflowRunNotFound,
    WorkflowRunPersistenceConflict,
    WorkflowRunReplayInvariantError,
    WorkflowRunServiceUnavailable,
    WorkflowRunTargetNotFound,
    WorkflowVersionUnavailable,
)
from taskforge.settings import Settings
from taskforge.worker.results import TaskExecutionFailureKind
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
            attempt_count=2,
            retry_attempt_count=1,
            maximum_attempts=4,
            retry_eligible_at=now,
            latest_failure_kind=TaskExecutionFailureKind.HANDLER_EXCEPTION,
        )
        event_id, failed_id, retry_id = uuid4(), uuid4(), uuid4()
        self.retry_page = InspectedRetryEventPage(
            (
                InspectedRetryEvent(
                    event_id,
                    self.run_id,
                    self.task_id,
                    RetryEventType.RETRY_SCHEDULED,
                    failed_id,
                    1,
                    retry_id,
                    2,
                    now,
                    None,
                    TaskExecutionFailureKind.HANDLER_EXCEPTION,
                    now,
                ),
            ),
            RetryEventCursor(self.task_id, now, event_id),
        )
        self.cancellation = WorkflowRunCancellationResult(
            self.run_id,
            WorkflowRunCancellationOutcome.NEWLY_ACCEPTED,
            WorkflowRunStatus.CANCELLING,
            AcceptedWorkflowRunCancellation(
                principal_id,
                "maintenance",
                now,
            ),
        )
        self.full_replay = CreatedFullWorkflowReplay(
            uuid4(), WorkflowReplayMode.FULL, self.created
        )
        self.failed_replay = CreatedFailedSubgraphWorkflowReplay(
            self.full_replay.source_workflow_run_id,
            WorkflowReplayMode.FAILED_SUBGRAPH,
            ("alpha", "beta"),
            ("alpha", "beta", "downstream"),
            self.created,
        )

    def _raise(self) -> None:
        if self.error is not None:
            raise self.error

    async def create_run(
        self,
        workflow_id: UUID,
        *,
        owner_filter: OwnerFilter,
        requested_by_principal_id: UUID,
        selection: WorkflowVersionSelection,
        input_snapshot: WorkflowRunInput,
        correlation_id: UUID | None = None,
    ) -> CreatedWorkflowRun:
        self.calls.append(
            (
                "create",
                workflow_id,
                owner_filter,
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
        owner_filter: OwnerFilter,
        requested_by_principal_id: UUID,
        selection: WorkflowVersionSelection,
        input_snapshot: WorkflowRunInput,
        idempotency_key: object,
        correlation_id: UUID | None = None,
    ) -> CreatedWorkflowRun:
        self.calls.append(
            (
                "create_idempotent",
                workflow_id,
                owner_filter,
                requested_by_principal_id,
                selection,
                input_snapshot,
                idempotency_key,
            )
        )
        self._raise()
        return self.created

    async def create_full_replay(
        self,
        source_workflow_run_id: UUID,
        owner_filter: object,
        *,
        requested_by_principal_id: UUID,
        correlation_id: UUID,
    ) -> CreatedFullWorkflowReplay:
        self.calls.append(
            (
                "create_full_replay",
                source_workflow_run_id,
                owner_filter,
                requested_by_principal_id,
                correlation_id,
            )
        )
        self._raise()
        return CreatedFullWorkflowReplay(
            source_workflow_run_id, WorkflowReplayMode.FULL, self.created
        )

    async def create_idempotent_full_replay(
        self,
        source_workflow_run_id: UUID,
        owner_filter: object,
        *,
        requested_by_principal_id: UUID,
        idempotency_key: object,
        correlation_id: UUID,
    ) -> CreatedFullWorkflowReplay:
        self.calls.append(
            (
                "create_idempotent_full_replay",
                source_workflow_run_id,
                owner_filter,
                requested_by_principal_id,
                idempotency_key,
                correlation_id,
            )
        )
        self._raise()
        return CreatedFullWorkflowReplay(
            source_workflow_run_id, WorkflowReplayMode.FULL, self.created
        )

    async def create_failed_subgraph_replay(
        self,
        source_workflow_run_id: UUID,
        owner_filter: object,
        *,
        requested_by_principal_id: UUID,
        failed_step_identifiers: object,
        correlation_id: UUID,
    ) -> CreatedFailedSubgraphWorkflowReplay:
        self.calls.append(
            (
                "create_failed_subgraph_replay",
                source_workflow_run_id,
                owner_filter,
                requested_by_principal_id,
                failed_step_identifiers,
                correlation_id,
            )
        )
        self._raise()
        return CreatedFailedSubgraphWorkflowReplay(
            source_workflow_run_id,
            WorkflowReplayMode.FAILED_SUBGRAPH,
            ("alpha", "beta"),
            ("alpha", "beta", "downstream"),
            self.created,
        )

    async def create_idempotent_failed_subgraph_replay(
        self,
        source_workflow_run_id: UUID,
        owner_filter: object,
        *,
        requested_by_principal_id: UUID,
        failed_step_identifiers: object,
        idempotency_key: object,
        correlation_id: UUID,
    ) -> CreatedFailedSubgraphWorkflowReplay:
        self.calls.append(
            (
                "create_idempotent_failed_subgraph_replay",
                source_workflow_run_id,
                owner_filter,
                requested_by_principal_id,
                failed_step_identifiers,
                idempotency_key,
                correlation_id,
            )
        )
        self._raise()
        return CreatedFailedSubgraphWorkflowReplay(
            source_workflow_run_id,
            WorkflowReplayMode.FAILED_SUBGRAPH,
            ("alpha", "beta"),
            ("alpha", "beta", "downstream"),
            self.created,
        )

    async def get_run(
        self, run_id: UUID, *, owner_filter: OwnerFilter
    ) -> InspectedWorkflowRun:
        self.calls.append(("get_run", run_id, owner_filter))
        self._raise()
        return self.inspected

    async def cancel_run(
        self,
        run_id: UUID,
        owner_filter: object,
        *,
        requested_by_principal_id: UUID,
        idempotency_key: str | None,
        reason: str | None,
        correlation_id: UUID | None = None,
    ) -> WorkflowRunCancellationResult:
        self.calls.append(
            (
                "cancel",
                run_id,
                owner_filter,
                requested_by_principal_id,
                idempotency_key,
                reason,
            )
        )
        self._raise()
        return self.cancellation

    async def list_task_runs(
        self, run_id: UUID, *, owner_filter: OwnerFilter
    ) -> tuple[InspectedTaskRun, ...]:
        self.calls.append(("list_tasks", run_id, owner_filter))
        self._raise()
        return (self.task,)

    async def get_task_run(
        self, task_run_id: UUID, *, owner_filter: OwnerFilter
    ) -> InspectedTaskRun:
        self.calls.append(("get_task", task_run_id, owner_filter))
        self._raise()
        return self.task

    async def list_retry_events(
        self,
        task_run_id: UUID,
        *,
        owner_filter: OwnerFilter,
        limit: int,
        cursor: RetryEventCursor | None,
    ) -> InspectedRetryEventPage:
        self.calls.append(
            ("list_retry_events", task_run_id, owner_filter, limit, cursor)
        )
        self._raise()
        return self.retry_page


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
        ReadinessCoordinator(AlwaysReady(), timeout_seconds=0.05),
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


def test_full_replay_route_is_owner_scoped_and_returns_safe_creation_facts() -> None:
    app, runtime, service = make_app(frozenset({Role.WORKFLOW_OPERATOR.value}))
    source_id = uuid4()

    response = request(
        app,
        "POST",
        f"/api/v1/workflow-runs/{source_id}/replay",
        runtime.api_credential,
        json={"mode": "full"},
    )

    assert response.status_code == 201
    assert response.headers["Location"].endswith(str(service.run_id))
    call = service.calls[0]
    assert call[0:2] == ("create_full_replay", source_id)
    assert call[2].principal_id == runtime.principal_id
    assert call[3] == runtime.principal_id
    assert isinstance(call[4], UUID)
    assert response.json() == {
        "id": str(service.run_id),
        "workflow_definition_id": str(service.workflow_id),
        "workflow_version_id": str(service.version_id),
        "version_number": 2,
        "requested_by_principal_id": str(runtime.principal_id),
        "status": "pending",
        "created_at": service.created.created_at.isoformat().replace("+00:00", "Z"),
        "source_workflow_run_id": str(source_id),
        "mode": "full",
        "failed_step_identifiers": None,
    }


def test_failed_replay_route_uses_optional_key_and_hides_derived_closure() -> None:
    app, runtime, service = make_app(frozenset({Role.WORKFLOW_OPERATOR.value}))
    source_id = uuid4()

    response = request(
        app,
        "POST",
        f"/api/v1/workflow-runs/{source_id}/replay",
        runtime.api_credential,
        headers={"Idempotency-Key": "replay-route-key-0001"},
        json={
            "mode": "failed_subgraph",
            "failed_step_identifiers": ["beta", "alpha"],
        },
    )

    assert response.status_code == 201
    call = service.calls[0]
    assert call[0] == "create_idempotent_failed_subgraph_replay"
    assert call[4] == ["beta", "alpha"]
    assert call[5] == "replay-route-key-0001"
    assert response.json()["failed_step_identifiers"] == ["alpha", "beta"]
    assert "selected_step_identifiers" not in response.json()


@pytest.mark.parametrize(
    "body",
    (
        {"mode": "full", "failed_step_identifiers": ["alpha"]},
        {"mode": "failed_subgraph", "failed_step_identifiers": []},
        {"mode": "unknown"},
    ),
)
def test_replay_route_rejects_structurally_invalid_bodies(body: object) -> None:
    app, runtime, service = make_app(frozenset({Role.WORKFLOW_OPERATOR.value}))

    response = request(
        app,
        "POST",
        f"/api/v1/workflow-runs/{uuid4()}/replay",
        runtime.api_credential,
        json=body,
    )

    assert response.status_code == 422
    assert service.calls == []


def test_replay_route_enforces_operator_authentication_boundary() -> None:
    app, runtime, service = make_app(frozenset({Role.VIEWER.value}))
    path = f"/api/v1/workflow-runs/{uuid4()}/replay"

    forbidden = request(
        app, "POST", path, runtime.api_credential, json={"mode": "full"}
    )
    worker = request(
        app, "POST", path, runtime.worker_credential, json={"mode": "full"}
    )
    unauthenticated = request(app, "POST", path, "", json={"mode": "full"})

    assert forbidden.status_code == 403
    assert worker.status_code == 401
    assert unauthenticated.status_code == 401
    assert service.calls == []


def test_replay_route_uses_unrestricted_owner_filter_for_administrator() -> None:
    app, runtime, service = make_app(frozenset({Role.ADMINISTRATOR.value}))

    response = request(
        app,
        "POST",
        f"/api/v1/workflow-runs/{uuid4()}/replay",
        runtime.api_credential,
        json={"mode": "full"},
    )

    assert response.status_code == 201
    assert service.calls[0][2].unrestricted is True


def test_replay_route_maps_scoped_missing_source_to_confidential_not_found() -> None:
    app, runtime, service = make_app(frozenset({Role.WORKFLOW_OPERATOR.value}))
    service.error = WorkflowRunNotFound()

    response = request(
        app,
        "POST",
        f"/api/v1/workflow-runs/{uuid4()}/replay",
        runtime.api_credential,
        json={"mode": "full"},
    )

    assert response.status_code == 404
    assert "owner" not in response.text.lower()


@pytest.mark.parametrize(
    ("error", "body", "expected_status", "expected_code"),
    (
        (
            InvalidWorkflowRunIdempotencyKey(),
            {"mode": "full"},
            422,
            "validation_failed",
        ),
        (
            InvalidFailedSubgraphReplayRequest(),
            {"mode": "failed_subgraph", "failed_step_identifiers": ["root"]},
            422,
            "validation_failed",
        ),
        (
            WorkflowRunReplayNotEligible(),
            {"mode": "full"},
            409,
            "workflow_run_not_replayable",
        ),
        (
            FailedSubgraphReplaySelectionInvalid(),
            {"mode": "failed_subgraph", "failed_step_identifiers": ["root"]},
            409,
            "failed_subgraph_replay_invalid",
        ),
        (
            WorkflowReplayIdempotencyConflict(),
            {"mode": "full"},
            409,
            "idempotency_conflict",
        ),
        (
            WorkflowRunReplayInvariantError(),
            {"mode": "full"},
            500,
            "internal_error",
        ),
    ),
)
def test_replay_route_maps_domain_errors_safely(
    error: Exception,
    body: dict[str, object],
    expected_status: int,
    expected_code: str,
) -> None:
    app, runtime, service = make_app(frozenset({Role.WORKFLOW_OPERATOR.value}))
    service.error = error

    response = request(
        app,
        "POST",
        f"/api/v1/workflow-runs/{uuid4()}/replay",
        runtime.api_credential,
        headers={"Idempotency-Key": "valid-replay-key-0001"},
        json=body,
    )

    assert response.status_code == expected_status
    assert response.json()["error"]["code"] == expected_code


def test_authorized_cancellation_is_owner_scoped_and_returns_canonical_request() -> (
    None
):
    app, runtime, service = make_app(frozenset({Role.WORKFLOW_OPERATOR.value}))

    response = request(
        app,
        "POST",
        f"/api/v1/workflow-runs/{service.run_id}/cancel",
        runtime.api_credential,
        headers={"Idempotency-Key": "cancel-request-0001"},
        json={"reason": " maintenance "},
    )

    assert response.status_code == 200
    assert service.cancellation.accepted_request is not None
    assert response.json() == {
        "workflow_run_id": str(service.run_id),
        "outcome": "newly_accepted",
        "status": "cancelling",
        "requested_by_principal_id": str(runtime.principal_id),
        "reason": "maintenance",
        "requested_at": service.cancellation.accepted_request.requested_at.isoformat().replace(
            "+00:00", "Z"
        ),
    }
    call = service.calls[0]
    assert call[0:2] == ("cancel", service.run_id)
    assert call[2].principal_id == runtime.principal_id
    assert call[3:] == (
        runtime.principal_id,
        "cancel-request-0001",
        " maintenance ",
    )


def test_cancellation_conceals_canonical_metadata_for_noncanonical_request() -> None:
    app, runtime, service = make_app(frozenset({Role.WORKFLOW_OPERATOR.value}))
    service.cancellation = WorkflowRunCancellationResult(
        service.run_id,
        WorkflowRunCancellationOutcome.ALREADY_CANCELLING,
        WorkflowRunStatus.CANCELLING,
    )

    response = request(
        app,
        "POST",
        f"/api/v1/workflow-runs/{service.run_id}/cancel",
        runtime.api_credential,
        headers={"Idempotency-Key": "different-key-0001"},
        json={"reason": "different"},
    )

    assert response.status_code == 200
    assert response.json()["outcome"] == "already_cancelling"
    assert response.json()["requested_by_principal_id"] is None
    assert response.json()["reason"] is None
    assert response.json()["requested_at"] is None


def test_cancellation_requires_operator_permission_and_idempotency_key() -> None:
    viewer_app, viewer_runtime, viewer_service = make_app(
        frozenset({Role.VIEWER.value})
    )
    operator_app, operator_runtime, operator_service = make_app(
        frozenset({Role.WORKFLOW_OPERATOR.value})
    )
    path = f"/api/v1/workflow-runs/{viewer_service.run_id}/cancel"

    forbidden = request(
        viewer_app,
        "POST",
        path,
        viewer_runtime.api_credential,
        headers={"Idempotency-Key": "cancel-request-0001"},
        json={},
    )
    operator_service.error = InvalidWorkflowRunCancellationIdempotencyKey()
    invalid = request(
        operator_app,
        "POST",
        f"/api/v1/workflow-runs/{operator_service.run_id}/cancel",
        operator_runtime.api_credential,
        json={},
    )

    assert forbidden.status_code == 403
    assert viewer_service.calls == []
    assert invalid.status_code == 422
    assert invalid.json()["error"]["code"] == "validation_failed"
    assert invalid.json()["error"]["details"][0]["code"] == "invalid_idempotency_key"
    assert invalid.json()["error"]["details"][0]["path"] == [
        "header",
        "Idempotency-Key",
    ]
    assert operator_service.calls[-1][4] is None


def test_short_cancellation_idempotency_key_has_structured_422() -> None:
    app, runtime, service = make_app(frozenset({Role.WORKFLOW_OPERATOR.value}))
    service.error = InvalidWorkflowRunCancellationIdempotencyKey()

    response = request(
        app,
        "POST",
        f"/api/v1/workflow-runs/{service.run_id}/cancel",
        runtime.api_credential,
        headers={"Idempotency-Key": "too-short"},
        json={},
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_failed"
    assert response.json()["error"]["details"] == [
        {
            "code": "invalid_idempotency_key",
            "path": ["header", "Idempotency-Key"],
            "message": "Idempotency-Key is invalid.",
        }
    ]
    assert service.calls[-1][4] == "too-short"
    assert "too-short" not in response.text


@pytest.mark.parametrize(
    ("error", "expected_status", "expected_code"),
    (
        (WorkflowRunNotFound(), 404, "resource_not_found"),
        (WorkflowRunCancellationIdempotencyConflict(), 409, "idempotency_conflict"),
        (
            WorkflowRunCancellationInvariantError(),
            500,
            "internal_error",
        ),
        (WorkflowRunServiceUnavailable(), 503, "service_unavailable"),
    ),
)
def test_cancellation_failures_use_safe_stable_contracts(
    error: Exception, expected_status: int, expected_code: str
) -> None:
    app, runtime, service = make_app(frozenset({Role.WORKFLOW_OPERATOR.value}))
    service.error = error

    response = request(
        app,
        "POST",
        f"/api/v1/workflow-runs/{service.run_id}/cancel",
        runtime.api_credential,
        headers={"Idempotency-Key": "secret-cancel-key"},
        json={},
    )

    assert response.status_code == expected_status
    assert response.json()["error"]["code"] == expected_code
    assert "secret-cancel-key" not in response.text


def test_terminal_cancellation_outcome_is_conflict() -> None:
    app, runtime, service = make_app(frozenset({Role.WORKFLOW_OPERATOR.value}))
    service.cancellation = WorkflowRunCancellationResult(
        service.run_id,
        WorkflowRunCancellationOutcome.TERMINAL_STATE_WON,
        WorkflowRunStatus.SUCCEEDED,
    )

    response = request(
        app,
        "POST",
        f"/api/v1/workflow-runs/{service.run_id}/cancel",
        runtime.api_credential,
        headers={"Idempotency-Key": "cancel-request-0001"},
        json={},
    )

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "workflow_run_not_cancellable"


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
    assert (
        task.json()
        | {
            "attempt_count": 2,
            "retry_attempt_count": 1,
            "maximum_attempts": 4,
            "latest_failure_kind": "handler_exception",
        }
        == task.json()
    )
    assert service.calls == [
        ("get_run", service.run_id, OwnerFilter.only(runtime.principal_id)),
        ("list_tasks", service.run_id, OwnerFilter.only(runtime.principal_id)),
        ("get_task", service.task_id, OwnerFilter.only(runtime.principal_id)),
    ]


def test_run_inspection_exposes_only_minimal_cancellation_history() -> None:
    app, runtime, service = make_app(frozenset({Role.VIEWER.value}))
    request_time = datetime.now(UTC)
    service.inspected = InspectedWorkflowRun(
        service.run_id,
        service.workflow_id,
        service.version_id,
        2,
        runtime.principal_id,
        WorkflowRunStatus.CANCELLED,
        request_time,
        request_time,
        cancellation=InspectedWorkflowRunCancellation(
            runtime.principal_id,
            "maintenance",
            request_time,
            2,
            tuple(WorkflowRunCancellationCaveat),
        ),
    )

    response = request(
        app,
        "GET",
        f"/api/v1/workflow-runs/{service.run_id}",
        runtime.api_credential,
    )

    assert response.status_code == 200
    assert response.json()["cancellation"] == {
        "requested_by_principal_id": str(runtime.principal_id),
        "reason": "maintenance",
        "requested_at": request_time.isoformat().replace("+00:00", "Z"),
        "recovered_cancellation_count": 2,
        "caveats": [caveat.value for caveat in WorkflowRunCancellationCaveat],
    }
    assert "idempotency_key_digest" not in response.text
    assert "request_fingerprint" not in response.text
    assert "terminal_task_outcomes" not in response.text
    assert "unsettled_task_count" not in response.text


def test_retry_history_is_owner_scoped_paginated_and_cursor_bound() -> None:
    app, runtime, service = make_app(frozenset({Role.VIEWER.value}))
    path = f"/api/v1/task-runs/{service.task_id}/retry-events"

    first = request(app, "GET", path, runtime.api_credential)

    assert first.status_code == 200
    body = first.json()
    assert body["items"][0]["event_type"] == "retry_scheduled"
    assert body["items"][0]["failure_kind"] == "handler_exception"
    assert "dispatch_id" not in body["items"][0]
    assert body["page"]["limit"] == 50
    cursor = body["page"]["next_cursor"]
    assert cursor is not None

    second = request(
        app,
        "GET",
        path,
        runtime.api_credential,
        params={"cursor": cursor, "limit": 10},
    )
    wrong_task = request(
        app,
        "GET",
        f"/api/v1/task-runs/{uuid4()}/retry-events",
        runtime.api_credential,
        params={"cursor": cursor},
    )

    assert second.status_code == 200
    assert service.calls[-1][0] == "list_retry_events"
    assert service.calls[-1][-1] is not None
    assert wrong_task.status_code == 422
    assert wrong_task.json()["error"]["details"][0]["code"] == "invalid_cursor"


def test_retry_history_absent_and_cross_owner_tasks_share_not_found_contract() -> None:
    app, runtime, service = make_app(frozenset({Role.VIEWER.value}))
    service.error = TaskRunNotFound()

    missing = request(
        app,
        "GET",
        f"/api/v1/task-runs/{uuid4()}/retry-events",
        runtime.api_credential,
    )
    hidden = request(
        app,
        "GET",
        f"/api/v1/task-runs/{uuid4()}/retry-events",
        runtime.api_credential,
    )

    assert missing.status_code == hidden.status_code == 404
    for field in ("code", "message", "version"):
        assert missing.json()["error"][field] == hidden.json()["error"][field]


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


def test_administrator_uses_unrestricted_scope_for_start_and_inspection() -> None:
    app, runtime, service = make_app(frozenset({Role.ADMINISTRATOR.value}))
    started = request(
        app,
        "POST",
        f"/api/v1/workflows/{uuid4()}/runs",
        runtime.api_credential,
        json={},
    )
    inspected = request(
        app,
        "GET",
        f"/api/v1/workflow-runs/{service.run_id}",
        runtime.api_credential,
    )
    assert started.status_code == 201
    assert inspected.status_code == 200
    assert service.calls[0][2] == OwnerFilter.all_owners()
    assert service.calls[0][3] == runtime.principal_id
    assert service.calls[1][2] == OwnerFilter.all_owners()
