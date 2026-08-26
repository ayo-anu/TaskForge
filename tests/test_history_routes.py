"""Authorization, filtering, cursor, and redacted history route contracts."""

from __future__ import annotations

import asyncio
import base64
import secrets
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx2
from fastapi import FastAPI
from pydantic import SecretStr

from taskforge.api.application import create_app
from taskforge.api.health import ReadinessCoordinator
from taskforge.history.domain import (
    HistoryItem,
    HistoryPage,
    HistoryRecordType,
)
from taskforge.history.service import HistoryNotFound
from taskforge.identity.authentication import APIAuthenticator, WorkerAuthenticator
from taskforge.identity.authorization import AuthorizationService, OwnerFilter, Role
from taskforge.identity.credentials import DEFAULT_VERIFIER_ALGORITHM, DEFAULT_VERIFIERS
from taskforge.identity.ports import CredentialRecord
from taskforge.settings import Settings


class AlwaysReady:
    async def start(self) -> None: ...
    async def is_ready(self) -> bool:
        return True

    async def close(self) -> None: ...


class CredentialRepository:
    def __init__(self, record: CredentialRecord) -> None:
        self.record = record

    async def find_api_credential(self, credential_id: UUID) -> CredentialRecord | None:
        return self.record if credential_id == self.record.credential_id else None

    async def find_worker_credential(self, credential_id: UUID) -> None:
        del credential_id


class WorkerCredentialRepository:
    async def find_worker_credential(self, credential_id: UUID) -> None:
        del credential_id


class RoleRepository:
    def __init__(self, role: Role) -> None:
        self.role = role

    async def find_role_names(self, principal_id: UUID) -> frozenset[str]:
        del principal_id
        return frozenset({self.role.value})


class HistoryServiceStub:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []
        self.not_found = False

    async def list(self, *args: object, **kwargs: object) -> HistoryPage:
        self.calls.append((*args, kwargs))
        if self.not_found:
            raise HistoryNotFound
        now = datetime(2026, 8, 26, tzinfo=UTC)
        identifier = uuid4()
        return HistoryPage(
            (
                HistoryItem(
                    HistoryRecordType.AUDIT_RECORD,
                    now,
                    10,
                    str(identifier),
                    "opaque-correlation",
                    {
                        "id": identifier,
                        "actor_kind": "system",
                        "api_principal_id": None,
                        "worker_identity_id": None,
                        "worker_session_id": None,
                        "system_component": "test",
                        "action": "workflow.publish",
                        "outcome": "accepted",
                        "reason_code": None,
                        "resource_type": "workflow",
                        "resource_id": uuid4(),
                        "diagnostic_provenance": {},
                    },
                ),
            ),
            None,
        )


class Runtime:
    def __init__(
        self,
        authenticator: APIAuthenticator,
        authorization: AuthorizationService,
        history: HistoryServiceStub,
    ) -> None:
        self.api_authenticator = authenticator
        self.worker_authenticator = WorkerAuthenticator(
            WorkerCredentialRepository(), timeout_seconds=0.05
        )
        self.authorization_service = authorization
        self.history_service = history

    async def close(self) -> None: ...


def _app(role: Role) -> tuple[FastAPI, str, HistoryServiceStub, UUID]:
    credential_id, principal_id = uuid4(), uuid4()
    secret = secrets.token_bytes(32)
    record = CredentialRecord(
        credential_id,
        principal_id,
        DEFAULT_VERIFIERS.encode(secret, algorithm=DEFAULT_VERIFIER_ALGORITHM),
        False,
        False,
        False,
    )
    repository = CredentialRepository(record)
    history = HistoryServiceStub()
    runtime = Runtime(
        APIAuthenticator(repository, timeout_seconds=0.05),
        AuthorizationService(RoleRepository(role), timeout_seconds=0.05),
        history,
    )
    settings = Settings(
        postgres_password=SecretStr("test"), rabbitmq_password=SecretStr("test")
    )
    app = create_app(
        settings=settings,
        readiness=ReadinessCoordinator((AlwaysReady(),), timeout_seconds=0.05),
        authentication=runtime,
    )
    encoded = base64.urlsafe_b64encode(secret).rstrip(b"=").decode()
    return app, f"tf_api_v1.{credential_id}.{encoded}", history, principal_id


def _get(app: FastAPI, credential: str | None, path: str) -> httpx2.Response:
    async def send() -> httpx2.Response:
        headers = {"Authorization": f"Bearer {credential}"} if credential else {}
        async with app.router.lifespan_context(app):
            async with httpx2.AsyncClient(
                transport=httpx2.ASGITransport(app=app), base_url="http://test"
            ) as client:
                return await client.get(path, headers=headers)

    return asyncio.run(send())


def test_global_audit_requires_administrator_and_accepts_filters() -> None:
    app, credential, history, _ = _app(Role.ADMINISTRATOR)
    response = _get(
        app,
        credential,
        "/api/v1/audit-records?action=workflow.publish&actor_kind=system&system_component=test",
    )
    assert response.status_code == 200
    assert response.json()["items"][0]["data"]["action"] == "workflow.publish"
    kwargs = history.calls[0][-1]
    assert isinstance(kwargs, dict)
    assert kwargs["filters"].action.value == "workflow.publish"
    assert history.calls[0][2] == OwnerFilter.all_owners()


def test_viewer_cannot_access_global_audit() -> None:
    app, credential, history, _ = _app(Role.VIEWER)
    response = _get(app, credential, "/api/v1/audit-records")
    assert response.status_code == 403
    assert history.calls == []


def test_resource_history_is_owner_scoped_and_not_found_is_confidential() -> None:
    app, credential, history, principal_id = _app(Role.VIEWER)
    history.not_found = True
    resource_id = uuid4()
    response = _get(app, credential, f"/api/v1/workflow-runs/{resource_id}/history")
    assert response.status_code == 404
    assert history.calls[0][2] == OwnerFilter.only(principal_id)
    assert str(resource_id) not in response.text


def test_anonymous_history_request_is_rejected_before_query() -> None:
    app, _, history, _ = _app(Role.ADMINISTRATOR)
    response = _get(app, None, "/api/v1/audit-records")
    assert response.status_code == 401
    assert history.calls == []


def test_invalid_cursor_and_page_bound_are_rejected_without_query() -> None:
    app, credential, history, _ = _app(Role.ADMINISTRATOR)
    invalid_cursor = _get(app, credential, "/api/v1/audit-records?cursor=not-a-cursor")
    assert invalid_cursor.status_code == 422
    assert invalid_cursor.json()["error"]["details"][0]["code"] == "invalid_cursor"
    invalid_limit = _get(app, credential, "/api/v1/audit-records?limit=101")
    assert invalid_limit.status_code == 422
    assert history.calls == []


def test_exactly_six_history_routes_are_registered() -> None:
    from taskforge.api.history import router

    paths = {
        route.path
        for route in router.routes
        if getattr(route, "path", "").endswith("history")
        or getattr(route, "path", "") == "/api/v1/audit-records"
    }
    assert paths == {
        "/api/v1/audit-records",
        "/api/v1/workflows/{resource_id}/history",
        "/api/v1/workflow-runs/{resource_id}/history",
        "/api/v1/task-runs/{resource_id}/history",
        "/api/v1/workers/{resource_id}/history",
        "/api/v1/dead-letters/{resource_id}/history",
    }
