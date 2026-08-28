"""FastAPI authorization adapter tests without external services."""

from __future__ import annotations

import asyncio
import base64
import secrets
from typing import Annotated
from uuid import UUID, uuid4

import httpx2
from fastapi import Depends, FastAPI
from pydantic import SecretStr

from taskforge.api.application import create_app
from taskforge.api.authorization import require_permission
from taskforge.api.health import ReadinessCoordinator
from taskforge.identity.authentication import APIAuthenticator, WorkerAuthenticator
from taskforge.identity.authorization import (
    AuthorizationContext,
    AuthorizationService,
    Permission,
    Role,
)
from taskforge.identity.credentials import (
    DEFAULT_VERIFIER_ALGORITHM,
    DEFAULT_VERIFIERS,
)
from taskforge.identity.ports import CredentialRecord
from taskforge.rate_limits import AllowAllRateLimiter
from taskforge.settings import Settings


class AlwaysReady:
    async def start(self) -> None:
        pass

    async def is_ready(self) -> bool:
        return True

    async def close(self) -> None:
        pass


class APIRepository:
    def __init__(self, record: CredentialRecord | None) -> None:
        self.record = record

    async def find_api_credential(self, credential_id: UUID) -> CredentialRecord | None:
        if self.record and self.record.credential_id == credential_id:
            return self.record
        return None


class WorkerRepository:
    async def find_worker_credential(
        self, credential_id: UUID
    ) -> CredentialRecord | None:
        return None


class RoleRepository:
    def __init__(
        self,
        roles: frozenset[str],
        error: Exception | None = None,
    ) -> None:
        self.roles = roles
        self.error = error

    async def find_role_names(self, principal_id: UUID) -> frozenset[str]:
        if self.error:
            raise self.error
        return self.roles


class Runtime:
    def __init__(
        self,
        api_authenticator: APIAuthenticator,
        authorization_service: AuthorizationService,
    ) -> None:
        self.api_authenticator = api_authenticator
        self.rate_limiter = AllowAllRateLimiter()
        self.worker_authenticator = WorkerAuthenticator(
            WorkerRepository(), timeout_seconds=0.05
        )
        self.authorization_service = authorization_service

    async def close(self) -> None:
        pass


def credential_value(credential_id: UUID, secret: bytes) -> str:
    encoded = base64.urlsafe_b64encode(secret).rstrip(b"=").decode("ascii")
    return f"tf_api_v1.{credential_id}.{encoded}"


def make_app(
    roles: frozenset[str],
    *,
    role_error: Exception | None = None,
) -> tuple[FastAPI, str]:
    credential_id, principal_id = uuid4(), uuid4()
    secret = secrets.token_bytes(32)
    record = CredentialRecord(
        credential_id=credential_id,
        identity_id=principal_id,
        credential_verifier=DEFAULT_VERIFIERS.encode(
            secret, algorithm=DEFAULT_VERIFIER_ALGORITHM
        ),
        revoked=False,
        expired=False,
        identity_disabled=False,
    )
    runtime = Runtime(
        APIAuthenticator(APIRepository(record), timeout_seconds=0.05),
        AuthorizationService(
            RoleRepository(roles, role_error),
            timeout_seconds=0.05,
        ),
    )
    settings = Settings(
        postgres_password=SecretStr("postgres-test-secret"),
        rabbitmq_password=SecretStr("rabbitmq-test-secret"),
    )
    app = create_app(
        settings=settings,
        readiness=ReadinessCoordinator(AlwaysReady(), timeout_seconds=0.05),
        authentication=runtime,
    )

    @app.get("/test-view")
    async def test_view(
        context: Annotated[
            AuthorizationContext,
            Depends(require_permission(Permission.VIEW)),
        ],
    ) -> dict[str, bool]:
        return {"allowed": context.allows(Permission.VIEW)}

    return app, credential_value(credential_id, secret)


def request(app: FastAPI, credential: str | None) -> httpx2.Response:
    async def send() -> httpx2.Response:
        headers = {"Authorization": f"Bearer {credential}"} if credential else {}
        transport = httpx2.ASGITransport(app=app)
        async with app.router.lifespan_context(app):
            async with httpx2.AsyncClient(
                transport=transport,
                base_url="http://testserver",
            ) as client:
                return await client.get("/test-view", headers=headers)

    return asyncio.run(send())


def test_permission_dependency_allows_matching_role() -> None:
    app, credential = make_app(frozenset({Role.VIEWER.value}))

    response = request(app, credential)

    assert response.status_code == 200
    assert response.json() == {"allowed": True}


def test_no_role_returns_generic_forbidden_response() -> None:
    app, credential = make_app(frozenset())

    response = request(app, credential)

    assert response.status_code == 403
    assert response.json()["error"]["code"] == "forbidden"
    assert credential not in response.text


def test_missing_authentication_remains_unauthorized() -> None:
    app, _ = make_app(frozenset({Role.ADMINISTRATOR.value}))

    response = request(app, None)

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "authentication_required"


def test_role_persistence_failure_is_safely_unavailable() -> None:
    sensitive_detail = "postgresql://user:secret@internal-host:5432/taskforge"
    app, credential = make_app(
        frozenset(),
        role_error=RuntimeError(sensitive_detail),
    )

    response = request(app, credential)

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "service_unavailable"
    assert sensitive_detail not in response.text
    assert credential not in response.text
