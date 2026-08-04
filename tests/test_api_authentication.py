"""FastAPI adapter tests with no real external services."""

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
from taskforge.api.authentication import (
    authenticate_api_principal,
    authenticate_worker,
)
from taskforge.api.health import ReadinessCoordinator
from taskforge.identity.authentication import (
    APIAuthenticator,
    AuthenticatedAPIPrincipal,
    AuthenticatedWorker,
    WorkerAuthenticator,
)
from taskforge.identity.credentials import (
    DEFAULT_VERIFIER_ALGORITHM,
    DEFAULT_VERIFIERS,
    CredentialScope,
)
from taskforge.identity.ports import CredentialRecord
from taskforge.settings import Settings


class AlwaysReady:
    async def start(self) -> None:
        pass

    async def is_ready(self) -> bool:
        return True

    async def close(self) -> None:
        pass


class APIRepository:
    def __init__(
        self,
        record: CredentialRecord | None,
        error: Exception | None = None,
    ) -> None:
        self.record = record
        self.error = error

    async def find_api_credential(self, credential_id: UUID) -> CredentialRecord | None:
        if self.error:
            raise self.error
        if self.record is not None and self.record.credential_id == credential_id:
            return self.record
        return None


class WorkerRepository:
    def __init__(self, record: CredentialRecord | None) -> None:
        self.record = record

    async def find_worker_credential(
        self, credential_id: UUID
    ) -> CredentialRecord | None:
        if self.record is not None and self.record.credential_id == credential_id:
            return self.record
        return None


class FakeRuntime:
    def __init__(
        self,
        api_authenticator: APIAuthenticator,
        worker_authenticator: WorkerAuthenticator,
    ) -> None:
        self.api_authenticator = api_authenticator
        self.worker_authenticator = worker_authenticator
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def credential_value(scope: CredentialScope, credential_id: UUID, secret: bytes) -> str:
    prefix = "tf_api_v1" if scope is CredentialScope.API else "tf_worker_v1"
    encoded = base64.urlsafe_b64encode(secret).rstrip(b"=").decode("ascii")
    return f"{prefix}.{credential_id}.{encoded}"


def credential_record(
    credential_id: UUID,
    identity_id: UUID,
    secret: bytes,
    *,
    revoked: bool = False,
) -> CredentialRecord:
    return CredentialRecord(
        credential_id=credential_id,
        identity_id=identity_id,
        credential_verifier=DEFAULT_VERIFIERS.encode(
            secret,
            algorithm=DEFAULT_VERIFIER_ALGORITHM,
        ),
        revoked=revoked,
        expired=False,
        identity_disabled=False,
    )


def make_app(
    api_repository: APIRepository,
    worker_repository: WorkerRepository,
) -> tuple[FastAPI, FakeRuntime]:
    runtime = FakeRuntime(
        APIAuthenticator(api_repository, timeout_seconds=0.05),
        WorkerAuthenticator(worker_repository, timeout_seconds=0.05),
    )
    settings = Settings(
        postgres_password=SecretStr("postgres-test-secret"),
        rabbitmq_password=SecretStr("rabbitmq-test-secret"),
    )
    app = create_app(
        settings=settings,
        readiness=ReadinessCoordinator((AlwaysReady(),), timeout_seconds=0.05),
        authentication=runtime,
    )

    @app.get("/test-api")
    async def test_api_route(
        identity: Annotated[
            AuthenticatedAPIPrincipal,
            Depends(authenticate_api_principal),
        ],
    ) -> dict[str, str]:
        return {"identity_id": str(identity.principal_id)}

    @app.get("/test-worker")
    async def test_worker_route(
        identity: Annotated[
            AuthenticatedWorker,
            Depends(authenticate_worker),
        ],
    ) -> dict[str, str]:
        return {"identity_id": str(identity.worker_identity_id)}

    return app, runtime


def request(
    app: FastAPI,
    path: str,
    credential: str | None = None,
) -> httpx2.Response:
    async def send() -> httpx2.Response:
        headers = {"Authorization": f"Bearer {credential}"} if credential else {}
        transport = httpx2.ASGITransport(app=app)
        async with app.router.lifespan_context(app):
            async with httpx2.AsyncClient(
                transport=transport,
                base_url="http://testserver",
            ) as client:
                return await client.get(path, headers=headers)

    return asyncio.run(send())


def test_http_adapter_authenticates_each_scope_and_closes_runtime() -> None:
    api_id, worker_id = uuid4(), uuid4()
    api_credential_id, worker_credential_id = uuid4(), uuid4()
    api_secret, worker_secret = secrets.token_bytes(32), secrets.token_bytes(32)
    app, runtime = make_app(
        APIRepository(credential_record(api_credential_id, api_id, api_secret)),
        WorkerRepository(
            credential_record(worker_credential_id, worker_id, worker_secret)
        ),
    )

    api_response = request(
        app,
        "/test-api",
        credential_value(CredentialScope.API, api_credential_id, api_secret),
    )
    worker_response = request(
        app,
        "/test-worker",
        credential_value(CredentialScope.WORKER, worker_credential_id, worker_secret),
    )

    assert api_response.json() == {"identity_id": str(api_id)}
    assert worker_response.json() == {"identity_id": str(worker_id)}
    assert runtime.closed is True


def test_missing_malformed_unknown_invalid_revoked_and_wrong_scope_are_uniform() -> (
    None
):
    credential_id, identity_id = uuid4(), uuid4()
    secret = secrets.token_bytes(32)
    app, _ = make_app(
        APIRepository(credential_record(credential_id, identity_id, secret)),
        WorkerRepository(None),
    )
    presented_values = (
        None,
        "malformed",
        credential_value(CredentialScope.API, uuid4(), secrets.token_bytes(32)),
        credential_value(CredentialScope.API, credential_id, secrets.token_bytes(32)),
        credential_value(CredentialScope.WORKER, credential_id, secret),
    )

    responses = [request(app, "/test-api", value) for value in presented_values]

    assert {response.status_code for response in responses} == {401}
    assert {
        (
            response.json()["error"]["version"],
            response.json()["error"]["code"],
            response.json()["error"]["message"],
        )
        for response in responses
    } == {("1", "authentication_required", "Authentication is required.")}
    assert all(
        response.headers["www-authenticate"] == "Bearer" for response in responses
    )

    revoked_app, _ = make_app(
        APIRepository(
            credential_record(credential_id, identity_id, secret, revoked=True)
        ),
        WorkerRepository(None),
    )
    revoked_response = request(
        revoked_app,
        "/test-api",
        credential_value(CredentialScope.API, credential_id, secret),
    )
    assert revoked_response.status_code == 401
    assert revoked_response.json()["error"]["code"] == "authentication_required"


def test_repository_failure_returns_safe_service_unavailable() -> None:
    sensitive_detail = "postgresql://user:secret@internal-host:5432/taskforge"
    app, _ = make_app(
        APIRepository(None, error=RuntimeError(sensitive_detail)),
        WorkerRepository(None),
    )
    raw_credential = credential_value(
        CredentialScope.API,
        uuid4(),
        secrets.token_bytes(32),
    )

    response = request(app, "/test-api", raw_credential)

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "service_unavailable"
    assert sensitive_detail not in response.text
    assert raw_credential not in response.text


def test_operational_routes_remain_unauthenticated() -> None:
    app, _ = make_app(APIRepository(None), WorkerRepository(None))

    assert request(app, "/health").status_code == 200
    assert request(app, "/ready").status_code == 200
