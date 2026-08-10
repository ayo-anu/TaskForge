"""Worker-facing registration API contract tests."""

from __future__ import annotations

import asyncio
import base64
import secrets
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import httpx2
from pydantic import SecretStr

from taskforge.api.application import create_app
from taskforge.api.health import ReadinessCoordinator
from taskforge.identity.authentication import (
    APIAuthenticator,
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
from taskforge.worker.domain import (
    InvalidWorkerRegistration,
    RegisteredWorkerSession,
    WorkerHealthProjection,
    WorkerRegistrationIssue,
)
from taskforge.worker.service import (
    ConflictingWorkerHeartbeatReplay,
    StaleWorkerHeartbeat,
    WorkerHeartbeatGap,
    WorkerHeartbeatRejected,
    WorkerHeartbeatServiceUnavailable,
    WorkerRegistrationConflict,
    WorkerRegistrationRejected,
    WorkerRegistrationServiceUnavailable,
    WorkerSessionInactive,
    WorkerSessionUnavailable,
)


class AlwaysReady:
    async def start(self) -> None:
        pass

    async def is_ready(self) -> bool:
        return True

    async def close(self) -> None:
        pass


class CredentialRepository:
    def __init__(self, record: CredentialRecord | None) -> None:
        self.record = record

    async def find_api_credential(self, credential_id: UUID) -> CredentialRecord | None:
        if self.record is not None and self.record.credential_id == credential_id:
            return self.record
        return None

    async def find_worker_credential(
        self, credential_id: UUID
    ) -> CredentialRecord | None:
        if self.record is not None and self.record.credential_id == credential_id:
            return self.record
        return None


class RegistrationServiceStub:
    def __init__(self, registered: RegisteredWorkerSession) -> None:
        self.registered = registered
        self.error: Exception | None = None
        self.calls: list[tuple[AuthenticatedWorker, tuple[str, ...]]] = []

    async def register(
        self,
        authenticated_worker: AuthenticatedWorker,
        capabilities: tuple[str, ...],
    ) -> RegisteredWorkerSession:
        self.calls.append((authenticated_worker, capabilities))
        if self.error is not None:
            raise self.error
        return self.registered


class HeartbeatServiceStub:
    def __init__(self) -> None:
        self.error: Exception | None = None
        self.calls: list[tuple[AuthenticatedWorker, UUID, int, bool]] = []
        now = datetime(2026, 8, 10, tzinfo=UTC)
        self.health = WorkerHealthProjection(uuid4(), 1, now, True, now)

    async def heartbeat(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        *,
        sequence: int,
        accepting_work: bool,
    ) -> WorkerHealthProjection:
        self.calls.append(
            (authenticated_worker, worker_session_id, sequence, accepting_work)
        )
        if self.error is not None:
            raise self.error
        return self.health


class Runtime:
    def __init__(
        self,
        worker_record: CredentialRecord,
        service: RegistrationServiceStub,
    ) -> None:
        repository = CredentialRepository(worker_record)
        self.api_authenticator = APIAuthenticator(repository, timeout_seconds=0.05)
        self.worker_authenticator = WorkerAuthenticator(
            repository, timeout_seconds=0.05
        )
        self.worker_registration_service: Any = service
        self.worker_heartbeat_service: Any = HeartbeatServiceStub()

    async def close(self) -> None:
        pass


def make_credential(
    scope: CredentialScope,
    identity_id: UUID,
) -> tuple[str, CredentialRecord]:
    credential_id = uuid4()
    secret = secrets.token_bytes(32)
    encoded = base64.urlsafe_b64encode(secret).rstrip(b"=").decode("ascii")
    prefix = "tf_worker_v1" if scope is CredentialScope.WORKER else "tf_api_v1"
    value = f"{prefix}.{credential_id}.{encoded}"
    return value, CredentialRecord(
        credential_id=credential_id,
        identity_id=identity_id,
        credential_verifier=DEFAULT_VERIFIERS.encode(
            secret, algorithm=DEFAULT_VERIFIER_ALGORITHM
        ),
        revoked=False,
        expired=False,
        identity_disabled=False,
    )


def make_app() -> tuple[Any, str, str, RegistrationServiceStub, AuthenticatedWorker]:
    worker_identity_id = uuid4()
    worker_value, worker_record = make_credential(
        CredentialScope.WORKER, worker_identity_id
    )
    api_value, _ = make_credential(CredentialScope.API, uuid4())
    registered = RegisteredWorkerSession(
        uuid4(), datetime(2026, 8, 10, tzinfo=UTC), ("documents", "email")
    )
    service = RegistrationServiceStub(registered)
    runtime = Runtime(worker_record, service)
    app = create_app(
        settings=Settings(
            postgres_password=SecretStr("postgres-test-secret"),
            rabbitmq_password=SecretStr("rabbitmq-test-secret"),
        ),
        readiness=ReadinessCoordinator((AlwaysReady(),), timeout_seconds=0.05),
        authentication=runtime,
    )
    app.state.test_heartbeat_service = runtime.worker_heartbeat_service
    return (
        app,
        worker_value,
        api_value,
        service,
        AuthenticatedWorker(worker_identity_id, worker_record.credential_id),
    )


def post(app: Any, body: object, credential: str | None) -> httpx2.Response:
    async def send() -> httpx2.Response:
        headers = {"Authorization": f"Bearer {credential}"} if credential else {}
        transport = httpx2.ASGITransport(app=app)
        async with app.router.lifespan_context(app):
            async with httpx2.AsyncClient(
                transport=transport, base_url="http://testserver"
            ) as client:
                return await client.post(
                    "/api/v1/worker-sessions", json=body, headers=headers
                )

    return asyncio.run(send())


def test_registration_uses_only_authenticated_worker_and_returns_narrow_contract() -> (
    None
):
    app, worker_value, _, service, authenticated = make_app()

    response = post(
        app,
        {"capabilities": ["email", "documents"]},
        worker_value,
    )

    assert response.status_code == 201
    assert response.json() == {
        "id": str(service.registered.id),
        "registered_at": "2026-08-10T00:00:00Z",
        "capabilities": ["documents", "email"],
    }
    assert response.headers["location"] == (
        f"/api/v1/worker-sessions/{service.registered.id}"
    )
    assert service.calls == [(authenticated, ("email", "documents"))]


def test_registration_rejects_identity_session_availability_and_metadata_fields() -> (
    None
):
    forbidden_fields: dict[str, object] = {
        "worker_identity_id": str(uuid4()),
        "credential_id": str(uuid4()),
        "session_id": str(uuid4()),
        "registered_at": "2026-08-10T00:00:00Z",
        "accepting_work": True,
        "metadata": {},
    }
    for field, value in forbidden_fields.items():
        app, worker_value, _, service, _ = make_app()
        response = post(
            app,
            {"capabilities": [], field: value},
            worker_value,
        )
        assert response.status_code == 422
        assert response.json()["error"]["details"][0]["code"] == "unexpected_field"
        assert service.calls == []


def test_registration_requires_worker_scoped_authentication() -> None:
    for credential_kind in ("missing", "api"):
        app, _, api_value, service, _ = make_app()
        response = post(
            app,
            {"capabilities": []},
            None if credential_kind == "missing" else api_value,
        )
        assert response.status_code == 401
        assert response.json()["error"]["code"] == "authentication_required"
        assert service.calls == []


def test_registration_maps_safe_service_failures() -> None:
    cases = (
        (WorkerRegistrationRejected(), 401, "authentication_required"),
        (WorkerRegistrationConflict(), 409, "resource_conflict"),
        (
            WorkerRegistrationServiceUnavailable(),
            503,
            "service_unavailable",
        ),
    )
    for error, status_code, code in cases:
        app, worker_value, _, service, _ = make_app()
        service.error = error
        response = post(app, {"capabilities": []}, worker_value)
        assert response.status_code == status_code
        assert response.json()["error"]["code"] == code


def test_registration_maps_domain_validation_details() -> None:
    app, worker_value, _, service, _ = make_app()
    service.error = InvalidWorkerRegistration(
        (
            WorkerRegistrationIssue(
                "unknown_capability",
                ("capabilities", 0),
                "Capability is not registered.",
            ),
        )
    )

    response = post(app, {"capabilities": ["unknown"]}, worker_value)

    assert response.status_code == 422
    assert response.json()["error"]["details"] == [
        {
            "code": "unknown_capability",
            "path": ["capabilities", 0],
            "message": "Capability is not registered.",
        }
    ]


def test_heartbeat_uses_path_session_authenticated_worker_and_narrow_body() -> None:
    app, worker_value, _, _, authenticated = make_app()
    heartbeat_service: HeartbeatServiceStub = app.state.test_heartbeat_service
    session_id = heartbeat_service.health.worker_session_id

    response = post_heartbeat(
        app,
        session_id,
        {"sequence": 1, "accepting_work": True},
        worker_value,
    )

    assert response.status_code == 200
    assert response.json() == {
        "worker_session_id": str(session_id),
        "last_sequence": 1,
        "last_seen_at": "2026-08-10T00:00:00Z",
        "accepting_work": True,
        "availability_changed_at": "2026-08-10T00:00:00Z",
    }
    assert heartbeat_service.calls == [(authenticated, session_id, 1, True)]


def test_heartbeat_rejects_timestamps_extra_fields_and_non_strict_values() -> None:
    invalid_bodies = (
        {"sequence": 1, "accepting_work": True, "received_at": "now"},
        {"sequence": True, "accepting_work": True},
        {"sequence": 1, "accepting_work": 1},
        {"sequence": 0, "accepting_work": False},
        {"sequence": 9_223_372_036_854_775_808, "accepting_work": False},
    )
    for body in invalid_bodies:
        app, worker_value, _, _, _ = make_app()
        service: HeartbeatServiceStub = app.state.test_heartbeat_service
        response = post_heartbeat(app, uuid4(), body, worker_value)
        assert response.status_code == 422
        assert service.calls == []


def test_heartbeat_maps_safe_failures_without_enumerating_foreign_sessions() -> None:
    cases = (
        (WorkerHeartbeatRejected(), 401, "authentication_required"),
        (WorkerSessionUnavailable(), 404, "resource_not_found"),
        (WorkerSessionInactive(), 409, "worker_session_inactive"),
        (StaleWorkerHeartbeat(), 409, "stale_heartbeat"),
        (WorkerHeartbeatGap(), 409, "heartbeat_sequence_gap"),
        (
            ConflictingWorkerHeartbeatReplay(),
            409,
            "heartbeat_replay_conflict",
        ),
        (WorkerHeartbeatServiceUnavailable(), 503, "service_unavailable"),
    )
    for error, status_code, code in cases:
        app, worker_value, _, _, _ = make_app()
        service: HeartbeatServiceStub = app.state.test_heartbeat_service
        service.error = error
        response = post_heartbeat(
            app,
            uuid4(),
            {"sequence": 1, "accepting_work": False},
            worker_value,
        )
        assert response.status_code == status_code
        assert response.json()["error"]["code"] == code


def post_heartbeat(
    app: Any,
    session_id: UUID,
    body: object,
    credential: str | None,
) -> httpx2.Response:
    async def send() -> httpx2.Response:
        headers = {"Authorization": f"Bearer {credential}"} if credential else {}
        transport = httpx2.ASGITransport(app=app)
        async with app.router.lifespan_context(app):
            async with httpx2.AsyncClient(
                transport=transport, base_url="http://testserver"
            ) as client:
                return await client.post(
                    f"/api/v1/worker-sessions/{session_id}/heartbeats",
                    json=body,
                    headers=headers,
                )

    return asyncio.run(send())
