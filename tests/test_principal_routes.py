"""API tests for the representative protected principal route."""

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
from taskforge.api.errors import REQUEST_ID_HEADER
from taskforge.api.health import ReadinessCoordinator
from taskforge.identity.authentication import APIAuthenticator, WorkerAuthenticator
from taskforge.identity.authorization import AuthorizationService, OwnerFilter, Role
from taskforge.identity.credentials import (
    DEFAULT_VERIFIER_ALGORITHM,
    DEFAULT_VERIFIERS,
    CredentialScope,
)
from taskforge.identity.ports import CredentialRecord
from taskforge.identity.principals import PrincipalProfile, PrincipalProfileService
from taskforge.settings import Settings


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
        if self.record and self.record.credential_id == credential_id:
            return self.record
        return None

    async def find_worker_credential(
        self, credential_id: UUID
    ) -> CredentialRecord | None:
        if self.record and self.record.credential_id == credential_id:
            return self.record
        return None


class RoleRepository:
    def __init__(self, roles: frozenset[str]) -> None:
        self.roles = roles

    async def find_role_names(self, principal_id: UUID) -> frozenset[str]:
        return self.roles


class ProfileRepository:
    def __init__(
        self,
        profiles: dict[UUID, PrincipalProfile],
        error: Exception | None = None,
    ) -> None:
        self.profiles = profiles
        self.error = error
        self.calls: list[tuple[UUID, OwnerFilter]] = []

    async def find_profile(
        self,
        principal_id: UUID,
        owner_filter: OwnerFilter,
    ) -> PrincipalProfile | None:
        self.calls.append((principal_id, owner_filter))
        if self.error:
            raise self.error
        profile = self.profiles.get(principal_id)
        if profile is None:
            return None
        if owner_filter.unrestricted or owner_filter.principal_id == principal_id:
            return profile
        return None


class Runtime:
    def __init__(
        self,
        api_authenticator: APIAuthenticator,
        worker_authenticator: WorkerAuthenticator,
        authorization_service: AuthorizationService,
        principal_profile_service: PrincipalProfileService,
    ) -> None:
        self.api_authenticator = api_authenticator
        self.worker_authenticator = worker_authenticator
        self.authorization_service = authorization_service
        self.principal_profile_service = principal_profile_service

    async def close(self) -> None:
        pass


def make_credential(
    scope: CredentialScope,
    identity_id: UUID,
) -> tuple[str, CredentialRecord]:
    credential_id = uuid4()
    secret = secrets.token_bytes(32)
    prefix = "tf_api_v1" if scope is CredentialScope.API else "tf_worker_v1"
    encoded_secret = base64.urlsafe_b64encode(secret).rstrip(b"=").decode("ascii")
    value = f"{prefix}.{credential_id}.{encoded_secret}"
    record = CredentialRecord(
        credential_id=credential_id,
        identity_id=identity_id,
        credential_verifier=DEFAULT_VERIFIERS.encode(
            secret,
            algorithm=DEFAULT_VERIFIER_ALGORITHM,
        ),
        revoked=False,
        expired=False,
        identity_disabled=False,
    )
    return value, record


def profile(principal_id: UUID, name: str) -> PrincipalProfile:
    return PrincipalProfile(principal_id, name, datetime.now(UTC))


def make_app(
    caller_id: UUID,
    roles: frozenset[str],
    profiles: dict[UUID, PrincipalProfile],
    *,
    profile_error: Exception | None = None,
) -> tuple[FastAPI, str, str, ProfileRepository]:
    api_value, api_record = make_credential(CredentialScope.API, caller_id)
    worker_value, worker_record = make_credential(CredentialScope.WORKER, uuid4())
    profile_repository = ProfileRepository(profiles, profile_error)
    runtime = Runtime(
        APIAuthenticator(CredentialRepository(api_record), timeout_seconds=0.05),
        WorkerAuthenticator(CredentialRepository(worker_record), timeout_seconds=0.05),
        AuthorizationService(RoleRepository(roles), timeout_seconds=0.05),
        PrincipalProfileService(profile_repository, timeout_seconds=0.05),
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
    return app, api_value, worker_value, profile_repository


def request(app: FastAPI, path: str, credential: str | None) -> httpx2.Response:
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


def test_viewer_can_read_own_profile() -> None:
    caller_id = uuid4()
    app, credential, _, _ = make_app(
        caller_id,
        frozenset({Role.VIEWER.value}),
        {caller_id: profile(caller_id, "viewer")},
    )

    response = request(app, f"/api/v1/principals/{caller_id}", credential)

    assert response.status_code == 200
    assert response.json()["id"] == str(caller_id)
    assert response.json()["name"] == "viewer"
    assert set(response.json()) == {"id", "name", "created_at"}


def test_administrator_can_read_another_profile() -> None:
    caller_id, target_id = uuid4(), uuid4()
    app, credential, _, _ = make_app(
        caller_id,
        frozenset({Role.ADMINISTRATOR.value}),
        {target_id: profile(target_id, "target")},
    )

    response = request(app, f"/api/v1/principals/{target_id}", credential)

    assert response.status_code == 200
    assert response.json()["id"] == str(target_id)


def test_hidden_and_nonexistent_profiles_are_indistinguishable() -> None:
    caller_id, hidden_id, nonexistent_id = uuid4(), uuid4(), uuid4()
    app, credential, _, repository = make_app(
        caller_id,
        frozenset({Role.VIEWER.value}),
        {hidden_id: profile(hidden_id, "hidden")},
    )

    hidden = request(app, f"/api/v1/principals/{hidden_id}", credential)
    nonexistent = request(app, f"/api/v1/principals/{nonexistent_id}", credential)

    assert hidden.status_code == nonexistent.status_code == 404
    hidden_error = hidden.json()["error"]
    nonexistent_error = nonexistent.json()["error"]
    assert {
        key: value for key, value in hidden_error.items() if key != "request_id"
    } == {key: value for key, value in nonexistent_error.items() if key != "request_id"}
    assert set(hidden.headers) == set(nonexistent.headers)
    assert hidden.headers[REQUEST_ID_HEADER] == hidden_error["request_id"]
    assert nonexistent.headers[REQUEST_ID_HEADER] == nonexistent_error["request_id"]
    assert repository.calls == [
        (
            hidden_id,
            OwnerFilter(unrestricted=False, principal_id=caller_id),
        ),
        (
            nonexistent_id,
            OwnerFilter(unrestricted=False, principal_id=caller_id),
        ),
    ]


def test_no_role_is_forbidden_before_profile_lookup() -> None:
    caller_id = uuid4()
    app, credential, _, repository = make_app(caller_id, frozenset(), {})

    response = request(app, f"/api/v1/principals/{caller_id}", credential)

    assert response.status_code == 403
    assert response.json()["error"]["code"] == "forbidden"
    assert repository.calls == []


def test_worker_credential_is_rejected_on_client_route() -> None:
    caller_id = uuid4()
    app, _, worker_credential, repository = make_app(
        caller_id,
        frozenset({Role.ADMINISTRATOR.value}),
        {},
    )

    response = request(
        app,
        f"/api/v1/principals/{caller_id}",
        worker_credential,
    )

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "authentication_required"
    assert repository.calls == []


def test_invalid_identifier_uses_safe_validation_contract() -> None:
    caller_id = uuid4()
    app, credential, _, _ = make_app(
        caller_id,
        frozenset({Role.VIEWER.value}),
        {},
    )

    response = request(app, "/api/v1/principals/not-a-uuid", credential)

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_failed"
    assert "not-a-uuid" not in response.text


def test_profile_persistence_failure_uses_safe_service_unavailable_contract() -> None:
    caller_id = uuid4()
    sensitive_detail = "postgresql://user:secret@internal-host:5432/taskforge"
    app, credential, _, _ = make_app(
        caller_id,
        frozenset({Role.VIEWER.value}),
        {},
        profile_error=RuntimeError(sensitive_detail),
    )

    response = request(app, f"/api/v1/principals/{caller_id}", credential)

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "service_unavailable"
    assert sensitive_detail not in response.text
    assert credential not in response.text


def test_openapi_documents_protected_route_errors() -> None:
    caller_id = uuid4()
    app, _, _, _ = make_app(caller_id, frozenset(), {})

    response = request(app, "/openapi.json", None)
    operation = response.json()["paths"]["/api/v1/principals/{principal_id}"]["get"]

    assert set(operation["responses"]) >= {"200", "401", "403", "404", "422", "503"}
