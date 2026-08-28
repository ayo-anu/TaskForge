"""Authorized, non-secret current task-claim inspection route tests."""

from __future__ import annotations

import asyncio
import base64
import secrets
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx2
from fastapi import FastAPI
from pydantic import SecretStr

from taskforge.api.application import create_app
from taskforge.api.health import ReadinessCoordinator
from taskforge.claims.domain import (
    InspectedTaskClaim,
    TaskClaimLeaseStatus,
)
from taskforge.claims.persistence_ports import (
    TaskClaimInspectionNotFound,
    TaskClaimInspectionPersistenceUnavailable,
)
from taskforge.claims.service import TaskClaimInspectionService
from taskforge.identity.authentication import APIAuthenticator, WorkerAuthenticator
from taskforge.identity.authorization import AuthorizationService, OwnerFilter, Role
from taskforge.identity.credentials import (
    DEFAULT_VERIFIER_ALGORITHM,
    DEFAULT_VERIFIERS,
    CredentialScope,
)
from taskforge.identity.ports import CredentialRecord
from taskforge.rate_limits import AllowAllRateLimiter
from taskforge.runs.domain import TaskRunStatus
from taskforge.settings import Settings


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


class InspectionRepository:
    def __init__(
        self,
        claim: InspectedTaskClaim,
        *,
        error: Exception | None = None,
    ) -> None:
        self.claim = claim
        self.error = error
        self.calls: list[tuple[UUID, OwnerFilter]] = []

    async def get_current_claim(
        self, task_attempt_id: UUID, owner_filter: OwnerFilter
    ) -> InspectedTaskClaim:
        self.calls.append((task_attempt_id, owner_filter))
        if self.error is not None:
            raise self.error
        if task_attempt_id != self.claim.task_attempt_id:
            raise TaskClaimInspectionNotFound
        return self.claim


class Runtime:
    def __init__(
        self,
        api_authenticator: APIAuthenticator,
        worker_authenticator: WorkerAuthenticator,
        authorization_service: AuthorizationService,
        task_claim_inspection_service: TaskClaimInspectionService,
    ) -> None:
        self.api_authenticator = api_authenticator
        self.rate_limiter = AllowAllRateLimiter()
        self.worker_authenticator = worker_authenticator
        self.authorization_service = authorization_service
        self.task_claim_inspection_service = task_claim_inspection_service

    async def close(self) -> None:
        pass


def make_credential(
    scope: CredentialScope, identity_id: UUID
) -> tuple[str, CredentialRecord]:
    credential_id = uuid4()
    secret = secrets.token_bytes(32)
    prefix = "tf_api_v1" if scope is CredentialScope.API else "tf_worker_v1"
    encoded = base64.urlsafe_b64encode(secret).rstrip(b"=").decode("ascii")
    return (
        f"{prefix}.{credential_id}.{encoded}",
        CredentialRecord(
            credential_id=credential_id,
            identity_id=identity_id,
            credential_verifier=DEFAULT_VERIFIERS.encode(
                secret, algorithm=DEFAULT_VERIFIER_ALGORITHM
            ),
            revoked=False,
            expired=False,
            identity_disabled=False,
        ),
    )


def inspected_claim() -> InspectedTaskClaim:
    observed = datetime.now(UTC)
    return InspectedTaskClaim(
        uuid4(),
        uuid4(),
        uuid4(),
        1,
        2,
        uuid4(),
        uuid4(),
        observed - timedelta(seconds=30),
        observed + timedelta(seconds=30),
        observed,
        TaskClaimLeaseStatus.UNEXPIRED,
        TaskRunStatus.CLAIMED,
    )


def make_app(
    caller_id: UUID,
    roles: frozenset[str],
    repository: InspectionRepository,
) -> tuple[FastAPI, str, str]:
    api_value, api_record = make_credential(CredentialScope.API, caller_id)
    worker_value, worker_record = make_credential(CredentialScope.WORKER, uuid4())
    runtime = Runtime(
        APIAuthenticator(CredentialRepository(api_record), timeout_seconds=0.05),
        WorkerAuthenticator(CredentialRepository(worker_record), timeout_seconds=0.05),
        AuthorizationService(RoleRepository(roles), timeout_seconds=0.05),
        TaskClaimInspectionService(repository),
    )
    settings = Settings(
        postgres_password=SecretStr("postgres-test-secret"),
        rabbitmq_password=SecretStr("rabbitmq-test-secret"),
    )
    return (
        create_app(
            settings=settings,
            readiness=ReadinessCoordinator(AlwaysReady(), timeout_seconds=0.05),
            authentication=runtime,
        ),
        api_value,
        worker_value,
    )


def request(app: FastAPI, path: str, credential: str) -> httpx2.Response:
    async def send() -> httpx2.Response:
        transport = httpx2.ASGITransport(app=app)
        async with app.router.lifespan_context(app):
            async with httpx2.AsyncClient(
                transport=transport, base_url="http://testserver"
            ) as client:
                return await client.get(
                    path, headers={"Authorization": f"Bearer {credential}"}
                )

    return asyncio.run(send())


def test_viewer_inspects_only_owned_current_claim_without_secrets() -> None:
    caller_id = uuid4()
    claim = inspected_claim()
    repository = InspectionRepository(claim)
    app, credential, _ = make_app(caller_id, frozenset({Role.VIEWER.value}), repository)

    response = request(
        app, f"/api/v1/task-attempts/{claim.task_attempt_id}/claim", credential
    )

    assert response.status_code == 200
    assert repository.calls == [(claim.task_attempt_id, OwnerFilter.only(caller_id))]
    assert set(response.json()) == {
        "task_attempt_id",
        "task_run_id",
        "workflow_run_id",
        "attempt_number",
        "generation",
        "worker_identity_id",
        "worker_session_id",
        "acquired_at",
        "lease_expires_at",
        "observed_at",
        "lease_status",
        "task_status",
    }
    assert "authority" not in response.text
    assert "credential" not in response.text


def test_administrator_uses_unrestricted_owner_filter() -> None:
    caller_id = uuid4()
    claim = inspected_claim()
    repository = InspectionRepository(claim)
    app, credential, _ = make_app(
        caller_id, frozenset({Role.ADMINISTRATOR.value}), repository
    )

    response = request(
        app, f"/api/v1/task-attempts/{claim.task_attempt_id}/claim", credential
    )

    assert response.status_code == 200
    assert repository.calls == [(claim.task_attempt_id, OwnerFilter.all_owners())]


def test_missing_and_out_of_scope_claims_share_safe_not_found_response() -> None:
    caller_id = uuid4()
    claim = inspected_claim()
    missing_repository = InspectionRepository(
        claim, error=TaskClaimInspectionNotFound()
    )
    missing_app, credential, _ = make_app(
        caller_id, frozenset({Role.VIEWER.value}), missing_repository
    )
    hidden_repository = InspectionRepository(claim, error=TaskClaimInspectionNotFound())
    hidden_app, hidden_credential, _ = make_app(
        caller_id, frozenset({Role.VIEWER.value}), hidden_repository
    )

    missing = request(missing_app, f"/api/v1/task-attempts/{uuid4()}/claim", credential)
    hidden = request(
        hidden_app,
        f"/api/v1/task-attempts/{claim.task_attempt_id}/claim",
        hidden_credential,
    )

    assert missing.status_code == hidden.status_code == 404
    assert missing.json()["error"]["code"] == hidden.json()["error"]["code"]


def test_permission_and_worker_scope_are_enforced_before_inspection() -> None:
    caller_id = uuid4()
    claim = inspected_claim()
    repository = InspectionRepository(claim)
    app, api_credential, worker_credential = make_app(
        caller_id, frozenset(), repository
    )
    path = f"/api/v1/task-attempts/{claim.task_attempt_id}/claim"

    assert request(app, path, api_credential).status_code == 403
    assert request(app, path, worker_credential).status_code == 401
    assert repository.calls == []


def test_inspection_persistence_failure_is_service_unavailable() -> None:
    caller_id = uuid4()
    claim = inspected_claim()
    repository = InspectionRepository(
        claim, error=TaskClaimInspectionPersistenceUnavailable()
    )
    app, credential, _ = make_app(caller_id, frozenset({Role.VIEWER.value}), repository)

    response = request(
        app, f"/api/v1/task-attempts/{claim.task_attempt_id}/claim", credential
    )

    assert response.status_code == 503
    assert "postgres" not in response.text.lower()
