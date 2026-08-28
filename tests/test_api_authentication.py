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
from taskforge.rate_limits import (
    AllowAllRateLimiter,
    BoundedLocalRateLimiter,
    RateLimit,
    RateLimiter,
    RateLimitPolicy,
    RateLimitRepositoryUnavailable,
)
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


class ConcurrentFailureRepository(APIRepository):
    def __init__(self, expected_calls: int) -> None:
        super().__init__(None)
        self.expected_calls = expected_calls
        self.calls = 0
        self.all_started = asyncio.Event()

    async def find_api_credential(self, credential_id: UUID) -> None:
        self.calls += 1
        if self.calls == self.expected_calls:
            self.all_started.set()
        await self.all_started.wait()
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
        rate_limiter: object | None = None,
    ) -> None:
        self.api_authenticator = api_authenticator
        self.worker_authenticator = worker_authenticator
        self.rate_limiter = rate_limiter or AllowAllRateLimiter()
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
    expired: bool = False,
    identity_disabled: bool = False,
) -> CredentialRecord:
    return CredentialRecord(
        credential_id=credential_id,
        identity_id=identity_id,
        credential_verifier=DEFAULT_VERIFIERS.encode(
            secret,
            algorithm=DEFAULT_VERIFIER_ALGORITHM,
        ),
        revoked=revoked,
        expired=expired,
        identity_disabled=identity_disabled,
    )


def make_app(
    api_repository: APIRepository,
    worker_repository: WorkerRepository,
    rate_limiter: object | None = None,
) -> tuple[FastAPI, FakeRuntime]:
    runtime = FakeRuntime(
        APIAuthenticator(api_repository, timeout_seconds=0.05),
        WorkerAuthenticator(worker_repository, timeout_seconds=0.05),
        rate_limiter,
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


class UnavailableRateRepository:
    async def consume(self, *args: object) -> object:
        raise RateLimitRepositoryUnavailable

    async def check(self, *args: object) -> object:
        raise RateLimitRepositoryUnavailable


def authentication_limiter(*, limit: int = 1) -> RateLimiter:
    return RateLimiter(
        UnavailableRateRepository(),  # type: ignore[arg-type]
        BoundedLocalRateLimiter(capacity=20),
        {
            RateLimitPolicy.API_AUTH_NETWORK: RateLimit(limit, 60),
            RateLimitPolicy.API_AUTH_CREDENTIAL: RateLimit(limit, 60),
            RateLimitPolicy.WORKER_AUTH_NETWORK: RateLimit(limit, 60),
            RateLimitPolicy.WORKER_AUTH_CREDENTIAL: RateLimit(limit, 60),
        },
    )


def test_unknown_canonical_and_malformed_credentials_remain_non_enumerating() -> None:
    unknown_id = uuid4()
    secret = secrets.token_bytes(32)
    canonical_unknown = credential_value(CredentialScope.API, unknown_id, secret)
    app, _ = make_app(
        APIRepository(None), WorkerRepository(None), authentication_limiter()
    )
    assert request(app, "/test-api", canonical_unknown).status_code == 401
    limited = request(app, "/test-api", canonical_unknown)
    assert limited.status_code == 429
    assert limited.json()["error"]["code"] == "rate_limit_exceeded"
    assert int(limited.headers["Retry-After"]) >= 1

    malformed_app, _ = make_app(
        APIRepository(None), WorkerRepository(None), authentication_limiter()
    )
    assert request(malformed_app, "/test-api", "malformed").status_code == 401
    malformed_limited = request(malformed_app, "/test-api", "malformed")
    assert malformed_limited.status_code == 429
    assert malformed_limited.json()["error"] == {
        **limited.json()["error"],
        "request_id": malformed_limited.json()["error"]["request_id"],
    }


def test_canonical_authentication_failures_have_uniform_limiter_behavior() -> None:
    identity_id = uuid4()
    valid_secret = secrets.token_bytes(32)
    invalid_secret = secrets.token_bytes(32)
    cases = (
        (
            APIRepository(None),
            credential_value(CredentialScope.API, uuid4(), valid_secret),
        ),
        (
            APIRepository(credential_record(uuid4(), identity_id, valid_secret)),
            None,
        ),
        (
            APIRepository(
                credential_record(
                    credential_id := uuid4(), identity_id, valid_secret, revoked=True
                )
            ),
            credential_value(CredentialScope.API, credential_id, valid_secret),
        ),
        (
            APIRepository(
                credential_record(
                    credential_id := uuid4(), identity_id, valid_secret, expired=True
                )
            ),
            credential_value(CredentialScope.API, credential_id, valid_secret),
        ),
        (
            APIRepository(
                credential_record(
                    credential_id := uuid4(),
                    identity_id,
                    valid_secret,
                    identity_disabled=True,
                )
            ),
            credential_value(CredentialScope.API, credential_id, valid_secret),
        ),
        (
            APIRepository(None),
            credential_value(CredentialScope.WORKER, uuid4(), valid_secret),
        ),
    )
    invalid_record = cases[1][0].record
    assert invalid_record is not None
    cases = (
        cases[0],
        (
            cases[1][0],
            credential_value(
                CredentialScope.API, invalid_record.credential_id, invalid_secret
            ),
        ),
        *cases[2:],
    )

    observed: list[tuple[int, int, str, str]] = []
    for repository, credential in cases:
        assert credential is not None
        app, _ = make_app(repository, WorkerRepository(None), authentication_limiter())
        first = request(app, "/test-api", credential)
        second = request(app, "/test-api", credential)
        observed.append(
            (
                first.status_code,
                second.status_code,
                first.json()["error"]["code"],
                second.json()["error"]["code"],
            )
        )

    assert observed == [
        (401, 429, "authentication_required", "rate_limit_exceeded")
    ] * len(cases)


def test_concurrent_authentication_failures_enforce_exact_shared_thresholds() -> None:
    async def exercise() -> None:
        limit = 4
        attempt_count = 12
        repository = ConcurrentFailureRepository(attempt_count)
        credential = credential_value(
            CredentialScope.API, uuid4(), secrets.token_bytes(32)
        )
        app, _ = make_app(
            repository,
            WorkerRepository(None),
            authentication_limiter(limit=limit),
        )
        transport = httpx2.ASGITransport(app=app, client=("198.51.100.10", 40000))
        async with app.router.lifespan_context(app):
            async with httpx2.AsyncClient(
                transport=transport, base_url="http://testserver"
            ) as client:
                responses = await asyncio.gather(
                    *(
                        client.get(
                            "/test-api",
                            headers={"Authorization": f"Bearer {credential}"},
                        )
                        for _ in range(attempt_count)
                    )
                )

        statuses = [response.status_code for response in responses]
        assert statuses.count(401) == limit
        assert statuses.count(429) == attempt_count - limit

    asyncio.run(exercise())


def test_concurrent_authentication_buckets_are_identity_independent() -> None:
    async def exercise() -> None:
        limit = 3
        attempts_per_identity = 7
        repository = ConcurrentFailureRepository(attempts_per_identity * 2)
        credentials = (
            credential_value(CredentialScope.API, uuid4(), secrets.token_bytes(32)),
            credential_value(CredentialScope.API, uuid4(), secrets.token_bytes(32)),
        )
        app, _ = make_app(
            repository,
            WorkerRepository(None),
            authentication_limiter(limit=limit),
        )
        transports = (
            httpx2.ASGITransport(app=app, client=("198.51.100.11", 40001)),
            httpx2.ASGITransport(app=app, client=("198.51.100.12", 40002)),
        )
        async with app.router.lifespan_context(app):
            async with (
                httpx2.AsyncClient(
                    transport=transports[0], base_url="http://testserver"
                ) as first,
                httpx2.AsyncClient(
                    transport=transports[1], base_url="http://testserver"
                ) as second,
            ):
                grouped = await asyncio.gather(
                    asyncio.gather(
                        *(
                            first.get(
                                "/test-api",
                                headers={"Authorization": f"Bearer {credentials[0]}"},
                            )
                            for _ in range(attempts_per_identity)
                        )
                    ),
                    asyncio.gather(
                        *(
                            second.get(
                                "/test-api",
                                headers={"Authorization": f"Bearer {credentials[1]}"},
                            )
                            for _ in range(attempts_per_identity)
                        )
                    ),
                )

        for responses in grouped:
            statuses = [response.status_code for response in responses]
            assert statuses.count(401) == limit
            assert statuses.count(429) == attempts_per_identity - limit

    asyncio.run(exercise())


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
