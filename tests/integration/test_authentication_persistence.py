"""Opt-in authentication verification against isolated real PostgreSQL."""

from __future__ import annotations

import asyncio
import os
import secrets
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from pydantic import SecretStr
from sqlalchemy.engine import URL

from taskforge.identity.authentication import (
    APIAuthenticator,
    AuthenticationFailure,
    AuthenticationFailureReason,
    WorkerAuthenticator,
)
from taskforge.identity.credentials import (
    DEFAULT_VERIFIER_ALGORITHM,
    DEFAULT_VERIFIERS,
    CredentialScope,
    PresentedCredential,
)
from taskforge.identity.schema import (
    api_credentials,
    api_principals,
    worker_credentials,
    worker_identities,
)
from taskforge.persistence.authentication import (
    SQLAlchemyAPICredentialRepository,
    SQLAlchemyWorkerCredentialRepository,
)
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.settings import Settings
from tests.integration.postgresql import (
    migration_database_url,
    temporary_database,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_AUTHENTICATION_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_AUTHENTICATION_INTEGRATION=1 explicitly",
    ),
]


def settings_for(database_url: URL) -> Settings:
    assert database_url.host is not None
    assert database_url.port is not None
    assert database_url.database is not None
    assert database_url.username is not None
    assert database_url.password is not None
    return Settings(
        postgres_host=database_url.host,
        postgres_port=database_url.port,
        postgres_database=database_url.database,
        postgres_user=database_url.username,
        postgres_password=SecretStr(database_url.password),
        rabbitmq_password=SecretStr("unused-in-focused-postgres-test"),
    )


async def verify_authentication(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    api_repository = SQLAlchemyAPICredentialRepository(sessions)
    worker_repository = SQLAlchemyWorkerCredentialRepository(sessions)
    api_authenticator = APIAuthenticator(api_repository, timeout_seconds=1)
    worker_authenticator = WorkerAuthenticator(worker_repository, timeout_seconds=1)

    now = datetime.now(UTC)
    api_identity_id, worker_identity_id = uuid4(), uuid4()
    disabled_identity_id = uuid4()
    valid_api = PresentedCredential(
        CredentialScope.API, uuid4(), secrets.token_bytes(32)
    )
    rotated_api = PresentedCredential(
        CredentialScope.API, uuid4(), secrets.token_bytes(32)
    )
    expired_api = PresentedCredential(
        CredentialScope.API, uuid4(), secrets.token_bytes(32)
    )
    revoked_worker = PresentedCredential(
        CredentialScope.WORKER, uuid4(), secrets.token_bytes(32)
    )
    disabled_worker = PresentedCredential(
        CredentialScope.WORKER, uuid4(), secrets.token_bytes(32)
    )
    valid_worker = PresentedCredential(
        CredentialScope.WORKER, uuid4(), secrets.token_bytes(32)
    )

    def verifier(presented: PresentedCredential) -> str:
        return DEFAULT_VERIFIERS.encode(
            presented.secret,
            algorithm=DEFAULT_VERIFIER_ALGORITHM,
        )

    try:
        async with engine.begin() as connection:
            await connection.execute(
                api_principals.insert(),
                [
                    {
                        "id": api_identity_id,
                        "name": f"api-{uuid4().hex}",
                        "created_at": now,
                    }
                ],
            )
            await connection.execute(
                worker_identities.insert(),
                [
                    {
                        "id": worker_identity_id,
                        "name": f"worker-{uuid4().hex}",
                        "created_at": now,
                        "disabled_at": None,
                    },
                    {
                        "id": disabled_identity_id,
                        "name": f"worker-{uuid4().hex}",
                        "created_at": now,
                        "disabled_at": now,
                    },
                ],
            )
            await connection.execute(
                api_credentials.insert(),
                [
                    {
                        "id": valid_api.credential_id,
                        "principal_id": api_identity_id,
                        "credential_verifier": verifier(valid_api),
                        "created_at": now,
                        "expires_at": None,
                        "revoked_at": None,
                    },
                    {
                        "id": rotated_api.credential_id,
                        "principal_id": api_identity_id,
                        "credential_verifier": verifier(rotated_api),
                        "created_at": now,
                        "expires_at": None,
                        "revoked_at": None,
                    },
                    {
                        "id": expired_api.credential_id,
                        "principal_id": api_identity_id,
                        "credential_verifier": verifier(expired_api),
                        "created_at": now - timedelta(hours=2),
                        "expires_at": now - timedelta(hours=1),
                        "revoked_at": None,
                    },
                ],
            )
            await connection.execute(
                worker_credentials.insert(),
                [
                    {
                        "id": valid_worker.credential_id,
                        "worker_identity_id": worker_identity_id,
                        "credential_verifier": verifier(valid_worker),
                        "created_at": now,
                        "expires_at": None,
                        "revoked_at": None,
                    },
                    {
                        "id": revoked_worker.credential_id,
                        "worker_identity_id": worker_identity_id,
                        "credential_verifier": verifier(revoked_worker),
                        "created_at": now,
                        "expires_at": None,
                        "revoked_at": now,
                    },
                    {
                        "id": disabled_worker.credential_id,
                        "worker_identity_id": disabled_identity_id,
                        "credential_verifier": verifier(disabled_worker),
                        "created_at": now,
                        "expires_at": None,
                        "revoked_at": None,
                    },
                ],
            )

        first = await api_authenticator.authenticate(valid_api)
        second = await api_authenticator.authenticate(rotated_api)
        worker = await worker_authenticator.authenticate(valid_worker)
        assert first.principal_id == second.principal_id == api_identity_id
        assert first.credential_expires_at is None
        assert first.credential_observed_at is not None
        assert first.credential_observed_at.tzinfo is not None
        assert worker.worker_identity_id == worker_identity_id

        for authenticator, presented, reason in (
            (api_authenticator, expired_api, AuthenticationFailureReason.EXPIRED),
            (worker_authenticator, revoked_worker, AuthenticationFailureReason.REVOKED),
            (
                worker_authenticator,
                disabled_worker,
                AuthenticationFailureReason.IDENTITY_DISABLED,
            ),
        ):
            with pytest.raises(AuthenticationFailure) as error:
                await authenticator.authenticate(presented)
            assert error.value.reason is reason

        with pytest.raises(AuthenticationFailure) as wrong_scope:
            await api_authenticator.authenticate(valid_worker)
        assert wrong_scope.value.reason is AuthenticationFailureReason.WRONG_SCOPE
    finally:
        await engine.dispose()


def test_real_persistence_enforces_authentication_lifecycle_and_scope() -> None:
    with temporary_database(
        "TASKFORGE_AUTHENTICATION_TEST_DATABASE_URL",
        "taskforge_auth_test",
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        with migration_database_url(alembic_url):
            command.upgrade(Config("alembic.ini"), "head")
        asyncio.run(verify_authentication(database_url))
