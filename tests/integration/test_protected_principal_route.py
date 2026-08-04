"""Opt-in full protected-route validation against isolated PostgreSQL."""

from __future__ import annotations

import asyncio
import base64
import os
import secrets
from uuid import UUID, uuid4

import httpx2
import pytest
from alembic import command
from alembic.config import Config
from pydantic import SecretStr
from sqlalchemy.engine import URL

from taskforge.api.application import create_app
from taskforge.api.health import ReadinessCoordinator
from taskforge.identity.authorization import Role
from taskforge.identity.credentials import (
    DEFAULT_VERIFIER_ALGORITHM,
    DEFAULT_VERIFIERS,
    CredentialScope,
)
from taskforge.identity.schema import (
    api_credentials,
    api_principal_roles,
    api_principals,
    worker_credentials,
    worker_identities,
)
from taskforge.persistence.database import build_async_engine
from taskforge.settings import Settings
from tests.integration.postgresql import (
    migration_database_url,
    temporary_database,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_PROTECTED_ROUTE_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_PROTECTED_ROUTE_INTEGRATION=1 explicitly",
    ),
]


class AlwaysReady:
    async def start(self) -> None:
        pass

    async def is_ready(self) -> bool:
        return True

    async def close(self) -> None:
        pass


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


def credential_value(
    scope: CredentialScope,
    credential_id: UUID,
    secret: bytes,
) -> str:
    prefix = "tf_api_v1" if scope is CredentialScope.API else "tf_worker_v1"
    encoded = base64.urlsafe_b64encode(secret).rstrip(b"=").decode("ascii")
    return f"{prefix}.{credential_id}.{encoded}"


async def verify_protected_route(database_url: URL) -> None:
    settings = settings_for(database_url)
    engine = build_async_engine(settings)
    viewer_id, administrator_id, no_role_id, worker_id = (
        uuid4(),
        uuid4(),
        uuid4(),
        uuid4(),
    )
    viewer_credential_id, administrator_credential_id, no_role_credential_id = (
        uuid4(),
        uuid4(),
        uuid4(),
    )
    worker_credential_id = uuid4()
    viewer_secret = secrets.token_bytes(32)
    administrator_secret = secrets.token_bytes(32)
    no_role_secret = secrets.token_bytes(32)
    worker_secret = secrets.token_bytes(32)

    def verifier(secret: bytes) -> str:
        return DEFAULT_VERIFIERS.encode(
            secret,
            algorithm=DEFAULT_VERIFIER_ALGORITHM,
        )

    try:
        async with engine.begin() as connection:
            await connection.execute(
                api_principals.insert(),
                [
                    {"id": viewer_id, "name": "viewer-principal"},
                    {"id": administrator_id, "name": "administrator-principal"},
                    {"id": no_role_id, "name": "unassigned-principal"},
                ],
            )
            await connection.execute(
                api_principal_roles.insert(),
                [
                    {"principal_id": viewer_id, "role": Role.VIEWER.value},
                    {
                        "principal_id": administrator_id,
                        "role": Role.ADMINISTRATOR.value,
                    },
                ],
            )
            await connection.execute(
                api_credentials.insert(),
                [
                    {
                        "id": viewer_credential_id,
                        "principal_id": viewer_id,
                        "credential_verifier": verifier(viewer_secret),
                    },
                    {
                        "id": administrator_credential_id,
                        "principal_id": administrator_id,
                        "credential_verifier": verifier(administrator_secret),
                    },
                    {
                        "id": no_role_credential_id,
                        "principal_id": no_role_id,
                        "credential_verifier": verifier(no_role_secret),
                    },
                ],
            )
            await connection.execute(
                worker_identities.insert(),
                {"id": worker_id, "name": "worker-identity"},
            )
            await connection.execute(
                worker_credentials.insert(),
                {
                    "id": worker_credential_id,
                    "worker_identity_id": worker_id,
                    "credential_verifier": verifier(worker_secret),
                },
            )
    finally:
        await engine.dispose()

    app = create_app(
        settings=settings,
        readiness=ReadinessCoordinator((AlwaysReady(),), timeout_seconds=1),
    )
    tokens = {
        "viewer": credential_value(
            CredentialScope.API, viewer_credential_id, viewer_secret
        ),
        "administrator": credential_value(
            CredentialScope.API,
            administrator_credential_id,
            administrator_secret,
        ),
        "no_role": credential_value(
            CredentialScope.API, no_role_credential_id, no_role_secret
        ),
        "worker": credential_value(
            CredentialScope.WORKER, worker_credential_id, worker_secret
        ),
    }

    async def get(
        client: httpx2.AsyncClient,
        principal_id: UUID,
        token_name: str,
    ) -> httpx2.Response:
        return await client.get(
            f"/api/v1/principals/{principal_id}",
            headers={"Authorization": f"Bearer {tokens[token_name]}"},
        )

    transport = httpx2.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with httpx2.AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            viewer_self = await get(client, viewer_id, "viewer")
            administrator_cross = await get(client, viewer_id, "administrator")
            hidden = await get(client, administrator_id, "viewer")
            nonexistent = await get(client, uuid4(), "viewer")
            forbidden = await get(client, no_role_id, "no_role")
            worker_rejected = await get(client, viewer_id, "worker")

    assert viewer_self.status_code == 200
    assert viewer_self.json()["id"] == str(viewer_id)
    assert administrator_cross.status_code == 200
    assert hidden.status_code == nonexistent.status_code == 404
    assert hidden.json()["error"]["code"] == "resource_not_found"
    assert nonexistent.json()["error"]["code"] == "resource_not_found"
    assert set(hidden.headers) == set(nonexistent.headers)
    assert forbidden.status_code == 403
    assert worker_rejected.status_code == 401


def test_protected_route_enforces_full_security_chain() -> None:
    with temporary_database(
        "TASKFORGE_PROTECTED_ROUTE_TEST_DATABASE_URL",
        "taskforge_protected_route_test",
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        with migration_database_url(alembic_url):
            command.upgrade(Config("alembic.ini"), "head")
        asyncio.run(verify_protected_route(database_url))
