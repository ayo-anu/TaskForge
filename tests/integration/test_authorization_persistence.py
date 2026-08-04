"""Opt-in role persistence and authorization tests against real PostgreSQL."""

from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from pydantic import SecretStr
from sqlalchemy import delete
from sqlalchemy.engine import URL

from taskforge.identity.authentication import AuthenticatedAPIPrincipal
from taskforge.identity.authorization import (
    AuthorizationDenied,
    AuthorizationService,
    Permission,
    Role,
)
from taskforge.identity.schema import api_principal_roles, api_principals
from taskforge.persistence.authorization import SQLAlchemyPrincipalRoleRepository
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.settings import Settings
from tests.integration.postgresql import (
    migration_database_url,
    temporary_database,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_AUTHORIZATION_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_AUTHORIZATION_INTEGRATION=1 explicitly",
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


async def verify_authorization(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    service = AuthorizationService(
        SQLAlchemyPrincipalRoleRepository(sessions),
        timeout_seconds=1,
    )
    viewer_id, operator_id, administrator_id, unassigned_id = (
        uuid4(),
        uuid4(),
        uuid4(),
        uuid4(),
    )
    identities = {
        Role.VIEWER: AuthenticatedAPIPrincipal(viewer_id, uuid4()),
        Role.WORKFLOW_OPERATOR: AuthenticatedAPIPrincipal(operator_id, uuid4()),
        Role.ADMINISTRATOR: AuthenticatedAPIPrincipal(administrator_id, uuid4()),
    }

    try:
        async with engine.begin() as connection:
            await connection.execute(
                api_principals.insert(),
                [
                    {"id": principal_id, "name": f"principal-{uuid4().hex}"}
                    for principal_id in (
                        viewer_id,
                        operator_id,
                        administrator_id,
                        unassigned_id,
                    )
                ],
            )
            await connection.execute(
                api_principal_roles.insert(),
                [
                    {"principal_id": identity.principal_id, "role": role.value}
                    for role, identity in identities.items()
                ],
            )

        expected = {
            Role.VIEWER: {Permission.VIEW},
            Role.WORKFLOW_OPERATOR: {
                Permission.VIEW,
                Permission.AUTHOR_WORKFLOW,
                Permission.OPERATE_WORKFLOW,
            },
            Role.ADMINISTRATOR: set(Permission),
        }
        for role, identity in identities.items():
            context = await service.context_for(identity)
            assert {
                permission for permission in Permission if context.allows(permission)
            } == (expected[role])

        unassigned = await service.context_for(
            AuthenticatedAPIPrincipal(unassigned_id, uuid4())
        )
        assert not any(unassigned.allows(permission) for permission in Permission)
        with pytest.raises(AuthorizationDenied):
            unassigned.require(Permission.VIEW)

        viewer_context = await service.context_for(identities[Role.VIEWER])
        assert (
            viewer_context.owner_filter_for(Permission.VIEW).principal_id == viewer_id
        )
        with pytest.raises(AuthorizationDenied):
            viewer_context.require_owned(Permission.VIEW, operator_id)

        administrator = await service.context_for(identities[Role.ADMINISTRATOR])
        administrator.require_owned(Permission.ADMINISTER, viewer_id)
        assert administrator.owner_filter_for(Permission.ADMINISTER).unrestricted

        async with engine.begin() as connection:
            await connection.execute(
                delete(api_principal_roles).where(
                    api_principal_roles.c.principal_id == viewer_id
                )
            )
            await connection.execute(
                api_principal_roles.insert(),
                {"principal_id": viewer_id, "role": Role.ADMINISTRATOR.value},
            )
        refreshed = await service.context_for(identities[Role.VIEWER])
        assert refreshed.roles == frozenset({Role.ADMINISTRATOR})
    finally:
        await engine.dispose()


def test_real_role_persistence_enforces_policy_and_ownership() -> None:
    with temporary_database(
        "TASKFORGE_AUTHORIZATION_TEST_DATABASE_URL",
        "taskforge_authorization_test",
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        with migration_database_url(alembic_url):
            command.upgrade(Config("alembic.ini"), "head")
        asyncio.run(verify_authorization(database_url))
