"""Opt-in end-to-end credential bootstrap verification with real PostgreSQL."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import UTC, datetime, timedelta

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import select, update
from sqlalchemy.engine import URL

from taskforge.audit.schema import audit_records
from taskforge.identity.authentication import (
    APIAuthenticator,
    AuthenticationFailure,
    AuthenticationFailureReason,
    WorkerAuthenticator,
)
from taskforge.identity.authorization import Role
from taskforge.identity.credentials import (
    CredentialFormatError,
    parse_presented_credential,
)
from taskforge.identity.provisioning import (
    CredentialIssuanceService,
    CredentialRevocationService,
    DuplicateIdentity,
    IdentityDisabled,
    IdentityProvisioningService,
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
from taskforge.persistence.provisioning import SQLAlchemyProvisioningRepository
from tests.integration.postgresql import migration_database_url, temporary_database
from tests.integration.test_authentication_persistence import settings_for

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_CREDENTIAL_BOOTSTRAP_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_CREDENTIAL_BOOTSTRAP_INTEGRATION=1 explicitly",
    ),
]


async def verify_credential_bootstrap(database_url: URL) -> None:
    engine = build_async_engine(settings_for(database_url))
    sessions = build_session_factory(engine)
    repository = SQLAlchemyProvisioningRepository(sessions)
    identities = IdentityProvisioningService()
    issuance = CredentialIssuanceService()
    revocation = CredentialRevocationService()
    expiration = datetime.now(UTC) + timedelta(days=30)

    try:
        async with repository.transaction() as transaction:
            principal_id = await identities.create_api_principal(
                transaction,
                name="bootstrap-administrator",
                roles=frozenset({Role.ADMINISTRATOR}),
            )
            first_api = await issuance.issue_api_credential(
                transaction,
                principal_id=principal_id,
                expires_at=expiration,
            )
            await transaction.commit()

        api_value = first_api.take_presented_value()
        presented_api = parse_presented_credential(api_value)
        api_authenticator = APIAuthenticator(
            SQLAlchemyAPICredentialRepository(sessions), timeout_seconds=1
        )
        authenticated_api = await api_authenticator.authenticate(presented_api)
        assert authenticated_api.principal_id == principal_id

        async with sessions() as session:
            stored_verifier = await session.scalar(
                select(api_credentials.c.credential_verifier).where(
                    api_credentials.c.id == first_api.credential_id
                )
            )
        assert stored_verifier == first_api.credential_verifier
        assert api_value not in stored_verifier
        assert api_value.rsplit(".", maxsplit=1)[1] not in stored_verifier
        with pytest.raises(CredentialFormatError):
            parse_presented_credential(stored_verifier)

        async with repository.transaction() as transaction:
            worker_id = await identities.create_worker_identity(
                transaction, name="bootstrap-worker"
            )
            first_worker = await issuance.issue_worker_credential(
                transaction,
                worker_id=worker_id,
                expires_at=expiration,
            )
            await transaction.commit()

        worker_value = first_worker.take_presented_value()
        presented_worker = parse_presented_credential(worker_value)
        worker_authenticator = WorkerAuthenticator(
            SQLAlchemyWorkerCredentialRepository(sessions), timeout_seconds=1
        )
        authenticated_worker = await worker_authenticator.authenticate(presented_worker)
        assert authenticated_worker.worker_identity_id == worker_id

        async with repository.transaction() as transaction:
            rotated_api = await issuance.issue_api_credential(
                transaction,
                principal_id=principal_id,
                expires_at=expiration,
            )
            rotated_worker = await issuance.issue_worker_credential(
                transaction,
                worker_id=worker_id,
                expires_at=expiration,
            )
            await transaction.commit()
        rotated_api_value = rotated_api.take_presented_value()
        rotated_worker_value = rotated_worker.take_presented_value()
        await api_authenticator.authenticate(
            parse_presented_credential(rotated_api_value)
        )
        await worker_authenticator.authenticate(
            parse_presented_credential(rotated_worker_value)
        )
        await api_authenticator.authenticate(presented_api)
        await worker_authenticator.authenticate(presented_worker)

        async with repository.transaction() as transaction:
            await revocation.revoke_api_credential(
                transaction,
                principal_id=principal_id,
                credential_id=first_api.credential_id,
            )
            await revocation.revoke_worker_credential(
                transaction,
                worker_id=worker_id,
                credential_id=first_worker.credential_id,
            )
            await transaction.commit()
        with pytest.raises(AuthenticationFailure) as api_error:
            await api_authenticator.authenticate(presented_api)
        assert api_error.value.reason is AuthenticationFailureReason.REVOKED
        with pytest.raises(AuthenticationFailure) as worker_error:
            await worker_authenticator.authenticate(presented_worker)
        assert worker_error.value.reason is AuthenticationFailureReason.REVOKED

        async with sessions() as session:
            original_revocations = (
                await session.scalar(
                    select(api_credentials.c.revoked_at).where(
                        api_credentials.c.id == first_api.credential_id
                    )
                ),
                await session.scalar(
                    select(worker_credentials.c.revoked_at).where(
                        worker_credentials.c.id == first_worker.credential_id
                    )
                ),
            )
        async with repository.transaction() as transaction:
            await revocation.revoke_api_credential(
                transaction,
                principal_id=principal_id,
                credential_id=first_api.credential_id,
            )
            await revocation.revoke_worker_credential(
                transaction,
                worker_id=worker_id,
                credential_id=first_worker.credential_id,
            )
            await transaction.commit()
        async with sessions() as session:
            repeated_revocations = (
                await session.scalar(
                    select(api_credentials.c.revoked_at).where(
                        api_credentials.c.id == first_api.credential_id
                    )
                ),
                await session.scalar(
                    select(worker_credentials.c.revoked_at).where(
                        worker_credentials.c.id == first_worker.credential_id
                    )
                ),
            )
            credential_audit = (
                await session.execute(
                    select(
                        audit_records.c.resource_id,
                        audit_records.c.diagnostic_provenance,
                    ).where(
                        audit_records.c.resource_id.in_(
                            (
                                first_api.credential_id,
                                first_worker.credential_id,
                                rotated_api.credential_id,
                                rotated_worker.credential_id,
                            )
                        )
                    )
                )
            ).all()
        assert repeated_revocations == original_revocations
        serialized_audit = json.dumps(
            [
                {
                    "resource_id": str(row.resource_id),
                    "provenance": row.diagnostic_provenance,
                }
                for row in credential_audit
            ],
            sort_keys=True,
        )
        for secret_value, verifier_value in (
            (api_value, first_api.credential_verifier),
            (worker_value, first_worker.credential_verifier),
            (rotated_api_value, rotated_api.credential_verifier),
            (rotated_worker_value, rotated_worker.credential_verifier),
        ):
            assert secret_value not in serialized_audit
            assert secret_value.rsplit(".", maxsplit=1)[1] not in serialized_audit
            assert verifier_value not in serialized_audit

        async with sessions.begin() as session:
            await session.execute(
                update(api_principals)
                .where(api_principals.c.id == principal_id)
                .values(disabled_at=datetime.now(UTC))
            )
            await session.execute(
                update(worker_identities)
                .where(worker_identities.c.id == worker_id)
                .values(disabled_at=datetime.now(UTC))
            )
        async with repository.transaction() as transaction:
            with pytest.raises(IdentityDisabled):
                await issuance.issue_api_credential(
                    transaction,
                    principal_id=principal_id,
                    expires_at=expiration,
                )
        async with repository.transaction() as transaction:
            with pytest.raises(IdentityDisabled):
                await issuance.issue_worker_credential(
                    transaction,
                    worker_id=worker_id,
                    expires_at=expiration,
                )

        try:
            async with repository.transaction() as transaction:
                await identities.create_worker_identity(
                    transaction, name="bootstrap-worker"
                )
                await transaction.commit()
        except DuplicateIdentity:
            pass
        else:
            pytest.fail("duplicate worker identity was accepted")

        async with sessions() as session:
            assert (
                await session.scalar(
                    select(worker_identities.c.id).where(
                        worker_identities.c.name == "bootstrap-worker"
                    )
                )
                == worker_id
            )
            assert (
                await session.scalar(
                    select(worker_credentials.c.credential_verifier).where(
                        worker_credentials.c.id == rotated_worker.credential_id
                    )
                )
                == rotated_worker.credential_verifier
            )
    finally:
        await engine.dispose()


def test_real_bootstrap_generation_rotation_revocation_and_rollback() -> None:
    with temporary_database(
        "TASKFORGE_CREDENTIAL_BOOTSTRAP_TEST_DATABASE_URL",
        "taskforge_credential_bootstrap",
    ) as database_url:
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        with migration_database_url(alembic_url):
            command.upgrade(Config("alembic.ini"), "head")
        asyncio.run(verify_credential_bootstrap(database_url))
