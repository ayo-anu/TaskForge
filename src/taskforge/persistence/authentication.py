"""Separate SQLAlchemy repositories for API and worker authentication."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.identity.ports import CredentialRecord
from taskforge.identity.schema import (
    api_credentials,
    api_principals,
    worker_credentials,
    worker_identities,
)


class SQLAlchemyAPICredentialRepository:
    """Read API credentials without consulting worker identity tables."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def find_api_credential(self, credential_id: UUID) -> CredentialRecord | None:
        statement = (
            select(
                api_credentials.c.id,
                api_credentials.c.principal_id,
                api_credentials.c.credential_verifier,
                api_credentials.c.expires_at,
                func.statement_timestamp().label("observed_at"),
                (api_credentials.c.revoked_at.is_not(None)).label("revoked"),
                case(
                    (
                        api_credentials.c.expires_at.is_(None),
                        False,
                    ),
                    else_=api_credentials.c.expires_at <= func.statement_timestamp(),
                ).label("expired"),
                (api_principals.c.disabled_at.is_not(None)).label("identity_disabled"),
            )
            .select_from(
                api_credentials.join(
                    api_principals,
                    api_credentials.c.principal_id == api_principals.c.id,
                )
            )
            .where(api_credentials.c.id == credential_id)
        )
        async with self._sessions() as session:
            row = (await session.execute(statement)).one_or_none()
        if row is None:
            return None
        return CredentialRecord(
            credential_id=row.id,
            identity_id=row.principal_id,
            credential_verifier=row.credential_verifier,
            revoked=row.revoked,
            expired=row.expired,
            identity_disabled=row.identity_disabled,
            expires_at=row.expires_at,
            observed_at=row.observed_at,
        )


class SQLAlchemyWorkerCredentialRepository:
    """Read worker credentials without consulting API-principal tables."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def find_worker_credential(
        self, credential_id: UUID
    ) -> CredentialRecord | None:
        statement = (
            select(
                worker_credentials.c.id,
                worker_credentials.c.worker_identity_id,
                worker_credentials.c.credential_verifier,
                worker_credentials.c.expires_at,
                func.statement_timestamp().label("observed_at"),
                (worker_credentials.c.revoked_at.is_not(None)).label("revoked"),
                case(
                    (
                        worker_credentials.c.expires_at.is_(None),
                        False,
                    ),
                    else_=worker_credentials.c.expires_at <= func.statement_timestamp(),
                ).label("expired"),
                (worker_identities.c.disabled_at.is_not(None)).label(
                    "identity_disabled"
                ),
            )
            .select_from(
                worker_credentials.join(
                    worker_identities,
                    worker_credentials.c.worker_identity_id == worker_identities.c.id,
                )
            )
            .where(worker_credentials.c.id == credential_id)
        )
        async with self._sessions() as session:
            row = (await session.execute(statement)).one_or_none()
        if row is None:
            return None
        return CredentialRecord(
            credential_id=row.id,
            identity_id=row.worker_identity_id,
            credential_verifier=row.credential_verifier,
            revoked=row.revoked,
            expired=row.expired,
            identity_disabled=row.identity_disabled,
            expires_at=row.expires_at,
            observed_at=row.observed_at,
        )
