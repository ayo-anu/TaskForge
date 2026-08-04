"""Transactional SQLAlchemy persistence for local credential provisioning."""

from __future__ import annotations

from datetime import datetime
from types import TracebackType
from uuid import UUID

from sqlalchemy import Table, func, insert, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.identity.authorization import Role
from taskforge.identity.provisioning_ports import (
    CredentialRecordNotFound,
    DuplicateIdentityRecord,
    IdentityRecordDisabled,
    IdentityRecordNotFound,
)
from taskforge.identity.schema import (
    api_credentials,
    api_principal_roles,
    api_principals,
    worker_credentials,
    worker_identities,
)


class SQLAlchemyProvisioningRepository:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    def transaction(self) -> SQLAlchemyProvisioningUnitOfWork:
        return SQLAlchemyProvisioningUnitOfWork(self._sessions)


class SQLAlchemyProvisioningUnitOfWork:
    """One explicit transaction shared by independently factored services."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions
        self._session: AsyncSession | None = None
        self._committed = False

    async def __aenter__(self) -> SQLAlchemyProvisioningUnitOfWork:
        self._session = self._sessions()
        await self._session.begin()
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exception_type, exception, traceback
        session, self._session = self._required_session(), None
        try:
            if not self._committed:
                await session.rollback()
        finally:
            await session.close()

    async def create_api_principal(self, principal_id: UUID, name: str) -> None:
        try:
            await self._required_session().execute(
                insert(api_principals).values(id=principal_id, name=name)
            )
        except IntegrityError as error:
            raise DuplicateIdentityRecord from error

    async def assign_api_roles(
        self,
        principal_id: UUID,
        roles: frozenset[Role],
    ) -> None:
        await self._required_session().execute(
            insert(api_principal_roles),
            [
                {"principal_id": principal_id, "role": role.value}
                for role in sorted(roles, key=lambda item: item.value)
            ],
        )

    async def create_worker_identity(self, worker_id: UUID, name: str) -> None:
        try:
            await self._required_session().execute(
                insert(worker_identities).values(id=worker_id, name=name)
            )
        except IntegrityError as error:
            raise DuplicateIdentityRecord from error

    async def add_api_credential(
        self,
        credential_id: UUID,
        principal_id: UUID,
        credential_verifier: str,
        expires_at: datetime,
    ) -> None:
        await self._require_enabled_identity(api_principals, principal_id)
        await self._required_session().execute(
            insert(api_credentials).values(
                id=credential_id,
                principal_id=principal_id,
                credential_verifier=credential_verifier,
                expires_at=expires_at,
            )
        )

    async def add_worker_credential(
        self,
        credential_id: UUID,
        worker_id: UUID,
        credential_verifier: str,
        expires_at: datetime,
    ) -> None:
        await self._require_enabled_identity(worker_identities, worker_id)
        await self._required_session().execute(
            insert(worker_credentials).values(
                id=credential_id,
                worker_identity_id=worker_id,
                credential_verifier=credential_verifier,
                expires_at=expires_at,
            )
        )

    async def revoke_api_credential(
        self,
        principal_id: UUID,
        credential_id: UUID,
    ) -> None:
        result = await self._required_session().execute(
            update(api_credentials)
            .where(
                api_credentials.c.id == credential_id,
                api_credentials.c.principal_id == principal_id,
            )
            .values(
                revoked_at=func.coalesce(
                    api_credentials.c.revoked_at,
                    func.current_timestamp(),
                )
            )
            .returning(api_credentials.c.id)
        )
        if result.scalar_one_or_none() is None:
            raise CredentialRecordNotFound

    async def revoke_worker_credential(
        self,
        worker_id: UUID,
        credential_id: UUID,
    ) -> None:
        result = await self._required_session().execute(
            update(worker_credentials)
            .where(
                worker_credentials.c.id == credential_id,
                worker_credentials.c.worker_identity_id == worker_id,
            )
            .values(
                revoked_at=func.coalesce(
                    worker_credentials.c.revoked_at,
                    func.current_timestamp(),
                )
            )
            .returning(worker_credentials.c.id)
        )
        if result.scalar_one_or_none() is None:
            raise CredentialRecordNotFound

    async def commit(self) -> None:
        await self._required_session().commit()
        self._committed = True

    async def _require_enabled_identity(self, table: Table, identity_id: UUID) -> None:
        identifier = table.c.id
        disabled_at = table.c.disabled_at
        result = await self._required_session().execute(
            select(identifier, disabled_at)
            .where(identifier == identity_id)
            .with_for_update()
        )
        row = result.one_or_none()
        if row is None:
            raise IdentityRecordNotFound
        if row.disabled_at is not None:
            raise IdentityRecordDisabled

    def _required_session(self) -> AsyncSession:
        if self._session is None:
            raise RuntimeError("provisioning transaction is not active")
        return self._session
