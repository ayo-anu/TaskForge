"""Transactional persistence port for local credential provisioning."""

from __future__ import annotations

from datetime import datetime
from types import TracebackType
from typing import Protocol
from uuid import UUID

from taskforge.identity.authorization import Role


class DuplicateIdentityRecord(Exception):
    pass


class IdentityRecordNotFound(Exception):
    pass


class IdentityRecordDisabled(Exception):
    pass


class CredentialRecordNotFound(Exception):
    pass


class ProvisioningTransaction(Protocol):
    async def create_api_principal(self, principal_id: UUID, name: str) -> None: ...

    async def assign_api_roles(
        self, principal_id: UUID, roles: frozenset[Role]
    ) -> None: ...

    async def create_worker_identity(self, worker_id: UUID, name: str) -> None: ...

    async def add_api_credential(
        self,
        credential_id: UUID,
        principal_id: UUID,
        credential_verifier: str,
        expires_at: datetime,
    ) -> None: ...

    async def add_worker_credential(
        self,
        credential_id: UUID,
        worker_id: UUID,
        credential_verifier: str,
        expires_at: datetime,
    ) -> None: ...

    async def revoke_api_credential(
        self, principal_id: UUID, credential_id: UUID
    ) -> None: ...

    async def revoke_worker_credential(
        self, worker_id: UUID, credential_id: UUID
    ) -> None: ...

    async def commit(self) -> None: ...


class ProvisioningUnitOfWork(Protocol):
    async def __aenter__(self) -> ProvisioningTransaction: ...

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...


class ProvisioningRepository(Protocol):
    def transaction(self) -> ProvisioningUnitOfWork: ...
