"""Separate identity, credential issuance, and revocation domain services."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from uuid import UUID, uuid4

from taskforge.identity.authorization import Role
from taskforge.identity.credentials import (
    CredentialScope,
    GeneratedCredential,
    generate_credential,
)
from taskforge.identity.provisioning_ports import (
    CredentialRecordNotFound,
    DuplicateIdentityRecord,
    IdentityRecordDisabled,
    IdentityRecordNotFound,
    ProvisioningTransaction,
)


class ProvisioningFailure(Exception):
    pass


class DuplicateIdentity(ProvisioningFailure):
    pass


class IdentityNotFound(ProvisioningFailure):
    pass


class IdentityDisabled(ProvisioningFailure):
    pass


class CredentialNotFound(ProvisioningFailure):
    pass


class InvalidProvisioningRequest(ProvisioningFailure):
    pass


class ProvisioningUnavailable(ProvisioningFailure):
    pass


class IdentityProvisioningService:
    """Create identities without implicitly issuing credential material."""

    def __init__(self, *, identifier_factory: Callable[[], UUID] = uuid4) -> None:
        self._identifier_factory = identifier_factory

    async def create_api_principal(
        self,
        transaction: ProvisioningTransaction,
        *,
        name: str,
        roles: frozenset[Role],
    ) -> UUID:
        _validate_name(name)
        if not roles:
            raise InvalidProvisioningRequest("at least one API role is required")
        principal_id = self._identifier_factory()
        try:
            await transaction.create_api_principal(principal_id, name)
            await transaction.assign_api_roles(principal_id, roles)
        except DuplicateIdentityRecord as error:
            raise DuplicateIdentity from error
        except Exception as error:
            raise ProvisioningUnavailable from error
        return principal_id

    async def create_worker_identity(
        self,
        transaction: ProvisioningTransaction,
        *,
        name: str,
    ) -> UUID:
        _validate_name(name)
        worker_id = self._identifier_factory()
        try:
            await transaction.create_worker_identity(worker_id, name)
        except DuplicateIdentityRecord as error:
            raise DuplicateIdentity from error
        except Exception as error:
            raise ProvisioningUnavailable from error
        return worker_id


class CredentialIssuanceService:
    """Issue new credentials separately from identity creation."""

    def __init__(
        self,
        *,
        credential_factory: Callable[[CredentialScope], GeneratedCredential] = (
            generate_credential
        ),
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._credential_factory = credential_factory
        self._clock = clock

    async def issue_api_credential(
        self,
        transaction: ProvisioningTransaction,
        *,
        principal_id: UUID,
        expires_at: datetime,
    ) -> GeneratedCredential:
        _validate_expiration(expires_at, self._clock())
        generated = self._generate(CredentialScope.API)
        try:
            await transaction.add_api_credential(
                generated.credential_id,
                principal_id,
                generated.credential_verifier,
                expires_at,
            )
        except IdentityRecordNotFound as error:
            raise IdentityNotFound from error
        except IdentityRecordDisabled as error:
            raise IdentityDisabled from error
        except Exception as error:
            raise ProvisioningUnavailable from error
        return generated

    async def issue_worker_credential(
        self,
        transaction: ProvisioningTransaction,
        *,
        worker_id: UUID,
        expires_at: datetime,
    ) -> GeneratedCredential:
        _validate_expiration(expires_at, self._clock())
        generated = self._generate(CredentialScope.WORKER)
        try:
            await transaction.add_worker_credential(
                generated.credential_id,
                worker_id,
                generated.credential_verifier,
                expires_at,
            )
        except IdentityRecordNotFound as error:
            raise IdentityNotFound from error
        except IdentityRecordDisabled as error:
            raise IdentityDisabled from error
        except Exception as error:
            raise ProvisioningUnavailable from error
        return generated

    def _generate(self, scope: CredentialScope) -> GeneratedCredential:
        try:
            return self._credential_factory(scope)
        except Exception as error:
            raise ProvisioningUnavailable from error


class CredentialRevocationService:
    async def revoke_api_credential(
        self,
        transaction: ProvisioningTransaction,
        *,
        principal_id: UUID,
        credential_id: UUID,
    ) -> None:
        try:
            await transaction.revoke_api_credential(principal_id, credential_id)
        except CredentialRecordNotFound as error:
            raise CredentialNotFound from error
        except Exception as error:
            raise ProvisioningUnavailable from error

    async def revoke_worker_credential(
        self,
        transaction: ProvisioningTransaction,
        *,
        worker_id: UUID,
        credential_id: UUID,
    ) -> None:
        try:
            await transaction.revoke_worker_credential(worker_id, credential_id)
        except CredentialRecordNotFound as error:
            raise CredentialNotFound from error
        except Exception as error:
            raise ProvisioningUnavailable from error


def _validate_name(name: str) -> None:
    if not name or name != name.strip() or len(name) > 128:
        raise InvalidProvisioningRequest("invalid identity name")


def _validate_expiration(expires_at: datetime, now: datetime) -> None:
    if expires_at.tzinfo is None or expires_at.utcoffset() is None:
        raise InvalidProvisioningRequest("expiration must include a timezone")
    if expires_at.astimezone(UTC) <= now.astimezone(UTC):
        raise InvalidProvisioningRequest("expiration must be in the future")
