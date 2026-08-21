"""Transport-agnostic API-principal and worker authentication services."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from uuid import UUID

from taskforge.identity.credentials import (
    DEFAULT_VERIFIER_ALGORITHM,
    DEFAULT_VERIFIERS,
    CredentialScope,
    PresentedCredential,
    VerifierRegistry,
)
from taskforge.identity.ports import (
    APICredentialRepository,
    CredentialRecord,
    WorkerCredentialRepository,
)

# Constructed exactly once and reused for every unknown-ID comparison.
_DUMMY_VERIFIER = DEFAULT_VERIFIERS.encode(
    bytes(32), algorithm=DEFAULT_VERIFIER_ALGORITHM
)


class AuthenticationFailureReason(StrEnum):
    """Internal reasons that must collapse at the transport boundary."""

    WRONG_SCOPE = "wrong_scope"
    UNKNOWN = "unknown"
    INVALID = "invalid"
    EXPIRED = "expired"
    REVOKED = "revoked"
    IDENTITY_DISABLED = "identity_disabled"


class AuthenticationFailure(Exception):
    """Safe credential rejection without credential material."""

    def __init__(self, reason: AuthenticationFailureReason) -> None:
        self.reason = reason
        super().__init__(reason.value)


class AuthenticationUnavailable(Exception):
    """Authentication persistence was unavailable or timed out."""


@dataclass(frozen=True)
class AuthenticatedAPIPrincipal:
    principal_id: UUID
    credential_id: UUID
    credential_expires_at: datetime | None = None
    credential_observed_at: datetime | None = None


@dataclass(frozen=True)
class AuthenticatedWorker:
    worker_identity_id: UUID
    credential_id: UUID


class APIAuthenticator:
    """Authenticate only already-parsed API-principal credentials."""

    def __init__(
        self,
        repository: APICredentialRepository,
        *,
        timeout_seconds: float,
        verifiers: VerifierRegistry = DEFAULT_VERIFIERS,
    ) -> None:
        self._repository = repository
        self._timeout_seconds = timeout_seconds
        self._verifiers = verifiers

    async def authenticate(
        self, credential: PresentedCredential
    ) -> AuthenticatedAPIPrincipal:
        if credential.scope is not CredentialScope.API:
            raise AuthenticationFailure(AuthenticationFailureReason.WRONG_SCOPE)
        try:
            async with asyncio.timeout(self._timeout_seconds):
                record = await self._repository.find_api_credential(
                    credential.credential_id
                )
        except asyncio.CancelledError:
            raise
        except TimeoutError as error:
            raise AuthenticationUnavailable from error
        except Exception as error:
            raise AuthenticationUnavailable from error
        _validate_record(credential, record, self._verifiers)
        assert record is not None
        return AuthenticatedAPIPrincipal(
            principal_id=record.identity_id,
            credential_id=record.credential_id,
            credential_expires_at=record.expires_at,
            credential_observed_at=record.observed_at,
        )


class WorkerAuthenticator:
    """Authenticate only already-parsed worker credentials."""

    def __init__(
        self,
        repository: WorkerCredentialRepository,
        *,
        timeout_seconds: float,
        verifiers: VerifierRegistry = DEFAULT_VERIFIERS,
    ) -> None:
        self._repository = repository
        self._timeout_seconds = timeout_seconds
        self._verifiers = verifiers

    async def authenticate(
        self, credential: PresentedCredential
    ) -> AuthenticatedWorker:
        if credential.scope is not CredentialScope.WORKER:
            raise AuthenticationFailure(AuthenticationFailureReason.WRONG_SCOPE)
        try:
            async with asyncio.timeout(self._timeout_seconds):
                record = await self._repository.find_worker_credential(
                    credential.credential_id
                )
        except asyncio.CancelledError:
            raise
        except TimeoutError as error:
            raise AuthenticationUnavailable from error
        except Exception as error:
            raise AuthenticationUnavailable from error
        _validate_record(credential, record, self._verifiers)
        assert record is not None
        return AuthenticatedWorker(
            worker_identity_id=record.identity_id,
            credential_id=record.credential_id,
        )


def _validate_record(
    credential: PresentedCredential,
    record: CredentialRecord | None,
    verifiers: VerifierRegistry,
) -> None:
    encoded_verifier = (
        record.credential_verifier if record is not None else _DUMMY_VERIFIER
    )
    valid = verifiers.verify(credential.secret, encoded_verifier)
    if record is None:
        raise AuthenticationFailure(AuthenticationFailureReason.UNKNOWN)
    if not valid:
        raise AuthenticationFailure(AuthenticationFailureReason.INVALID)
    if record.revoked:
        raise AuthenticationFailure(AuthenticationFailureReason.REVOKED)
    if record.expired:
        raise AuthenticationFailure(AuthenticationFailureReason.EXPIRED)
    if record.identity_disabled:
        raise AuthenticationFailure(AuthenticationFailureReason.IDENTITY_DISABLED)
