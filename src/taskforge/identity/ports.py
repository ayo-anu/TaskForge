"""Persistence ports required by authentication services."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID


@dataclass(frozen=True, repr=False)
class CredentialRecord:
    """Credential state loaded without exposing verifier material in repr."""

    credential_id: UUID
    identity_id: UUID
    credential_verifier: str
    revoked: bool
    expired: bool
    identity_disabled: bool
    expires_at: datetime | None = None
    observed_at: datetime | None = None

    def __repr__(self) -> str:
        return (
            "CredentialRecord(credential_id=<redacted>, identity_id=<redacted>, "
            "credential_verifier=<redacted>, "
            f"revoked={self.revoked!r}, expired={self.expired!r}, "
            f"identity_disabled={self.identity_disabled!r})"
        )


class APICredentialRepository(Protocol):
    """Look up only API-principal credential records."""

    async def find_api_credential(self, credential_id: UUID) -> CredentialRecord | None:
        """Return an API credential and current lifecycle state."""


class WorkerCredentialRepository(Protocol):
    """Look up only worker credential records."""

    async def find_worker_credential(
        self, credential_id: UUID
    ) -> CredentialRecord | None:
        """Return a worker credential and current lifecycle state."""
