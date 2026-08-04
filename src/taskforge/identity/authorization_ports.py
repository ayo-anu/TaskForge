"""Persistence ports required by API-principal authorization."""

from __future__ import annotations

from typing import Protocol
from uuid import UUID


class PrincipalRoleRepository(Protocol):
    """Load current persisted roles for one API principal."""

    async def find_role_names(self, principal_id: UUID) -> frozenset[str]:
        """Return raw persisted values for fail-closed domain validation."""
