"""Persistence port for ownership-filtered principal profile reads."""

from __future__ import annotations

from typing import Protocol
from uuid import UUID

from taskforge.identity.authorization import OwnerFilter
from taskforge.identity.principals import PrincipalProfile


class PrincipalProfileRepository(Protocol):
    async def find_profile(
        self,
        principal_id: UUID,
        owner_filter: OwnerFilter,
    ) -> PrincipalProfile | None:
        """Return only a profile visible through the supplied owner filter."""
