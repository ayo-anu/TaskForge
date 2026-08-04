"""Principal profile query service with resource-aware authorization."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

from taskforge.identity.authorization import AuthorizationContext, Permission

if TYPE_CHECKING:
    from taskforge.identity.principal_ports import PrincipalProfileRepository


@dataclass(frozen=True)
class PrincipalProfile:
    id: UUID
    name: str
    created_at: datetime


class PrincipalNotFound(Exception):
    """A principal is absent or hidden by the same repository path."""


class PrincipalServiceUnavailable(Exception):
    """Principal persistence was unavailable or timed out."""


class PrincipalProfileService:
    def __init__(
        self,
        repository: PrincipalProfileRepository,
        *,
        timeout_seconds: float,
    ) -> None:
        self._repository = repository
        self._timeout_seconds = timeout_seconds

    async def get(
        self,
        principal_id: UUID,
        context: AuthorizationContext,
    ) -> PrincipalProfile:
        owner_filter = context.owner_filter_for(Permission.VIEW)
        try:
            async with asyncio.timeout(self._timeout_seconds):
                profile = await self._repository.find_profile(
                    principal_id,
                    owner_filter,
                )
        except asyncio.CancelledError:
            raise
        except TimeoutError as error:
            raise PrincipalServiceUnavailable from error
        except Exception as error:
            raise PrincipalServiceUnavailable from error
        if profile is None:
            raise PrincipalNotFound
        return profile
