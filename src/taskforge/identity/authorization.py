"""Transport-independent role and resource-ownership authorization policy."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

from taskforge.identity.authentication import (
    AuthenticatedAPIPrincipal,
    AuthenticatedWorker,
)
from taskforge.identity.authorization_ports import PrincipalRoleRepository


class Role(StrEnum):
    VIEWER = "viewer"
    WORKFLOW_OPERATOR = "workflow_operator"
    ADMINISTRATOR = "administrator"


class Permission(StrEnum):
    VIEW = "view"
    AUTHOR_WORKFLOW = "author_workflow"
    OPERATE_WORKFLOW = "operate_workflow"
    ADMINISTER = "administer"


ROLE_PERMISSIONS: dict[Role, frozenset[Permission]] = {
    Role.VIEWER: frozenset({Permission.VIEW}),
    Role.WORKFLOW_OPERATOR: frozenset(
        {
            Permission.VIEW,
            Permission.AUTHOR_WORKFLOW,
            Permission.OPERATE_WORKFLOW,
        }
    ),
    Role.ADMINISTRATOR: frozenset(Permission),
}


class AuthorizationDenied(Exception):
    """An identity cannot perform an action; details stay inside the domain."""


class AuthorizationUnavailable(Exception):
    """Current authorization state could not be loaded safely."""


@dataclass(frozen=True)
class OwnerFilter:
    """Explicit query constraint without nullable-scope ambiguity."""

    unrestricted: bool
    principal_id: UUID | None

    def __post_init__(self) -> None:
        if self.unrestricted is (self.principal_id is not None):
            raise ValueError("owner filter must be unrestricted or principal-bound")

    @classmethod
    def all_owners(cls) -> OwnerFilter:
        return cls(unrestricted=True, principal_id=None)

    @classmethod
    def only(cls, principal_id: UUID) -> OwnerFilter:
        return cls(unrestricted=False, principal_id=principal_id)


@dataclass(frozen=True)
class AuthorizationContext:
    """Current API-principal roles and reusable resource policy hooks."""

    principal_id: UUID
    roles: frozenset[Role]

    def require(self, permission: Permission) -> None:
        if not self.allows(permission):
            raise AuthorizationDenied

    def allows(self, permission: Permission) -> bool:
        return any(permission in ROLE_PERMISSIONS[role] for role in self.roles)

    def require_owned(self, permission: Permission, owner_principal_id: UUID) -> None:
        self.require(permission)
        if (
            Role.ADMINISTRATOR not in self.roles
            and owner_principal_id != self.principal_id
        ):
            raise AuthorizationDenied

    def owner_filter_for(self, permission: Permission) -> OwnerFilter:
        """Return the owner predicate a future repository must apply."""
        self.require(permission)
        if Role.ADMINISTRATOR in self.roles:
            return OwnerFilter.all_owners()
        return OwnerFilter.only(self.principal_id)


class AuthorizationService:
    """Load current roles and create policy contexts for API principals only."""

    def __init__(
        self,
        repository: PrincipalRoleRepository,
        *,
        timeout_seconds: float,
    ) -> None:
        self._repository = repository
        self._timeout_seconds = timeout_seconds

    async def context_for(
        self,
        identity: AuthenticatedAPIPrincipal | AuthenticatedWorker,
    ) -> AuthorizationContext:
        if isinstance(identity, AuthenticatedWorker):
            raise AuthorizationDenied
        try:
            async with asyncio.timeout(self._timeout_seconds):
                raw_roles = await self._repository.find_role_names(
                    identity.principal_id
                )
        except asyncio.CancelledError:
            raise
        except TimeoutError as error:
            raise AuthorizationUnavailable from error
        except Exception as error:
            raise AuthorizationUnavailable from error
        try:
            roles = frozenset(Role(role) for role in raw_roles)
        except ValueError as error:
            raise AuthorizationUnavailable from error
        return AuthorizationContext(principal_id=identity.principal_id, roles=roles)
