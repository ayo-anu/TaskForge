"""Identity and credential persistence boundary."""

from taskforge.identity.authentication import (
    AuthenticatedAPIPrincipal,
    AuthenticatedWorker,
)
from taskforge.identity.authorization import (
    AuthorizationContext,
    AuthorizationService,
    OwnerFilter,
    Permission,
    Role,
)
from taskforge.identity.credentials import CredentialScope, PresentedCredential
from taskforge.identity.schema import (
    API_ROLES,
    api_credentials,
    api_principal_roles,
    api_principals,
    worker_credentials,
    worker_identities,
)

__all__ = [
    "API_ROLES",
    "AuthenticatedAPIPrincipal",
    "AuthenticatedWorker",
    "AuthorizationContext",
    "AuthorizationService",
    "CredentialScope",
    "OwnerFilter",
    "Permission",
    "PresentedCredential",
    "Role",
    "api_credentials",
    "api_principal_roles",
    "api_principals",
    "worker_credentials",
    "worker_identities",
]
