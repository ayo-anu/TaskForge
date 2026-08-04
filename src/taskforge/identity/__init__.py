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
from taskforge.identity.credentials import (
    CredentialScope,
    GeneratedCredential,
    PresentedCredential,
)
from taskforge.identity.principals import PrincipalProfile, PrincipalProfileService
from taskforge.identity.provisioning import (
    CredentialIssuanceService,
    CredentialRevocationService,
    IdentityProvisioningService,
)
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
    "CredentialIssuanceService",
    "CredentialRevocationService",
    "CredentialScope",
    "GeneratedCredential",
    "IdentityProvisioningService",
    "OwnerFilter",
    "Permission",
    "PresentedCredential",
    "PrincipalProfile",
    "PrincipalProfileService",
    "Role",
    "api_credentials",
    "api_principal_roles",
    "api_principals",
    "worker_credentials",
    "worker_identities",
]
