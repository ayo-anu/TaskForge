"""Role matrix, ownership, and authorization-service tests."""

from __future__ import annotations

import asyncio
from uuid import UUID, uuid4

import pytest

from taskforge.identity.authentication import (
    AuthenticatedAPIPrincipal,
    AuthenticatedWorker,
)
from taskforge.identity.authorization import (
    ROLE_PERMISSIONS,
    AuthorizationContext,
    AuthorizationDenied,
    AuthorizationService,
    AuthorizationUnavailable,
    OwnerFilter,
    Permission,
    Role,
)
from taskforge.identity.schema import API_ROLES


class FakeRoleRepository:
    def __init__(
        self,
        roles: frozenset[str],
        *,
        error: Exception | None = None,
        delay: float = 0,
    ) -> None:
        self.roles = roles
        self.error = error
        self.delay = delay
        self.lookups: list[UUID] = []

    async def find_role_names(self, principal_id: UUID) -> frozenset[str]:
        self.lookups.append(principal_id)
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.error:
            raise self.error
        return self.roles


EXPECTED_ROLE_MATRIX = {
    Role.VIEWER: {Permission.VIEW},
    Role.WORKFLOW_OPERATOR: {
        Permission.VIEW,
        Permission.AUTHOR_WORKFLOW,
        Permission.OPERATE_WORKFLOW,
    },
    Role.ADMINISTRATOR: set(Permission),
}


@pytest.mark.parametrize("role", tuple(Role))
@pytest.mark.parametrize("permission", tuple(Permission))
def test_complete_role_permission_matrix(role: Role, permission: Permission) -> None:
    context = AuthorizationContext(uuid4(), frozenset({role}))

    assert context.allows(permission) is (permission in EXPECTED_ROLE_MATRIX[role])
    if permission in EXPECTED_ROLE_MATRIX[role]:
        context.require(permission)
    else:
        with pytest.raises(AuthorizationDenied):
            context.require(permission)


def test_declared_policy_matches_the_reviewable_role_matrix() -> None:
    assert {
        role: set(permissions) for role, permissions in ROLE_PERMISSIONS.items()
    } == (EXPECTED_ROLE_MATRIX)
    assert {role.value for role in Role} == set(API_ROLES)


def test_multiple_roles_combine_permissions_and_empty_roles_deny() -> None:
    combined = AuthorizationContext(
        uuid4(),
        frozenset({Role.VIEWER, Role.WORKFLOW_OPERATOR}),
    )
    empty = AuthorizationContext(uuid4(), frozenset())

    assert combined.allows(Permission.VIEW) is True
    assert combined.allows(Permission.OPERATE_WORKFLOW) is True
    assert combined.allows(Permission.ADMINISTER) is False
    assert all(empty.allows(permission) is False for permission in Permission)


@pytest.mark.parametrize(
    ("unrestricted", "principal_id"),
    ((True, uuid4()), (False, None)),
)
def test_owner_filter_rejects_ambiguous_states(
    unrestricted: bool,
    principal_id: UUID | None,
) -> None:
    with pytest.raises(ValueError):
        OwnerFilter(unrestricted=unrestricted, principal_id=principal_id)


@pytest.mark.parametrize("role", (Role.VIEWER, Role.WORKFLOW_OPERATOR))
def test_non_administrators_are_isolated_to_owned_resources(role: Role) -> None:
    principal_id = uuid4()
    context = AuthorizationContext(principal_id, frozenset({role}))

    context.require_owned(Permission.VIEW, principal_id)
    with pytest.raises(AuthorizationDenied):
        context.require_owned(Permission.VIEW, uuid4())

    owner_filter = context.owner_filter_for(Permission.VIEW)
    assert owner_filter.unrestricted is False
    assert owner_filter.principal_id == principal_id


def test_administrator_can_cross_ownership_boundaries_explicitly() -> None:
    context = AuthorizationContext(uuid4(), frozenset({Role.ADMINISTRATOR}))

    context.require_owned(Permission.ADMINISTER, uuid4())
    owner_filter = context.owner_filter_for(Permission.ADMINISTER)

    assert owner_filter.unrestricted is True
    assert owner_filter.principal_id is None


def test_ownership_never_overrides_a_missing_permission() -> None:
    principal_id = uuid4()
    viewer = AuthorizationContext(principal_id, frozenset({Role.VIEWER}))

    with pytest.raises(AuthorizationDenied):
        viewer.require_owned(Permission.OPERATE_WORKFLOW, principal_id)
    with pytest.raises(AuthorizationDenied):
        viewer.owner_filter_for(Permission.OPERATE_WORKFLOW)


def test_service_loads_current_roles_for_the_authenticated_principal() -> None:
    identity = AuthenticatedAPIPrincipal(uuid4(), uuid4())
    repository = FakeRoleRepository(frozenset({Role.WORKFLOW_OPERATOR.value}))
    service = AuthorizationService(repository, timeout_seconds=0.1)

    context = asyncio.run(service.context_for(identity))

    assert context.principal_id == identity.principal_id
    assert context.roles == frozenset({Role.WORKFLOW_OPERATOR})
    assert repository.lookups == [identity.principal_id]


def test_fresh_context_observes_changed_roles() -> None:
    identity = AuthenticatedAPIPrincipal(uuid4(), uuid4())
    repository = FakeRoleRepository(frozenset({Role.VIEWER.value}))
    service = AuthorizationService(repository, timeout_seconds=0.1)

    first = asyncio.run(service.context_for(identity))
    repository.roles = frozenset({Role.ADMINISTRATOR.value})
    second = asyncio.run(service.context_for(identity))

    assert first.roles == frozenset({Role.VIEWER})
    assert second.roles == frozenset({Role.ADMINISTRATOR})
    assert len(repository.lookups) == 2


def test_authenticated_worker_is_rejected_immediately_at_service_boundary() -> None:
    repository = FakeRoleRepository(frozenset({Role.ADMINISTRATOR.value}))
    service = AuthorizationService(repository, timeout_seconds=0.1)
    worker = AuthenticatedWorker(uuid4(), uuid4())

    with pytest.raises(AuthorizationDenied):
        asyncio.run(service.context_for(worker))

    assert repository.lookups == []


@pytest.mark.parametrize(
    "repository",
    (
        FakeRoleRepository(frozenset({"unexpected-role"})),
        FakeRoleRepository(frozenset(), error=RuntimeError("database detail")),
        FakeRoleRepository(frozenset(), delay=0.05),
    ),
)
def test_invalid_data_failure_and_timeout_fail_closed(
    repository: FakeRoleRepository,
) -> None:
    service = AuthorizationService(repository, timeout_seconds=0.001)
    identity = AuthenticatedAPIPrincipal(uuid4(), uuid4())

    with pytest.raises(AuthorizationUnavailable) as error:
        asyncio.run(service.context_for(identity))

    assert str(error.value) == ""


def test_denials_do_not_represent_identity_or_owner_details() -> None:
    principal_id, owner_id = uuid4(), uuid4()
    context = AuthorizationContext(principal_id, frozenset({Role.VIEWER}))

    with pytest.raises(AuthorizationDenied) as error:
        context.require_owned(Permission.VIEW, owner_id)

    rendered = repr(error.value)
    assert str(principal_id) not in rendered
    assert str(owner_id) not in rendered
