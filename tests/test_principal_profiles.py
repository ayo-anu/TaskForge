"""Principal profile service ownership and failure-path tests."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from taskforge.identity.authorization import (
    AuthorizationContext,
    AuthorizationDenied,
    OwnerFilter,
    Role,
)
from taskforge.identity.principals import (
    PrincipalNotFound,
    PrincipalProfile,
    PrincipalProfileService,
    PrincipalServiceUnavailable,
)


class FakeProfileRepository:
    def __init__(
        self,
        profile: PrincipalProfile | None,
        *,
        error: Exception | None = None,
        delay: float = 0,
    ) -> None:
        self.profile = profile
        self.error = error
        self.delay = delay
        self.calls: list[tuple[UUID, OwnerFilter]] = []

    async def find_profile(
        self,
        principal_id: UUID,
        owner_filter: OwnerFilter,
    ) -> PrincipalProfile | None:
        self.calls.append((principal_id, owner_filter))
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.error:
            raise self.error
        return self.profile


def profile(principal_id: UUID) -> PrincipalProfile:
    return PrincipalProfile(
        id=principal_id,
        name="principal-profile",
        created_at=datetime.now(UTC),
    )


def test_non_administrator_profile_read_is_owner_filtered() -> None:
    principal_id = uuid4()
    repository = FakeProfileRepository(profile(principal_id))
    service = PrincipalProfileService(repository, timeout_seconds=0.1)
    context = AuthorizationContext(principal_id, frozenset({Role.VIEWER}))

    result = asyncio.run(service.get(principal_id, context))

    assert result.id == principal_id
    assert repository.calls == [
        (
            principal_id,
            OwnerFilter(unrestricted=False, principal_id=principal_id),
        )
    ]


def test_administrator_profile_read_is_explicitly_unrestricted() -> None:
    target_id = uuid4()
    repository = FakeProfileRepository(profile(target_id))
    service = PrincipalProfileService(repository, timeout_seconds=0.1)
    context = AuthorizationContext(uuid4(), frozenset({Role.ADMINISTRATOR}))

    asyncio.run(service.get(target_id, context))

    assert repository.calls == [
        (target_id, OwnerFilter(unrestricted=True, principal_id=None))
    ]


def test_hidden_and_nonexistent_profiles_follow_the_identical_not_found_path() -> None:
    repository = FakeProfileRepository(None)
    service = PrincipalProfileService(repository, timeout_seconds=0.1)
    context = AuthorizationContext(uuid4(), frozenset({Role.VIEWER}))
    hidden_id, nonexistent_id = uuid4(), uuid4()

    for target_id in (hidden_id, nonexistent_id):
        with pytest.raises(PrincipalNotFound) as error:
            asyncio.run(service.get(target_id, context))
        assert str(error.value) == ""

    assert [call[0] for call in repository.calls] == [hidden_id, nonexistent_id]
    assert repository.calls[0][1] == repository.calls[1][1]


def test_missing_permission_denies_before_repository_access() -> None:
    repository = FakeProfileRepository(None)
    service = PrincipalProfileService(repository, timeout_seconds=0.1)
    context = AuthorizationContext(uuid4(), frozenset())

    with pytest.raises(AuthorizationDenied):
        asyncio.run(service.get(uuid4(), context))

    assert repository.calls == []


@pytest.mark.parametrize(
    "repository",
    (
        FakeProfileRepository(None, error=RuntimeError("database detail")),
        FakeProfileRepository(None, delay=0.05),
    ),
)
def test_profile_repository_failures_are_safely_normalized(
    repository: FakeProfileRepository,
) -> None:
    context = AuthorizationContext(uuid4(), frozenset({Role.VIEWER}))
    service = PrincipalProfileService(repository, timeout_seconds=0.001)

    with pytest.raises(PrincipalServiceUnavailable) as error:
        asyncio.run(service.get(context.principal_id, context))

    assert str(error.value) == ""
