"""Credential generation and separately factored provisioning service tests."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from taskforge.identity.authorization import Role
from taskforge.identity.credentials import (
    CredentialAlreadyConsumed,
    CredentialScope,
    generate_credential,
    parse_presented_credential,
)
from taskforge.identity.provisioning import (
    CredentialIssuanceService,
    CredentialNotFound,
    CredentialRevocationService,
    DuplicateIdentity,
    IdentityDisabled,
    IdentityNotFound,
    IdentityProvisioningService,
    InvalidProvisioningRequest,
    ProvisioningUnavailable,
)
from taskforge.identity.provisioning_ports import (
    CredentialRecordNotFound,
    DuplicateIdentityRecord,
    IdentityRecordDisabled,
    IdentityRecordNotFound,
)


class FakeTransaction:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []
        self.error: Exception | None = None

    async def create_api_principal(self, principal_id: UUID, name: str) -> None:
        self._record("create_api_principal", principal_id, name)

    async def assign_api_roles(
        self, principal_id: UUID, roles: frozenset[Role]
    ) -> None:
        self._record("assign_api_roles", principal_id, roles)

    async def create_worker_identity(self, worker_id: UUID, name: str) -> None:
        self._record("create_worker_identity", worker_id, name)

    async def add_api_credential(
        self,
        credential_id: UUID,
        principal_id: UUID,
        credential_verifier: str,
        expires_at: datetime,
    ) -> None:
        self._record(
            "add_api_credential",
            credential_id,
            principal_id,
            credential_verifier,
            expires_at,
        )

    async def add_worker_credential(
        self,
        credential_id: UUID,
        worker_id: UUID,
        credential_verifier: str,
        expires_at: datetime,
    ) -> None:
        self._record(
            "add_worker_credential",
            credential_id,
            worker_id,
            credential_verifier,
            expires_at,
        )

    async def revoke_api_credential(
        self, principal_id: UUID, credential_id: UUID
    ) -> None:
        self._record("revoke_api_credential", principal_id, credential_id)

    async def revoke_worker_credential(
        self, worker_id: UUID, credential_id: UUID
    ) -> None:
        self._record("revoke_worker_credential", worker_id, credential_id)

    async def commit(self) -> None:
        self._record("commit")

    def _record(self, *values: object) -> None:
        if self.error:
            raise self.error
        self.calls.append(values)


@pytest.mark.parametrize("scope", tuple(CredentialScope))
def test_generated_credentials_match_existing_format_and_are_one_time(
    scope: CredentialScope,
) -> None:
    generated = generate_credential(scope)

    rendered = f"{generated!r} {generated!s}"
    presented_value = generated.take_presented_value()
    parsed = parse_presented_credential(presented_value)

    assert parsed.scope is scope
    assert parsed.credential_id == generated.credential_id
    assert presented_value not in rendered
    assert generated.credential_verifier not in rendered
    with pytest.raises(CredentialAlreadyConsumed):
        generated.take_presented_value()


@pytest.mark.parametrize(
    "random_bytes",
    (
        lambda size: b"short",
        lambda size: "not-bytes",
        lambda size: (_ for _ in ()).throw(RuntimeError("random source detail")),
    ),
)
def test_invalid_secret_generation_fails_without_credential_material(
    random_bytes: object,
) -> None:
    with pytest.raises(RuntimeError) as error:
        generate_credential(
            CredentialScope.API,
            random_bytes=random_bytes,  # type: ignore[arg-type]
        )

    assert "random source detail" not in str(error.value)


def test_identity_creation_and_credential_issuance_are_distinct_operations() -> None:
    transaction = FakeTransaction()
    identities = IdentityProvisioningService()
    issuance = CredentialIssuanceService()
    expiration = datetime.now(UTC) + timedelta(days=30)

    principal_id = asyncio.run(
        identities.create_api_principal(
            transaction,
            name="local-administrator",
            roles=frozenset({Role.ADMINISTRATOR}),
        )
    )
    generated = asyncio.run(
        issuance.issue_api_credential(
            transaction,
            principal_id=principal_id,
            expires_at=expiration,
        )
    )

    assert [call[0] for call in transaction.calls] == [
        "create_api_principal",
        "assign_api_roles",
        "add_api_credential",
    ]
    assert generated.credential_id


def test_worker_creation_never_assigns_api_roles() -> None:
    transaction = FakeTransaction()
    service = IdentityProvisioningService()

    asyncio.run(service.create_worker_identity(transaction, name="local-worker"))

    assert [call[0] for call in transaction.calls] == ["create_worker_identity"]


@pytest.mark.parametrize("name", ("", " padded", "padded ", "x" * 129))
def test_invalid_names_are_rejected_before_persistence(name: str) -> None:
    transaction = FakeTransaction()

    with pytest.raises(InvalidProvisioningRequest):
        asyncio.run(
            IdentityProvisioningService().create_worker_identity(
                transaction,
                name=name,
            )
        )

    assert transaction.calls == []


def test_api_principal_requires_at_least_one_role() -> None:
    transaction = FakeTransaction()

    with pytest.raises(InvalidProvisioningRequest):
        asyncio.run(
            IdentityProvisioningService().create_api_principal(
                transaction,
                name="no-role",
                roles=frozenset(),
            )
        )


@pytest.mark.parametrize(
    ("expiration", "expected"),
    (
        (datetime.now(), InvalidProvisioningRequest),
        (datetime.now(UTC) - timedelta(seconds=1), InvalidProvisioningRequest),
    ),
)
def test_invalid_expirations_are_rejected_before_generation(
    expiration: datetime,
    expected: type[Exception],
) -> None:
    transaction = FakeTransaction()

    with pytest.raises(expected):
        asyncio.run(
            CredentialIssuanceService().issue_api_credential(
                transaction,
                principal_id=uuid4(),
                expires_at=expiration,
            )
        )

    assert transaction.calls == []


@pytest.mark.parametrize(
    ("persistence_error", "domain_error"),
    (
        (DuplicateIdentityRecord(), DuplicateIdentity),
        (IdentityRecordNotFound(), IdentityNotFound),
        (IdentityRecordDisabled(), IdentityDisabled),
        (RuntimeError("database detail"), ProvisioningUnavailable),
    ),
)
def test_persistence_failures_are_safely_normalized(
    persistence_error: Exception,
    domain_error: type[Exception],
) -> None:
    transaction = FakeTransaction()
    transaction.error = persistence_error
    operation: Awaitable[object]

    if isinstance(persistence_error, DuplicateIdentityRecord):
        operation = IdentityProvisioningService().create_worker_identity(
            transaction, name="duplicate"
        )
    else:
        operation = CredentialIssuanceService().issue_worker_credential(
            transaction,
            worker_id=uuid4(),
            expires_at=datetime.now(UTC) + timedelta(days=30),
        )

    with pytest.raises(domain_error) as error:
        asyncio.run(operation)
    assert "database detail" not in str(error.value)


def test_scope_specific_revocation_and_missing_record_behavior() -> None:
    transaction = FakeTransaction()
    service = CredentialRevocationService()
    principal_id, api_credential_id = uuid4(), uuid4()
    worker_id, worker_credential_id = uuid4(), uuid4()

    asyncio.run(
        service.revoke_api_credential(
            transaction,
            principal_id=principal_id,
            credential_id=api_credential_id,
        )
    )
    asyncio.run(
        service.revoke_worker_credential(
            transaction,
            worker_id=worker_id,
            credential_id=worker_credential_id,
        )
    )
    assert transaction.calls == [
        ("revoke_api_credential", principal_id, api_credential_id),
        ("revoke_worker_credential", worker_id, worker_credential_id),
    ]

    transaction.error = CredentialRecordNotFound()
    with pytest.raises(CredentialNotFound):
        asyncio.run(
            service.revoke_api_credential(
                transaction,
                principal_id=principal_id,
                credential_id=uuid4(),
            )
        )
