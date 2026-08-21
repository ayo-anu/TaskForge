"""Transport-independent authentication service tests."""

from __future__ import annotations

import asyncio
import secrets
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from taskforge.identity.authentication import (
    APIAuthenticator,
    AuthenticationFailure,
    AuthenticationFailureReason,
    AuthenticationUnavailable,
    WorkerAuthenticator,
)
from taskforge.identity.credentials import (
    DEFAULT_VERIFIER_ALGORITHM,
    DEFAULT_VERIFIERS,
    CredentialScope,
    PresentedCredential,
)
from taskforge.identity.ports import CredentialRecord


class FakeAPIRepository:
    def __init__(
        self,
        record: CredentialRecord | None,
        *,
        error: Exception | None = None,
        delay: float = 0,
    ) -> None:
        self.record = record
        self.error = error
        self.delay = delay
        self.lookups: list[UUID] = []

    async def find_api_credential(self, credential_id: UUID) -> CredentialRecord | None:
        self.lookups.append(credential_id)
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.error:
            raise self.error
        return self.record


class FakeWorkerRepository:
    def __init__(self, record: CredentialRecord | None) -> None:
        self.record = record
        self.lookups: list[UUID] = []

    async def find_worker_credential(
        self, credential_id: UUID
    ) -> CredentialRecord | None:
        self.lookups.append(credential_id)
        return self.record


def credential(scope: CredentialScope, secret: bytes) -> PresentedCredential:
    return PresentedCredential(scope=scope, credential_id=uuid4(), secret=secret)


def record(
    presented: PresentedCredential,
    *,
    secret: bytes | None = None,
    revoked: bool = False,
    expired: bool = False,
    disabled: bool = False,
) -> CredentialRecord:
    return CredentialRecord(
        credential_id=presented.credential_id,
        identity_id=uuid4(),
        credential_verifier=DEFAULT_VERIFIERS.encode(
            secret or presented.secret,
            algorithm=DEFAULT_VERIFIER_ALGORITHM,
        ),
        revoked=revoked,
        expired=expired,
        identity_disabled=disabled,
    )


def test_authenticates_api_principal_and_worker_as_distinct_types() -> None:
    api_credential = credential(CredentialScope.API, secrets.token_bytes(32))
    worker_credential = credential(CredentialScope.WORKER, secrets.token_bytes(32))
    api_record = record(api_credential)
    worker_record = record(worker_credential)

    api_identity = asyncio.run(
        APIAuthenticator(
            FakeAPIRepository(api_record), timeout_seconds=0.1
        ).authenticate(api_credential)
    )
    worker_identity = asyncio.run(
        WorkerAuthenticator(
            FakeWorkerRepository(worker_record), timeout_seconds=0.1
        ).authenticate(worker_credential)
    )

    assert api_identity.principal_id == api_record.identity_id
    assert worker_identity.worker_identity_id == worker_record.identity_id


def test_api_authentication_preserves_database_observed_expiry_metadata() -> None:
    api_credential = credential(CredentialScope.API, secrets.token_bytes(32))
    observed_at = datetime.now(UTC)
    expires_at = observed_at + timedelta(minutes=5)
    original = record(api_credential)
    api_record = CredentialRecord(
        original.credential_id,
        original.identity_id,
        original.credential_verifier,
        original.revoked,
        original.expired,
        original.identity_disabled,
        expires_at,
        observed_at,
    )

    identity = asyncio.run(
        APIAuthenticator(
            FakeAPIRepository(api_record), timeout_seconds=0.1
        ).authenticate(api_credential)
    )

    assert identity.credential_expires_at == expires_at
    assert identity.credential_observed_at == observed_at


@pytest.mark.parametrize(
    ("record_changes", "expected_reason"),
    (
        ({"secret": secrets.token_bytes(32)}, AuthenticationFailureReason.INVALID),
        ({"revoked": True}, AuthenticationFailureReason.REVOKED),
        ({"expired": True}, AuthenticationFailureReason.EXPIRED),
        ({"disabled": True}, AuthenticationFailureReason.IDENTITY_DISABLED),
    ),
)
def test_rejects_each_invalid_api_credential_state(
    record_changes: dict[str, object],
    expected_reason: AuthenticationFailureReason,
) -> None:
    presented = credential(CredentialScope.API, secrets.token_bytes(32))
    repository = FakeAPIRepository(record(presented, **record_changes))  # type: ignore[arg-type]

    with pytest.raises(AuthenticationFailure) as error:
        asyncio.run(
            APIAuthenticator(repository, timeout_seconds=0.1).authenticate(presented)
        )

    assert error.value.reason is expected_reason


def test_unknown_credential_runs_dummy_verification_and_is_rejected() -> None:
    presented = credential(CredentialScope.API, secrets.token_bytes(32))

    with pytest.raises(AuthenticationFailure) as error:
        asyncio.run(
            APIAuthenticator(FakeAPIRepository(None), timeout_seconds=0.1).authenticate(
                presented
            )
        )

    assert error.value.reason is AuthenticationFailureReason.UNKNOWN


def test_wrong_scopes_are_rejected_without_repository_access() -> None:
    api_repository = FakeAPIRepository(None)
    worker_repository = FakeWorkerRepository(None)
    worker_credential = credential(CredentialScope.WORKER, secrets.token_bytes(32))
    api_credential = credential(CredentialScope.API, secrets.token_bytes(32))

    with pytest.raises(AuthenticationFailure) as api_error:
        asyncio.run(
            APIAuthenticator(api_repository, timeout_seconds=0.1).authenticate(
                worker_credential
            )
        )
    with pytest.raises(AuthenticationFailure) as worker_error:
        asyncio.run(
            WorkerAuthenticator(worker_repository, timeout_seconds=0.1).authenticate(
                api_credential
            )
        )

    assert api_error.value.reason is AuthenticationFailureReason.WRONG_SCOPE
    assert worker_error.value.reason is AuthenticationFailureReason.WRONG_SCOPE
    assert api_repository.lookups == []
    assert worker_repository.lookups == []


@pytest.mark.parametrize(
    "repository",
    (
        FakeAPIRepository(None, error=RuntimeError("database topology detail")),
        FakeAPIRepository(None, delay=0.05),
    ),
)
def test_repository_failure_and_timeout_are_safely_normalized(
    repository: FakeAPIRepository,
) -> None:
    presented = credential(CredentialScope.API, secrets.token_bytes(32))

    with pytest.raises(AuthenticationUnavailable) as error:
        asyncio.run(
            APIAuthenticator(repository, timeout_seconds=0.001).authenticate(presented)
        )

    assert str(error.value) == ""


def test_authentication_failures_do_not_represent_credentials() -> None:
    presented = credential(CredentialScope.API, secrets.token_bytes(32))
    invalid_record = record(presented, secret=secrets.token_bytes(32))

    with pytest.raises(AuthenticationFailure) as error:
        asyncio.run(
            APIAuthenticator(
                FakeAPIRepository(invalid_record), timeout_seconds=0.1
            ).authenticate(presented)
        )

    rendered = repr(error.value)
    assert str(presented.credential_id) not in rendered
    assert presented.secret.hex() not in rendered
    assert invalid_record.credential_verifier not in rendered
