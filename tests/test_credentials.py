"""Tests for scoped credential parsing and self-describing verifiers."""

from __future__ import annotations

import base64
import secrets
from uuid import UUID, uuid4

import pytest

from taskforge.identity.credentials import (
    DEFAULT_VERIFIER_ALGORITHM,
    DEFAULT_VERIFIERS,
    CredentialFormatError,
    CredentialScope,
    VerifierRegistry,
    parse_presented_credential,
)


class ReverseVerifier:
    name = "reverse"

    def derive(self, secret: bytes) -> bytes:
        return secret[::-1]


def encode_secret(secret: bytes) -> str:
    return base64.urlsafe_b64encode(secret).rstrip(b"=").decode("ascii")


def presented_value(
    scope: CredentialScope,
    secret: bytes,
    *,
    credential_id: UUID | str | None = None,
) -> str:
    prefix = "tf_api_v1" if scope is CredentialScope.API else "tf_worker_v1"
    return f"{prefix}.{credential_id or uuid4()}.{encode_secret(secret)}"


@pytest.mark.parametrize("scope", tuple(CredentialScope))
def test_parses_valid_scoped_credentials(scope: CredentialScope) -> None:
    secret = secrets.token_bytes(32)
    credential_id = uuid4()

    parsed = parse_presented_credential(
        presented_value(scope, secret, credential_id=credential_id)
    )

    assert parsed.scope is scope
    assert parsed.credential_id == credential_id
    assert parsed.secret == secret


@pytest.mark.parametrize(
    "value",
    (
        "",
        "not-a-credential",
        "tf_api_v2.invalid.invalid",
        "tf_unknown_v1.00000000-0000-0000-0000-000000000000.invalid",
        "x" * 129,
    ),
)
def test_rejects_malformed_credentials(value: str) -> None:
    with pytest.raises(CredentialFormatError, match="invalid credential format"):
        parse_presented_credential(value)


def test_rejects_noncanonical_uuid_and_secret_encodings() -> None:
    secret = secrets.token_bytes(32)
    credential_id = uuid4()

    with pytest.raises(CredentialFormatError):
        parse_presented_credential(
            presented_value(
                CredentialScope.API,
                secret,
                credential_id=str(credential_id).upper(),
            )
        )
    with pytest.raises(CredentialFormatError):
        parse_presented_credential(
            f"tf_api_v1.{credential_id}.{encode_secret(secret)}="
        )


def test_presented_credential_representations_are_redacted() -> None:
    raw_secret = secrets.token_bytes(32)
    credential_id = uuid4()
    parsed = parse_presented_credential(
        presented_value(
            CredentialScope.API,
            raw_secret,
            credential_id=credential_id,
        )
    )

    rendered = f"{parsed!r} {parsed!s}"

    assert encode_secret(raw_secret) not in rendered
    assert str(credential_id) not in rendered
    assert "redacted" in rendered


def test_verifier_values_are_self_describing_and_algorithm_dispatched() -> None:
    secret = secrets.token_bytes(32)
    registry = VerifierRegistry((ReverseVerifier(),))
    encoded = registry.encode(secret, algorithm="reverse")

    assert encoded.startswith("v1$reverse$")
    assert registry.verify(secret, encoded) is True
    assert registry.verify(secrets.token_bytes(32), encoded) is False
    assert registry.verify(secret, encoded.replace("reverse", "unknown")) is False
    assert registry.verify(secret, encoded.replace("v1$", "v2$")) is False


def test_default_verifier_accepts_only_the_matching_secret() -> None:
    secret = secrets.token_bytes(32)
    encoded = DEFAULT_VERIFIERS.encode(
        secret,
        algorithm=DEFAULT_VERIFIER_ALGORITHM,
    )

    assert DEFAULT_VERIFIERS.verify(secret, encoded) is True
    assert DEFAULT_VERIFIERS.verify(secrets.token_bytes(32), encoded) is False
