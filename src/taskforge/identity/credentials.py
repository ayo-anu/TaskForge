"""Transport-neutral credential parsing and verifier handling."""

from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol
from uuid import UUID, uuid4

SECRET_BYTES = 32
SECRET_ENCODED_LENGTH = 43
MAX_PRESENTED_CREDENTIAL_LENGTH = 128
VERIFIER_FORMAT_VERSION = "v1"


class CredentialScope(StrEnum):
    """Scopes that remain distinct throughout authentication."""

    API = "api"
    WORKER = "worker"


SCOPE_PREFIXES = {
    CredentialScope.API: "tf_api_v1",
    CredentialScope.WORKER: "tf_worker_v1",
}


class CredentialFormatError(ValueError):
    """A presented credential is not structurally valid."""


@dataclass(frozen=True, repr=False)
class PresentedCredential:
    """Parsed credential whose secret must never enter a representation."""

    scope: CredentialScope
    credential_id: UUID
    secret: bytes

    def __repr__(self) -> str:
        return (
            "PresentedCredential(scope="
            f"{self.scope!r}, credential_id=<redacted>, secret=<redacted>)"
        )

    def __str__(self) -> str:
        return "<redacted credential>"


class CredentialAlreadyConsumed(RuntimeError):
    """A newly issued credential cannot be displayed twice."""


class GeneratedCredential:
    """New verifier plus a consumable one-time presentation value."""

    def __init__(
        self,
        *,
        credential_id: UUID,
        credential_verifier: str,
        presented_value: str,
    ) -> None:
        self.credential_id = credential_id
        self.credential_verifier = credential_verifier
        self._presented_value: str | None = presented_value

    def take_presented_value(self) -> str:
        value, self._presented_value = self._presented_value, None
        if value is None:
            raise CredentialAlreadyConsumed("credential has already been consumed")
        return value

    def __repr__(self) -> str:
        return (
            "GeneratedCredential(credential_id=<redacted>, "
            "credential_verifier=<redacted>, presented_value=<redacted>)"
        )

    def __str__(self) -> str:
        return "<redacted generated credential>"


class VerifierAlgorithm(Protocol):
    """One algorithm supported by a self-describing verifier registry."""

    name: str

    def derive(self, secret: bytes) -> bytes:
        """Derive comparison material from a high-entropy secret."""


class SHA256Verifier:
    """Initial verifier for uniformly random machine credentials."""

    name = "sha256"

    def derive(self, secret: bytes) -> bytes:
        return hashlib.sha256(secret).digest()


class VerifierRegistry:
    """Encode and verify versioned values without coupling callers to an algorithm."""

    def __init__(self, algorithms: tuple[VerifierAlgorithm, ...]) -> None:
        self._algorithms = {algorithm.name: algorithm for algorithm in algorithms}

    def encode(self, secret: bytes, *, algorithm: str) -> str:
        selected = self._algorithms[algorithm]
        derived = _encode_base64url(selected.derive(secret))
        return f"{VERIFIER_FORMAT_VERSION}${selected.name}${derived}"

    def verify(self, secret: bytes, encoded_verifier: str) -> bool:
        try:
            version, algorithm_name, expected = encoded_verifier.split("$", maxsplit=2)
            algorithm = self._algorithms[algorithm_name]
        except (KeyError, ValueError):
            return False
        if version != VERIFIER_FORMAT_VERSION:
            return False
        actual = _encode_base64url(algorithm.derive(secret))
        return hmac.compare_digest(actual, expected)


DEFAULT_VERIFIERS = VerifierRegistry((SHA256Verifier(),))
DEFAULT_VERIFIER_ALGORITHM = SHA256Verifier.name


def generate_credential(
    scope: CredentialScope,
    *,
    random_bytes: Callable[[int], bytes] = secrets.token_bytes,
    credential_id_factory: Callable[[], UUID] = uuid4,
    verifiers: VerifierRegistry = DEFAULT_VERIFIERS,
    algorithm: str = DEFAULT_VERIFIER_ALGORITHM,
) -> GeneratedCredential:
    """Generate a scoped machine credential and its durable verifier."""
    try:
        secret = random_bytes(SECRET_BYTES)
        credential_id = credential_id_factory()
    except Exception as error:
        raise RuntimeError("secure credential generation failed") from error
    if not isinstance(secret, bytes) or len(secret) != SECRET_BYTES:
        raise RuntimeError("secure credential generation failed")
    if not isinstance(credential_id, UUID):
        raise RuntimeError("credential identifier generation failed")
    encoded_secret = _encode_base64url(secret)
    presented_value = f"{SCOPE_PREFIXES[scope]}.{credential_id}.{encoded_secret}"
    return GeneratedCredential(
        credential_id=credential_id,
        credential_verifier=verifiers.encode(secret, algorithm=algorithm),
        presented_value=presented_value,
    )


def parse_presented_credential(value: str) -> PresentedCredential:
    """Parse a bounded credential value without transport-specific behavior."""
    if not value or len(value) > MAX_PRESENTED_CREDENTIAL_LENGTH:
        raise CredentialFormatError("invalid credential format")
    try:
        prefix, raw_credential_id, encoded_secret = value.split(".")
        scope = next(
            scope
            for scope, expected_prefix in SCOPE_PREFIXES.items()
            if prefix == expected_prefix
        )
        credential_id = UUID(raw_credential_id)
        if str(credential_id) != raw_credential_id:
            raise ValueError
        if len(encoded_secret) != SECRET_ENCODED_LENGTH:
            raise ValueError
        secret = _decode_base64url(encoded_secret)
        if len(secret) != SECRET_BYTES:
            raise ValueError
    except (StopIteration, ValueError) as error:
        raise CredentialFormatError("invalid credential format") from error
    return PresentedCredential(
        scope=scope,
        credential_id=credential_id,
        secret=secret,
    )


def _encode_base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _decode_base64url(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    decoded = base64.b64decode(value + padding, altchars=b"-_", validate=True)
    if _encode_base64url(decoded) != value:
        raise ValueError
    return decoded
