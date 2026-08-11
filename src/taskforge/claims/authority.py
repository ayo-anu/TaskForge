"""Opaque, result-specific authority for one task claim generation."""

from __future__ import annotations

import base64
import hashlib
import hmac
from dataclasses import dataclass
from uuid import UUID

from taskforge.claims.domain import TaskClaimResultAuthority

_DOMAIN = b"taskforge.claim.result-authority\x00"
_PREFIX = "tf_claim_result_v1"
_VERSION = 1
_MINIMUM_SECRET_BYTES = 32


@dataclass(frozen=True, repr=False)
class TaskClaimResultAuthorityIssuer:
    """Issue deterministic result authority without persisting bearer material."""

    _secret: bytes

    def __post_init__(self) -> None:
        if len(self._secret) < _MINIMUM_SECRET_BYTES:
            raise ValueError("claim result authority secret must be at least 32 bytes")

    def __repr__(self) -> str:
        return "TaskClaimResultAuthorityIssuer(secret=<redacted>)"

    def issue(
        self,
        *,
        worker_identity_id: UUID,
        worker_session_id: UUID,
        task_attempt_id: UUID,
        generation: int,
    ) -> TaskClaimResultAuthority:
        digest = hmac.new(
            self._secret,
            _authority_message(
                worker_identity_id,
                worker_session_id,
                task_attempt_id,
                generation,
            ),
            hashlib.sha256,
        ).digest()
        encoded = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
        return TaskClaimResultAuthority(f"{_PREFIX}.{encoded}")

    def verify(
        self,
        authority: TaskClaimResultAuthority,
        *,
        worker_identity_id: UUID,
        worker_session_id: UUID,
        task_attempt_id: UUID,
        generation: int,
    ) -> bool:
        expected = self.issue(
            worker_identity_id=worker_identity_id,
            worker_session_id=worker_session_id,
            task_attempt_id=task_attempt_id,
            generation=generation,
        )
        return hmac.compare_digest(authority.presented_value, expected.presented_value)


def _authority_message(
    worker_identity_id: UUID,
    worker_session_id: UUID,
    task_attempt_id: UUID,
    generation: int,
) -> bytes:
    if generation <= 0 or generation > (2**63 - 1):
        raise ValueError("claim generation must be a positive BIGINT")
    return b"".join(
        (
            _DOMAIN,
            _VERSION.to_bytes(1, "big"),
            worker_identity_id.bytes,
            worker_session_id.bytes,
            task_attempt_id.bytes,
            generation.to_bytes(8, "big", signed=True),
        )
    )
