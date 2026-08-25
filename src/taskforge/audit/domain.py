"""Safe, bounded contracts for durable audit records."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from uuid import UUID

MAX_AUDIT_PROVENANCE_BYTES = 2048
_NAME = re.compile(r"\A[a-z][a-z0-9_.-]{0,127}\Z")


class AuditActorKind(StrEnum):
    API_PRINCIPAL = "api_principal"
    WORKER = "worker"
    SYSTEM = "system"


class AuditOutcome(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"


@dataclass(frozen=True)
class AuditActor:
    kind: AuditActorKind
    api_principal_id: UUID | None = None
    worker_identity_id: UUID | None = None
    worker_session_id: UUID | None = None
    system_component: str | None = None

    def __post_init__(self) -> None:
        api = self.api_principal_id is not None
        worker = self.worker_identity_id is not None
        system = self.system_component is not None
        if self.kind is AuditActorKind.API_PRINCIPAL and not (
            api and not worker and not system and self.worker_session_id is None
        ):
            raise ValueError("API audit actor shape is invalid")
        if self.kind is AuditActorKind.WORKER and not (
            worker and not api and not system
        ):
            raise ValueError("worker audit actor shape is invalid")
        if self.kind is AuditActorKind.SYSTEM and not (
            system and not api and not worker and self.worker_session_id is None
        ):
            raise ValueError("system audit actor shape is invalid")
        if (
            self.system_component is not None
            and _NAME.fullmatch(self.system_component) is None
        ):
            raise ValueError("system audit component is invalid")


@dataclass(frozen=True)
class AuditRecord:
    id: UUID
    actor: AuditActor
    action: str
    outcome: AuditOutcome
    resource_type: str
    resource_id: UUID | None
    correlation_id: str | None
    provenance: Mapping[str, object]
    reason_code: str | None = None

    def __post_init__(self) -> None:
        for value in (self.action, self.resource_type):
            if _NAME.fullmatch(value) is None:
                raise ValueError("audit name is invalid")
        if (self.outcome is AuditOutcome.REJECTED) is (self.reason_code is None):
            raise ValueError("audit rejection reason shape is invalid")
        if self.reason_code is not None and _NAME.fullmatch(self.reason_code) is None:
            raise ValueError("audit reason is invalid")
        if self.correlation_id is not None and not (
            1 <= len(self.correlation_id) <= 128
            and all(32 <= ord(char) <= 126 for char in self.correlation_id)
        ):
            raise ValueError("audit correlation is invalid")
        copied = json.loads(
            json.dumps(dict(self.provenance), separators=(",", ":"), sort_keys=True)
        )
        if not isinstance(copied, dict) or len(copied) > 16 or _depth(copied) > 2:
            raise ValueError("audit provenance shape is invalid")
        if (
            len(json.dumps(copied, ensure_ascii=False, sort_keys=True).encode())
            > MAX_AUDIT_PROVENANCE_BYTES
        ):
            raise ValueError("audit provenance is too large")
        object.__setattr__(self, "provenance", MappingProxyType(copied))


class AuditRejected(Exception):
    """Required rejected-operation audit could not be committed."""


def bounded_string_set(values: tuple[str, ...] | frozenset[str]) -> dict[str, object]:
    """Represent arbitrary valid domain lists without exceeding the audit envelope."""
    ordered = sorted(values)
    encoded = json.dumps(ordered, ensure_ascii=False, separators=(",", ":")).encode()
    return {"count": len(ordered), "sha256": hashlib.sha256(encoded).hexdigest()}


def _depth(value: object) -> int:
    if isinstance(value, dict):
        return 1 + max((_depth(item) for item in value.values()), default=0)
    if isinstance(value, list):
        return 1 + max((_depth(item) for item in value), default=0)
    return 0
