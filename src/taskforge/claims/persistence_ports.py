"""Persistence boundary for atomic task claim acquisition."""

from __future__ import annotations

from typing import Protocol
from uuid import UUID

from taskforge.claims.domain import TaskClaimResult
from taskforge.dispatch.envelope import DispatchEnvelope
from taskforge.identity.authentication import AuthenticatedWorker


class TaskClaimDispatchRejected(Exception): ...


class TaskClaimAttemptStale(Exception): ...


class TaskClaimNotEligible(Exception): ...


class TaskClaimAlreadyOwned(Exception): ...


class TaskClaimAuthorityRejected(Exception): ...


class TaskClaimSessionUnavailable(Exception): ...


class TaskClaimSessionInactive(Exception): ...


class TaskClaimWorkerUnavailable(Exception): ...


class TaskClaimCapabilityMismatch(Exception): ...


class TaskClaimInvariantViolation(Exception): ...


class TaskClaimPersistenceUnavailable(Exception): ...


class TaskClaimRepository(Protocol):
    async def acquire_claim(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        dispatch: DispatchEnvelope,
        *,
        lease_seconds: int,
    ) -> TaskClaimResult: ...
