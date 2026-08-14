"""Persistence boundary for atomic task claim acquisition."""

from __future__ import annotations

from typing import Protocol
from uuid import UUID

from taskforge.claims.domain import (
    InspectedTaskClaim,
    TaskClaimRenewalRequest,
    TaskClaimRenewalResult,
    TaskClaimResult,
)
from taskforge.dispatch.envelope import DispatchEnvelope
from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.identity.authorization import OwnerFilter


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


class TaskClaimRenewalExpired(Exception): ...


class TaskClaimRenewalRecovered(Exception): ...


class TaskClaimRenewalStale(Exception): ...


class TaskClaimRenewalTaskInactive(Exception): ...


class TaskClaimInspectionNotFound(Exception): ...


class TaskClaimInspectionInvariantViolation(Exception): ...


class TaskClaimInspectionPersistenceUnavailable(Exception): ...


class TaskClaimRepository(Protocol):
    async def acquire_claim(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        dispatch: DispatchEnvelope,
        *,
        lease_seconds: int,
    ) -> TaskClaimResult: ...

    async def renew_claim(
        self,
        authenticated_worker: AuthenticatedWorker,
        request: TaskClaimRenewalRequest,
        *,
        lease_seconds: int,
    ) -> TaskClaimRenewalResult: ...


class TaskClaimInspectionRepository(Protocol):
    async def get_current_claim(
        self, task_attempt_id: UUID, owner_filter: OwnerFilter
    ) -> InspectedTaskClaim: ...
