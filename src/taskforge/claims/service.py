"""Application service for atomic task claim acquisition."""

from uuid import UUID

from taskforge.claims.authority import TaskClaimResultAuthorityIssuer
from taskforge.claims.domain import (
    InspectedTaskClaim,
    IssuedTaskClaim,
    TaskClaimOutcome,
    TaskClaimRejected,
    TaskClaimRejectionReason,
    TaskClaimRenewalRequest,
    TaskClaimRenewalResult,
)
from taskforge.claims.persistence_ports import (
    TaskClaimAlreadyOwned,
    TaskClaimAttemptStale,
    TaskClaimAuthorityRejected,
    TaskClaimCapabilityMismatch,
    TaskClaimDispatchRejected,
    TaskClaimInspectionInvariantViolation,
    TaskClaimInspectionNotFound,
    TaskClaimInspectionPersistenceUnavailable,
    TaskClaimInspectionRepository,
    TaskClaimNotEligible,
    TaskClaimRepository,
    TaskClaimSessionInactive,
    TaskClaimSessionUnavailable,
    TaskClaimWorkerUnavailable,
)
from taskforge.dispatch.envelope import DispatchEnvelope
from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.identity.authorization import OwnerFilter

_ACQUISITION_REJECTION_REASONS: dict[type[Exception], TaskClaimRejectionReason] = {
    TaskClaimDispatchRejected: TaskClaimRejectionReason.INVALID_OR_STALE_DISPATCH,
    TaskClaimAttemptStale: TaskClaimRejectionReason.INVALID_OR_STALE_DISPATCH,
    TaskClaimNotEligible: TaskClaimRejectionReason.TASK_NOT_CLAIMABLE,
    TaskClaimAuthorityRejected: TaskClaimRejectionReason.WORKER_AUTHORITY_REJECTED,
    TaskClaimSessionUnavailable: TaskClaimRejectionReason.WORKER_AUTHORITY_REJECTED,
    TaskClaimSessionInactive: TaskClaimRejectionReason.WORKER_AUTHORITY_REJECTED,
    TaskClaimWorkerUnavailable: TaskClaimRejectionReason.WORKER_NOT_ELIGIBLE,
    TaskClaimCapabilityMismatch: TaskClaimRejectionReason.WORKER_NOT_ELIGIBLE,
    TaskClaimAlreadyOwned: TaskClaimRejectionReason.ALREADY_AUTHORITATIVE,
}
_EXPECTED_ACQUISITION_REJECTIONS = tuple(_ACQUISITION_REJECTION_REASONS)


class TaskClaimService:
    def __init__(
        self,
        repository: TaskClaimRepository,
        authority_issuer: TaskClaimResultAuthorityIssuer,
        *,
        lease_seconds: int,
    ) -> None:
        if lease_seconds <= 0:
            raise ValueError("claim lease duration must be positive")
        self._repository = repository
        self._authority_issuer = authority_issuer
        self._lease_seconds = lease_seconds

    async def claim_task(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        dispatch: DispatchEnvelope,
    ) -> IssuedTaskClaim:
        try:
            result = await self._repository.acquire_claim(
                authenticated_worker,
                worker_session_id,
                dispatch,
                lease_seconds=self._lease_seconds,
            )
        except _EXPECTED_ACQUISITION_REJECTIONS as error:
            raise TaskClaimRejected(
                _ACQUISITION_REJECTION_REASONS[type(error)]
            ) from error
        authority = None
        if result.outcome is not TaskClaimOutcome.REPLAYED_EXPIRED:
            authority = self._authority_issuer.issue(
                worker_identity_id=authenticated_worker.worker_identity_id,
                worker_session_id=worker_session_id,
                task_attempt_id=result.claim.task_attempt_id,
                generation=result.claim.generation,
            )
        return IssuedTaskClaim(result.outcome, result.claim, authority)

    async def renew_claim(
        self,
        authenticated_worker: AuthenticatedWorker,
        request: TaskClaimRenewalRequest,
    ) -> TaskClaimRenewalResult:
        return await self._repository.renew_claim(
            authenticated_worker,
            request,
            lease_seconds=self._lease_seconds,
        )


class TaskClaimInspectionService:
    def __init__(self, repository: TaskClaimInspectionRepository) -> None:
        self._repository = repository

    async def get_current_claim(
        self, task_attempt_id: UUID, owner_filter: OwnerFilter
    ) -> InspectedTaskClaim:
        try:
            return await self._repository.get_current_claim(
                task_attempt_id, owner_filter
            )
        except (
            TaskClaimInspectionNotFound,
            TaskClaimInspectionInvariantViolation,
            TaskClaimInspectionPersistenceUnavailable,
        ):
            raise
