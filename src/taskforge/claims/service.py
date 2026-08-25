"""Application service for atomic task claim acquisition."""

from uuid import UUID, uuid4

from taskforge.audit.domain import (
    AuditActor,
    AuditActorKind,
    AuditOutcome,
    AuditRecord,
    AuditRejected,
)
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
    TaskClaimInvariantViolation,
    TaskClaimNotEligible,
    TaskClaimPersistenceUnavailable,
    TaskClaimRenewalExpired,
    TaskClaimRenewalRecovered,
    TaskClaimRenewalStale,
    TaskClaimRenewalTaskInactive,
    TaskClaimRepository,
    TaskClaimSessionInactive,
    TaskClaimSessionUnavailable,
    TaskClaimWorkerUnavailable,
)
from taskforge.dispatch.envelope import DispatchEnvelope
from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.identity.authorization import OwnerFilter
from taskforge.persistence.audit import RejectedAuditRecorder

_ACQUISITION_REJECTION_REASONS: dict[type[Exception], TaskClaimRejectionReason] = {
    TaskClaimDispatchRejected: TaskClaimRejectionReason.INVALID_DISPATCH,
    TaskClaimAttemptStale: TaskClaimRejectionReason.STALE_ATTEMPT,
    TaskClaimNotEligible: TaskClaimRejectionReason.OBSOLETE_TASK,
    TaskClaimAuthorityRejected: TaskClaimRejectionReason.WORKER_AUTHORITY_REJECTED,
    TaskClaimSessionUnavailable: TaskClaimRejectionReason.WORKER_SESSION_UNAVAILABLE,
    TaskClaimSessionInactive: TaskClaimRejectionReason.WORKER_SESSION_INACTIVE,
    TaskClaimWorkerUnavailable: TaskClaimRejectionReason.WORKER_UNAVAILABLE,
    TaskClaimCapabilityMismatch: TaskClaimRejectionReason.CAPABILITY_MISMATCH,
    TaskClaimAlreadyOwned: TaskClaimRejectionReason.ALREADY_AUTHORITATIVE,
}
_EXPECTED_ACQUISITION_REJECTIONS = tuple(_ACQUISITION_REJECTION_REASONS)
_RENEWAL_AUDIT_REASONS: dict[type[Exception], str] = {
    TaskClaimRenewalExpired: "claim_expired",
    TaskClaimRenewalRecovered: "claim_recovered",
    TaskClaimRenewalStale: "stale_claim",
    TaskClaimRenewalTaskInactive: "task_inactive",
}
_EXPECTED_RENEWAL_REJECTIONS = tuple(_RENEWAL_AUDIT_REASONS)


class TaskClaimServiceInvariantError(Exception):
    """Durable claim state violated an internal invariant."""


class TaskClaimServiceUnavailable(Exception):
    """Claim persistence is operationally unavailable."""


class TaskClaimService:
    def __init__(
        self,
        repository: TaskClaimRepository,
        authority_issuer: TaskClaimResultAuthorityIssuer,
        *,
        lease_seconds: int,
        rejected_audit: RejectedAuditRecorder | None = None,
    ) -> None:
        if lease_seconds <= 0:
            raise ValueError("claim lease duration must be positive")
        self._repository = repository
        self._authority_issuer = authority_issuer
        self._lease_seconds = lease_seconds
        self._rejected_audit = rejected_audit

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
            reason = _ACQUISITION_REJECTION_REASONS[type(error)]
            await self._audit_rejection(
                authenticated_worker,
                worker_session_id,
                action="task_claim.acquire",
                reason_code=reason.value,
                task_attempt_id=dispatch.task_attempt_id,
                correlation_id=dispatch.correlation_id,
                provenance={"attempt_number": dispatch.attempt_number},
            )
            raise TaskClaimRejected(reason) from error
        except TaskClaimInvariantViolation as error:
            raise TaskClaimServiceInvariantError from error
        except TaskClaimPersistenceUnavailable as error:
            raise TaskClaimServiceUnavailable from error
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
        try:
            return await self._repository.renew_claim(
                authenticated_worker,
                request,
                lease_seconds=self._lease_seconds,
            )
        except _EXPECTED_RENEWAL_REJECTIONS as error:
            await self._audit_rejection(
                authenticated_worker,
                request.worker_session_id,
                action="task_claim.renew",
                reason_code=_RENEWAL_AUDIT_REASONS[type(error)],
                task_attempt_id=request.task_attempt_id,
                correlation_id=request.correlation_id,
                provenance={"claim_generation": request.generation},
            )
            raise

    async def _audit_rejection(
        self,
        worker: AuthenticatedWorker,
        worker_session_id: UUID,
        *,
        action: str,
        reason_code: str,
        task_attempt_id: UUID,
        correlation_id: str | None,
        provenance: dict[str, object],
    ) -> None:
        if self._rejected_audit is None:
            return
        try:
            await self._rejected_audit.record(
                AuditRecord(
                    uuid4(),
                    AuditActor(
                        AuditActorKind.WORKER,
                        worker_identity_id=worker.worker_identity_id,
                        worker_session_id=worker_session_id,
                    ),
                    action,
                    AuditOutcome.REJECTED,
                    "task_attempt",
                    task_attempt_id,
                    correlation_id,
                    provenance,
                    reason_code,
                )
            )
        except AuditRejected as error:
            raise TaskClaimServiceUnavailable from error


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
