"""Application service for atomic task claim acquisition."""

from uuid import UUID

from taskforge.claims.authority import TaskClaimResultAuthorityIssuer
from taskforge.claims.domain import (
    IssuedTaskClaim,
    TaskClaimOutcome,
    TaskClaimRenewalRequest,
    TaskClaimRenewalResult,
)
from taskforge.claims.persistence_ports import TaskClaimRepository
from taskforge.dispatch.envelope import DispatchEnvelope
from taskforge.identity.authentication import AuthenticatedWorker


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
        result = await self._repository.acquire_claim(
            authenticated_worker,
            worker_session_id,
            dispatch,
            lease_seconds=self._lease_seconds,
        )
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
