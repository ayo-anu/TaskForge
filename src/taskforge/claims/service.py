"""Application service for atomic task claim acquisition."""

from uuid import UUID

from taskforge.claims.domain import TaskClaimResult
from taskforge.claims.persistence_ports import TaskClaimRepository
from taskforge.dispatch.envelope import DispatchEnvelope
from taskforge.identity.authentication import AuthenticatedWorker


class TaskClaimService:
    def __init__(self, repository: TaskClaimRepository, *, lease_seconds: int) -> None:
        if lease_seconds <= 0:
            raise ValueError("claim lease duration must be positive")
        self._repository = repository
        self._lease_seconds = lease_seconds

    async def claim_task(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        dispatch: DispatchEnvelope,
    ) -> TaskClaimResult:
        return await self._repository.acquire_claim(
            authenticated_worker,
            worker_session_id,
            dispatch,
            lease_seconds=self._lease_seconds,
        )
