"""Application service for authorized dead-letter operations."""

from __future__ import annotations

from uuid import UUID, uuid4

from taskforge.audit.domain import (
    AuditActor,
    AuditActorKind,
    AuditOutcome,
    AuditRecord,
    AuditRejected,
)
from taskforge.dead_letters.domain import (
    CreatedDeadLetterRedrive,
    DeadLetterActionCursor,
    DeadLetterActionPage,
    DeadLetterCursor,
    DeadLetterDetail,
    DeadLetterFilters,
    DeadLetterPage,
    DeadLetterRedriveIdempotencyConflict,
    DeadLetterStatus,
    InvalidDeadLetterRedriveIdempotencyKey,
    create_dead_letter_redrive_idempotency,
)
from taskforge.dead_letters.persistence_ports import (
    DeadLetterPersistenceUnavailable,
    DeadLetterRedriveLimitExceeded,
    DeadLetterRedriveNotEligible,
    DeadLetterRepository,
    DeadLetterTransitionConflict,
)
from taskforge.identity.authorization import OwnerFilter
from taskforge.persistence.audit import RejectedAuditRecorder


class DeadLetterNotFound(Exception):
    """The item is absent from the caller's authorized scope."""


_DEAD_LETTER_AUDIT_REASONS: dict[type[Exception], str] = {
    DeadLetterNotFound: "dead_letter_not_visible",
    InvalidDeadLetterRedriveIdempotencyKey: "invalid_idempotency_key",
    DeadLetterTransitionConflict: "transition_conflict",
    DeadLetterRedriveNotEligible: "redrive_not_eligible",
    DeadLetterRedriveLimitExceeded: "redrive_limit_exceeded",
    DeadLetterRedriveIdempotencyConflict: "idempotency_conflict",
}


class DeadLetterService:
    def __init__(
        self,
        repository: DeadLetterRepository,
        rejected_audit: RejectedAuditRecorder | None = None,
    ) -> None:
        self._repository = repository
        self._rejected_audit = rejected_audit

    async def list_items(
        self,
        owner_filter: OwnerFilter,
        filters: DeadLetterFilters,
        *,
        limit: int,
        cursor: DeadLetterCursor | None,
    ) -> DeadLetterPage:
        return await self._repository.list_items(
            owner_filter, filters, limit=limit, cursor=cursor
        )

    async def get_item(
        self, item_id: UUID, owner_filter: OwnerFilter
    ) -> DeadLetterDetail:
        item = await self._repository.get_item(item_id, owner_filter)
        if item is None:
            raise DeadLetterNotFound
        return item

    async def list_actions(
        self,
        item_id: UUID,
        owner_filter: OwnerFilter,
        *,
        limit: int,
        cursor: DeadLetterActionCursor | None,
    ) -> DeadLetterActionPage:
        page = await self._repository.list_actions(
            item_id, owner_filter, limit=limit, cursor=cursor
        )
        if page is None:
            raise DeadLetterNotFound
        return page

    async def acknowledge(
        self,
        item_id: UUID,
        owner_filter: OwnerFilter,
        *,
        operator_principal_id: UUID,
        reason: str | None,
        correlation_id: UUID,
    ) -> DeadLetterDetail:
        return await self._transition(
            item_id,
            owner_filter,
            operator_principal_id=operator_principal_id,
            target_status=DeadLetterStatus.ACKNOWLEDGED,
            reason=reason,
            correlation_id=correlation_id,
        )

    async def resolve(
        self,
        item_id: UUID,
        owner_filter: OwnerFilter,
        *,
        operator_principal_id: UUID,
        reason: str,
        correlation_id: UUID,
    ) -> DeadLetterDetail:
        return await self._transition(
            item_id,
            owner_filter,
            operator_principal_id=operator_principal_id,
            target_status=DeadLetterStatus.RESOLVED,
            reason=reason,
            correlation_id=correlation_id,
        )

    async def redrive(
        self,
        item_id: UUID,
        owner_filter: OwnerFilter,
        *,
        operator_principal_id: UUID,
        idempotency_key: object,
        reason: str | None,
        correlation_id: UUID,
    ) -> CreatedDeadLetterRedrive:
        normalized_reason = reason.strip() if reason is not None else None
        try:
            idempotency = create_dead_letter_redrive_idempotency(
                idempotency_key,
                dead_letter_item_id=item_id,
                requested_by_principal_id=operator_principal_id,
                reason=normalized_reason,
            )
            result = await self._repository.redrive(
                item_id,
                owner_filter,
                operator_principal_id=operator_principal_id,
                idempotency=idempotency,
                reason=normalized_reason,
                correlation_id=correlation_id,
            )
        except (
            InvalidDeadLetterRedriveIdempotencyKey,
            DeadLetterRedriveIdempotencyConflict,
            DeadLetterRedriveNotEligible,
            DeadLetterRedriveLimitExceeded,
        ) as error:
            await self._audit_rejection(
                error,
                item_id=item_id,
                principal_id=operator_principal_id,
                action="dead_letter.redrive",
                correlation_id=correlation_id,
            )
            raise
        if result is None:
            await self._audit_rejection(
                DeadLetterNotFound(),
                item_id=item_id,
                principal_id=operator_principal_id,
                action="dead_letter.redrive",
                correlation_id=correlation_id,
            )
            raise DeadLetterNotFound
        return result

    async def _transition(
        self,
        item_id: UUID,
        owner_filter: OwnerFilter,
        *,
        operator_principal_id: UUID,
        target_status: DeadLetterStatus,
        reason: str | None,
        correlation_id: UUID,
    ) -> DeadLetterDetail:
        action = (
            "dead_letter.acknowledge"
            if target_status is DeadLetterStatus.ACKNOWLEDGED
            else "dead_letter.resolve"
        )
        try:
            item = await self._repository.transition(
                item_id,
                owner_filter,
                operator_principal_id=operator_principal_id,
                target_status=target_status,
                reason=reason,
                correlation_id=correlation_id,
            )
        except DeadLetterTransitionConflict as error:
            await self._audit_rejection(
                error,
                item_id=item_id,
                principal_id=operator_principal_id,
                action=action,
                correlation_id=correlation_id,
            )
            raise
        if item is None:
            await self._audit_rejection(
                DeadLetterNotFound(),
                item_id=item_id,
                principal_id=operator_principal_id,
                action=action,
                correlation_id=correlation_id,
            )
            raise DeadLetterNotFound
        return item

    async def _audit_rejection(
        self,
        error: Exception,
        *,
        item_id: UUID,
        principal_id: UUID,
        action: str,
        correlation_id: UUID,
    ) -> None:
        if self._rejected_audit is None:
            return
        try:
            await self._rejected_audit.record(
                AuditRecord(
                    uuid4(),
                    AuditActor(
                        AuditActorKind.API_PRINCIPAL, api_principal_id=principal_id
                    ),
                    action,
                    AuditOutcome.REJECTED,
                    "dead_letter",
                    item_id,
                    str(correlation_id),
                    {},
                    _DEAD_LETTER_AUDIT_REASONS[type(error)],
                )
            )
        except AuditRejected as audit_error:
            raise DeadLetterPersistenceUnavailable from audit_error
