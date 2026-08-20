"""Application service for authorized dead-letter operations."""

from __future__ import annotations

from uuid import UUID

from taskforge.dead_letters.domain import (
    CreatedDeadLetterRedrive,
    DeadLetterActionCursor,
    DeadLetterActionPage,
    DeadLetterCursor,
    DeadLetterDetail,
    DeadLetterFilters,
    DeadLetterPage,
    DeadLetterStatus,
    create_dead_letter_redrive_idempotency,
)
from taskforge.dead_letters.persistence_ports import DeadLetterRepository
from taskforge.identity.authorization import OwnerFilter


class DeadLetterNotFound(Exception):
    """The item is absent from the caller's authorized scope."""


class DeadLetterService:
    def __init__(self, repository: DeadLetterRepository) -> None:
        self._repository = repository

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
        if result is None:
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
        item = await self._repository.transition(
            item_id,
            owner_filter,
            operator_principal_id=operator_principal_id,
            target_status=target_status,
            reason=reason,
            correlation_id=correlation_id,
        )
        if item is None:
            raise DeadLetterNotFound
        return item
