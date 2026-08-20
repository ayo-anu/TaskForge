"""Persistence boundaries consumed by dead-letter inspection and commands."""

from __future__ import annotations

from typing import Protocol
from uuid import UUID

from taskforge.dead_letters.domain import (
    CreatedDeadLetterRedrive,
    DeadLetterActionCursor,
    DeadLetterActionPage,
    DeadLetterCursor,
    DeadLetterDetail,
    DeadLetterFilters,
    DeadLetterPage,
    DeadLetterRedriveIdempotency,
    DeadLetterStatus,
)
from taskforge.identity.authorization import OwnerFilter


class DeadLetterPersistenceUnavailable(Exception):
    """Dead-letter persistence was operationally unavailable."""


class DeadLetterPersistenceInvariantViolation(Exception):
    """Durable dead-letter facts violate the expected schema contract."""


class DeadLetterTransitionConflict(Exception):
    """The requested command is invalid for the current status."""


class DeadLetterRedriveNotEligible(Exception):
    """The item or its source execution is not eligible for a new redrive."""


class DeadLetterRedriveLimitExceeded(Exception):
    """The item already owns a successfully materialized redrive."""


class DeadLetterRepository(Protocol):
    async def list_items(
        self,
        owner_filter: OwnerFilter,
        filters: DeadLetterFilters,
        *,
        limit: int,
        cursor: DeadLetterCursor | None,
    ) -> DeadLetterPage: ...

    async def get_item(
        self, item_id: UUID, owner_filter: OwnerFilter
    ) -> DeadLetterDetail | None: ...

    async def list_actions(
        self,
        item_id: UUID,
        owner_filter: OwnerFilter,
        *,
        limit: int,
        cursor: DeadLetterActionCursor | None,
    ) -> DeadLetterActionPage | None: ...

    async def transition(
        self,
        item_id: UUID,
        owner_filter: OwnerFilter,
        *,
        operator_principal_id: UUID,
        target_status: DeadLetterStatus,
        reason: str | None,
        correlation_id: UUID,
    ) -> DeadLetterDetail | None: ...

    async def redrive(
        self,
        item_id: UUID,
        owner_filter: OwnerFilter,
        *,
        operator_principal_id: UUID,
        idempotency: DeadLetterRedriveIdempotency,
        reason: str | None,
        correlation_id: UUID,
    ) -> CreatedDeadLetterRedrive | None: ...
