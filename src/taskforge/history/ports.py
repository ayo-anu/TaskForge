"""Read-only persistence port for authorized histories."""

from typing import Protocol
from uuid import UUID

from taskforge.history.domain import HistoryCursor, HistoryFilters, HistoryPage
from taskforge.identity.authorization import OwnerFilter


class HistoryRepository(Protocol):
    async def list_history(
        self,
        scope_type: str,
        scope_id: UUID | None,
        owner_filter: OwnerFilter,
        *,
        limit: int,
        cursor: HistoryCursor | None,
        filters: HistoryFilters,
    ) -> HistoryPage: ...
