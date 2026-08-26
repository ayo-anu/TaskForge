"""Application service for read-only immutable history inspection."""

from uuid import UUID

from taskforge.history.domain import HistoryCursor, HistoryFilters, HistoryPage
from taskforge.history.ports import HistoryRepository
from taskforge.identity.authorization import OwnerFilter


class HistoryNotFound(Exception):
    pass


class HistoryUnavailable(Exception):
    pass


class HistoryService:
    def __init__(self, repository: HistoryRepository) -> None:
        self._repository = repository

    async def list(
        self,
        scope_type: str,
        scope_id: UUID | None,
        owner_filter: OwnerFilter,
        *,
        limit: int,
        cursor: HistoryCursor | None,
        filters: HistoryFilters,
    ) -> HistoryPage:
        try:
            return await self._repository.list_history(
                scope_type,
                scope_id,
                owner_filter,
                limit=limit,
                cursor=cursor,
                filters=filters,
            )
        except HistoryNotFound:
            raise
        except Exception as error:
            raise HistoryUnavailable from error
