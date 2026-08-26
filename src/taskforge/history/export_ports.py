"""Ports for bounded, authorized immutable-history exports."""

from typing import Protocol
from uuid import UUID

from taskforge.history.domain import HistoryCursor, HistoryFilters, HistoryItem
from taskforge.history.export import ExportInitialization
from taskforge.identity.authorization import OwnerFilter


class HistoryExportRepository(Protocol):
    async def initialize_export(
        self,
        scope_type: str,
        scope_id: UUID | None,
        owner_filter: OwnerFilter,
        filters: HistoryFilters,
    ) -> ExportInitialization: ...

    async def list_export_page(
        self,
        scope_type: str,
        scope_id: UUID | None,
        owner_filter: OwnerFilter,
        *,
        limit: int,
        after: HistoryCursor | None,
        high_water: HistoryCursor,
        current_export_audit_id: UUID,
        filters: HistoryFilters,
    ) -> tuple[HistoryItem, ...]: ...
