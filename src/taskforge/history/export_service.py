"""Authorized initialization and bounded traversal for history exports."""

from __future__ import annotations

from collections.abc import AsyncIterator
from uuid import UUID, uuid4

from taskforge.audit.domain import (
    AuditAction,
    AuditActor,
    AuditActorKind,
    AuditOutcome,
    AuditRecord,
)
from taskforge.history.domain import (
    HistoryCursor,
    HistoryFilters,
    HistoryItem,
    filter_fingerprint,
)
from taskforge.history.export import (
    EXPORT_PAGE_SIZE,
    EXPORT_SCHEMA_VERSION,
    ExportState,
)
from taskforge.history.export_ports import HistoryExportRepository
from taskforge.history.service import HistoryNotFound
from taskforge.identity.authorization import OwnerFilter
from taskforge.persistence.audit import AuditRecorder


class HistoryExportUnavailable(Exception):
    pass


class HistoryExportService:
    def __init__(
        self, repository: HistoryExportRepository, audit: AuditRecorder
    ) -> None:
        self._repository = repository
        self._audit = audit

    async def initialize(
        self,
        scope_type: str,
        scope_id: UUID | None,
        owner_filter: OwnerFilter,
        principal_id: UUID,
        correlation_id: UUID,
        filters: HistoryFilters,
    ) -> ExportState:
        fingerprint = filter_fingerprint(filters.normalized())
        try:
            initialized = await self._repository.initialize_export(
                scope_type, scope_id, owner_filter, filters
            )
            audit_id = uuid4()
            action = (
                AuditAction.AUDIT_EXPORT
                if scope_type == "audit"
                else AuditAction.WORKFLOW_RUN_HISTORY_EXPORT
            )
            await self._audit.record(
                AuditRecord(
                    audit_id,
                    AuditActor(
                        AuditActorKind.API_PRINCIPAL, api_principal_id=principal_id
                    ),
                    action.value,
                    AuditOutcome.ACCEPTED,
                    "audit_records" if scope_type == "audit" else "workflow_run",
                    scope_id,
                    str(correlation_id),
                    {
                        "export_schema_version": EXPORT_SCHEMA_VERSION,
                        "filter_fingerprint": fingerprint,
                        "high_water_present": initialized.high_water is not None,
                    },
                )
            )
        except Exception as error:
            if isinstance(error, HistoryNotFound):
                raise
            raise HistoryExportUnavailable from error
        return ExportState(
            scope_type,
            scope_id,
            filters,
            owner_filter,
            fingerprint,
            initialized.generated_at,
            initialized.high_water,
            audit_id,
        )

    async def items(self, state: ExportState) -> AsyncIterator[HistoryItem]:
        if state.high_water is None:
            return
        after: HistoryCursor | None = None
        previous_key = (
            state.high_water.occurred_at,
            state.high_water.source_rank,
            state.high_water.source_key,
        )
        first = True
        while True:
            try:
                page = await self._repository.list_export_page(
                    state.scope_type,
                    state.scope_id,
                    state.owner_filter,
                    limit=EXPORT_PAGE_SIZE,
                    after=after,
                    high_water=state.high_water,
                    current_export_audit_id=state.audit_record_id,
                    filters=state.filters,
                )
            except Exception as error:
                raise HistoryExportUnavailable from error
            if not page:
                return
            for item in page:
                item_key = (item.occurred_at, item.source_rank, item.source_key)
                if item_key > previous_key or (not first and item_key == previous_key):
                    raise HistoryExportUnavailable
                first = False
                previous_key = item_key
                yield item
            if len(page) < EXPORT_PAGE_SIZE:
                return
            last = page[-1]
            after = HistoryCursor(
                state.scope_type,
                state.scope_id,
                state.filter_fingerprint,
                last.occurred_at,
                last.record_type,
                last.source_rank,
                last.source_key,
            )
