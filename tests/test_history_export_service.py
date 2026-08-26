"""Initialization auditing and frozen bounded export behavior."""

import asyncio
import json
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from taskforge.api.history import _export_stream
from taskforge.audit.domain import AuditAction
from taskforge.history.domain import (
    HistoryCursor,
    HistoryFilters,
    HistoryItem,
    HistoryRecordType,
)
from taskforge.history.export import ExportInitialization
from taskforge.history.export_service import (
    HistoryExportService,
    HistoryExportUnavailable,
)
from taskforge.identity.authorization import OwnerFilter


class Repository:
    def __init__(self, high_water: HistoryCursor | None) -> None:
        self.high_water = high_water
        self.page_calls = 0
        self.items: tuple[HistoryItem, ...] = ()

    async def initialize_export(self, *args: object) -> ExportInitialization:
        return ExportInitialization(datetime(2026, 8, 26, tzinfo=UTC), self.high_water)

    async def list_export_page(
        self, *args: object, **kwargs: object
    ) -> tuple[HistoryItem, ...]:
        self.page_calls += 1
        return self.items


class Recorder:
    def __init__(self) -> None:
        self.records: list[object] = []

    async def record(self, record: object) -> None:
        self.records.append(record)


class FailingRecorder:
    async def record(self, record: object) -> None:
        del record
        raise RuntimeError("database unavailable")


class PagedRepository(Repository):
    def __init__(
        self,
        high_water: HistoryCursor,
        pages: list[tuple[HistoryItem, ...] | Exception],
    ) -> None:
        super().__init__(high_water)
        self.pages = pages

    async def list_export_page(
        self, *args: object, **kwargs: object
    ) -> tuple[HistoryItem, ...]:
        del args, kwargs
        self.page_calls += 1
        page = self.pages[self.page_calls - 1]
        if isinstance(page, Exception):
            raise page
        return page


def _high_water() -> HistoryCursor:
    return HistoryCursor(
        "audit",
        None,
        "",
        datetime(2026, 8, 26, tzinfo=UTC),
        HistoryRecordType.AUDIT_RECORD,
        10,
        "ffffffff-ffff-4fff-8fff-ffffffffffff",
    )


def _audit_item(source_key: str) -> HistoryItem:
    identifier = UUID(source_key)
    return HistoryItem(
        HistoryRecordType.AUDIT_RECORD,
        datetime(2026, 8, 26, tzinfo=UTC),
        10,
        source_key,
        "safe-correlation",
        {
            "id": identifier,
            "actor_kind": "api_principal",
            "api_principal_id": uuid4(),
            "worker_identity_id": None,
            "worker_session_id": None,
            "system_component": None,
            "action": "workflow.publish",
            "outcome": "accepted",
            "reason_code": None,
            "resource_type": "workflow",
            "resource_id": uuid4(),
            "diagnostic_provenance": {},
        },
    )


def test_empty_high_water_is_frozen_and_never_queries_pages() -> None:
    async def run() -> None:
        repository = Repository(None)
        recorder = Recorder()
        service = HistoryExportService(repository, recorder)  # type: ignore[arg-type]
        state = await service.initialize(
            "audit",
            None,
            OwnerFilter.all_owners(),
            uuid4(),
            uuid4(),
            HistoryFilters(),
        )
        repository.items = (
            HistoryItem(
                HistoryRecordType.AUDIT_RECORD,
                datetime(2026, 8, 26, tzinfo=UTC),
                10,
                str(uuid4()),
                None,
                {},
            ),
        )
        assert [item async for item in service.items(state)] == []
        assert repository.page_calls == 0
        record = recorder.records[0]
        assert record.action == AuditAction.AUDIT_EXPORT.value  # type: ignore[attr-defined]
        assert record.provenance["high_water_present"] is False  # type: ignore[attr-defined]

    asyncio.run(run())


def test_run_export_keeps_exact_audit_id_private_for_page_exclusion() -> None:
    async def run() -> None:
        high_water = HistoryCursor(
            "run",
            UUID("11111111-1111-4111-8111-111111111111"),
            "",
            datetime(2026, 8, 26, tzinfo=UTC),
            HistoryRecordType.AUDIT_RECORD,
            10,
            "ffffffff-ffff-4fff-8fff-ffffffffffff",
        )
        repository = Repository(high_water)
        recorder = Recorder()
        service = HistoryExportService(repository, recorder)  # type: ignore[arg-type]
        state = await service.initialize(
            "run",
            high_water.scope_id,
            OwnerFilter.only(uuid4()),
            uuid4(),
            uuid4(),
            HistoryFilters(),
        )
        assert state.audit_record_id == recorder.records[0].id  # type: ignore[attr-defined]
        assert (
            recorder.records[0].action == AuditAction.WORKFLOW_RUN_HISTORY_EXPORT.value
        )  # type: ignore[attr-defined]

    asyncio.run(run())


def test_audit_failure_is_fail_closed_before_state_is_returned() -> None:
    async def run() -> None:
        service = HistoryExportService(Repository(None), FailingRecorder())  # type: ignore[arg-type]
        try:
            await service.initialize(
                "audit",
                None,
                OwnerFilter.all_owners(),
                uuid4(),
                uuid4(),
                HistoryFilters(),
            )
        except HistoryExportUnavailable:
            return
        raise AssertionError("export initialization unexpectedly succeeded")

    asyncio.run(run())


def test_second_page_failure_emits_no_completion_or_duplicate_and_is_not_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def run() -> None:
        monkeypatch.setattr("taskforge.history.export_service.EXPORT_PAGE_SIZE", 1)
        first = _audit_item("dddddddd-dddd-4ddd-8ddd-dddddddddddd")
        repository = PagedRepository(
            _high_water(), [(first,), RuntimeError("second page unavailable")]
        )
        service = HistoryExportService(repository, Recorder())  # type: ignore[arg-type]
        state = await service.initialize(
            "audit",
            None,
            OwnerFilter.all_owners(),
            uuid4(),
            uuid4(),
            HistoryFilters(),
        )
        iterator = _export_stream(service, state).__aiter__()
        emitted = [await iterator.__anext__(), await iterator.__anext__()]
        with pytest.raises(HistoryExportUnavailable):
            await iterator.__anext__()
        decoded = [json.loads(line) for line in emitted]
        assert [line["kind"] for line in decoded] == ["manifest", "record"]
        assert [line["data"]["id"] for line in decoded if line["kind"] == "record"] == [
            str(first.data["id"])
        ]
        assert all(line["kind"] != "completion" for line in decoded)
        assert repository.page_calls == 2

    asyncio.run(run())


@pytest.mark.parametrize(
    "second_key",
    [
        "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
    ],
)
def test_cross_page_duplicate_or_ascending_key_emits_no_invalid_record_or_completion(
    monkeypatch: pytest.MonkeyPatch, second_key: str
) -> None:
    async def run() -> None:
        monkeypatch.setattr("taskforge.history.export_service.EXPORT_PAGE_SIZE", 1)
        first = _audit_item("dddddddd-dddd-4ddd-8ddd-dddddddddddd")
        repository = PagedRepository(
            _high_water(), [(first,), (_audit_item(second_key),)]
        )
        service = HistoryExportService(repository, Recorder())  # type: ignore[arg-type]
        state = await service.initialize(
            "audit",
            None,
            OwnerFilter.all_owners(),
            uuid4(),
            uuid4(),
            HistoryFilters(),
        )
        iterator = _export_stream(service, state).__aiter__()
        emitted = [await iterator.__anext__(), await iterator.__anext__()]
        with pytest.raises(HistoryExportUnavailable):
            await iterator.__anext__()
        assert [json.loads(line)["kind"] for line in emitted] == ["manifest", "record"]
        assert repository.page_calls == 2

    asyncio.run(run())
