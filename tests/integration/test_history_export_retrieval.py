"""PostgreSQL high-water, exclusion, alias, and confidentiality export contracts."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import UTC, datetime
from uuid import UUID

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from taskforge.audit.domain import AuditAction
from taskforge.history.domain import HistoryFilters
from taskforge.identity.authorization import OwnerFilter
from taskforge.persistence.history import SQLAlchemyHistoryRepository
from tests.integration.postgresql import (
    asyncpg_dsn,
    migration_database_url,
    temporary_database,
)
from tests.integration.test_dead_letter_migrations import seed_facts

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_MIGRATION_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_MIGRATION_INTEGRATION=1 explicitly",
    ),
]

FIXED_TIME = datetime(2099, 8, 26, 12, tzinfo=UTC)
HIGH = UUID("ffffffff-ffff-4fff-8fff-ffffffffffff")
PREVIOUS = UUID("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee")
CURRENT = UUID("00000000-0000-4000-8000-000000000001")
ALIAS = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")


def test_global_and_run_exports_exclude_only_current_initiation_audit() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_history_export"
    ) as database_url:
        config = Config("alembic.ini")
        url = database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        with migration_database_url(url):
            command.upgrade(config, "head")
        asyncio.run(_assert_exports(database_url, url))


async def _assert_exports(database_url: object, sqlalchemy_url: str) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))  # type: ignore[arg-type]
    try:
        facts = await seed_facts(connection)
        run_id = await connection.fetchval(
            "SELECT workflow_run_id FROM task_runs WHERE id=$1", facts.task_run
        )
        provenance = json.dumps(
            {
                "export_schema_version": "taskforge.history-export.v1",
                "filter_fingerprint": "0" * 64,
                "high_water_present": True,
            }
        )
        await connection.executemany(
            "INSERT INTO audit_records(id,actor_kind,api_principal_id,action,outcome,"
            "resource_type,resource_id,correlation_id,diagnostic_provenance,occurred_at) "
            "VALUES($1,'api_principal',$2,$3,'accepted',$4,$5,'request-export',$6,$7)",
            [
                (
                    HIGH,
                    facts.principal,
                    "workflow.publish",
                    "workflow",
                    None,
                    "{}",
                    FIXED_TIME,
                ),
                (
                    PREVIOUS,
                    facts.principal,
                    "audit.export",
                    "audit_records",
                    None,
                    provenance,
                    FIXED_TIME,
                ),
                (
                    ALIAS,
                    facts.principal,
                    "workflow.version_published",
                    "workflow",
                    None,
                    "{}",
                    FIXED_TIME,
                ),
            ],
        )
    finally:
        await connection.close()

    engine = create_async_engine(sqlalchemy_url)
    repository = SQLAlchemyHistoryRepository(
        async_sessionmaker(engine, expire_on_commit=False)
    )
    try:
        initialized = await repository.initialize_export(
            "audit", None, OwnerFilter.all_owners(), HistoryFilters()
        )
        assert initialized.generated_at.tzinfo is not None
        assert initialized.high_water is not None
        assert initialized.high_water.source_key == str(HIGH)

        connection = await asyncpg.connect(asyncpg_dsn(database_url))  # type: ignore[arg-type]
        try:
            await connection.execute(
                "INSERT INTO audit_records(id,actor_kind,api_principal_id,action,outcome,"
                "resource_type,correlation_id,diagnostic_provenance,occurred_at) "
                "VALUES($1,'api_principal',$2,'audit.export','accepted','audit_records',"
                "'current-export',$3,$4)",
                CURRENT,
                facts.principal,
                provenance,
                FIXED_TIME,
            )
        finally:
            await connection.close()

        assert str(CURRENT) < initialized.high_water.source_key
        global_page = await repository.list_export_page(
            "audit",
            None,
            OwnerFilter.all_owners(),
            limit=100,
            after=None,
            high_water=initialized.high_water,
            current_export_audit_id=CURRENT,
            filters=HistoryFilters(),
        )
        global_ids = {UUID(item.source_key) for item in global_page}
        assert CURRENT not in global_ids
        assert PREVIOUS in global_ids

        alias_filters = HistoryFilters(action=AuditAction.WORKFLOW_PUBLISH)
        alias_initialized = await repository.initialize_export(
            "audit", None, OwnerFilter.all_owners(), alias_filters
        )
        assert alias_initialized.high_water is not None
        alias_page = await repository.list_export_page(
            "audit",
            None,
            OwnerFilter.all_owners(),
            limit=100,
            after=None,
            high_water=alias_initialized.high_water,
            current_export_audit_id=CURRENT,
            filters=alias_filters,
        )
        assert ALIAS in {UUID(item.source_key) for item in alias_page}
        assert {item.data["action"] for item in alias_page} == {"workflow.publish"}

        connection = await asyncpg.connect(asyncpg_dsn(database_url))  # type: ignore[arg-type]
        try:
            run_high = UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd")
            run_previous = UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")
            run_current = UUID("00000000-0000-4000-8000-000000000002")
            await connection.executemany(
                "INSERT INTO audit_records(id,actor_kind,api_principal_id,action,outcome,"
                "resource_type,resource_id,diagnostic_provenance,occurred_at) "
                "VALUES($1,'api_principal',$2,$3,'accepted','workflow_run',$4,$5,$6)",
                [
                    (
                        run_high,
                        facts.principal,
                        "workflow_run.history_export",
                        run_id,
                        provenance,
                        FIXED_TIME,
                    ),
                    (
                        run_previous,
                        facts.principal,
                        "workflow_run.history_export",
                        run_id,
                        provenance,
                        FIXED_TIME,
                    ),
                ],
            )
            run_initialized = await repository.initialize_export(
                "run", run_id, OwnerFilter.only(facts.principal), HistoryFilters()
            )
            assert run_initialized.high_water is not None
            assert run_initialized.high_water.source_key == str(run_high)
            await connection.execute(
                "INSERT INTO audit_records(id,actor_kind,api_principal_id,action,outcome,"
                "resource_type,resource_id,diagnostic_provenance,occurred_at) "
                "VALUES($1,'api_principal',$2,'workflow_run.history_export','accepted',"
                "'workflow_run',$3,$4,$5)",
                run_current,
                facts.principal,
                run_id,
                provenance,
                FIXED_TIME,
            )
        finally:
            await connection.close()
        run_page = await repository.list_export_page(
            "run",
            run_id,
            OwnerFilter.only(facts.principal),
            limit=100,
            after=None,
            high_water=run_initialized.high_water,
            current_export_audit_id=run_current,
            filters=HistoryFilters(),
        )
        assert str(run_current) < run_initialized.high_water.source_key
        run_ids = {
            UUID(item.source_key)
            for item in run_page
            if item.record_type.value == "audit_record"
        }
        assert run_current not in run_ids
        assert run_previous in run_ids
    finally:
        await engine.dispose()
