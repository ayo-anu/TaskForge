"""PostgreSQL retrieval, pagination, alias, redaction, and actor-plan contracts."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import UTC, datetime
from uuid import UUID, uuid4

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from taskforge.audit.domain import AuditAction
from taskforge.history.domain import HistoryFilters, HistoryRecordType
from taskforge.identity.authorization import OwnerFilter
from taskforge.persistence.history import _SCOPE_SQL as SCOPE_SQL
from taskforge.persistence.history import SQLAlchemyHistoryRepository, _filter_sql
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

FIXED_TIME = datetime(2026, 8, 26, 12, tzinfo=UTC)


def test_authorized_audit_retrieval_alias_pagination_and_redaction() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_history_queries"
    ) as database_url:
        config = Config("alembic.ini")
        url = database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        with migration_database_url(url):
            command.upgrade(config, "head")
        asyncio.run(_assert_retrieval(database_url, url))


async def _assert_retrieval(database_url: object, sqlalchemy_url: str) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))  # type: ignore[arg-type]
    principal_id = uuid4()
    workflow_id = uuid4()
    ids = sorted((uuid4(), uuid4(), uuid4()), reverse=True)
    try:
        await connection.execute(
            "INSERT INTO api_principals(id,name) VALUES($1,'history-owner')",
            principal_id,
        )
        await connection.executemany(
            "INSERT INTO audit_records(id,actor_kind,api_principal_id,action,outcome,"
            "resource_type,resource_id,correlation_id,diagnostic_provenance,occurred_at) "
            "VALUES($1,'api_principal',$2,$3,'accepted','workflow',$4,$5,$6,$7)",
            [
                (
                    ids[index],
                    principal_id,
                    action,
                    workflow_id,
                    "request-correlation",
                    json.dumps({"step_count": index}),
                    FIXED_TIME,
                )
                for index, action in enumerate(
                    (
                        "workflow.publish",
                        "workflow.version_published",
                        "workflow.create",
                    )
                )
            ],
        )
    finally:
        await connection.close()

    engine = create_async_engine(sqlalchemy_url)
    repository = SQLAlchemyHistoryRepository(
        async_sessionmaker(engine, expire_on_commit=False)
    )
    try:
        at_upper_bound = await repository.list_history(
            "audit",
            None,
            OwnerFilter.all_owners(),
            limit=10,
            cursor=None,
            filters=HistoryFilters(occurred_to=FIXED_TIME),
        )
        assert at_upper_bound.items == ()
        filters = HistoryFilters(action=AuditAction.WORKFLOW_PUBLISH)
        first = await repository.list_history(
            "audit",
            None,
            OwnerFilter.all_owners(),
            limit=1,
            cursor=None,
            filters=filters,
        )
        assert len(first.items) == 1 and first.next_cursor is not None
        inserted_between_pages = UUID("ffffffff-ffff-4fff-8fff-ffffffffffff")
        connection = await asyncpg.connect(asyncpg_dsn(database_url))
        try:
            await connection.execute(
                "INSERT INTO audit_records(id,actor_kind,api_principal_id,action,outcome,"
                "resource_type,resource_id,diagnostic_provenance,occurred_at) "
                "VALUES($1,'api_principal',$2,'workflow.publish','accepted','workflow',$3,'{}',$4)",
                inserted_between_pages,
                principal_id,
                workflow_id,
                FIXED_TIME,
            )
        finally:
            await connection.close()
        second = await repository.list_history(
            "audit",
            None,
            OwnerFilter.all_owners(),
            limit=2,
            cursor=first.next_cursor,
            filters=filters,
        )
        combined = first.items + second.items
        assert len(combined) == 2
        assert inserted_between_pages not in {
            UUID(item.source_key) for item in combined
        }
        assert all(item.data["action"] == "workflow.publish" for item in combined)
        assert [item.source_key for item in combined] == sorted(
            [item.source_key for item in combined], reverse=True
        )
        assert all("request_body" not in item.data for item in combined)
        assert all("input" not in item.data for item in combined)
    finally:
        await engine.dispose()


def test_all_six_scope_queries_have_postgresql_source_key_parity() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_history_queries"
    ) as database_url:
        config = Config("alembic.ini")
        url = database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        with migration_database_url(url):
            command.upgrade(config, "head")
        asyncio.run(_assert_scope_sql(database_url, url))


def test_wide_actor_index_is_usable_for_all_actor_query_shapes() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_history_queries"
    ) as database_url:
        config = Config("alembic.ini")
        url = database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        with migration_database_url(url):
            command.upgrade(config, "head")
        asyncio.run(_assert_actor_plans(database_url))


def test_run_and_task_generic_audit_scopes_are_complete_and_isolated() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_history_queries"
    ) as database_url:
        config = Config("alembic.ini")
        url = database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        with migration_database_url(url):
            command.upgrade(config, "head")
        asyncio.run(_assert_run_and_task_audits(database_url, url))


def test_worker_history_uses_actor_or_session_target_without_duplicates() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_history_queries"
    ) as database_url:
        config = Config("alembic.ini")
        url = database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        with migration_database_url(url):
            command.upgrade(config, "head")
        asyncio.run(_assert_worker_audit_fallback(database_url, url))


async def _assert_scope_sql(database_url: object, sqlalchemy_url: str) -> None:
    engine = create_async_engine(sqlalchemy_url)
    repository = SQLAlchemyHistoryRepository(
        async_sessionmaker(engine, expire_on_commit=False)
    )
    connection = await asyncpg.connect(asyncpg_dsn(database_url))  # type: ignore[arg-type]
    try:
        for scope in ("audit", "workflow", "run", "task", "worker", "dead_letter"):
            if scope == "audit":
                await repository.list_history(
                    scope,
                    None,
                    OwnerFilter.all_owners(),
                    limit=1,
                    cursor=None,
                    filters=HistoryFilters(),
                )
                continue
            # EXPLAIN parses every UNION branch without requiring a resource fixture.
            statement = "EXPLAIN " + SCOPE_SQL[scope] + _filter_sql(HistoryFilters())
            rendered = statement
            replacements = {
                ":id": f"'{uuid4()}'::uuid",
                ":limit": "2",
                ":cursor_time": "NULL::timestamptz",
                ":cursor_rank": "NULL::integer",
                ":cursor_key": "NULL::text",
            }
            for marker, value in replacements.items():
                rendered = rendered.replace(marker, value)
            await connection.fetch(rendered)
        session_id = uuid4()
        expected = f"{session_id}:00000000000000000042"
        actual = await connection.fetchval(
            "SELECT $1::uuid::text||':'||lpad($2::bigint::text,20,'0')",
            session_id,
            42,
        )
        assert actual == expected
    finally:
        await connection.close()
        await engine.dispose()


async def _assert_actor_plans(database_url: object) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))  # type: ignore[arg-type]
    principal_ids = [uuid4() for _ in range(100)]
    worker_ids = [uuid4() for _ in range(100)]
    try:
        await connection.executemany(
            "INSERT INTO api_principals(id,name) VALUES($1,$2)",
            [
                (identifier, f"plan-principal-{index}")
                for index, identifier in enumerate(principal_ids)
            ],
        )
        await connection.executemany(
            "INSERT INTO worker_identities(id,name) VALUES($1,$2)",
            [
                (identifier, f"plan-worker-{index}")
                for index, identifier in enumerate(worker_ids)
            ],
        )
        records: list[tuple[object, ...]] = []
        for index in range(6000):
            variant = index % 3
            common = (
                uuid4(),
                "workflow.create",
                "accepted",
                "workflow",
                None,
                json.dumps({}),
                FIXED_TIME,
            )
            if variant == 0:
                records.append(
                    (
                        common[0],
                        "api_principal",
                        principal_ids[index % 100],
                        None,
                        None,
                        *common[1:],
                    )
                )
            elif variant == 1:
                records.append(
                    (
                        common[0],
                        "worker",
                        None,
                        worker_ids[index % 100],
                        None,
                        *common[1:],
                    )
                )
            else:
                records.append(
                    (
                        common[0],
                        "system",
                        None,
                        None,
                        f"component_{index % 100}",
                        *common[1:],
                    )
                )
        await connection.executemany(
            "INSERT INTO audit_records(id,actor_kind,api_principal_id,worker_identity_id,"
            "system_component,action,outcome,resource_type,resource_id,"
            "diagnostic_provenance,occurred_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
            records,
        )
        await connection.execute("ANALYZE audit_records")
        predicates = (
            ("actor_kind='api_principal' AND api_principal_id=$1", principal_ids[0]),
            (
                "actor_kind='worker' AND api_principal_id IS NULL AND worker_identity_id=$1",
                worker_ids[0],
            ),
            (
                "actor_kind='system' AND api_principal_id IS NULL AND worker_identity_id IS NULL AND system_component=$1",
                "component_0",
            ),
        )
        for predicate, value in predicates:
            plan_rows = await connection.fetch(
                "EXPLAIN (ANALYZE, COSTS OFF, FORMAT TEXT) SELECT id FROM audit_records "
                f"WHERE {predicate} ORDER BY occurred_at DESC,id DESC LIMIT 20",
                value,
            )
            plan = "\n".join(row[0] for row in plan_rows)
            assert "ix_audit_records_actor_occurred_at_id" in plan, plan
    finally:
        await connection.close()


async def _assert_run_and_task_audits(
    database_url: object, sqlalchemy_url: str
) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))  # type: ignore[arg-type]
    try:
        facts = await seed_facts(connection)
        run_id = await connection.fetchval(
            "SELECT workflow_run_id FROM task_runs WHERE id=$1", facts.task_run
        )
        unrelated_run = uuid4()
        unrelated_task = uuid4()
        await connection.execute(
            "INSERT INTO workflow_runs(id,workflow_definition_id,workflow_version_id,"
            "requested_by_principal_id,status) SELECT $1,workflow_definition_id,"
            "workflow_version_id,requested_by_principal_id,'failed' FROM workflow_runs "
            "WHERE id=$2",
            unrelated_run,
            run_id,
        )
        await connection.execute(
            "INSERT INTO task_runs(id,workflow_run_id,workflow_version_id,step_identifier,"
            "status) SELECT $1,$2,workflow_version_id,'one','failed' FROM workflow_runs "
            "WHERE id=$2",
            unrelated_task,
            unrelated_run,
        )
        approved = {
            uuid4(): ("workflow_run", run_id),
            uuid4(): ("task_run", facts.task_run),
            uuid4(): ("task_attempt", facts.attempt),
            uuid4(): ("dead_letter", facts.item),
        }
        task_approved = {
            uuid4(): ("task_run", facts.task_run),
            uuid4(): ("task_attempt", facts.attempt),
            uuid4(): ("dead_letter", facts.item),
        }
        run_unrelated = {
            uuid4(): ("workflow_run", unrelated_run),
            uuid4(): ("task_run", unrelated_task),
        }
        task_unrelated = {
            uuid4(): ("task_attempt", facts.other_attempt),
            uuid4(): ("dead_letter", facts.other_item),
        }
        await connection.executemany(
            "INSERT INTO audit_records(id,actor_kind,system_component,action,outcome,"
            "resource_type,resource_id,diagnostic_provenance) VALUES"
            "($1,'system','bootstrap','identity.api_principal_created','accepted',$2,$3,'{}')",
            [
                (identifier, resource_type, resource_id)
                for identifier, (resource_type, resource_id) in (
                    approved | task_approved | run_unrelated | task_unrelated
                ).items()
            ],
        )
    finally:
        await connection.close()

    engine = create_async_engine(sqlalchemy_url)
    repository = SQLAlchemyHistoryRepository(
        async_sessionmaker(engine, expire_on_commit=False)
    )
    try:
        run_page = await repository.list_history(
            "run",
            run_id,
            OwnerFilter.only(facts.principal),
            limit=100,
            cursor=None,
            filters=HistoryFilters(record_type=HistoryRecordType.AUDIT_RECORD),
        )
        run_ids = {UUID(item.source_key) for item in run_page.items}
        assert set(approved).issubset(run_ids)
        assert run_ids.isdisjoint(run_unrelated)

        task_page = await repository.list_history(
            "task",
            facts.task_run,
            OwnerFilter.only(facts.principal),
            limit=100,
            cursor=None,
            filters=HistoryFilters(record_type=HistoryRecordType.AUDIT_RECORD),
        )
        task_ids = {UUID(item.source_key) for item in task_page.items}
        assert set(task_approved).issubset(task_ids)
        assert task_ids.isdisjoint(run_unrelated | task_unrelated)
    finally:
        await engine.dispose()


async def _assert_worker_audit_fallback(
    database_url: object, sqlalchemy_url: str
) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))  # type: ignore[arg-type]
    worker, other_worker = uuid4(), uuid4()
    session, other_session = uuid4(), uuid4()
    actor_only, target_only, both, unrelated = uuid4(), uuid4(), uuid4(), uuid4()
    try:
        await connection.executemany(
            "INSERT INTO worker_identities(id,name) VALUES($1,$2)",
            [(worker, "history-worker"), (other_worker, "history-other-worker")],
        )
        await connection.executemany(
            "INSERT INTO worker_sessions(id,worker_identity_id) VALUES($1,$2)",
            [(session, worker), (other_session, other_worker)],
        )
        await connection.execute(
            "INSERT INTO audit_records(id,actor_kind,worker_identity_id,system_component,action,outcome,"
            "resource_type,resource_id,diagnostic_provenance) VALUES"
            "($1,'worker',$2,NULL,'worker_session.heartbeat','accepted','worker_session',NULL,'{}'),"
            "($3,'system',NULL,'test_component','worker_session.ended_stale','accepted','worker_session',$4,'{}'),"
            "($5,'worker',$2,NULL,'worker_session.capabilities_replace','accepted','worker_session',$4,'{}'),"
            "($6,'system',NULL,'test_component','worker_session.ended_stale','accepted','worker_session',$7,'{}')",
            actor_only,
            worker,
            target_only,
            session,
            both,
            unrelated,
            other_session,
        )
    finally:
        await connection.close()

    engine = create_async_engine(sqlalchemy_url)
    repository = SQLAlchemyHistoryRepository(
        async_sessionmaker(engine, expire_on_commit=False)
    )
    try:
        page = await repository.list_history(
            "worker",
            worker,
            OwnerFilter.all_owners(),
            limit=100,
            cursor=None,
            filters=HistoryFilters(record_type=HistoryRecordType.AUDIT_RECORD),
        )
        identifiers = [UUID(item.source_key) for item in page.items]
        assert set(identifiers) == {actor_only, target_only, both}
        assert identifiers.count(both) == 1
        assert unrelated not in identifiers
    finally:
        await engine.dispose()
