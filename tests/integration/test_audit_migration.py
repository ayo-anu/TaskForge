"""PostgreSQL enforcement for prospective append-only generic audit records."""

from __future__ import annotations

import asyncio
import os
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.ext.asyncio import create_async_engine

from taskforge.claims.service import TaskClaimService, TaskClaimServiceUnavailable
from taskforge.dead_letters.persistence_ports import DeadLetterPersistenceUnavailable
from taskforge.dead_letters.service import DeadLetterNotFound, DeadLetterService
from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.persistence.audit import RejectedAuditUnitOfWork
from taskforge.persistence.database import build_session_factory
from taskforge.runs.service import WorkflowRunService, WorkflowRunServiceUnavailable
from taskforge.worker.result_submission import (
    TaskResultAuthorityRejected,
    TaskResultServiceUnavailable,
    TaskResultSubmissionService,
)
from taskforge.worker.service import WorkerRejectedAuditUnavailable, _worker_rejected
from taskforge.worker.start import (
    TaskStartRequest,
    TaskStartService,
    TaskStartServiceUnavailable,
)
from taskforge.worker.start_persistence_ports import TaskStartClaimStale
from taskforge.workflows.service import (
    WorkflowNotFound,
    WorkflowService,
    WorkflowServiceUnavailable,
)
from tests.integration.postgresql import (
    asyncpg_dsn,
    migration_database_url,
    temporary_database,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_MIGRATION_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_MIGRATION_INTEGRATION=1 explicitly",
    ),
]


async def assert_audit_enforcement(database_url: object) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))  # type: ignore[arg-type]
    principal_id = uuid4()
    worker_identity_id = uuid4()
    worker_session_id = uuid4()
    audit_id = uuid4()
    try:
        actor_constraints = await connection.fetch(
            "SELECT conrelid::regclass::text AS table_name, "
            "pg_get_constraintdef(oid) AS definition, convalidated "
            "FROM pg_constraint WHERE "
            "(conrelid = 'task_result_events'::regclass AND "
            "pg_get_constraintdef(oid) LIKE "
            "'CHECK (((actor_component IS NULL)%') OR "
            "(conrelid = 'task_retry_events'::regclass AND "
            "pg_get_constraintdef(oid) LIKE "
            "'CHECK (((actor_component IS NOT NULL)%') "
            "ORDER BY table_name"
        )
        assert [tuple(row) for row in actor_constraints] == [
            (
                "task_result_events",
                "CHECK (((actor_component IS NULL) OR ((actor_component)::text = ANY "
                "((ARRAY['expired_claim_recovery'::character varying, "
                "'cancellation_recovery'::character varying])::text[]))))",
                True,
            ),
            (
                "task_retry_events",
                "CHECK (((actor_component IS NOT NULL) AND ((actor_component)::text = "
                "ANY ((ARRAY['retry_transition'::character varying, "
                "'retry_dispatch'::character varying, "
                "'expired_claim_recovery'::character varying])::text[])))) NOT VALID",
                False,
            ),
        ]
        await connection.execute(
            "INSERT INTO api_principals (id, name) VALUES ($1, $2)",
            principal_id,
            f"audit-{principal_id.hex}",
        )
        await connection.execute(
            "INSERT INTO worker_identities (id, name) VALUES ($1, $2)",
            worker_identity_id,
            f"audit-worker-{worker_identity_id.hex}",
        )
        await connection.execute(
            "INSERT INTO worker_sessions (id, worker_identity_id) VALUES ($1, $2)",
            worker_session_id,
            worker_identity_id,
        )
        await connection.execute(
            "INSERT INTO audit_records "
            "(id, actor_kind, worker_identity_id, action, outcome, reason_code, "
            "resource_type, diagnostic_provenance) "
            "VALUES ($1, 'worker', $2, 'worker_session.register', 'rejected', "
            "'registration_conflict', 'worker_session', '{}')",
            audit_id,
            worker_identity_id,
        )
        row = await connection.fetchrow(
            "SELECT worker_identity_id, worker_session_id, reason_code "
            "FROM audit_records WHERE id=$1",
            audit_id,
        )
        assert row is not None
        assert row["worker_identity_id"] == worker_identity_id
        assert row["worker_session_id"] is None
        assert row["reason_code"] == "registration_conflict"

        for statement in (
            "UPDATE audit_records SET reason_code='changed' WHERE id=$1",
            "DELETE FROM audit_records WHERE id=$1",
        ):
            with pytest.raises(
                asyncpg.PostgresError, match="audit records are immutable"
            ):
                await connection.execute(statement, audit_id)
        with pytest.raises(asyncpg.PostgresError, match="audit records are immutable"):
            await connection.execute("TRUNCATE audit_records")

        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                "INSERT INTO audit_records "
                "(id, actor_kind, api_principal_id, action, outcome, reason_code, "
                "resource_type, diagnostic_provenance) "
                "VALUES ($1, 'system', $2, 'workflow.create', 'rejected', "
                "'owner_disabled', 'workflow', '{}')",
                uuid4(),
                principal_id,
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                "INSERT INTO audit_records "
                "(id, actor_kind, api_principal_id, action, outcome, reason_code, "
                "resource_type, diagnostic_provenance) "
                "VALUES ($1, 'api_principal', $2, 'workflow.create', 'rejected', "
                "'workflow_invalid', 'workflow', $3::jsonb)",
                uuid4(),
                principal_id,
                '{"value":"' + "x" * 2048 + '"}',
            )
    finally:
        await connection.close()


async def assert_all_rejected_families_fail_closed(database_url: object) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))  # type: ignore[arg-type]
    await connection.execute(
        "CREATE FUNCTION reject_required_audit() RETURNS trigger LANGUAGE plpgsql "
        "AS $$ BEGIN RAISE EXCEPTION 'forced rejected-audit failure'; END; $$"
    )
    await connection.execute(
        "CREATE TRIGGER reject_required_audit BEFORE INSERT ON audit_records "
        "FOR EACH ROW EXECUTE FUNCTION reject_required_audit()"
    )
    await connection.close()

    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg")  # type: ignore[attr-defined]
    )
    recorder = RejectedAuditUnitOfWork(build_session_factory(engine))
    principal_id, correlation_id, resource_id = uuid4(), uuid4(), uuid4()
    authenticated = AuthenticatedWorker(uuid4(), uuid4())
    try:
        claim = TaskClaimService(Any, Any, lease_seconds=30, rejected_audit=recorder)  # type: ignore[arg-type]
        for action in ("task_claim.acquire", "task_claim.renew"):
            with pytest.raises(TaskClaimServiceUnavailable):
                await claim._audit_rejection(
                    authenticated,
                    uuid4(),
                    action=action,
                    reason_code="obsolete_task",
                    task_attempt_id=resource_id,
                    correlation_id=str(correlation_id),
                    provenance={},
                )

        class StartRepository:
            async def start_task(self, *args: object, **kwargs: object) -> Any:
                raise TaskStartClaimStale

        with pytest.raises(TaskStartServiceUnavailable):
            await TaskStartService(StartRepository(), recorder).start_task(
                authenticated,
                uuid4(),
                TaskStartRequest(uuid4(), resource_id, 1, str(correlation_id)),
            )

        result = TaskResultSubmissionService(Any, Any, recorder)  # type: ignore[arg-type]
        with pytest.raises(TaskResultServiceUnavailable):
            await result._audit_rejection(
                authenticated,
                uuid4(),
                SimpleNamespace(
                    task_attempt_id=resource_id,
                    correlation_id=str(correlation_id),
                    claim_generation=1,
                ),  # type: ignore[arg-type]
                TaskResultAuthorityRejected(),
            )

        workflow = WorkflowService(Any, Any, recorder)  # type: ignore[arg-type]
        for action in (
            "workflow.create",
            "workflow.publish",
            "workflow.availability_change",
        ):
            with pytest.raises(WorkflowServiceUnavailable):
                await workflow._audit_rejection(
                    WorkflowNotFound(),
                    action=action,
                    workflow_id=resource_id,
                    principal_id=principal_id,
                    correlation_id=correlation_id,
                )

        runs = WorkflowRunService(Any, recorder)  # type: ignore[arg-type]
        for action, resource_type in (
            ("workflow_run.cancel", "workflow_run"),
            ("workflow_run.create", "workflow"),
        ):
            with pytest.raises(WorkflowRunServiceUnavailable):
                await runs._audit_command_rejection(
                    WorkflowNotFound(),
                    action=action,
                    resource_id=resource_id,
                    principal_id=principal_id,
                    correlation_id=correlation_id,
                    reasons={WorkflowNotFound: "workflow_run_not_visible"},
                    resource_type=resource_type,
                )

        dead_letters = DeadLetterService(Any, recorder)  # type: ignore[arg-type]
        for action in (
            "dead_letter.acknowledge",
            "dead_letter.resolve",
            "dead_letter.redrive",
        ):
            with pytest.raises(DeadLetterPersistenceUnavailable):
                await dead_letters._audit_rejection(
                    DeadLetterNotFound(),
                    item_id=resource_id,
                    principal_id=principal_id,
                    action=action,
                    correlation_id=correlation_id,
                )

        for action, reason in (
            ("worker_session.register", "registration_conflict"),
            ("worker_session.capabilities_replace", "worker_session_inactive"),
            ("worker_session.heartbeat", "stale_heartbeat"),
        ):
            with pytest.raises(WorkerRejectedAuditUnavailable):
                await _worker_rejected(
                    recorder,
                    authenticated,
                    None,
                    action,
                    reason,
                    correlation_id,
                    {},
                )
    finally:
        await engine.dispose()


def test_audit_migration_upgrade_downgrade_and_enforcement() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_audit_mig"
    ) as database_url:
        configuration = Config("alembic.ini")
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        with migration_database_url(alembic_url):
            command.upgrade(configuration, "head")
            asyncio.run(assert_audit_enforcement(database_url))
            asyncio.run(assert_all_rejected_families_fail_closed(database_url))
            command.downgrade(configuration, "0024_run_replay_lineage")
            command.upgrade(configuration, "head")
