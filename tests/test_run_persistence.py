"""Workflow run target SQLAlchemy adapter tests."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import DBAPIError, IntegrityError

from taskforge.persistence.runs import (
    SQLAlchemyWorkflowRunCreationTransaction,
    SQLAlchemyWorkflowRunRepository,
    _active_run_progression_lock_statement,
    _definition_lock_statement,
    _dependency_failure_propagation_statement,
    _idempotent_run_statement,
    _is_idempotency_scope_conflict,
    _locked_version_statement,
    _owner_scoped_run_exists_statement,
    _pending_to_running_statement,
    _run_inspection_statement,
    _runnable_transition_statement,
    _running_terminal_transition_statement,
    _task_run_inspection_statement,
    _task_run_list_statement,
    _version_resolution_statement,
    _workflow_run_evaluation_lock_statement,
)
from taskforge.runs.domain import (
    ExplicitWorkflowVersion,
    LatestWorkflowVersion,
    NewTaskRun,
    NewWorkflowRun,
    TaskRunStatus,
    WorkflowRunEvaluationResult,
    WorkflowRunInput,
    WorkflowRunStatus,
    WorkflowRunVersionSnapshot,
)
from taskforge.runs.persistence_ports import (
    PreparedWorkflowRunCreation,
    WorkflowRunPersistenceUnavailable,
    WorkflowRunTimestamps,
)
from taskforge.workflows.domain import WorkflowDefinitionStatus


def normalized_sql(statement: object) -> str:
    return " ".join(str(statement).split())


def test_explicit_resolution_is_owner_and_workflow_scoped_without_locking() -> None:
    statement = _version_resolution_statement(
        uuid4(), uuid4(), ExplicitWorkflowVersion(4)
    )
    sql = normalized_sql(statement)

    assert "workflow_definitions.id =" in sql
    assert "workflow_definitions.owner_principal_id =" in sql
    assert "workflow_versions.workflow_definition_id =" in sql
    assert "workflow_versions.version_number =" in sql
    assert "FOR UPDATE" not in sql
    assert "FOR SHARE" not in sql
    assert "FOR KEY SHARE" not in sql


def test_latest_resolution_orders_only_by_unique_version_number() -> None:
    statement = _version_resolution_statement(uuid4(), uuid4(), LatestWorkflowVersion())
    sql = normalized_sql(statement)

    assert "ORDER BY workflow_versions.version_number DESC" in sql
    assert "workflow_versions.id DESC" not in sql
    assert "LEFT OUTER JOIN LATERAL" in sql
    assert statement._for_update_arg is None


def test_creation_admission_lock_is_exclusive_and_owner_scoped() -> None:
    statement = _definition_lock_statement(uuid4(), uuid4())
    sql = normalized_sql(statement)

    assert "workflow_definitions.id =" in sql
    assert "workflow_definitions.owner_principal_id =" in sql
    assert "FOR UPDATE" in sql


def test_locked_creation_resolution_keeps_explicit_and_latest_rules() -> None:
    explicit = normalized_sql(
        _locked_version_statement(uuid4(), ExplicitWorkflowVersion(5))
    )
    latest = normalized_sql(_locked_version_statement(uuid4(), LatestWorkflowVersion()))

    assert "workflow_versions.workflow_definition_id =" in explicit
    assert "workflow_versions.version_number =" in explicit
    assert "ORDER BY" not in explicit
    assert "ORDER BY workflow_versions.version_number DESC" in latest
    assert "workflow_versions.id DESC" not in latest


def test_idempotency_lookup_is_fully_scoped_and_loads_original_result() -> None:
    statement = _idempotent_run_statement(uuid4(), uuid4(), "sha256:v1:digest")
    sql = normalized_sql(statement)

    assert "workflow_run_idempotency.principal_id =" in sql
    assert "workflow_run_idempotency.workflow_definition_id =" in sql
    assert "workflow_run_idempotency.idempotency_key_digest =" in sql
    assert "workflow_runs" in sql
    assert "workflow_versions" in sql


def test_run_and_task_inspection_sql_is_owner_scoped_read_only_and_ordered() -> None:
    run = normalized_sql(_run_inspection_statement(uuid4(), uuid4()))
    exists = normalized_sql(_owner_scoped_run_exists_statement(uuid4(), uuid4()))
    tasks = normalized_sql(_task_run_list_statement(uuid4(), uuid4()))
    task = normalized_sql(_task_run_inspection_statement(uuid4(), uuid4()))

    for sql in (run, exists, tasks, task):
        assert "workflow_definitions.owner_principal_id =" in sql
        assert "FOR UPDATE" not in sql
        assert "FOR SHARE" not in sql
    assert "workflow_versions.version_number" in run
    assert "workflow_run_inputs" not in run
    assert "workflow_run_idempotency" not in run
    assert "ORDER BY task_runs.step_identifier" in tasks


def test_runnable_transition_sql_owns_immutable_dependency_evaluation() -> None:
    sql = normalized_sql(
        _runnable_transition_statement(uuid4()).compile(
            dialect=postgresql.dialect()  # type: ignore[no-untyped-call]
        )
    )

    assert "WITH eligible_runnable_tasks AS" in sql
    assert "UPDATE task_runs SET status=" in sql
    assert "task_runs.status =" in sql
    assert "candidate_run.status IN" in sql
    assert "FOR UPDATE OF candidate_run" in sql
    assert "workflow_version_dependencies" in sql
    assert "required_predecessor.status =" in sql
    assert "required_predecessor, task_runs AS runnable_candidate" not in sql
    assert "NOT (EXISTS" in sql
    assert "workflow_definitions" not in sql
    assert "workflow_steps" not in sql
    assert "RETURNING task_runs.id, task_runs.step_identifier" in sql


def test_dependency_failure_sql_is_recursive_and_and_only() -> None:
    run_id, version_id = uuid4(), uuid4()
    sql = normalized_sql(
        _dependency_failure_propagation_statement(run_id, version_id).compile(
            dialect=postgresql.dialect()  # type: ignore[no-untyped-call]
        )
    )

    assert "WITH RECURSIVE dependency_failed_descendants" in sql
    assert " UNION SELECT " in sql
    assert "dependency_failed_predecessor.status IN" in sql
    assert "failure_seed_successor.status IN" in sql
    assert "propagated_failure_successor.status IN" in sql
    assert "workflow_version_dependencies" in sql
    assert "task_runs.status =" in sql
    assert "SET status=" in sql
    assert "RETURNING task_runs.id, task_runs.step_identifier" in sql
    assert "workflow_definitions" not in sql
    assert "UPDATE workflow_runs" not in sql


def test_dependency_failure_progression_lock_is_active_run_scoped() -> None:
    sql = normalized_sql(
        _active_run_progression_lock_statement(uuid4()).compile(
            dialect=postgresql.dialect()  # type: ignore[no-untyped-call]
        )
    )

    assert "workflow_runs.id =" in sql
    assert "workflow_runs.status IN" in sql
    assert "FOR UPDATE" in sql


def test_workflow_run_evaluation_lock_preserves_shared_lock_order() -> None:
    sql = normalized_sql(
        _workflow_run_evaluation_lock_statement(uuid4()).compile(
            dialect=postgresql.dialect()  # type: ignore[no-untyped-call]
        )
    )

    assert "FROM workflow_runs" in sql
    assert "workflow_runs.id =" in sql
    assert "task_runs" not in sql
    assert "FOR UPDATE" in sql


def test_pending_evaluation_only_transitions_to_running_on_execution_evidence() -> None:
    sql = normalized_sql(
        _pending_to_running_statement(uuid4()).compile(
            dialect=postgresql.dialect()  # type: ignore[no-untyped-call]
        )
    )

    assert "UPDATE workflow_runs SET status=" in sql
    assert "workflow_runs.status =" in sql
    assert "EXISTS (SELECT 1 FROM task_runs" in sql
    assert "RETURNING workflow_runs.status" in sql
    assert "CASE" not in sql
    assert "workflow_definitions" not in sql
    assert "workflow_versions" not in sql
    assert "workflow_version_dependencies" not in sql


def test_running_terminal_evaluation_is_failure_first_and_task_relational() -> None:
    sql = normalized_sql(
        _running_terminal_transition_statement(uuid4()).compile(
            dialect=postgresql.dialect()  # type: ignore[no-untyped-call]
        )
    )

    assert "UPDATE workflow_runs SET status=CAST(CASE WHEN" in sql
    assert sql.count("WHEN") == 2
    assert "EXISTS (SELECT 1 FROM task_runs" in sql
    assert "workflow_runs.status =" in sql
    assert "RETURNING workflow_runs.status" in sql
    assert "workflow_definitions" not in sql
    assert "workflow_versions" not in sql
    assert "workflow_version_dependencies" not in sql


class PostgreSQLMetadataError(Exception):
    def __init__(self, sqlstate: str | None, constraint_name: str | None) -> None:
        self.sqlstate = sqlstate
        self.constraint_name = constraint_name


@pytest.mark.parametrize(
    ("sqlstate", "constraint_name", "expected"),
    (
        ("23505", "pk_workflow_run_idempotency", True),
        ("23503", "pk_workflow_run_idempotency", False),
        ("23505", "another_constraint", False),
        (None, "pk_workflow_run_idempotency", False),
    ),
)
def test_idempotency_conflict_classification_requires_state_and_constraint(
    sqlstate: str | None,
    constraint_name: str | None,
    expected: bool,
) -> None:
    metadata_error = PostgreSQLMetadataError(sqlstate, constraint_name)
    integrity_error = IntegrityError("INSERT", {}, metadata_error)

    assert _is_idempotency_scope_conflict(integrity_error) is expected


class FakeResult:
    def __init__(self, rows: list[Any]) -> None:
        self.rows = rows

    def one_or_none(self) -> Any | None:
        return self.rows[0] if self.rows else None

    def one(self) -> Any:
        return self.rows[0]

    def all(self) -> list[Any]:
        return self.rows


class FakeSession:
    def __init__(
        self,
        results: list[FakeResult] | None = None,
        *,
        begin_failure: BaseException | None = None,
        execute_failure_at: int | None = None,
        execute_failure: BaseException | None = None,
    ) -> None:
        self.results = list(results or [])
        self.begin_failure = begin_failure
        self.execute_failure_at = execute_failure_at
        self.execute_failure = execute_failure
        self.calls: list[str] = []
        self.execute_count = 0

    async def begin(self) -> None:
        self.calls.append("begin")
        if self.begin_failure is not None:
            raise self.begin_failure

    async def execute(self, statement: object, parameters: object = None) -> FakeResult:
        del statement, parameters
        self.calls.append("execute")
        self.execute_count += 1
        if (
            self.execute_failure_at == self.execute_count
            and self.execute_failure is not None
        ):
            raise self.execute_failure
        return self.results.pop(0) if self.results else FakeResult([])

    async def rollback(self) -> None:
        self.calls.append("rollback")

    async def commit(self) -> None:
        self.calls.append("commit")

    async def close(self) -> None:
        self.calls.append("close")


class FakeRepositoryTransactionContext:
    def __init__(self, session: FakeSession) -> None:
        self.session = session

    async def __aenter__(self) -> FakeSession:
        self.session.calls.append("context_enter")
        return self.session

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: object,
    ) -> None:
        del exception_type, exception, traceback
        self.session.calls.append("rollback" if self.session.execute_failure else "commit")
        self.session.calls.append("context_exit")


class FakeRepositorySessions:
    def __init__(self, session: FakeSession) -> None:
        self.session = session

    def begin(self) -> FakeRepositoryTransactionContext:
        return FakeRepositoryTransactionContext(self.session)


def evaluation_repository(session: FakeSession) -> SQLAlchemyWorkflowRunRepository:
    return SQLAlchemyWorkflowRunRepository(
        FakeRepositorySessions(session)  # type: ignore[arg-type]
    )


@pytest.mark.parametrize(
    ("source", "returned", "expected"),
    (
        (
            WorkflowRunStatus.PENDING,
            WorkflowRunStatus.RUNNING,
            WorkflowRunStatus.RUNNING,
        ),
        (
            WorkflowRunStatus.RUNNING,
            WorkflowRunStatus.SUCCEEDED,
            WorkflowRunStatus.SUCCEEDED,
        ),
        (WorkflowRunStatus.RUNNING, None, WorkflowRunStatus.RUNNING),
        (WorkflowRunStatus.CANCELLING, None, WorkflowRunStatus.CANCELLING),
        (WorkflowRunStatus.FAILED, None, WorkflowRunStatus.FAILED),
    ),
)
def test_repository_evaluates_at_most_one_transition_and_returns_result(
    source: WorkflowRunStatus,
    returned: WorkflowRunStatus | None,
    expected: WorkflowRunStatus,
) -> None:
    run_id = uuid4()
    results = [FakeResult([SimpleNamespace(status=source.value)])]
    if source in (WorkflowRunStatus.PENDING, WorkflowRunStatus.RUNNING):
        results.append(
            FakeResult(
                [SimpleNamespace(status=returned.value)] if returned is not None else []
            )
        )
    session = FakeSession(results)

    result = asyncio.run(
        evaluation_repository(session).evaluate_workflow_run_state(run_id)
    )

    assert result == WorkflowRunEvaluationResult(run_id, True, source, expected)
    assert session.execute_count <= 2
    assert session.calls[-2:] == ["commit", "context_exit"]


def test_repository_returns_explicit_missing_run_result_without_update() -> None:
    run_id = uuid4()
    session = FakeSession([FakeResult([])])

    result = asyncio.run(
        evaluation_repository(session).evaluate_workflow_run_state(run_id)
    )

    assert result == WorkflowRunEvaluationResult(run_id, False, None, None)
    assert session.execute_count == 1
    assert session.calls[-2:] == ["commit", "context_exit"]


def test_repository_rolls_back_and_normalizes_evaluation_database_failure() -> None:
    database_error = DBAPIError("UPDATE", {}, Exception("injected failure"))
    session = FakeSession(
        [FakeResult([SimpleNamespace(status=WorkflowRunStatus.PENDING.value)])],
        execute_failure_at=2,
        execute_failure=database_error,
    )

    with pytest.raises(WorkflowRunPersistenceUnavailable) as caught:
        asyncio.run(
            evaluation_repository(session).evaluate_workflow_run_state(uuid4())
        )

    assert caught.value.__cause__ is database_error
    assert session.calls[-2:] == ["rollback", "context_exit"]


def transaction(session: FakeSession) -> SQLAlchemyWorkflowRunCreationTransaction:
    return SQLAlchemyWorkflowRunCreationTransaction(
        lambda: session  # type: ignore[arg-type]
    )


def test_creation_transaction_rolls_back_without_explicit_commit() -> None:
    session = FakeSession()

    async def exercise() -> None:
        async with transaction(session):
            pass

    asyncio.run(exercise())

    assert session.calls == ["begin", "rollback", "close"]


def test_creation_transaction_resets_committed_state_when_reentered() -> None:
    session = FakeSession()
    creation = transaction(session)
    creation._committed = True

    async def exercise() -> None:
        async with creation:
            pass

    asyncio.run(exercise())

    assert session.calls == ["begin", "rollback", "close"]


def test_begin_database_failure_closes_without_rollback_and_is_normalized() -> None:
    database_error = DBAPIError("BEGIN", {}, Exception("database unavailable"))
    session = FakeSession(begin_failure=database_error)
    creation = transaction(session)

    with pytest.raises(WorkflowRunPersistenceUnavailable) as caught:
        asyncio.run(creation.__aenter__())

    assert caught.value.__cause__ is database_error
    assert creation._session is None
    assert session.calls == ["begin", "close"]


@pytest.mark.parametrize("failure", (asyncio.CancelledError(), RuntimeError("bug")))
def test_begin_non_database_failure_closes_and_propagates_unchanged(
    failure: BaseException,
) -> None:
    session = FakeSession(begin_failure=failure)
    creation = transaction(session)

    with pytest.raises(type(failure)) as caught:
        asyncio.run(creation.__aenter__())

    assert caught.value is failure
    assert creation._session is None
    assert session.calls == ["begin", "close"]


def test_creation_transaction_prepares_snapshot_in_locked_transaction() -> None:
    workflow_id, version_id = uuid4(), uuid4()
    session = FakeSession(
        [
            FakeResult([SimpleNamespace(id=workflow_id, status="enabled")]),
            FakeResult(
                [
                    SimpleNamespace(
                        id=version_id,
                        workflow_definition_id=workflow_id,
                        version_number=3,
                    )
                ]
            ),
            FakeResult(
                [
                    SimpleNamespace(step_identifier="leaf"),
                    SimpleNamespace(step_identifier="root"),
                ]
            ),
            FakeResult(
                [
                    SimpleNamespace(
                        predecessor_step_identifier="root",
                        successor_step_identifier="leaf",
                    )
                ]
            ),
        ]
    )

    async def exercise() -> PreparedWorkflowRunCreation | None:
        async with transaction(session) as creation:
            return await creation.prepare_creation_target(
                workflow_id, uuid4(), LatestWorkflowVersion()
            )

    prepared = asyncio.run(exercise())

    assert prepared is not None
    assert prepared.snapshot is not None
    assert prepared.snapshot.workflow_version_id == version_id
    assert prepared.snapshot.step_identifiers == ("leaf", "root")
    assert prepared.snapshot.dependencies[0].successor_identifier == "leaf"
    assert session.calls == [
        "begin",
        "execute",
        "execute",
        "execute",
        "execute",
        "rollback",
        "close",
    ]


def test_complete_insert_returns_database_timestamps_and_commits() -> None:
    now = datetime.now(UTC)
    workflow_id, version_id = uuid4(), uuid4()
    prepared_session = FakeSession(
        [FakeResult([SimpleNamespace(created_at=now, updated_at=now)])]
    )
    prepared = PreparedWorkflowRunCreation(
        workflow_id,
        WorkflowDefinitionStatus.ENABLED,
        WorkflowRunVersionSnapshot(workflow_id, version_id, 1, ("root",), ()),
    )
    run = NewWorkflowRun(uuid4(), uuid4(), WorkflowRunStatus.PENDING)

    async def exercise() -> WorkflowRunTimestamps:
        async with transaction(prepared_session) as creation:
            timestamps = await creation.insert_complete_run(
                prepared,
                run,
                WorkflowRunInput({}, {}),
                (NewTaskRun(uuid4(), "root", TaskRunStatus.RUNNABLE),),
            )
            await creation.commit()
            return timestamps

    timestamps = asyncio.run(exercise())

    assert timestamps.created_at == now
    assert prepared_session.calls == [
        "begin",
        "execute",
        "execute",
        "execute",
        "commit",
        "close",
    ]


def test_failure_after_run_insert_rolls_back_without_attempting_commit() -> None:
    now = datetime.now(UTC)
    database_error = DBAPIError("INSERT", {}, Exception("input insert failed"))
    session = FakeSession(
        [FakeResult([SimpleNamespace(created_at=now, updated_at=now)])],
        execute_failure_at=2,
        execute_failure=database_error,
    )
    workflow_id, version_id = uuid4(), uuid4()
    prepared = PreparedWorkflowRunCreation(
        workflow_id,
        WorkflowDefinitionStatus.ENABLED,
        WorkflowRunVersionSnapshot(workflow_id, version_id, 1, ("root",), ()),
    )

    async def exercise() -> None:
        async with transaction(session) as creation:
            await creation.insert_complete_run(
                prepared,
                NewWorkflowRun(uuid4(), uuid4()),
                WorkflowRunInput({}, {}),
                (NewTaskRun(uuid4(), "root", TaskRunStatus.RUNNABLE),),
            )

    with pytest.raises(WorkflowRunPersistenceUnavailable):
        asyncio.run(exercise())

    assert session.calls == ["begin", "execute", "execute", "rollback", "close"]
