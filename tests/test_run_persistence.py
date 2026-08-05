"""Workflow run target SQLAlchemy adapter tests."""

from uuid import uuid4

from taskforge.persistence.runs import _version_resolution_statement
from taskforge.runs.domain import ExplicitWorkflowVersion, LatestWorkflowVersion


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
