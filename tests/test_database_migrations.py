"""Fail-closed migration state classification tests."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from typing import Any, cast

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy.ext.asyncio import AsyncConnection

from taskforge import database_migrations
from taskforge.database_migrations import (
    _PUBLIC_SCHEMA_HAS_OBJECTS,
    ALEMBIC_CONFIGURATION,
    MigrationAction,
    UnsafeDatabaseState,
    classify_database_state,
    main,
)
from taskforge.persistence.migration_lock import MigrationLockTimeout
from taskforge.persistence.schema_compatibility import EXPECTED_SCHEMA_REVISION


class ScalarResult:
    def __init__(self, values: tuple[str, ...]) -> None:
        self.values = values

    def scalars(self) -> ScalarResult:
        return self

    def __iter__(self) -> Iterator[str]:
        return iter(self.values)


class Connection:
    def __init__(
        self,
        *,
        version_table: bool,
        revisions: tuple[str, ...] = (),
        public_objects: bool = False,
    ) -> None:
        self.scalar_results = [version_table]
        if not version_table:
            self.scalar_results.append(public_objects)
        self.revisions = revisions

    async def scalar(self, statement: object) -> object:
        del statement
        return self.scalar_results.pop(0)

    async def execute(self, statement: object) -> ScalarResult:
        del statement
        return ScalarResult(self.revisions)


def _script() -> ScriptDirectory:
    return ScriptDirectory.from_config(Config(ALEMBIC_CONFIGURATION))


def _classify(connection: Connection) -> MigrationAction:
    return asyncio.run(
        classify_database_state(cast(AsyncConnection, cast(Any, connection)), _script())
    )


def test_repository_head_matches_the_application_schema_contract() -> None:
    assert _script().get_current_head() == EXPECTED_SCHEMA_REVISION


def test_empty_schema_classification_includes_standalone_types() -> None:
    assert "pg_catalog.pg_type" in str(_PUBLIC_SCHEMA_HAS_OBJECTS)


def test_empty_database_is_migratable() -> None:
    assert _classify(Connection(version_table=False)) is MigrationAction.UPGRADE


def test_exact_head_is_verified_without_migration() -> None:
    assert (
        _classify(Connection(version_table=True, revisions=(EXPECTED_SCHEMA_REVISION,)))
        is MigrationAction.VERIFY
    )


def test_known_ancestor_is_migratable() -> None:
    assert (
        _classify(
            Connection(version_table=True, revisions=("0031_lock_worker_authority",))
        )
        is MigrationAction.UPGRADE
    )


@pytest.mark.parametrize(
    "connection",
    (
        Connection(version_table=False, public_objects=True),
        Connection(version_table=True),
        Connection(version_table=True, revisions=("unrecognized_revision",)),
        Connection(
            version_table=True,
            revisions=(EXPECTED_SCHEMA_REVISION, "other_revision"),
        ),
    ),
)
def test_unsafe_states_are_refused_before_migration(connection: Connection) -> None:
    with pytest.raises(UnsafeDatabaseState):
        _classify(connection)


@pytest.mark.parametrize(
    ("password", "timeout"),
    ((None, "300"), ("owner-secret", "0"), ("owner-secret", "3601")),
)
def test_invalid_migration_configuration_exits_two_without_details(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Any,
    password: str | None,
    timeout: str,
) -> None:
    monkeypatch.chdir(tmp_path)
    for name, value in {
        "POSTGRES_HOST": "postgres",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "taskforge",
        "POSTGRES_OWNER_USER": "taskforge_owner",
        "TASKFORGE_MIGRATION_LOCK_TIMEOUT_SECONDS": timeout,
    }.items():
        monkeypatch.setenv(name, value)
    if password is None:
        monkeypatch.delenv("POSTGRES_OWNER_PASSWORD", raising=False)
    else:
        monkeypatch.setenv("POSTGRES_OWNER_PASSWORD", password)

    assert main() == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "TaskForge migration configuration is invalid\n"
    assert "owner-secret" not in captured.err


@pytest.mark.parametrize(
    "failure",
    (
        ConnectionError("database failure containing owner-secret"),
        MigrationLockTimeout("lock failure containing owner-secret"),
        UnsafeDatabaseState("migration failure containing owner-secret"),
    ),
)
def test_runtime_migration_failure_exits_one_with_bounded_diagnostic(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    failure: Exception,
) -> None:
    async def fail() -> None:
        raise failure

    monkeypatch.setattr(database_migrations, "_run", fail)

    assert main() == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == (
        f"TaskForge migration failed error_type={type(failure).__name__}\n"
    )
    assert "owner-secret" not in captured.err
