"""Exact application/database revision contract tests."""

from __future__ import annotations

import asyncio
from types import TracebackType
from typing import Any, cast

import pytest
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from taskforge.persistence.schema_compatibility import (
    EXPECTED_SCHEMA_REVISION,
    IncompatibleSchemaError,
    require_compatible_schema,
    schema_is_compatible,
)


class Connection:
    def __init__(self, revisions: object) -> None:
        self.revisions = revisions
        self.statements: list[str] = []

    async def scalar(self, statement: object) -> object:
        self.statements.append(str(statement))
        return self.revisions


class ConnectionContext:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    async def __aenter__(self) -> Connection:
        return self.connection

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None


class Engine:
    def __init__(self, revisions: object) -> None:
        self.connection = Connection(revisions)

    def connect(self) -> ConnectionContext:
        return ConnectionContext(self.connection)


@pytest.mark.parametrize(
    ("revisions", "expected"),
    (
        ([EXPECTED_SCHEMA_REVISION], True),
        ([], False),
        (["0031_lock_worker_authority"], False),
        ([EXPECTED_SCHEMA_REVISION, "other"], False),
        (None, False),
    ),
)
def test_schema_compatibility_requires_exact_single_head(
    revisions: object, expected: bool
) -> None:
    connection = Connection(revisions)

    result = asyncio.run(
        schema_is_compatible(cast(AsyncConnection, cast(Any, connection)))
    )

    assert result is expected
    assert connection.statements == ["SELECT public.taskforge_schema_revisions()"]


def test_required_schema_check_uses_engine_and_hides_revision_details() -> None:
    engine = Engine(["future-secret-revision"])

    with pytest.raises(IncompatibleSchemaError) as raised:
        asyncio.run(require_compatible_schema(cast(AsyncEngine, cast(Any, engine))))

    assert "future-secret-revision" not in str(raised.value)
