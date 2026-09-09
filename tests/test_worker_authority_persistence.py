"""Schema-qualified worker-authority persistence adapter contract."""

from __future__ import annotations

import asyncio
from typing import Any, cast
from uuid import uuid4

from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.ext.asyncio import AsyncSession

from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.persistence.worker_authority import lock_valid_worker_authority


class Result:
    def __init__(self, authorized: bool) -> None:
        self.authorized = authorized

    def scalar_one(self) -> bool:
        return self.authorized


class Session:
    def __init__(self, authorized: bool) -> None:
        self.authorized = authorized
        self.statement: Any = None

    async def execute(self, statement: object) -> Result:
        self.statement = statement
        return Result(self.authorized)


def test_adapter_calls_exact_schema_qualified_function_with_uuid_parameters() -> None:
    authority = AuthenticatedWorker(uuid4(), uuid4())
    session = Session(True)

    assert asyncio.run(
        lock_valid_worker_authority(cast(AsyncSession, session), authority)
    )

    assert str(session.statement) == (
        "SELECT public.lock_valid_worker_authority("
        "CAST(:worker_identity_id AS pg_catalog.uuid), "
        "CAST(:credential_id AS pg_catalog.uuid)) AS authorized"
    )
    compiled = session.statement.compile()
    assert compiled.params == {
        "worker_identity_id": authority.worker_identity_id,
        "credential_id": authority.credential_id,
    }
    for parameter in ("worker_identity_id", "credential_id"):
        parameter_type = compiled.binds[parameter].type
        assert isinstance(parameter_type, PostgreSQLUUID)
        assert parameter_type.as_uuid is True


def test_adapter_preserves_function_rejection() -> None:
    session = Session(False)
    assert not asyncio.run(
        lock_valid_worker_authority(
            cast(AsyncSession, session),
            AuthenticatedWorker(uuid4(), uuid4()),
        )
    )
