"""Narrow database-backed worker authority locking.

The runtime role can hold these locks by keeping its transaction open, which can
delay administrative disablement or revocation. That accepted capability is
narrower than granting runtime permission to mutate either authority table.
"""

from __future__ import annotations

from sqlalchemy import Boolean, bindparam, text
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.ext.asyncio import AsyncSession

from taskforge.identity.authentication import AuthenticatedWorker

_LOCK_VALID_WORKER_AUTHORITY = text(
    "SELECT public.lock_valid_worker_authority("
    "CAST(:worker_identity_id AS pg_catalog.uuid), "
    "CAST(:credential_id AS pg_catalog.uuid)) AS authorized"
).columns(authorized=Boolean())


async def lock_valid_worker_authority(
    session: AsyncSession,
    authenticated_worker: AuthenticatedWorker,
) -> bool:
    """Validate and lock authority rows until the caller's transaction ends."""
    statement = _LOCK_VALID_WORKER_AUTHORITY.bindparams(
        bindparam(
            "worker_identity_id",
            value=authenticated_worker.worker_identity_id,
            type_=PostgreSQLUUID(as_uuid=True),
        ),
        bindparam(
            "credential_id",
            value=authenticated_worker.credential_id,
            type_=PostgreSQLUUID(as_uuid=True),
        ),
    )
    result = await session.execute(statement)
    return bool(result.scalar_one())
