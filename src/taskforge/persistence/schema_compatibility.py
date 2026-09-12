"""Exact database schema compatibility checks shared by production roles."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

EXPECTED_SCHEMA_REVISION = "0032_schema_revision_contract"
_SCHEMA_REVISIONS = text("SELECT public.taskforge_schema_revisions()")


class IncompatibleSchemaError(RuntimeError):
    """Raised without exposing database revision details to process callers."""


async def schema_is_compatible(connection: AsyncConnection) -> bool:
    """Return whether the database reports exactly this build's one head."""
    revisions = await connection.scalar(_SCHEMA_REVISIONS)
    return bool(revisions == [EXPECTED_SCHEMA_REVISION])


async def require_compatible_schema(engine: AsyncEngine) -> None:
    """Fail closed unless the authoritative database is at the exact head."""
    async with engine.connect() as connection:
        if not await schema_is_compatible(connection):
            raise IncompatibleSchemaError(
                "database schema is incompatible with this TaskForge build"
            )
