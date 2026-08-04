"""SQLAlchemy role repository for API-principal authorization."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.identity.schema import api_principal_roles


class SQLAlchemyPrincipalRoleRepository:
    """Load only API-principal roles; worker tables are outside this boundary."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def find_role_names(self, principal_id: UUID) -> frozenset[str]:
        statement = select(api_principal_roles.c.role).where(
            api_principal_roles.c.principal_id == principal_id
        )
        async with self._sessions() as session:
            roles = (await session.scalars(statement)).all()
        return frozenset(roles)
