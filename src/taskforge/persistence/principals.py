"""SQLAlchemy principal profile reads with ownership in the query predicate."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.identity.authorization import OwnerFilter
from taskforge.identity.principals import PrincipalProfile
from taskforge.identity.schema import api_principals


class SQLAlchemyPrincipalProfileRepository:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def find_profile(
        self,
        principal_id: UUID,
        owner_filter: OwnerFilter,
    ) -> PrincipalProfile | None:
        statement = select(
            api_principals.c.id,
            api_principals.c.name,
            api_principals.c.created_at,
        ).where(api_principals.c.id == principal_id)
        if not owner_filter.unrestricted:
            assert owner_filter.principal_id is not None
            statement = statement.where(
                api_principals.c.id == owner_filter.principal_id
            )
        async with self._sessions() as session:
            row = (await session.execute(statement)).one_or_none()
        if row is None:
            return None
        return PrincipalProfile(id=row.id, name=row.name, created_at=row.created_at)
