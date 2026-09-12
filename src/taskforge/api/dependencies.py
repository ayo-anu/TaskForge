"""Infrastructure-specific readiness probing owned by the API process."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine

from taskforge.api.health import ReadinessCoordinator
from taskforge.persistence.schema_compatibility import schema_is_compatible
from taskforge.settings import Settings


class SQLAlchemyPostgreSQLReadinessProbe:
    """Probe the authoritative API SQLAlchemy runtime without mutation."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def is_ready(self) -> bool:
        async with self._engine.connect() as connection:
            return await schema_is_compatible(connection)


def build_readiness_coordinator(
    settings: Settings, engine: AsyncEngine
) -> ReadinessCoordinator:
    """Build readiness from the API's authoritative database runtime."""
    return ReadinessCoordinator(
        SQLAlchemyPostgreSQLReadinessProbe(engine),
        timeout_seconds=settings.readiness_timeout_seconds,
    )
