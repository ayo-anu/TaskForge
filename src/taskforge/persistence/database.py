"""SQLAlchemy async engine construction from typed settings."""

from sqlalchemy import URL
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from taskforge.settings import Settings


def build_async_engine(settings: Settings) -> AsyncEngine:
    """Build a lazy engine without rendering credentials into a URL string."""
    url = URL.create(
        drivername="postgresql+asyncpg",
        username=settings.postgres_user,
        password=settings.postgres_password.get_secret_value(),
        host=settings.postgres_host,
        port=settings.postgres_port,
        database=settings.postgres_database,
    )
    return create_async_engine(
        url,
        hide_parameters=True,
        pool_size=settings.database_pool_size,
        max_overflow=0,
        pool_timeout=settings.database_pool_timeout_seconds,
    )


def build_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Create short-lived sessions with no post-commit object expiry."""
    return async_sessionmaker(engine, expire_on_commit=False)
