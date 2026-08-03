"""Infrastructure-specific readiness adapters owned by the API process."""

from __future__ import annotations

import asyncio
from types import TracebackType
from typing import Protocol

import aio_pika
import asyncpg
from aio_pika.abc import AbstractConnection

from taskforge.api.health import ReadinessCoordinator
from taskforge.settings import Settings


class PostgreSQLConnection(Protocol):
    async def fetchval(self, query: str) -> object:
        """Return the first value produced by a query."""


class PostgreSQLPoolAcquisition(Protocol):
    async def __aenter__(self) -> PostgreSQLConnection:
        """Acquire a connection from the pool."""

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Release the acquired connection."""


class PostgreSQLPool(Protocol):
    def acquire(self) -> PostgreSQLPoolAcquisition:
        """Create a pool acquisition context manager."""

    async def close(self) -> None:
        """Close the pool gracefully."""

    def terminate(self) -> None:
        """Terminate the pool immediately."""


class PostgreSQLReadinessAdapter:
    """Manage a small lazy pool and encapsulate the PostgreSQL probe."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._pool: PostgreSQLPool | None = None

    async def start(self) -> None:
        """Create a pool that does not connect until the first acquisition."""
        self._pool = await asyncpg.create_pool(
            host=self._settings.postgres_host,
            port=self._settings.postgres_port,
            database=self._settings.postgres_database,
            user=self._settings.postgres_user,
            password=self._settings.postgres_password.get_secret_value(),
            min_size=0,
            max_size=2,
            command_timeout=self._settings.readiness_timeout_seconds,
        )

    async def is_ready(self) -> bool:
        """Probe PostgreSQL without exposing the query to endpoint code."""
        if self._pool is None:
            return False
        async with self._pool.acquire() as connection:
            result = await connection.fetchval("SELECT 1")
            return result == 1

    async def close(self) -> None:
        """Close the pool within a bound, then terminate it if necessary."""
        if self._pool is None:
            return
        pool, self._pool = self._pool, None
        try:
            async with asyncio.timeout(self._settings.readiness_timeout_seconds):
                await pool.close()
        except TimeoutError:
            pool.terminate()


class RabbitMQReadinessAdapter:
    """Lazily connect to RabbitMQ and encapsulate the AMQP probe."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._connection: AbstractConnection | None = None
        self._connection_lock = asyncio.Lock()

    async def start(self) -> None:
        """Defer network access so dependency outages do not prevent startup."""

    async def is_ready(self) -> bool:
        """Verify an AMQP connection by opening and closing a channel."""
        connection = await self._get_connection()
        channel = await connection.channel()
        await channel.close()
        return True

    async def close(self) -> None:
        """Close the cached AMQP connection within the readiness bound."""
        connection, self._connection = self._connection, None
        if connection is None or connection.is_closed:
            return
        try:
            async with asyncio.timeout(self._settings.readiness_timeout_seconds):
                await connection.close()
        except TimeoutError:
            return

    async def _get_connection(self) -> AbstractConnection:
        connection = self._connection
        if connection is not None and not connection.is_closed:
            return connection

        async with self._connection_lock:
            connection = self._connection
            if connection is None or connection.is_closed:
                connection = await aio_pika.connect(
                    host=self._settings.rabbitmq_host,
                    port=self._settings.rabbitmq_port,
                    login=self._settings.rabbitmq_user,
                    password=self._settings.rabbitmq_password.get_secret_value(),
                    virtualhost=self._settings.rabbitmq_vhost,
                )
                self._connection = connection
            return connection


def build_readiness_coordinator(settings: Settings) -> ReadinessCoordinator:
    """Build the API's required readiness dependencies."""
    return ReadinessCoordinator(
        adapters=(
            PostgreSQLReadinessAdapter(settings),
            RabbitMQReadinessAdapter(settings),
        ),
        timeout_seconds=settings.readiness_timeout_seconds,
    )
