"""Unit tests for API-scoped infrastructure readiness adapters."""

from __future__ import annotations

import asyncio
from types import TracebackType
from typing import cast

import pytest
from aio_pika.abc import AbstractConnection
from pydantic import SecretStr

from taskforge.api.dependencies import (
    PostgreSQLPool,
    PostgreSQLReadinessAdapter,
    RabbitMQReadinessAdapter,
)
from taskforge.settings import Settings


class FakePostgreSQLConnection:
    def __init__(self) -> None:
        self.queries: list[str] = []

    async def fetchval(self, query: str) -> int:
        self.queries.append(query)
        return 1


class FakePoolAcquisition:
    def __init__(self, connection: FakePostgreSQLConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> FakePostgreSQLConnection:
        return self._connection

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None


class FakePostgreSQLPool:
    def __init__(self) -> None:
        self.connection = FakePostgreSQLConnection()
        self.closed = False
        self.terminated = False

    def acquire(self) -> FakePoolAcquisition:
        return FakePoolAcquisition(self.connection)

    async def close(self) -> None:
        self.closed = True

    def terminate(self) -> None:
        self.terminated = True


class FakeRabbitMQChannel:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class FakeRabbitMQConnection:
    def __init__(self) -> None:
        self.is_closed = False
        self.channels: list[FakeRabbitMQChannel] = []
        self.close_count = 0

    async def channel(self) -> FakeRabbitMQChannel:
        channel = FakeRabbitMQChannel()
        self.channels.append(channel)
        return channel

    async def close(self) -> None:
        self.close_count += 1
        self.is_closed = True


def make_settings() -> Settings:
    return Settings(
        postgres_host="postgres.test",
        postgres_port=55432,
        postgres_database="taskforge_test",
        postgres_user="postgres-test-user",
        postgres_password=SecretStr("postgres-test-secret"),
        rabbitmq_host="rabbitmq.test",
        rabbitmq_port=55672,
        rabbitmq_user="rabbitmq-test-user",
        rabbitmq_password=SecretStr("rabbitmq-test-secret"),
        rabbitmq_vhost="taskforge_test",
    )


def test_postgresql_adapter_owns_pool_lifecycle_and_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_pool = FakePostgreSQLPool()
    connection_arguments: dict[str, object] = {}

    async def fake_create_pool(**kwargs: object) -> PostgreSQLPool:
        connection_arguments.update(kwargs)
        return cast(PostgreSQLPool, fake_pool)

    monkeypatch.setattr(
        "taskforge.api.dependencies.asyncpg.create_pool",
        fake_create_pool,
    )
    adapter = PostgreSQLReadinessAdapter(make_settings())

    async def exercise() -> None:
        await adapter.start()
        assert await adapter.is_ready() is True
        await adapter.close()

    asyncio.run(exercise())

    assert fake_pool.connection.queries == ["SELECT 1"]
    assert fake_pool.closed is True
    assert fake_pool.terminated is False
    assert connection_arguments == {
        "host": "postgres.test",
        "port": 55432,
        "database": "taskforge_test",
        "user": "postgres-test-user",
        "password": "postgres-test-secret",
        "min_size": 0,
        "max_size": 2,
        "command_timeout": 2.0,
    }


def test_rabbitmq_adapter_lazily_reuses_and_closes_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_connection = FakeRabbitMQConnection()
    connection_arguments: dict[str, object] = {}
    connection_count = 0

    async def fake_connect(**kwargs: object) -> AbstractConnection:
        nonlocal connection_count
        connection_count += 1
        connection_arguments.update(kwargs)
        return cast(AbstractConnection, fake_connection)

    monkeypatch.setattr("taskforge.api.dependencies.aio_pika.connect", fake_connect)
    adapter = RabbitMQReadinessAdapter(make_settings())

    async def exercise() -> None:
        await adapter.start()
        assert connection_count == 0
        assert await adapter.is_ready() is True
        assert await adapter.is_ready() is True
        await adapter.close()

    asyncio.run(exercise())

    assert connection_count == 1
    assert len(fake_connection.channels) == 2
    assert all(channel.closed for channel in fake_connection.channels)
    assert fake_connection.close_count == 1
    assert connection_arguments == {
        "host": "rabbitmq.test",
        "port": 55672,
        "login": "rabbitmq-test-user",
        "password": "rabbitmq-test-secret",
        "virtualhost": "taskforge_test",
    }


def test_rabbitmq_adapter_retries_after_initial_connection_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_connection = FakeRabbitMQConnection()
    attempts = 0

    async def flaky_connect(**kwargs: object) -> AbstractConnection:
        del kwargs
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ConnectionError("temporary broker failure")
        return cast(AbstractConnection, fake_connection)

    monkeypatch.setattr("taskforge.api.dependencies.aio_pika.connect", flaky_connect)
    adapter = RabbitMQReadinessAdapter(make_settings())

    async def exercise() -> None:
        with pytest.raises(ConnectionError):
            await adapter.is_ready()
        assert await adapter.is_ready() is True
        await adapter.close()

    asyncio.run(exercise())

    assert attempts == 2
