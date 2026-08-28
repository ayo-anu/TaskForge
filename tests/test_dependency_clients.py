"""Unit tests for authoritative API PostgreSQL readiness probing."""

from __future__ import annotations

import asyncio
from types import TracebackType
from typing import cast

from sqlalchemy.ext.asyncio import AsyncEngine

from taskforge.api.dependencies import (
    SQLAlchemyPostgreSQLReadinessProbe,
    build_readiness_coordinator,
)
from taskforge.settings import Settings


class FakeConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    async def scalar(self, statement: object) -> int:
        self.statements.append(str(statement))
        return 1


class FakeConnectionContext:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.exited = False

    async def __aenter__(self) -> FakeConnection:
        return self.connection

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.exited = True


class FakeEngine:
    def __init__(self) -> None:
        self.connection = FakeConnection()
        self.contexts: list[FakeConnectionContext] = []

    def connect(self) -> FakeConnectionContext:
        context = FakeConnectionContext(self.connection)
        self.contexts.append(context)
        return context


def test_probe_uses_supplied_authoritative_engine_without_mutation() -> None:
    engine = FakeEngine()
    probe = SQLAlchemyPostgreSQLReadinessProbe(cast(AsyncEngine, engine))

    assert asyncio.run(probe.is_ready()) is True
    assert engine.connection.statements == ["SELECT 1"]
    assert len(engine.contexts) == 1
    assert engine.contexts[0].exited is True


def test_coordinator_uses_exact_supplied_engine() -> None:
    engine = FakeEngine()
    settings = Settings(
        postgres_password="postgres-secret",
        rabbitmq_password="rabbit-secret",
    )
    coordinator = build_readiness_coordinator(settings, cast(AsyncEngine, engine))

    async def exercise() -> None:
        await coordinator.start()
        await coordinator.snapshot()

    asyncio.run(exercise())
    assert len(engine.contexts) == 2
