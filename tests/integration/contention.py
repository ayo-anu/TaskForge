"""Bounded, test-only helpers for M21 PostgreSQL contention scenarios."""

from __future__ import annotations

import asyncio
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Protocol
from uuid import UUID

import asyncpg
from sqlalchemy import Select, text
from sqlalchemy.sql import visitors
from sqlalchemy.sql.elements import BindParameter

_SAFE_CATEGORY = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,63}$")


class StatementMatcher(Protocol):
    def __call__(self, statement: Any, parameters: Any) -> bool: ...


@dataclass(frozen=True)
class LockStatementIdentity:
    """Match one SQLAlchemy SELECT FOR UPDATE by table object and target UUID."""

    table: Any
    target: UUID

    def __call__(self, statement: Any, parameters: Any) -> bool:
        if not isinstance(statement, Select):
            return False
        # SQLAlchemy has no public Select accessor for this clause. Keep the
        # compatibility-sensitive check isolated here and cover it with fast
        # tests instead of parsing rendered SQL.
        if getattr(statement, "_for_update_arg", None) is None:
            return False
        if not any(
            from_clause is self.table or from_clause.is_derived_from(self.table)
            for from_clause in statement.get_final_froms()
        ):
            return False
        nodes = tuple(visitors.iterate(statement))
        values = {node.value for node in nodes if isinstance(node, BindParameter)}
        if isinstance(parameters, dict):
            values.update(parameters.values())
        return self.target in values


@dataclass
class PostLockPause:
    """Pause once after one unchanged production lock statement returns."""

    matcher: StatementMatcher
    acquired: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)
    matched: bool = False


class PostLockPausingSession:
    """Delegate a session, pausing only after a configured execute completes."""

    def __init__(self, session: Any, pause: PostLockPause) -> None:
        self._session = session
        self._pause = pause

    def __getattr__(self, name: str) -> Any:
        return getattr(self._session, name)

    async def execute(
        self, statement: Any, parameters: Any = None, **kwargs: Any
    ) -> Any:
        result = await self._session.execute(statement, parameters, **kwargs)
        pause = self._pause
        if not pause.matched and pause.matcher(statement, parameters):
            pause.matched = True
            pause.acquired.set()
            await pause.release.wait()
        return result


class ContenderSession(PostLockPausingSession):
    """Label an explicitly begun session before production statements execute."""

    def __init__(
        self,
        session: Any,
        application_name: str,
        pause: PostLockPause | None,
    ) -> None:
        super().__init__(session, pause or PostLockPause(lambda _s, _p: False))
        self._application_name = application_name

    async def begin(self) -> Any:
        transaction = await self._session.begin()
        await self._session.execute(
            text("SELECT set_config('application_name', :application_name, true)"),
            {"application_name": self._application_name},
        )
        return transaction


class ContenderSessionFactory:
    """Label one real transaction and optionally pause after its target lock."""

    def __init__(
        self,
        sessions: Any,
        application_name: str,
        pause: PostLockPause | None = None,
    ) -> None:
        if _SAFE_CATEGORY.fullmatch(application_name) is None:
            raise ValueError("unsafe PostgreSQL application name")
        self._sessions = sessions
        self._application_name = application_name
        self._pause = pause

    @asynccontextmanager
    async def begin(self) -> Any:
        async with self._sessions.begin() as session:
            await session.execute(
                text("SELECT set_config('application_name', :application_name, true)"),
                {"application_name": self._application_name},
            )
            if self._pause is None:
                yield session
            else:
                yield PostLockPausingSession(session, self._pause)

    def __call__(self) -> Any:
        return ContenderSession(self._sessions(), self._application_name, self._pause)


async def observe_blocked_followers(
    observer: asyncpg.Connection[asyncpg.Record],
    *,
    owner_application: str,
    follower_applications: list[str],
    timeout_seconds: float = 2.0,
) -> list[dict[str, Any]]:
    """Prove every named follower is lock-waiting on the named owner."""

    async def observe() -> list[dict[str, Any]]:
        while True:
            rows = await observer.fetch(
                "SELECT owner.pid AS owner_pid, follower.pid AS follower_pid, "
                "follower.application_name, follower.wait_event_type, "
                "follower.wait_event, owner.pid = ANY(pg_blocking_pids(follower.pid)) "
                "AS blocked_by_owner FROM pg_stat_activity follower "
                "JOIN pg_stat_activity owner ON owner.application_name = $1 "
                "WHERE follower.application_name = ANY($2::text[])",
                owner_application,
                follower_applications,
            )
            if (
                len(rows) == len(follower_applications)
                and len({row["follower_pid"] for row in rows}) == len(rows)
                and all(
                    row["wait_event_type"] == "Lock"
                    and row["blocked_by_owner"] is True
                    and row["owner_pid"] != row["follower_pid"]
                    for row in rows
                )
            ):
                return [
                    {
                        "relationship_proven": True,
                        "owner_pid": row["owner_pid"],
                        "follower_pid": row["follower_pid"],
                        "distinct_backends": row["owner_pid"] != row["follower_pid"],
                        "wait_event_type": "Lock",
                        "wait_event": _safe_wait_event(row["wait_event"]),
                    }
                    for row in rows
                ]
            await asyncio.sleep(0.01)

    return await asyncio.wait_for(observe(), timeout=timeout_seconds)


def _safe_wait_event(value: object) -> str:
    if isinstance(value, str) and _SAFE_CATEGORY.fullmatch(value):
        return value
    return "Lock"
