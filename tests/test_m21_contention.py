"""Deterministic contracts for the M21 Task 3 contention harness."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import select, update

from taskforge.runs.schema import task_runs, workflow_runs
from tests.integration.contention import (
    LockStatementIdentity,
    PostLockPause,
    PostLockPausingSession,
)


class RecordingSession:
    def __init__(self, entered: asyncio.Event | None = None) -> None:
        self.statements: list[object] = []
        self.entered = entered
        self.complete = asyncio.Event()
        self.transaction_open = True

    async def execute(self, statement: object, parameters: object = None) -> object:
        del parameters
        self.statements.append(statement)
        if self.entered is not None:
            self.entered.set()
            await self.complete.wait()
        return SimpleNamespace(statement=statement)


def test_post_lock_wrapper_ignores_unrelated_statements() -> None:
    async def exercise() -> None:
        target = object()
        unrelated = object()
        pause = PostLockPause(lambda statement, _parameters: statement is target)
        delegate = RecordingSession()
        session = PostLockPausingSession(delegate, pause)

        result = await session.execute(unrelated)

        assert result.statement is unrelated
        assert delegate.statements == [unrelated]
        assert not pause.acquired.is_set()
        assert not pause.matched

    asyncio.run(exercise())


def test_post_lock_wrapper_cannot_signal_before_real_execute_completes() -> None:
    async def exercise() -> None:
        target = object()
        entered = asyncio.Event()
        delegate = RecordingSession(entered)
        pause = PostLockPause(lambda statement, _parameters: statement is target)
        session = PostLockPausingSession(delegate, pause)
        pending = asyncio.create_task(session.execute(target))
        await entered.wait()

        assert not pause.acquired.is_set()
        delegate.complete.set()
        await pause.acquired.wait()
        assert not pending.done()
        assert delegate.transaction_open
        pause.release.set()
        returned = await pending
        assert returned.statement is target
        assert returned is not None

    asyncio.run(exercise())


def test_lock_statement_identity_is_narrow() -> None:
    target = uuid4()
    matcher = LockStatementIdentity(workflow_runs, target)
    intended = (
        select(workflow_runs.c.id).where(workflow_runs.c.id == target).with_for_update()
    )
    wrong_target = (
        select(workflow_runs.c.id)
        .where(workflow_runs.c.id == uuid4())
        .with_for_update()
    )
    wrong_table = (
        select(task_runs.c.id).where(task_runs.c.id == target).with_for_update()
    )
    wrong_operation = update(workflow_runs).where(workflow_runs.c.id == target)
    unrelated_for_update = select(workflow_runs.c.id).with_for_update()

    assert matcher(intended, None)
    assert not matcher(wrong_target, None)
    assert not matcher(wrong_table, None)
    assert not matcher(wrong_operation, None)
    assert not matcher(unrelated_for_update, None)


def test_post_lock_wrapper_propagates_execute_failure_without_signalling() -> None:
    class FailingSession:
        async def execute(self, statement: object, parameters: object = None) -> object:
            del statement, parameters
            raise LookupError("unchanged")

    async def exercise() -> None:
        target = object()
        pause = PostLockPause(lambda statement, _parameters: statement is target)
        session = PostLockPausingSession(FailingSession(), pause)
        with pytest.raises(LookupError, match="unchanged") as caught:
            await session.execute(target)
        assert type(caught.value) is LookupError
        assert not pause.acquired.is_set()
        assert not pause.matched

    asyncio.run(exercise())


def test_cancelled_pause_never_resumes_or_commits() -> None:
    async def exercise() -> None:
        target = object()
        delegate = RecordingSession()
        pause = PostLockPause(lambda statement, _parameters: statement is target)
        pending = asyncio.create_task(
            PostLockPausingSession(delegate, pause).execute(target)
        )
        await pause.acquired.wait()
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert delegate.transaction_open
        assert not pause.release.is_set()

    asyncio.run(exercise())
