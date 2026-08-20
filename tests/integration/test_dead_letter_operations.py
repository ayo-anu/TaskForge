"""Real PostgreSQL tests for dead-letter inspection and operator commands."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable, Coroutine
from uuid import UUID, uuid4

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL

from taskforge.dead_letters.domain import (
    DeadLetterFilters,
    DeadLetterReason,
    DeadLetterStatus,
)
from taskforge.dead_letters.persistence_ports import (
    DeadLetterPersistenceUnavailable,
    DeadLetterTransitionConflict,
)
from taskforge.identity.authorization import OwnerFilter
from taskforge.persistence.database import build_async_engine, build_session_factory
from tests.integration.postgresql import (
    asyncpg_dsn,
    migration_database_url,
    temporary_database,
)
from tests.integration.test_authentication_persistence import settings_for
from tests.integration.test_dead_letter_migrations import seed_facts

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_DEAD_LETTER_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_DEAD_LETTER_INTEGRATION=1 explicitly",
    ),
]


async def verify_operations(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        facts = await seed_facts(connection)
    finally:
        await connection.close()

    engine = build_async_engine(settings_for(database_url))
    from taskforge.persistence.dead_letter_operations import (
        SQLAlchemyDeadLetterRepository,
    )

    repository = SQLAlchemyDeadLetterRepository(build_session_factory(engine))
    try:
        owner = OwnerFilter.only(facts.principal)
        page = await repository.list_items(
            owner, DeadLetterFilters(), limit=1, cursor=None
        )
        assert len(page.items) == 1
        assert page.next_cursor is not None
        second = await repository.list_items(
            owner, DeadLetterFilters(), limit=1, cursor=page.next_cursor
        )
        assert len(second.items) == 1
        assert second.items[0].id != page.items[0].id
        assert second.next_cursor is None

        filtered = await repository.list_items(
            owner,
            DeadLetterFilters(
                status=DeadLetterStatus.OPEN,
                reason=DeadLetterReason.PERMANENT_FAILURE,
                task_run_id=facts.task_run,
                source_task_attempt_id=facts.attempt,
            ),
            limit=50,
            cursor=None,
        )
        assert [item.id for item in filtered.items] == [facts.item]
        detail = await repository.get_item(facts.item, owner)
        assert detail is not None
        assert detail.workflow_definition_id
        assert detail.result_kind.value == "permanent_failure"
        assert detail.failure_kind is not None
        assert not hasattr(detail, "output")
        assert (
            await repository.get_item(
                facts.item, OwnerFilter.only(facts.other_principal)
            )
            is None
        )

        correlation = uuid4()
        acknowledged = await repository.transition(
            facts.item,
            owner,
            operator_principal_id=facts.principal,
            target_status=DeadLetterStatus.ACKNOWLEDGED,
            reason="investigating",
            correlation_id=correlation,
        )
        assert acknowledged is not None
        assert acknowledged.status is DeadLetterStatus.ACKNOWLEDGED
        actions = await repository.list_actions(
            facts.item, owner, limit=50, cursor=None
        )
        assert actions is not None
        assert len(actions.items) == 1
        action = actions.items[0]
        assert (action.previous_status, action.new_status) == (
            DeadLetterStatus.OPEN,
            DeadLetterStatus.ACKNOWLEDGED,
        )
        assert action.operator_principal_id == facts.principal
        assert action.correlation_id == correlation
        assert action.occurred_at == acknowledged.status_updated_at

        resolved = await repository.transition(
            facts.item,
            owner,
            operator_principal_id=facts.principal,
            target_status=DeadLetterStatus.RESOLVED,
            reason="root cause corrected",
            correlation_id=uuid4(),
        )
        assert resolved is not None
        assert resolved.status is DeadLetterStatus.RESOLVED
        action_first = await repository.list_actions(
            facts.item, owner, limit=1, cursor=None
        )
        assert action_first is not None
        assert action_first.next_cursor is not None
        action_second = await repository.list_actions(
            facts.item, owner, limit=1, cursor=action_first.next_cursor
        )
        assert action_second is not None
        assert len(action_second.items) == 1
        assert action_second.items[0].id != action_first.items[0].id
        assert action_second.next_cursor is None
        with pytest.raises(DeadLetterTransitionConflict):
            await repository.transition(
                facts.item,
                owner,
                operator_principal_id=facts.principal,
                target_status=DeadLetterStatus.RESOLVED,
                reason="duplicate",
                correlation_id=uuid4(),
            )

        # Live keyset pages reapply mutable filters. Resolving a row after the
        # first request can remove it from a later status-filtered page.
        live = await repository.list_items(
            owner,
            DeadLetterFilters(status=DeadLetterStatus.OPEN),
            limit=50,
            cursor=None,
        )
        assert [item.id for item in live.items] == [facts.other_item]
    finally:
        await engine.dispose()


async def verify_concurrency_and_rollback(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    facts = await seed_facts(connection)
    acknowledge_facts = await seed_facts(connection)
    mixed_facts = await seed_facts(connection)
    await connection.close()
    engine = build_async_engine(settings_for(database_url))
    from taskforge.persistence.dead_letter_operations import (
        SQLAlchemyDeadLetterRepository,
    )

    repository = SQLAlchemyDeadLetterRepository(build_session_factory(engine))
    owner = OwnerFilter.all_owners()

    async def command(item_id: UUID, target: DeadLetterStatus) -> str:
        try:
            await repository.transition(
                item_id,
                owner,
                operator_principal_id=facts.principal,
                target_status=target,
                reason=(
                    "concurrent resolution"
                    if target is DeadLetterStatus.RESOLVED
                    else None
                ),
                correlation_id=uuid4(),
            )
            return "accepted"
        except DeadLetterTransitionConflict:
            return "conflict"

    try:
        assert sorted(
            await asyncio.gather(
                command(facts.item, DeadLetterStatus.RESOLVED),
                command(facts.item, DeadLetterStatus.RESOLVED),
            )
        ) == [
            "accepted",
            "conflict",
        ]
        assert sorted(
            await asyncio.gather(
                command(acknowledge_facts.item, DeadLetterStatus.ACKNOWLEDGED),
                command(acknowledge_facts.item, DeadLetterStatus.ACKNOWLEDGED),
            )
        ) == ["accepted", "conflict"]
        mixed = await asyncio.gather(
            command(mixed_facts.item, DeadLetterStatus.ACKNOWLEDGED),
            command(mixed_facts.item, DeadLetterStatus.RESOLVED),
        )
        # ACK can win first and permit a later RESOLVE, or RESOLVE can win and
        # make ACK conflict. Both outcomes serialize to a valid history.
        assert sorted(mixed) in (["accepted", "accepted"], ["accepted", "conflict"])
        check = await asyncpg.connect(asyncpg_dsn(database_url))
        try:
            assert (
                await check.fetchval(
                    "SELECT count(*) FROM dead_letter_operator_actions "
                    "WHERE dead_letter_item_id = $1",
                    facts.item,
                )
                == 1
            )
            acknowledge_count = await check.fetchval(
                "SELECT count(*) FROM dead_letter_operator_actions "
                "WHERE dead_letter_item_id = $1",
                acknowledge_facts.item,
            )
            assert acknowledge_count == 1
            mixed_rows = await check.fetch(
                "SELECT previous_status, new_status FROM "
                "dead_letter_operator_actions WHERE dead_letter_item_id = $1 "
                "ORDER BY occurred_at, id",
                mixed_facts.item,
            )
            assert len(mixed_rows) in (1, 2)
            assert mixed_rows[0]["previous_status"] == "open"
            if len(mixed_rows) == 2:
                assert tuple(mixed_rows[1]) == ("acknowledged", "resolved")
            await check.execute(
                "CREATE FUNCTION reject_dlq_action() RETURNS trigger LANGUAGE "
                "plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected audit failure'; END $$"
            )
            await check.execute(
                "CREATE TRIGGER reject_dlq_action BEFORE INSERT ON "
                "dead_letter_operator_actions FOR EACH ROW EXECUTE FUNCTION "
                "reject_dlq_action()"
            )
        finally:
            await check.close()

        with pytest.raises(DeadLetterPersistenceUnavailable):
            await repository.transition(
                facts.other_item,
                owner,
                operator_principal_id=facts.principal,
                target_status=DeadLetterStatus.ACKNOWLEDGED,
                reason=None,
                correlation_id=uuid4(),
            )
        check = await asyncpg.connect(asyncpg_dsn(database_url))
        try:
            assert (
                await check.fetchval(
                    "SELECT status FROM dead_letter_status "
                    "WHERE dead_letter_item_id = $1",
                    facts.other_item,
                )
                == "open"
            )
            assert (
                await check.fetchval(
                    "SELECT count(*) FROM dead_letter_operator_actions "
                    "WHERE dead_letter_item_id = $1",
                    facts.other_item,
                )
                == 0
            )
        finally:
            await check.close()
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    "verification", (verify_operations, verify_concurrency_and_rollback)
)
def test_dead_letter_operations(
    verification: Callable[[URL], Coroutine[object, object, None]],
) -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_dead_letter_ops"
    ) as database_url:
        config = Config("alembic.ini")
        with migration_database_url(database_url.render_as_string(hide_password=False)):
            command.upgrade(config, "head")
        asyncio.run(verification(database_url))
