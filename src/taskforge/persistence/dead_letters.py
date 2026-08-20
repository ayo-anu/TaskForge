"""Transaction-scoped persistence for authoritative dead-letter facts."""

from __future__ import annotations

from enum import StrEnum
from uuid import UUID

from sqlalchemy import and_, exists, insert, literal, or_, select
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from taskforge.dead_letters.schema import dead_letter_items, dead_letter_status
from taskforge.runs.schema import (
    task_attempt_results,
    task_attempts,
    task_retry_events,
)
from taskforge.worker.results import TaskExecutionResultKind


class DeadLetterReason(StrEnum):
    PERMANENT_FAILURE = "permanent_failure"
    RETRY_EXHAUSTED = "retry_exhausted"


class DeadLetterInsertOutcome(StrEnum):
    CREATED = "created"
    ALREADY_PRESENT = "already_present"


class DeadLetterPersistenceInvariantViolation(Exception):
    """Authoritative evidence and dead-letter persistence disagree."""


async def ensure_dead_letter(
    session: AsyncSession,
    *,
    item_id: UUID,
    task_run_id: UUID,
    source_task_attempt_id: UUID,
    reason: DeadLetterReason,
) -> DeadLetterInsertOutcome:
    """Create one item and status using the caller's active transaction."""
    evidence = _authoritative_evidence(
        task_run_id=task_run_id,
        source_task_attempt_id=source_task_attempt_id,
        reason=reason,
    )
    candidate = select(
        literal(item_id),
        literal(task_run_id),
        literal(source_task_attempt_id),
        literal(reason.value),
    ).where(evidence)
    created = (
        await session.execute(
            postgresql_insert(dead_letter_items)
            .from_select(
                (
                    dead_letter_items.c.id,
                    dead_letter_items.c.task_run_id,
                    dead_letter_items.c.source_task_attempt_id,
                    dead_letter_items.c.reason,
                ),
                candidate,
            )
            .on_conflict_do_nothing(
                index_elements=(dead_letter_items.c.source_task_attempt_id,)
            )
            .returning(dead_letter_items.c.id)
        )
    ).one_or_none()
    if created is not None:
        await session.execute(
            insert(dead_letter_status).values(
                dead_letter_item_id=created.id,
                status="open",
            )
        )
        return DeadLetterInsertOutcome.CREATED

    existing = (
        await session.execute(
            select(
                dead_letter_items.c.task_run_id,
                dead_letter_items.c.source_task_attempt_id,
                dead_letter_items.c.reason,
                dead_letter_status.c.dead_letter_item_id.label("status_item_id"),
                evidence.label("evidence_valid"),
            )
            .outerjoin(
                dead_letter_status,
                dead_letter_status.c.dead_letter_item_id == dead_letter_items.c.id,
            )
            .where(dead_letter_items.c.source_task_attempt_id == source_task_attempt_id)
        )
    ).one_or_none()
    if (
        existing is None
        or existing.task_run_id != task_run_id
        or existing.source_task_attempt_id != source_task_attempt_id
        or existing.reason != reason.value
        or existing.status_item_id is None
        or not existing.evidence_valid
    ):
        raise DeadLetterPersistenceInvariantViolation
    return DeadLetterInsertOutcome.ALREADY_PRESENT


def _authoritative_evidence(
    *,
    task_run_id: UUID,
    source_task_attempt_id: UUID,
    reason: DeadLetterReason,
) -> ColumnElement[bool]:
    attempt_result = and_(
        task_attempts.c.id == source_task_attempt_id,
        task_attempts.c.task_run_id == task_run_id,
        task_attempt_results.c.task_attempt_id == task_attempts.c.id,
    )
    if reason is DeadLetterReason.PERMANENT_FAILURE:
        return exists(
            select(literal(1))
            .select_from(
                task_attempts.join(
                    task_attempt_results,
                    task_attempt_results.c.task_attempt_id == task_attempts.c.id,
                )
            )
            .where(
                attempt_result,
                task_attempt_results.c.result_kind
                == TaskExecutionResultKind.PERMANENT_FAILURE.value,
            )
        )
    return exists(
        select(literal(1))
        .select_from(
            task_attempts.join(
                task_attempt_results,
                task_attempt_results.c.task_attempt_id == task_attempts.c.id,
            ).join(
                task_retry_events,
                and_(
                    task_retry_events.c.task_run_id == task_attempts.c.task_run_id,
                    task_retry_events.c.failed_attempt_number
                    == task_attempts.c.attempt_number,
                ),
            )
        )
        .where(
            attempt_result,
            task_attempt_results.c.result_kind
            == TaskExecutionResultKind.RETRYABLE_FAILURE.value,
            task_retry_events.c.event_type == "retry_not_scheduled",
            or_(
                task_retry_events.c.decision_reason == "exhausted",
                task_retry_events.c.decision_reason == "no_policy",
            ),
        )
    )
