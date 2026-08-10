"""Atomic PostgreSQL persistence for authenticated worker registration."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    DateTime,
    and_,
    case,
    cast,
    delete,
    func,
    insert,
    literal,
    or_,
    select,
)
from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlalchemy.engine import Row
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.sql import Select

from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.identity.schema import worker_credentials, worker_identities
from taskforge.worker.domain import (
    InspectedWorkerHealth,
    InspectedWorkerHeartbeat,
    InspectedWorkerHeartbeatPage,
    InspectedWorkerIdentity,
    InspectedWorkerSession,
    InspectedWorkerSessionPage,
    InspectedWorkerSessionResource,
    RegisteredWorkerSession,
    ReplacedWorkerCapabilities,
    WorkerCapabilityReplacement,
    WorkerHealthProjection,
    WorkerHealthThresholds,
    WorkerHeartbeat,
    WorkerInspectionObservation,
    WorkerRegistration,
    WorkerSessionHealthStatus,
    WorkerSessionPageCursor,
)
from taskforge.worker.persistence_ports import (
    WorkerCapabilityAuthorityRejected,
    WorkerCapabilityInvariantViolation,
    WorkerCapabilityPersistenceUnavailable,
    WorkerCapabilitySessionInactive,
    WorkerCapabilitySessionUnavailable,
    WorkerHeartbeatAuthorityRejected,
    WorkerHeartbeatInvariantViolation,
    WorkerHeartbeatPersistenceUnavailable,
    WorkerHeartbeatReplayConflict,
    WorkerHeartbeatSequenceGap,
    WorkerHeartbeatSessionInactive,
    WorkerHeartbeatSessionUnavailable,
    WorkerHeartbeatStale,
    WorkerInspectionInvariantViolation,
    WorkerInspectionNotFound,
    WorkerInspectionPersistenceUnavailable,
    WorkerRegistrationAuthorityRejected,
    WorkerRegistrationPersistenceUnavailable,
    WorkerRegistrationRecordConflict,
)
from taskforge.worker.schema import (
    worker_heartbeats,
    worker_session_capabilities,
    worker_session_health,
    worker_sessions,
)


class SQLAlchemyWorkerRegistrationRepository:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def register_session(
        self,
        authenticated_worker: AuthenticatedWorker,
        session_id: UUID,
        registration: WorkerRegistration,
    ) -> RegisteredWorkerSession:
        try:
            async with self._sessions.begin() as session:
                identity = (
                    await session.execute(
                        select(worker_identities.c.id, worker_identities.c.disabled_at)
                        .where(
                            worker_identities.c.id
                            == authenticated_worker.worker_identity_id
                        )
                        .with_for_update(read=True)
                    )
                ).one_or_none()
                if identity is None or identity.disabled_at is not None:
                    raise WorkerRegistrationAuthorityRejected

                credential = (
                    await session.execute(
                        select(worker_credentials.c.id)
                        .where(
                            worker_credentials.c.id
                            == authenticated_worker.credential_id,
                            worker_credentials.c.worker_identity_id
                            == authenticated_worker.worker_identity_id,
                            worker_credentials.c.revoked_at.is_(None),
                            or_(
                                worker_credentials.c.expires_at.is_(None),
                                worker_credentials.c.expires_at
                                > func.statement_timestamp(),
                            ),
                        )
                        .with_for_update(read=True)
                    )
                ).one_or_none()
                if credential is None:
                    raise WorkerRegistrationAuthorityRejected

                session_row = (
                    await session.execute(
                        insert(worker_sessions)
                        .values(
                            id=session_id,
                            worker_identity_id=authenticated_worker.worker_identity_id,
                        )
                        .returning(worker_sessions.c.registered_at)
                    )
                ).one()
                if registration.capabilities:
                    await session.execute(
                        insert(worker_session_capabilities),
                        [
                            {
                                "worker_session_id": session_id,
                                "capability": capability,
                            }
                            for capability in registration.capabilities
                        ],
                    )
                await session.execute(
                    insert(worker_session_health).values(
                        worker_session_id=session_id,
                        last_sequence=0,
                        last_seen_at=session_row.registered_at,
                        accepting_work=False,
                        availability_changed_at=session_row.registered_at,
                    )
                )
        except WorkerRegistrationAuthorityRejected:
            raise
        except IntegrityError as error:
            raise WorkerRegistrationRecordConflict from error
        except DBAPIError as error:
            raise WorkerRegistrationPersistenceUnavailable from error

        return RegisteredWorkerSession(
            session_id,
            session_row.registered_at,
            registration.capabilities,
        )


class SQLAlchemyWorkerHeartbeatRepository:
    """Serialize heartbeat commands through one session health row."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def apply_heartbeat(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        heartbeat: WorkerHeartbeat,
    ) -> WorkerHealthProjection:
        try:
            async with self._sessions.begin() as session:
                await _lock_heartbeat_authority(session, authenticated_worker)
                worker_session = (
                    await session.execute(
                        select(worker_sessions.c.id, worker_sessions.c.ended_at)
                        .where(
                            worker_sessions.c.id == worker_session_id,
                            worker_sessions.c.worker_identity_id
                            == authenticated_worker.worker_identity_id,
                        )
                        .with_for_update(read=True)
                    )
                ).one_or_none()
                if worker_session is None:
                    raise WorkerHeartbeatSessionUnavailable
                if worker_session.ended_at is not None:
                    raise WorkerHeartbeatSessionInactive

                health = (
                    await session.execute(
                        select(worker_session_health)
                        .where(
                            worker_session_health.c.worker_session_id
                            == worker_session_id
                        )
                        .with_for_update()
                    )
                ).one_or_none()
                if health is None:
                    raise WorkerHeartbeatInvariantViolation

                if heartbeat.sequence < health.last_sequence:
                    raise WorkerHeartbeatStale
                if heartbeat.sequence == health.last_sequence:
                    return await _replay_current_heartbeat(
                        session,
                        worker_session_id,
                        heartbeat,
                        health,
                    )
                if heartbeat.sequence > health.last_sequence + 1:
                    raise WorkerHeartbeatSequenceGap

                heartbeat_row = (
                    await session.execute(
                        insert(worker_heartbeats)
                        .values(
                            worker_session_id=worker_session_id,
                            sequence=heartbeat.sequence,
                            accepting_work=heartbeat.accepting_work,
                        )
                        .returning(worker_heartbeats.c.received_at)
                    )
                ).one()
                availability_changed_at = (
                    heartbeat_row.received_at
                    if heartbeat.accepting_work != health.accepting_work
                    else health.availability_changed_at
                )
                updated = (
                    await session.execute(
                        worker_session_health.update()
                        .where(
                            worker_session_health.c.worker_session_id
                            == worker_session_id,
                            worker_session_health.c.last_sequence
                            == health.last_sequence,
                        )
                        .values(
                            last_sequence=heartbeat.sequence,
                            last_seen_at=heartbeat_row.received_at,
                            accepting_work=heartbeat.accepting_work,
                            availability_changed_at=availability_changed_at,
                        )
                        .returning(*worker_session_health.c)
                    )
                ).one_or_none()
                if updated is None:
                    raise WorkerHeartbeatInvariantViolation
                return _health_projection(updated)
        except (
            WorkerHeartbeatAuthorityRejected,
            WorkerHeartbeatInvariantViolation,
            WorkerHeartbeatReplayConflict,
            WorkerHeartbeatSequenceGap,
            WorkerHeartbeatSessionInactive,
            WorkerHeartbeatSessionUnavailable,
            WorkerHeartbeatStale,
        ):
            raise
        except DBAPIError as error:
            raise WorkerHeartbeatPersistenceUnavailable from error


async def _lock_heartbeat_authority(
    session: AsyncSession,
    authenticated_worker: AuthenticatedWorker,
) -> None:
    identity = (
        await session.execute(
            select(worker_identities.c.id, worker_identities.c.disabled_at)
            .where(worker_identities.c.id == authenticated_worker.worker_identity_id)
            .with_for_update(read=True)
        )
    ).one_or_none()
    if identity is None or identity.disabled_at is not None:
        raise WorkerHeartbeatAuthorityRejected

    credential = (
        await session.execute(
            select(worker_credentials.c.id)
            .where(
                worker_credentials.c.id == authenticated_worker.credential_id,
                worker_credentials.c.worker_identity_id
                == authenticated_worker.worker_identity_id,
                worker_credentials.c.revoked_at.is_(None),
                or_(
                    worker_credentials.c.expires_at.is_(None),
                    worker_credentials.c.expires_at > func.statement_timestamp(),
                ),
            )
            .with_for_update(read=True)
        )
    ).one_or_none()
    if credential is None:
        raise WorkerHeartbeatAuthorityRejected


async def _replay_current_heartbeat(
    session: AsyncSession,
    worker_session_id: UUID,
    heartbeat: WorkerHeartbeat,
    health: Row[Any],
) -> WorkerHealthProjection:
    history = (
        await session.execute(
            select(worker_heartbeats.c.accepting_work).where(
                worker_heartbeats.c.worker_session_id == worker_session_id,
                worker_heartbeats.c.sequence == heartbeat.sequence,
            )
        )
    ).one_or_none()
    if history is None:
        raise WorkerHeartbeatInvariantViolation
    if history.accepting_work != heartbeat.accepting_work:
        raise WorkerHeartbeatReplayConflict
    return _health_projection(health)


def _health_projection(row: Row[Any]) -> WorkerHealthProjection:
    return WorkerHealthProjection(
        row.worker_session_id,
        row.last_sequence,
        row.last_seen_at,
        row.accepting_work,
        row.availability_changed_at,
    )


class SQLAlchemyWorkerCapabilityRepository:
    """Atomically replace one authenticated live session's capability set."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def replace_capabilities(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        replacement: WorkerCapabilityReplacement,
    ) -> ReplacedWorkerCapabilities:
        try:
            async with self._sessions.begin() as session:
                await _lock_capability_authority(session, authenticated_worker)
                worker_session = (
                    await session.execute(
                        _capability_session_lock_statement(
                            authenticated_worker.worker_identity_id,
                            worker_session_id,
                        )
                    )
                ).one_or_none()
                if worker_session is None:
                    raise WorkerCapabilitySessionUnavailable
                if worker_session.ended_at is not None:
                    raise WorkerCapabilitySessionInactive

                current = tuple(
                    (
                        await session.execute(
                            select(worker_session_capabilities.c.capability)
                            .where(
                                worker_session_capabilities.c.worker_session_id
                                == worker_session_id
                            )
                            .order_by(worker_session_capabilities.c.capability)
                        )
                    ).scalars()
                )
                if current == replacement.capabilities:
                    return ReplacedWorkerCapabilities(worker_session_id, current)

                current_set = set(current)
                replacement_set = set(replacement.capabilities)
                removed = tuple(sorted(current_set - replacement_set))
                added = tuple(sorted(replacement_set - current_set))
                if removed:
                    await session.execute(
                        delete(worker_session_capabilities).where(
                            worker_session_capabilities.c.worker_session_id
                            == worker_session_id,
                            worker_session_capabilities.c.capability.in_(removed),
                        )
                    )
                if added:
                    await session.execute(
                        insert(worker_session_capabilities),
                        [
                            {
                                "worker_session_id": worker_session_id,
                                "capability": capability,
                            }
                            for capability in added
                        ],
                    )
                return ReplacedWorkerCapabilities(
                    worker_session_id, replacement.capabilities
                )
        except (
            WorkerCapabilityAuthorityRejected,
            WorkerCapabilityInvariantViolation,
            WorkerCapabilitySessionInactive,
            WorkerCapabilitySessionUnavailable,
        ):
            raise
        except IntegrityError as error:
            raise WorkerCapabilityInvariantViolation from error
        except DBAPIError as error:
            raise WorkerCapabilityPersistenceUnavailable from error


async def _lock_capability_authority(
    session: AsyncSession,
    authenticated_worker: AuthenticatedWorker,
) -> None:
    identity = (
        await session.execute(
            select(worker_identities.c.id, worker_identities.c.disabled_at)
            .where(worker_identities.c.id == authenticated_worker.worker_identity_id)
            .with_for_update(read=True)
        )
    ).one_or_none()
    if identity is None or identity.disabled_at is not None:
        raise WorkerCapabilityAuthorityRejected

    credential = (
        await session.execute(
            select(worker_credentials.c.id)
            .where(
                worker_credentials.c.id == authenticated_worker.credential_id,
                worker_credentials.c.worker_identity_id
                == authenticated_worker.worker_identity_id,
                worker_credentials.c.revoked_at.is_(None),
                or_(
                    worker_credentials.c.expires_at.is_(None),
                    worker_credentials.c.expires_at > func.statement_timestamp(),
                ),
            )
            .with_for_update(read=True)
        )
    ).one_or_none()
    if credential is None:
        raise WorkerCapabilityAuthorityRejected


def _capability_session_lock_statement(
    worker_identity_id: UUID,
    worker_session_id: UUID,
) -> Select[Any]:
    return (
        select(worker_sessions.c.id, worker_sessions.c.ended_at)
        .where(
            worker_sessions.c.id == worker_session_id,
            worker_sessions.c.worker_identity_id == worker_identity_id,
        )
        .with_for_update(key_share=True)
    )


class SQLAlchemyWorkerInspectionRepository:
    """Read worker inspection snapshots without acquiring write locks."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def get_session(
        self, worker_session_id: UUID, thresholds: WorkerHealthThresholds
    ) -> InspectedWorkerSessionResource:
        try:
            async with self._sessions() as session:
                statement = _session_inspection_statement(
                    thresholds, reference_time=None
                ).where(worker_sessions.c.id == worker_session_id)
                row = (await session.execute(statement)).one_or_none()
                if row is None:
                    exists = await session.scalar(
                        select(worker_sessions.c.id).where(
                            worker_sessions.c.id == worker_session_id
                        )
                    )
                    if exists is not None:
                        raise WorkerInspectionInvariantViolation
                    raise WorkerInspectionNotFound
                return InspectedWorkerSessionResource(
                    _inspected_session(row),
                    WorkerInspectionObservation(row.reference_time, thresholds),
                )
        except (WorkerInspectionInvariantViolation, WorkerInspectionNotFound):
            raise
        except DBAPIError as error:
            raise WorkerInspectionPersistenceUnavailable from error

    async def list_sessions(
        self,
        *,
        worker_identity_id: UUID | None,
        health_status: WorkerSessionHealthStatus | None,
        thresholds: WorkerHealthThresholds,
        limit: int,
        cursor: WorkerSessionPageCursor | None,
    ) -> InspectedWorkerSessionPage:
        reference_time = cursor.reference_time if cursor is not None else None
        try:
            async with self._sessions() as session:
                statement = _session_inspection_statement(
                    thresholds, reference_time=reference_time
                )
                if worker_identity_id is not None:
                    statement = statement.where(
                        worker_sessions.c.worker_identity_id == worker_identity_id
                    )
                if health_status is not None:
                    statement = statement.where(
                        statement.selected_columns.health_status == health_status.value
                    )
                if cursor is not None:
                    statement = statement.where(
                        or_(
                            worker_session_health.c.last_seen_at > cursor.last_seen_at,
                            and_(
                                worker_session_health.c.last_seen_at
                                == cursor.last_seen_at,
                                worker_sessions.c.id > cursor.worker_session_id,
                            ),
                        )
                    )
                rows = (
                    await session.execute(
                        statement.order_by(
                            worker_session_health.c.last_seen_at,
                            worker_sessions.c.id,
                        ).limit(limit + 1)
                    )
                ).all()
                page_rows = rows[:limit]
                resolved_reference = (
                    rows[0].reference_time
                    if rows
                    else await _resolve_reference_time(session, reference_time)
                )
                next_cursor = None
                if len(rows) > limit:
                    last = page_rows[-1]
                    next_cursor = WorkerSessionPageCursor(
                        resolved_reference,
                        last.last_seen_at,
                        last.worker_session_id,
                        worker_identity_id,
                        health_status,
                        thresholds,
                    )
                return InspectedWorkerSessionPage(
                    tuple(_inspected_session(row) for row in page_rows),
                    WorkerInspectionObservation(resolved_reference, thresholds),
                    next_cursor,
                )
        except WorkerInspectionInvariantViolation:
            raise
        except DBAPIError as error:
            raise WorkerInspectionPersistenceUnavailable from error

    async def list_heartbeats(
        self,
        worker_session_id: UUID,
        *,
        before_sequence: int | None,
        limit: int,
    ) -> InspectedWorkerHeartbeatPage:
        try:
            async with self._sessions() as session:
                if not await session.scalar(
                    select(worker_sessions.c.id).where(
                        worker_sessions.c.id == worker_session_id
                    )
                ):
                    raise WorkerInspectionNotFound
                statement = select(worker_heartbeats).where(
                    worker_heartbeats.c.worker_session_id == worker_session_id
                )
                if before_sequence is not None:
                    statement = statement.where(
                        worker_heartbeats.c.sequence < before_sequence
                    )
                rows = (
                    await session.execute(
                        statement.order_by(worker_heartbeats.c.sequence.desc()).limit(
                            limit + 1
                        )
                    )
                ).all()
                page_rows = rows[:limit]
                next_sequence = page_rows[-1].sequence if len(rows) > limit else None
                return InspectedWorkerHeartbeatPage(
                    tuple(
                        InspectedWorkerHeartbeat(
                            row.sequence, row.received_at, row.accepting_work
                        )
                        for row in page_rows
                    ),
                    next_sequence,
                )
        except WorkerInspectionNotFound:
            raise
        except DBAPIError as error:
            raise WorkerInspectionPersistenceUnavailable from error


def _session_inspection_statement(
    thresholds: WorkerHealthThresholds,
    *,
    reference_time: datetime | None,
) -> Select[Any]:
    reference_expression = (
        func.statement_timestamp()
        if reference_time is None
        else cast(literal(reference_time), DateTime(timezone=True))
    )
    context = select(reference_expression.label("reference_time")).cte(
        "inspection_context"
    )
    status = case(
        (
            worker_sessions.c.ended_at.is_not(None),
            WorkerSessionHealthStatus.ENDED.value,
        ),
        (
            worker_session_health.c.last_seen_at
            > context.c.reference_time
            - func.make_interval(0, 0, 0, 0, 0, 0, thresholds.stale_after_seconds),
            WorkerSessionHealthStatus.HEALTHY.value,
        ),
        (
            worker_session_health.c.last_seen_at
            > context.c.reference_time
            - func.make_interval(0, 0, 0, 0, 0, 0, thresholds.offline_after_seconds),
            WorkerSessionHealthStatus.STALE.value,
        ),
        else_=WorkerSessionHealthStatus.OFFLINE.value,
    ).label("health_status")
    capabilities = (
        select(
            func.array_agg(
                aggregate_order_by(
                    worker_session_capabilities.c.capability,
                    worker_session_capabilities.c.capability,
                )
            )
        )
        .where(worker_session_capabilities.c.worker_session_id == worker_sessions.c.id)
        .scalar_subquery()
        .label("capabilities")
    )
    return select(
        worker_sessions.c.id.label("worker_session_id"),
        worker_sessions.c.registered_at,
        worker_sessions.c.ended_at,
        worker_identities.c.id.label("worker_identity_id"),
        worker_identities.c.name.label("worker_identity_name"),
        worker_identities.c.disabled_at,
        worker_session_health.c.last_sequence,
        worker_session_health.c.last_seen_at,
        worker_session_health.c.accepting_work,
        worker_session_health.c.availability_changed_at,
        capabilities,
        status,
        context.c.reference_time,
    ).select_from(
        worker_sessions.join(
            worker_identities,
            worker_identities.c.id == worker_sessions.c.worker_identity_id,
        )
        .join(
            worker_session_health,
            worker_session_health.c.worker_session_id == worker_sessions.c.id,
        )
        .join(context, literal(True))
    )


async def _resolve_reference_time(
    session: AsyncSession, reference_time: datetime | None
) -> datetime:
    if reference_time is not None:
        return reference_time
    resolved = (await session.execute(select(func.statement_timestamp()))).scalar_one()
    if not isinstance(resolved, datetime):
        raise WorkerInspectionInvariantViolation
    return resolved


def _inspected_session(row: Row[Any]) -> InspectedWorkerSession:
    return InspectedWorkerSession(
        row.worker_session_id,
        InspectedWorkerIdentity(
            row.worker_identity_id,
            row.worker_identity_name,
            row.disabled_at is None,
        ),
        row.registered_at,
        row.ended_at,
        tuple(row.capabilities or ()),
        InspectedWorkerHealth(
            WorkerSessionHealthStatus(row.health_status),
            row.last_sequence,
            row.last_seen_at,
            row.accepting_work,
            row.availability_changed_at,
        ),
    )
