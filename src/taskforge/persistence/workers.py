"""Atomic PostgreSQL persistence for authenticated worker registration."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy import func, insert, or_, select
from sqlalchemy.engine import Row
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.identity.schema import worker_credentials, worker_identities
from taskforge.worker.domain import (
    RegisteredWorkerSession,
    WorkerHealthProjection,
    WorkerHeartbeat,
    WorkerRegistration,
)
from taskforge.worker.persistence_ports import (
    WorkerHeartbeatAuthorityRejected,
    WorkerHeartbeatInvariantViolation,
    WorkerHeartbeatPersistenceUnavailable,
    WorkerHeartbeatReplayConflict,
    WorkerHeartbeatSequenceGap,
    WorkerHeartbeatSessionInactive,
    WorkerHeartbeatSessionUnavailable,
    WorkerHeartbeatStale,
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
