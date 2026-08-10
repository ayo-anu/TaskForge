"""Atomic PostgreSQL persistence for authenticated worker registration."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import func, insert, or_, select
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.identity.schema import worker_credentials, worker_identities
from taskforge.worker.domain import RegisteredWorkerSession, WorkerRegistration
from taskforge.worker.persistence_ports import (
    WorkerRegistrationAuthorityRejected,
    WorkerRegistrationPersistenceUnavailable,
    WorkerRegistrationRecordConflict,
)
from taskforge.worker.schema import (
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
