"""Append-only audit persistence and service-owned rejected UoW."""

from typing import Protocol

from sqlalchemy import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.audit.domain import AuditRecord, AuditRejected
from taskforge.audit.schema import audit_records


class RejectedAuditRecorder(Protocol):
    """Service-owned boundary for one independent rejected-command audit."""

    async def record(self, record: AuditRecord) -> None: ...


async def append_audit_record(session: AsyncSession, record: AuditRecord) -> None:
    await session.execute(
        insert(audit_records).values(
            id=record.id,
            actor_kind=record.actor.kind.value,
            api_principal_id=record.actor.api_principal_id,
            worker_identity_id=record.actor.worker_identity_id,
            worker_session_id=record.actor.worker_session_id,
            system_component=record.actor.system_component,
            action=record.action,
            outcome=record.outcome.value,
            reason_code=record.reason_code,
            resource_type=record.resource_type,
            resource_id=record.resource_id,
            correlation_id=record.correlation_id,
            diagnostic_provenance=dict(record.provenance),
        )
    )


class RejectedAuditUnitOfWork:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def record(self, record: AuditRecord) -> None:
        try:
            async with self._sessions.begin() as session:
                await append_audit_record(session, record)
        except SQLAlchemyError as error:
            raise AuditRejected from error
