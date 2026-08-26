"""Resource-scoped SQL history retrieval; authorization is applied in SQL."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskforge.audit.domain import canonical_audit_action, stored_audit_actions
from taskforge.history.domain import (
    HistoryCursor,
    HistoryFilters,
    HistoryItem,
    HistoryPage,
    HistoryRecordType,
)
from taskforge.history.export import ExportInitialization
from taskforge.history.service import HistoryNotFound
from taskforge.identity.authorization import OwnerFilter


class SQLAlchemyHistoryRepository:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def list_history(
        self,
        scope_type: str,
        scope_id: UUID | None,
        owner_filter: OwnerFilter,
        *,
        limit: int,
        cursor: HistoryCursor | None,
        filters: HistoryFilters,
    ) -> HistoryPage:
        async with self._sessions() as session:
            owner_id = owner_filter.principal_id
            if scope_type != "audit":
                exists = await session.scalar(
                    text(_AUTH_SQL[scope_type]),
                    {
                        "id": scope_id,
                        "owner": owner_id,
                        "unrestricted": owner_filter.unrestricted,
                    },
                )
                if exists is None:
                    raise HistoryNotFound
            sql = _SCOPE_SQL[scope_type] + _filter_sql(filters)
            params: dict[str, Any] = {
                "id": scope_id,
                "limit": limit + 1,
                "cursor_time": cursor.occurred_at if cursor else None,
                "cursor_rank": cursor.source_rank if cursor else None,
                "cursor_key": cursor.source_key if cursor else None,
                "record_type": filters.record_type.value
                if filters.record_type
                else None,
                "resource_type": filters.resource_type,
                "resource_id": filters.resource_id,
                "actions": list(stored_audit_actions(filters.action))
                if filters.action
                else None,
                "outcome": filters.outcome.value if filters.outcome else None,
                "actor_kind": filters.actor_kind.value if filters.actor_kind else None,
                "actor_id": filters.actor_id,
                "system_component": filters.system_component,
                "correlation_id": filters.correlation_id,
                "reason_code": filters.reason_code,
                "occurred_from": filters.occurred_from,
                "occurred_to": filters.occurred_to,
            }
            rows = (await session.execute(text(sql), params)).mappings().all()
        items = tuple(_item(row) for row in rows[:limit])
        next_cursor = None
        if len(rows) > limit and items:
            last = items[-1]
            next_cursor = HistoryCursor(
                scope_type,
                scope_id,
                "",
                last.occurred_at,
                last.record_type,
                last.source_rank,
                last.source_key,
            )
        return HistoryPage(items, next_cursor)

    async def initialize_export(
        self,
        scope_type: str,
        scope_id: UUID | None,
        owner_filter: OwnerFilter,
        filters: HistoryFilters,
    ) -> ExportInitialization:
        async with self._sessions() as session:
            await _authorize(session, scope_type, scope_id, owner_filter)
            generated_at = await session.scalar(select(func.statement_timestamp()))
            rows = (
                (
                    await session.execute(
                        text(_SCOPE_SQL[scope_type] + _filter_sql(filters)),
                        _query_params(
                            scope_id,
                            limit=1,
                            cursor=None,
                            filters=filters,
                        ),
                    )
                )
                .mappings()
                .all()
            )
        if generated_at is None:
            raise RuntimeError("PostgreSQL did not return export initialization time")
        high_water = None
        if rows:
            item = _item(rows[0])
            high_water = HistoryCursor(
                scope_type,
                scope_id,
                "",
                item.occurred_at,
                item.record_type,
                item.source_rank,
                item.source_key,
            )
        return ExportInitialization(generated_at, high_water)

    async def list_export_page(
        self,
        scope_type: str,
        scope_id: UUID | None,
        owner_filter: OwnerFilter,
        *,
        limit: int,
        after: HistoryCursor | None,
        high_water: HistoryCursor,
        current_export_audit_id: UUID,
        filters: HistoryFilters,
    ) -> tuple[HistoryItem, ...]:
        async with self._sessions() as session:
            await _authorize(session, scope_type, scope_id, owner_filter)
            params = _query_params(
                scope_id,
                limit=limit,
                cursor=after,
                filters=filters,
            )
            params.update(
                {
                    "high_water_time": high_water.occurred_at,
                    "high_water_rank": high_water.source_rank,
                    "high_water_key": high_water.source_key,
                    "current_export_audit_id": str(current_export_audit_id),
                }
            )
            rows = (
                (
                    await session.execute(
                        text(_EXPORT_SCOPE_SQL[scope_type] + _filter_sql(filters)),
                        params,
                    )
                )
                .mappings()
                .all()
            )
        return tuple(_item(row) for row in rows)


async def _authorize(
    session: AsyncSession,
    scope_type: str,
    scope_id: UUID | None,
    owner_filter: OwnerFilter,
) -> None:
    if scope_type == "audit":
        return
    exists = await session.scalar(
        text(_AUTH_SQL[scope_type]),
        {
            "id": scope_id,
            "owner": owner_filter.principal_id,
            "unrestricted": owner_filter.unrestricted,
        },
    )
    if exists is None:
        raise HistoryNotFound


def _query_params(
    scope_id: UUID | None,
    *,
    limit: int,
    cursor: HistoryCursor | None,
    filters: HistoryFilters,
) -> dict[str, Any]:
    return {
        "id": scope_id,
        "limit": limit,
        "cursor_time": cursor.occurred_at if cursor else None,
        "cursor_rank": cursor.source_rank if cursor else None,
        "cursor_key": cursor.source_key if cursor else None,
        "record_type": filters.record_type.value if filters.record_type else None,
        "resource_type": filters.resource_type,
        "resource_id": filters.resource_id,
        "actions": list(stored_audit_actions(filters.action))
        if filters.action
        else None,
        "outcome": filters.outcome.value if filters.outcome else None,
        "actor_kind": filters.actor_kind.value if filters.actor_kind else None,
        "actor_id": filters.actor_id,
        "system_component": filters.system_component,
        "correlation_id": filters.correlation_id,
        "reason_code": filters.reason_code,
        "occurred_from": filters.occurred_from,
        "occurred_to": filters.occurred_to,
    }


def _item(row: Any) -> HistoryItem:
    record_type = HistoryRecordType(row["record_type"])
    data = dict(row["data"])
    if record_type is HistoryRecordType.AUDIT_RECORD:
        data["action"] = canonical_audit_action(data["action"]).value
    return HistoryItem(
        record_type,
        row["occurred_at"],
        row["source_rank"],
        row["source_key"],
        row["correlation_id"],
        data,
    )


_AUTH_SQL = {
    "workflow": "SELECT w.id FROM workflow_definitions w WHERE w.id=:id AND (:unrestricted OR w.owner_principal_id=:owner)",
    "run": "SELECT r.id FROM workflow_runs r JOIN workflow_definitions w ON w.id=r.workflow_definition_id WHERE r.id=:id AND (:unrestricted OR w.owner_principal_id=:owner)",
    "task": "SELECT t.id FROM task_runs t JOIN workflow_runs r ON r.id=t.workflow_run_id JOIN workflow_definitions w ON w.id=r.workflow_definition_id WHERE t.id=:id AND (:unrestricted OR w.owner_principal_id=:owner)",
    "worker": "SELECT id FROM worker_identities WHERE id=:id",
    "dead_letter": "SELECT d.id FROM dead_letter_items d JOIN task_runs t ON t.id=d.task_run_id JOIN workflow_runs r ON r.id=t.workflow_run_id JOIN workflow_definitions w ON w.id=r.workflow_definition_id WHERE d.id=:id AND (:unrestricted OR w.owner_principal_id=:owner)",
}

_FILTER_COLUMNS = ",NULL::text resource_type,NULL::uuid resource_id,NULL::text audit_action,NULL::text audit_outcome,NULL::text actor_kind,NULL::uuid api_principal_id,NULL::uuid worker_identity_id,NULL::text system_component,NULL::text reason_code"
_WORKER_FILTER_COLUMNS = ",NULL::text resource_type,NULL::uuid resource_id,NULL::text audit_action,NULL::text audit_outcome,'worker'::text actor_kind,NULL::uuid api_principal_id,{identity} worker_identity_id,NULL::text system_component,NULL::text reason_code"
_API_FILTER_COLUMNS = ",NULL::text resource_type,NULL::uuid resource_id,NULL::text audit_action,NULL::text audit_outcome,'api_principal'::text actor_kind,{principal} api_principal_id,NULL::uuid worker_identity_id,NULL::text system_component,NULL::text reason_code"
_BOUNDARY = "AND (CAST(:cursor_time AS timestamptz) IS NULL OR (occurred_at, source_rank, source_key) < (CAST(:cursor_time AS timestamptz), CAST(:cursor_rank AS integer), CAST(:cursor_key AS text)))"
_HIGH_WATER_BOUNDARY = "AND (occurred_at, source_rank, source_key) <= (CAST(:high_water_time AS timestamptz), CAST(:high_water_rank AS integer), CAST(:high_water_key AS text))"
_CURRENT_EXPORT_AUDIT_EXCLUSION = "AND (record_type <> 'audit_record' OR source_key <> CAST(:current_export_audit_id AS text))"
_ORDER = " ORDER BY occurred_at DESC, source_rank DESC, source_key DESC LIMIT :limit"
_AUDIT_SELECT = "SELECT 'audit_record' record_type,10 source_rank,a.id::text source_key,a.occurred_at,a.correlation_id,jsonb_build_object('id',a.id,'actor_kind',a.actor_kind,'api_principal_id',a.api_principal_id,'worker_identity_id',a.worker_identity_id,'worker_session_id',a.worker_session_id,'system_component',a.system_component,'action',a.action,'outcome',a.outcome,'reason_code',a.reason_code,'resource_type',a.resource_type,'resource_id',a.resource_id,'diagnostic_provenance',a.diagnostic_provenance) data,a.resource_type,a.resource_id,a.action audit_action,a.outcome audit_outcome,a.actor_kind,a.api_principal_id,a.worker_identity_id,a.system_component,a.reason_code FROM audit_records a"
_EXEC = (
    "SELECT 'execution_event' record_type,110 source_rank,e.id::text source_key,e.occurred_at,NULL::text correlation_id,jsonb_build_object('id',e.id,'workflow_run_id',e.workflow_run_id,'task_run_id',e.task_run_id,'cursor',e.cursor,'event_type',e.event_type,'payload',e.payload) data"
    + _FILTER_COLUMNS
    + " FROM workflow_run_execution_events e"
)
_CLAIM = (
    "SELECT 'claim_event' record_type,100 source_rank,c.id::text source_key,c.occurred_at,c.correlation_id,jsonb_build_object('id',c.id,'task_attempt_id',c.task_attempt_id,'generation',c.generation,'worker_identity_id',c.worker_identity_id,'worker_session_id',c.worker_session_id,'event_type',c.event_type,'previous_lease_expires_at',c.previous_lease_expires_at,'lease_expires_at',c.lease_expires_at) data"
    + ",NULL::text resource_type,NULL::uuid resource_id,NULL::text audit_action,NULL::text audit_outcome,CASE WHEN c.worker_identity_id IS NOT NULL THEN 'worker' END actor_kind,NULL::uuid api_principal_id,c.worker_identity_id,NULL::text system_component,NULL::text reason_code"
    + " FROM task_claim_events c JOIN task_attempts ta ON ta.id=c.task_attempt_id"
)
_RESULT = (
    "SELECT 'result_event' record_type,90 source_rank,x.id::text source_key,x.occurred_at,x.correlation_id,jsonb_build_object('id',x.id,'task_attempt_id',x.task_attempt_id,'claim_generation',x.claim_generation,'worker_identity_id',x.worker_identity_id,'worker_session_id',x.worker_session_id,'actor_component',x.actor_component,'event_type',x.event_type,'result_kind',x.result_kind,'failure_kind',x.failure_kind) data"
    + ",NULL::text resource_type,NULL::uuid resource_id,NULL::text audit_action,NULL::text audit_outcome,CASE WHEN x.worker_identity_id IS NOT NULL THEN 'worker' WHEN x.actor_component IS NOT NULL THEN 'system' END actor_kind,NULL::uuid api_principal_id,x.worker_identity_id,x.actor_component system_component,NULL::text reason_code"
    + " FROM task_result_events x JOIN task_attempts ta ON ta.id=x.task_attempt_id"
)
_RETRY = (
    "SELECT 'retry_event' record_type,80 source_rank,x.id::text source_key,x.occurred_at,x.correlation_id,jsonb_build_object('id',x.id,'task_run_id',x.task_run_id,'event_type',x.event_type,'actor_component',x.actor_component,'failed_attempt_number',x.failed_attempt_number,'retry_attempt_number',x.retry_attempt_number,'next_eligible_at',x.next_eligible_at,'decision_reason',x.decision_reason) data"
    + ",NULL::text resource_type,NULL::uuid resource_id,NULL::text audit_action,NULL::text audit_outcome,CASE WHEN x.actor_component IS NOT NULL THEN 'system' END actor_kind,NULL::uuid api_principal_id,NULL::uuid worker_identity_id,x.actor_component system_component,NULL::text reason_code"
    + " FROM task_retry_events x"
)
_CANCEL = (
    "SELECT 'cancellation_requested' record_type,70 source_rank,c.workflow_run_id::text source_key,c.requested_at occurred_at,c.correlation_id::text,jsonb_build_object('workflow_run_id',c.workflow_run_id,'requested_by_principal_id',c.requested_by_principal_id,'reason_present',c.reason IS NOT NULL) data"
    + _API_FILTER_COLUMNS.format(principal="c.requested_by_principal_id")
    + " FROM workflow_run_cancellation_requests c"
)
_REPLAY = (
    "SELECT 'replay_created' record_type,60 source_rank,p.workflow_run_id::text source_key,p.created_at occurred_at,NULL::text correlation_id,jsonb_build_object('workflow_run_id',p.workflow_run_id,'source_workflow_run_id',p.source_workflow_run_id,'mode',p.mode,'requested_scope',p.requested_scope) data"
    + _FILTER_COLUMNS
    + " FROM workflow_run_replays p"
)
_DL = (
    "SELECT 'dead_letter_created' record_type,50 source_rank,d.id::text source_key,d.created_at occurred_at,NULL::text correlation_id,jsonb_build_object('id',d.id,'task_run_id',d.task_run_id,'source_task_attempt_id',d.source_task_attempt_id,'reason',d.reason) data"
    + _FILTER_COLUMNS
    + " FROM dead_letter_items d"
)
_DLA = (
    "SELECT 'dead_letter_action' record_type,40 source_rank,x.id::text source_key,x.occurred_at,x.correlation_id::text,jsonb_build_object('id',x.id,'dead_letter_item_id',x.dead_letter_item_id,'operator_principal_id',x.operator_principal_id,'action_type',x.action_type,'previous_status',x.previous_status,'new_status',x.new_status,'reason_present',x.reason IS NOT NULL) data"
    + _API_FILTER_COLUMNS.format(principal="x.operator_principal_id")
    + " FROM dead_letter_operator_actions x"
)
_DLR = (
    "SELECT 'dead_letter_redrive' record_type,30 source_rank,x.id::text source_key,x.requested_at occurred_at,x.correlation_id::text,jsonb_build_object('id',x.id,'dead_letter_item_id',x.dead_letter_item_id,'requested_by_principal_id',x.requested_by_principal_id,'target_workflow_run_id',x.target_workflow_run_id,'reason_present',x.reason IS NOT NULL) data"
    + _API_FILTER_COLUMNS.format(principal="x.requested_by_principal_id")
    + " FROM dead_letter_redrive_requests x"
)
_HEART = (
    "SELECT 'heartbeat' record_type,20 source_rank,h.worker_session_id::text||':'||lpad(h.sequence::text,20,'0') source_key,h.received_at occurred_at,h.correlation_id,jsonb_build_object('worker_session_id',h.worker_session_id,'worker_identity_id',h.worker_identity_id,'sequence',h.sequence,'accepting_work',h.accepting_work) data"
    + _WORKER_FILTER_COLUMNS.format(identity="h.worker_identity_id")
    + " FROM worker_heartbeats h JOIN worker_sessions s ON s.id=h.worker_session_id"
)


def _union(parts: list[str]) -> str:
    return (
        "SELECT * FROM ("
        + " UNION ALL ".join(parts)
        + ") history WHERE true "
        + _BOUNDARY
    )


def _filter_sql(filters: HistoryFilters) -> str:
    clauses: list[str] = []
    if filters.record_type:
        clauses.append("record_type=:record_type")
    if filters.resource_type:
        clauses.append("resource_type=:resource_type")
    if filters.resource_id:
        clauses.append("resource_id=:resource_id")
    if filters.action:
        clauses.append("audit_action=ANY(CAST(:actions AS text[]))")
    if filters.outcome:
        clauses.append("audit_outcome=:outcome")
    if filters.actor_kind:
        clauses.append("actor_kind=:actor_kind")
    if filters.actor_id:
        if filters.actor_kind is None:
            raise ValueError("actor_id requires actor_kind")
        if filters.actor_kind.value == "api_principal":
            clauses.append("api_principal_id=:actor_id")
        else:
            clauses.append("api_principal_id IS NULL AND worker_identity_id=:actor_id")
    if filters.system_component:
        clauses.append(
            "api_principal_id IS NULL AND worker_identity_id IS NULL AND system_component=:system_component"
        )
    if filters.correlation_id:
        clauses.append("correlation_id=:correlation_id")
    if filters.reason_code:
        clauses.append("reason_code=:reason_code")
    if filters.occurred_from:
        clauses.append("occurred_at>=:occurred_from")
    if filters.occurred_to:
        clauses.append("occurred_at<:occurred_to")
    suffix = "".join(f" AND {clause}" for clause in clauses)
    return suffix + _ORDER


_SCOPE_SQL = {
    "audit": _union([_AUDIT_SELECT]),
    "workflow": _union(
        [_AUDIT_SELECT + " WHERE a.resource_type='workflow' AND a.resource_id=:id"]
    ),
    "run": _union(
        [
            _EXEC + " WHERE e.workflow_run_id=:id",
            _CLAIM
            + " JOIN task_runs t ON t.id=ta.task_run_id WHERE t.workflow_run_id=:id",
            _RESULT
            + " JOIN task_runs t ON t.id=ta.task_run_id WHERE t.workflow_run_id=:id",
            _RETRY
            + " JOIN task_runs t ON t.id=x.task_run_id WHERE t.workflow_run_id=:id",
            _CANCEL + " WHERE c.workflow_run_id=:id",
            _REPLAY + " WHERE p.workflow_run_id=:id OR p.source_workflow_run_id=:id",
            _DL + " JOIN task_runs t ON t.id=d.task_run_id WHERE t.workflow_run_id=:id",
            _DLA
            + " JOIN dead_letter_items d ON d.id=x.dead_letter_item_id JOIN task_runs t ON t.id=d.task_run_id WHERE t.workflow_run_id=:id",
            _DLR
            + " JOIN dead_letter_items d ON d.id=x.dead_letter_item_id JOIN task_runs t ON t.id=d.task_run_id WHERE t.workflow_run_id=:id",
            _AUDIT_SELECT
            + " WHERE (a.resource_type='workflow_run' AND a.resource_id=:id) OR (a.resource_type='task_run' AND EXISTS (SELECT 1 FROM task_runs tt WHERE tt.id=a.resource_id AND tt.workflow_run_id=:id)) OR (a.resource_type='task_attempt' AND EXISTS (SELECT 1 FROM task_attempts aa JOIN task_runs tt ON tt.id=aa.task_run_id WHERE aa.id=a.resource_id AND tt.workflow_run_id=:id)) OR (a.resource_type='dead_letter' AND EXISTS (SELECT 1 FROM dead_letter_items dd JOIN task_runs tt ON tt.id=dd.task_run_id WHERE dd.id=a.resource_id AND tt.workflow_run_id=:id))",
        ]
    ),
    "task": _union(
        [
            _EXEC + " WHERE e.task_run_id=:id",
            _CLAIM + " WHERE ta.task_run_id=:id",
            _RESULT + " WHERE ta.task_run_id=:id",
            _RETRY + " WHERE x.task_run_id=:id",
            _DL + " WHERE d.task_run_id=:id",
            _DLA
            + " JOIN dead_letter_items d ON d.id=x.dead_letter_item_id WHERE d.task_run_id=:id",
            _DLR
            + " JOIN dead_letter_items d ON d.id=x.dead_letter_item_id WHERE d.task_run_id=:id",
            _AUDIT_SELECT
            + " WHERE (a.resource_type='task_run' AND a.resource_id=:id) OR (a.resource_type='task_attempt' AND EXISTS (SELECT 1 FROM task_attempts aa WHERE aa.id=a.resource_id AND aa.task_run_id=:id)) OR (a.resource_type='dead_letter' AND EXISTS (SELECT 1 FROM dead_letter_items dd WHERE dd.id=a.resource_id AND dd.task_run_id=:id))",
        ]
    ),
    "worker": _union(
        [
            _HEART + " WHERE coalesce(h.worker_identity_id,s.worker_identity_id)=:id",
            _AUDIT_SELECT
            + " WHERE a.worker_identity_id=:id OR (a.resource_type='worker_session' AND EXISTS (SELECT 1 FROM worker_sessions ws WHERE ws.id=a.resource_id AND ws.worker_identity_id=:id))",
        ]
    ),
    "dead_letter": _union(
        [
            _DL + " WHERE d.id=:id",
            _DLA + " WHERE x.dead_letter_item_id=:id",
            _DLR + " WHERE x.dead_letter_item_id=:id",
            _AUDIT_SELECT
            + " WHERE a.resource_type='dead_letter' AND a.resource_id=:id",
        ]
    ),
}

_EXPORT_SCOPE_SQL = {
    scope: sql.replace(
        _BOUNDARY,
        _BOUNDARY + _HIGH_WATER_BOUNDARY + _CURRENT_EXPORT_AUDIT_EXCLUSION,
    )
    for scope, sql in _SCOPE_SQL.items()
    if scope in {"audit", "run"}
}
