"""API contract tests for dead-letter inspection and commands."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest

from taskforge.api.dead_letters import (
    _decode_item_cursor,
    _encode_item_cursor,
)
from taskforge.dead_letters.domain import (
    CreatedDeadLetterRedrive,
    DeadLetterActionPage,
    DeadLetterCursor,
    DeadLetterDetail,
    DeadLetterFilters,
    DeadLetterPage,
    DeadLetterReason,
    DeadLetterStatus,
    DeadLetterSummary,
    InvalidDeadLetterRedriveIdempotencyKey,
)
from taskforge.dead_letters.persistence_ports import DeadLetterTransitionConflict
from taskforge.dead_letters.service import DeadLetterNotFound
from taskforge.identity.authorization import OwnerFilter, Role
from taskforge.worker.results import TaskExecutionFailureKind, TaskExecutionResultKind
from tests.test_run_routes import Runtime, make_app, request


class DeadLetterServiceStub:
    def __init__(self, principal_id: UUID) -> None:
        self.calls: list[tuple[Any, ...]] = []
        self.error: Exception | None = None
        now = datetime.now(UTC)
        self.summary = DeadLetterSummary(
            uuid4(),
            uuid4(),
            uuid4(),
            uuid4(),
            DeadLetterReason.PERMANENT_FAILURE,
            DeadLetterStatus.OPEN,
            now,
            now,
            2,
        )
        self.detail = DeadLetterDetail(
            **self.summary.__dict__,
            workflow_definition_id=uuid4(),
            workflow_version_id=uuid4(),
            step_identifier="safe-step",
            result_kind=TaskExecutionResultKind.PERMANENT_FAILURE,
            failure_kind=TaskExecutionFailureKind.HANDLER_REPORTED,
            retry_decision_reason=None,
        )
        self.principal_id = principal_id
        self.redrive_result = CreatedDeadLetterRedrive(
            uuid4(),
            self.summary.id,
            self.summary.workflow_run_id,
            self.summary.task_run_id,
            self.summary.source_task_attempt_id,
            uuid4(),
            self.detail.workflow_definition_id,
            self.detail.workflow_version_id,
            principal_id,
            None,
            now,
        )

    def _raise(self) -> None:
        if self.error:
            raise self.error

    async def list_items(
        self,
        owner_filter: OwnerFilter,
        filters: DeadLetterFilters,
        *,
        limit: int,
        cursor: DeadLetterCursor | None,
    ) -> DeadLetterPage:
        self.calls.append(("list", owner_filter, filters, limit, cursor))
        self._raise()
        return DeadLetterPage((self.summary,), None)

    async def get_item(
        self, item_id: UUID, owner_filter: OwnerFilter
    ) -> DeadLetterDetail:
        self.calls.append(("get", item_id, owner_filter))
        self._raise()
        return self.detail

    async def list_actions(
        self, item_id: UUID, owner_filter: OwnerFilter, *, limit: int, cursor: object
    ) -> DeadLetterActionPage:
        self.calls.append(("actions", item_id, owner_filter, limit, cursor))
        self._raise()
        return DeadLetterActionPage((), None)

    async def acknowledge(
        self, item_id: UUID, owner_filter: OwnerFilter, **kwargs: Any
    ) -> DeadLetterDetail:
        self.calls.append(("ack", item_id, owner_filter, kwargs))
        self._raise()
        return self.detail

    async def resolve(
        self, item_id: UUID, owner_filter: OwnerFilter, **kwargs: Any
    ) -> DeadLetterDetail:
        self.calls.append(("resolve", item_id, owner_filter, kwargs))
        self._raise()
        return self.detail

    async def redrive(
        self, item_id: UUID, owner_filter: OwnerFilter, **kwargs: Any
    ) -> CreatedDeadLetterRedrive:
        self.calls.append(("redrive", item_id, owner_filter, kwargs))
        self._raise()
        return self.redrive_result


def dlq_app(roles: frozenset[str]) -> tuple[Any, Runtime, DeadLetterServiceStub]:
    app, runtime, _ = make_app(roles)
    service = DeadLetterServiceStub(runtime.principal_id)
    dynamic_runtime: Any = runtime
    dynamic_runtime.dead_letter_service = service
    return app, runtime, service


def test_list_uses_view_owner_scope_and_compact_shape() -> None:
    app, runtime, service = dlq_app(frozenset({Role.VIEWER.value}))
    response = request(
        app, "GET", "/api/v1/dead-letters?status=open&limit=7", runtime.api_credential
    )
    assert response.status_code == 200
    assert service.calls[0][1] == OwnerFilter.only(runtime.principal_id)
    assert service.calls[0][2].status is DeadLetterStatus.OPEN
    assert service.calls[0][3] == 7
    assert "result_kind" not in response.json()["items"][0]


def test_detail_is_richer_without_payload_fields() -> None:
    app, runtime, service = dlq_app(frozenset({Role.VIEWER.value}))
    response = request(
        app, "GET", f"/api/v1/dead-letters/{service.summary.id}", runtime.api_credential
    )
    assert response.status_code == 200
    assert response.json()["result_kind"] == "permanent_failure"
    assert not ({"output", "payload", "dispatch", "worker"} & set(response.json()))


def test_viewer_cannot_mutate_and_operator_correlation_is_request_id() -> None:
    app, runtime, service = dlq_app(frozenset({Role.VIEWER.value}))
    denied = request(
        app,
        "POST",
        f"/api/v1/dead-letters/{service.summary.id}/acknowledge",
        runtime.api_credential,
        json={},
    )
    assert denied.status_code == 403
    assert not service.calls

    app, runtime, service = dlq_app(frozenset({Role.WORKFLOW_OPERATOR.value}))
    accepted = request(
        app,
        "POST",
        f"/api/v1/dead-letters/{service.summary.id}/resolve",
        runtime.api_credential,
        json={"reason": "handled"},
    )
    assert accepted.status_code == 200
    assert service.calls[0][2] == OwnerFilter.only(runtime.principal_id)
    assert (
        str(service.calls[0][3]["correlation_id"]) == accepted.headers["X-Request-ID"]
    )


@pytest.mark.parametrize("body", ({"reason": "   "}, {"reason": "x" * 2001}, {}))
def test_resolution_reason_validation(body: dict[str, str]) -> None:
    app, runtime, service = dlq_app(frozenset({Role.WORKFLOW_OPERATOR.value}))
    response = request(
        app,
        "POST",
        f"/api/v1/dead-letters/{service.summary.id}/resolve",
        runtime.api_credential,
        json=body,
    )
    assert response.status_code == 422
    assert not service.calls


@pytest.mark.parametrize(
    "error,status_code",
    ((DeadLetterNotFound(), 404), (DeadLetterTransitionConflict(), 409)),
)
def test_command_maps_expected_outcomes(error: Exception, status_code: int) -> None:
    app, runtime, service = dlq_app(frozenset({Role.WORKFLOW_OPERATOR.value}))
    service.error = error
    response = request(
        app,
        "POST",
        f"/api/v1/dead-letters/{service.summary.id}/acknowledge",
        runtime.api_credential,
        json={},
    )
    assert response.status_code == status_code


def test_admin_list_uses_unrestricted_scope() -> None:
    app, runtime, service = dlq_app(frozenset({Role.ADMINISTRATOR.value}))
    response = request(app, "GET", "/api/v1/dead-letters", runtime.api_credential)
    assert response.status_code == 200
    assert service.calls[0][1] == OwnerFilter.all_owners()


def test_redrive_requires_operator_and_idempotency_key() -> None:
    app, runtime, service = dlq_app(frozenset({Role.VIEWER.value}))
    denied = request(
        app,
        "POST",
        f"/api/v1/dead-letters/{service.summary.id}/redrive",
        runtime.api_credential,
        json={},
        headers={"Idempotency-Key": "abcdefghijklmnop"},
    )
    assert denied.status_code == 403
    assert not service.calls

    app, runtime, service = dlq_app(frozenset({Role.WORKFLOW_OPERATOR.value}))
    service.error = InvalidDeadLetterRedriveIdempotencyKey()
    missing = request(
        app,
        "POST",
        f"/api/v1/dead-letters/{service.summary.id}/redrive",
        runtime.api_credential,
        json={},
    )
    assert missing.status_code == 422
    assert service.calls[0][3]["idempotency_key"] is None


def test_redrive_returns_lineage_location_and_owner_scope() -> None:
    app, runtime, service = dlq_app(frozenset({Role.WORKFLOW_OPERATOR.value}))
    response = request(
        app,
        "POST",
        f"/api/v1/dead-letters/{service.summary.id}/redrive",
        runtime.api_credential,
        json={"reason": "  corrected configuration  "},
        headers={"Idempotency-Key": "abcdefghijklmnop"},
    )
    assert response.status_code == 201
    assert response.headers["Location"].endswith(
        str(service.redrive_result.target_workflow_run_id)
    )
    call = service.calls[0]
    assert call[2] == OwnerFilter.only(runtime.principal_id)
    assert call[3]["reason"] == "corrected configuration"
    assert call[3]["idempotency_key"] == "abcdefghijklmnop"
    assert "idempotency" not in response.text


def test_admin_redrive_uses_unrestricted_scope() -> None:
    app, runtime, service = dlq_app(frozenset({Role.ADMINISTRATOR.value}))
    response = request(
        app,
        "POST",
        f"/api/v1/dead-letters/{service.summary.id}/redrive",
        runtime.api_credential,
        json={},
        headers={"Idempotency-Key": "abcdefghijklmnop"},
    )
    assert response.status_code == 201
    assert service.calls[0][2] == OwnerFilter.all_owners()


def test_cursor_binds_filters_but_not_principal() -> None:
    filters = DeadLetterFilters(status=DeadLetterStatus.OPEN, task_run_id=uuid4())
    cursor = DeadLetterCursor(datetime.now(UTC), uuid4())
    encoded = _encode_item_cursor(cursor, filters)
    assert _decode_item_cursor(encoded, filters) == cursor
    with pytest.raises(ValueError):
        _decode_item_cursor(
            encoded, DeadLetterFilters(status=DeadLetterStatus.RESOLVED)
        )
    assert "principal" not in encoded


def test_malformed_cursor_is_validation_error() -> None:
    app, runtime, service = dlq_app(frozenset({Role.VIEWER.value}))
    response = request(
        app, "GET", "/api/v1/dead-letters?cursor=not-base64!", runtime.api_credential
    )
    assert response.status_code == 422
    assert not service.calls


def test_invalid_date_range_is_a_cross_field_query_error() -> None:
    app, runtime, service = dlq_app(frozenset({Role.VIEWER.value}))
    response = request(
        app,
        "GET",
        "/api/v1/dead-letters?"
        "created_after=2026-08-20T12:00:00Z&"
        "created_before=2026-08-20T11:00:00Z",
        runtime.api_credential,
    )

    assert response.status_code == 422
    assert response.json()["error"]["details"] == [
        {
            "code": "invalid_date_range",
            "path": ["query"],
            "message": "created_after must precede created_before.",
        }
    ]
    assert not service.calls
