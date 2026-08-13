"""Authorized workflow draft creation and owner-scoped detail routes."""

from __future__ import annotations

import base64
import binascii
import json
from datetime import UTC, datetime
from typing import Annotated, Any, Protocol, cast
from uuid import UUID, uuid4

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Path,
    Query,
    Request,
    Response,
    status,
)
from pydantic import BaseModel, ConfigDict, Field

from taskforge.api.authorization import require_permission
from taskforge.api.errors import (
    ERROR_CONTRACTS,
    ErrorDetail,
    ErrorResponse,
    error_response,
)
from taskforge.identity.authorization import AuthorizationContext, Permission
from taskforge.workflows.dag_validation import (
    DAGEdge,
    DAGValidationIssue,
    DAGViolationCode,
    validate_dag,
)
from taskforge.workflows.domain import (
    DraftDependency,
    DraftWorkflowStep,
    PublishedWorkflowVersion,
    WorkflowDefinitionStatus,
    WorkflowVersionSnapshot,
    _create_workflow_draft_with_validation,
    create_draft_dependency,
    create_draft_step,
    create_workflow_draft,
)
from taskforge.workflows.persistence_ports import (
    StoredWorkflowDraft,
    WorkflowPageCursor,
    WorkflowSummary,
    WorkflowVersionPageCursor,
    WorkflowVersionSummary,
)
from taskforge.workflows.service import (
    WorkflowNotFound,
    WorkflowOwnerDisabled,
    WorkflowOwnerNotFound,
    WorkflowPersistenceConflict,
    WorkflowService,
    WorkflowServiceUnavailable,
)
from taskforge.workflows.task_types import (
    JSONMapping,
    JSONValue,
    TaskTypeRegistry,
    WorkflowValidationError,
    WorkflowValidationIssue,
)


class CreateWorkflowStepRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    identifier: str
    task_type: str
    parameters: dict[str, JSONValue]
    execution_policy: dict[str, JSONValue] | None = None


class CreateWorkflowDependencyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    predecessor: str
    successor: str


class CreateWorkflowRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    description: str | None = None
    execution_policy: dict[str, JSONValue] | None = None
    steps: list[CreateWorkflowStepRequest]
    dependencies: list[CreateWorkflowDependencyRequest] = Field(default_factory=list)


class WorkflowStepResponse(BaseModel):
    id: UUID
    identifier: str
    task_type: str
    parameters: JSONMapping
    execution_policy: JSONMapping | None


class WorkflowDependencyResponse(BaseModel):
    id: UUID
    predecessor: str
    successor: str


class WorkflowResponse(BaseModel):
    id: UUID
    owner_principal_id: UUID
    name: str
    description: str | None
    status: WorkflowDefinitionStatus
    execution_policy: JSONMapping | None
    created_at: datetime
    updated_at: datetime
    steps: list[WorkflowStepResponse]
    dependencies: list[WorkflowDependencyResponse]


class WorkflowSummaryResponse(BaseModel):
    id: UUID
    name: str
    description: str | None
    status: WorkflowDefinitionStatus
    created_at: datetime
    updated_at: datetime


class WorkflowPageMetadataResponse(BaseModel):
    limit: int
    next_cursor: str | None


class WorkflowListResponse(BaseModel):
    items: list[WorkflowSummaryResponse]
    page: WorkflowPageMetadataResponse


class WorkflowValidationResponse(BaseModel):
    valid: bool
    topological_order: list[str]


class PublishedWorkflowVersionResponse(BaseModel):
    id: UUID
    workflow_definition_id: UUID
    version_number: int
    published_at: datetime


class WorkflowVersionSummaryResponse(BaseModel):
    id: UUID
    version_number: int
    published_at: datetime


class WorkflowVersionListResponse(BaseModel):
    items: list[WorkflowVersionSummaryResponse]
    page: WorkflowPageMetadataResponse


class WorkflowVersionStepResponse(BaseModel):
    identifier: str
    task_type: str
    parameters: JSONMapping
    execution_policy: JSONMapping | None


class WorkflowVersionDependencyResponse(BaseModel):
    predecessor: str
    successor: str


class WorkflowVersionResponse(BaseModel):
    id: UUID
    workflow_definition_id: UUID
    version_number: int
    name: str
    description: str | None
    execution_policy: JSONMapping | None
    published_at: datetime
    steps: list[WorkflowVersionStepResponse]
    dependencies: list[WorkflowVersionDependencyResponse]


class WorkflowRuntimeProtocol(Protocol):
    workflow_service: WorkflowService
    task_type_registry: TaskTypeRegistry


router = APIRouter(prefix="/api/v1/workflows", tags=["workflows"])

COMMON_RESPONSES: dict[int | str, dict[str, Any]] = {
    status.HTTP_401_UNAUTHORIZED: {"model": ErrorResponse},
    status.HTTP_403_FORBIDDEN: {"model": ErrorResponse},
    status.HTTP_422_UNPROCESSABLE_CONTENT: {"model": ErrorResponse},
    status.HTTP_503_SERVICE_UNAVAILABLE: {"model": ErrorResponse},
}

DEFAULT_WORKFLOW_PAGE_SIZE = 50
MAX_WORKFLOW_PAGE_SIZE = 100
MAX_CURSOR_LENGTH = 256
MAX_DECODED_CURSOR_BYTES = 128
_CURSOR_VERSION = 1


@router.get(
    "",
    response_model=WorkflowListResponse,
    responses=COMMON_RESPONSES,
)
async def list_workflows(
    request: Request,
    context: Annotated[
        AuthorizationContext,
        Depends(require_permission(Permission.VIEW)),
    ],
    limit: Annotated[
        int,
        Query(ge=1, le=MAX_WORKFLOW_PAGE_SIZE),
    ] = DEFAULT_WORKFLOW_PAGE_SIZE,
    cursor: Annotated[
        str | None,
        Query(max_length=MAX_CURSOR_LENGTH),
    ] = None,
) -> WorkflowListResponse | Response:
    """List the authenticated owner's workflows in stable keyset order."""
    try:
        decoded_cursor = _decode_cursor(cursor) if cursor is not None else None
    except ValueError:
        return _validation_error(
            request,
            (
                WorkflowValidationIssue(
                    "invalid_cursor",
                    ("query", "cursor"),
                    "Cursor is invalid.",
                ),
            ),
        )
    try:
        page = await _runtime(request).workflow_service.list(
            owner_principal_id=context.principal_id,
            limit=limit,
            cursor=decoded_cursor,
        )
    except WorkflowServiceUnavailable as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE) from error
    return WorkflowListResponse(
        items=[_workflow_summary_response(item) for item in page.items],
        page=WorkflowPageMetadataResponse(
            limit=limit,
            next_cursor=(
                _encode_cursor(page.next_cursor)
                if page.next_cursor is not None
                else None
            ),
        ),
    )


@router.post(
    "",
    response_model=WorkflowResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        **COMMON_RESPONSES,
        status.HTTP_409_CONFLICT: {"model": ErrorResponse},
    },
)
async def create_workflow(
    body: CreateWorkflowRequest,
    request: Request,
    response: Response,
    context: Annotated[
        AuthorizationContext,
        Depends(require_permission(Permission.AUTHOR_WORKFLOW)),
    ],
) -> WorkflowResponse | Response:
    """Create an owner-bound draft after authentication and authorization."""
    runtime = _runtime(request)
    try:
        graph_result = validate_dag(
            tuple(step.identifier for step in body.steps),
            tuple(
                DAGEdge(dependency.predecessor, dependency.successor)
                for dependency in body.dependencies
            ),
        )
        if not graph_result.is_valid:
            return _graph_validation_error(request, graph_result.issues)
        steps = _create_steps(body.steps, runtime.task_type_registry)
        dependencies = _create_dependencies(body.dependencies)
        workflow = create_workflow_draft(
            workflow_id=uuid4(),
            owner_principal_id=context.principal_id,
            name=body.name,
            description=body.description,
            status=WorkflowDefinitionStatus.DRAFT,
            steps=steps,
            dependencies=dependencies,
            execution_policy=body.execution_policy,
        )
        stored = await runtime.workflow_service.create(workflow)
    except WorkflowValidationError as error:
        if error.graph_result is not None:
            return _graph_validation_error(request, error.graph_result.issues)
        return _validation_error(request, error.issues)
    except (WorkflowOwnerNotFound, WorkflowOwnerDisabled) as error:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN) from error
    except WorkflowPersistenceConflict as error:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT) from error
    except WorkflowServiceUnavailable as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE) from error
    response.headers["Location"] = f"/api/v1/workflows/{stored.draft.id}"
    return _workflow_response(stored)


@router.post(
    "/validate",
    response_model=WorkflowValidationResponse,
    responses=COMMON_RESPONSES,
)
async def validate_workflow(
    body: CreateWorkflowRequest,
    request: Request,
    context: Annotated[
        AuthorizationContext,
        Depends(require_permission(Permission.AUTHOR_WORKFLOW)),
    ],
) -> WorkflowValidationResponse | Response:
    """Validate a prospective workflow without reading or writing workflow state."""
    runtime = _runtime(request)
    try:
        steps = _create_steps(body.steps, runtime.task_type_registry)
        dependencies = _create_dependencies(body.dependencies)
        _, graph_result = _create_workflow_draft_with_validation(
            workflow_id=uuid4(),
            owner_principal_id=context.principal_id,
            name=body.name,
            description=body.description,
            status=WorkflowDefinitionStatus.DRAFT,
            steps=steps,
            dependencies=dependencies,
            execution_policy=body.execution_policy,
        )
    except WorkflowValidationError as error:
        if error.graph_result is not None:
            return _graph_validation_error(request, error.graph_result.issues)
        return _validation_error(request, error.issues)
    assert graph_result.topological_order is not None
    return WorkflowValidationResponse(
        valid=True,
        topological_order=list(graph_result.topological_order),
    )


@router.post(
    "/{workflow_id}/versions",
    response_model=PublishedWorkflowVersionResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        **COMMON_RESPONSES,
        status.HTTP_404_NOT_FOUND: {"model": ErrorResponse},
        status.HTTP_409_CONFLICT: {"model": ErrorResponse},
    },
)
async def publish_workflow(
    workflow_id: UUID,
    request: Request,
    response: Response,
    context: Annotated[
        AuthorizationContext,
        Depends(require_permission(Permission.OPERATE_WORKFLOW)),
    ],
) -> PublishedWorkflowVersionResponse | Response:
    try:
        published = await _runtime(request).workflow_service.publish(
            workflow_id,
            owner_principal_id=context.principal_id,
        )
    except WorkflowValidationError as error:
        if error.graph_result is not None:
            return _graph_validation_error(request, error.graph_result.issues)
        return _validation_error(request, error.issues)
    except WorkflowNotFound as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND) from error
    except (WorkflowOwnerNotFound, WorkflowOwnerDisabled) as error:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN) from error
    except WorkflowPersistenceConflict as error:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT) from error
    except WorkflowServiceUnavailable as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE) from error
    response.headers["Location"] = (
        f"/api/v1/workflows/{workflow_id}/versions/{published.version_number}"
    )
    return _published_version_response(published)


@router.get(
    "/{workflow_id}/versions",
    response_model=WorkflowVersionListResponse,
    responses={
        **COMMON_RESPONSES,
        status.HTTP_404_NOT_FOUND: {"model": ErrorResponse},
    },
)
async def list_workflow_versions(
    workflow_id: UUID,
    request: Request,
    context: Annotated[
        AuthorizationContext,
        Depends(require_permission(Permission.VIEW)),
    ],
    limit: Annotated[int, Query(ge=1, le=MAX_WORKFLOW_PAGE_SIZE)] = (
        DEFAULT_WORKFLOW_PAGE_SIZE
    ),
    cursor: Annotated[str | None, Query(max_length=MAX_CURSOR_LENGTH)] = None,
) -> WorkflowVersionListResponse | Response:
    try:
        decoded = _decode_version_cursor(cursor) if cursor is not None else None
    except ValueError:
        return _validation_error(
            request,
            (
                WorkflowValidationIssue(
                    "invalid_cursor", ("query", "cursor"), "Cursor is invalid."
                ),
            ),
        )
    try:
        page = await _runtime(request).workflow_service.list_versions(
            workflow_id,
            owner_principal_id=context.principal_id,
            limit=limit,
            cursor=decoded,
        )
    except WorkflowNotFound as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND) from error
    except WorkflowServiceUnavailable as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE) from error
    return WorkflowVersionListResponse(
        items=[_version_summary_response(item) for item in page.items],
        page=WorkflowPageMetadataResponse(
            limit=limit,
            next_cursor=(
                _encode_version_cursor(page.next_cursor)
                if page.next_cursor is not None
                else None
            ),
        ),
    )


@router.get(
    "/{workflow_id}/versions/{version_number}",
    response_model=WorkflowVersionResponse,
    responses={
        **COMMON_RESPONSES,
        status.HTTP_404_NOT_FOUND: {"model": ErrorResponse},
    },
)
async def get_workflow_version(
    workflow_id: UUID,
    version_number: Annotated[int, Path(gt=0)],
    request: Request,
    context: Annotated[
        AuthorizationContext,
        Depends(require_permission(Permission.VIEW)),
    ],
) -> WorkflowVersionResponse:
    try:
        version = await _runtime(request).workflow_service.get_version(
            workflow_id,
            version_number,
            owner_principal_id=context.principal_id,
        )
    except WorkflowNotFound as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND) from error
    except WorkflowServiceUnavailable as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE) from error
    return _workflow_version_response(version)


@router.get(
    "/{workflow_id}",
    response_model=WorkflowResponse,
    responses={
        **COMMON_RESPONSES,
        status.HTTP_404_NOT_FOUND: {"model": ErrorResponse},
    },
)
async def get_workflow(
    workflow_id: UUID,
    request: Request,
    context: Annotated[
        AuthorizationContext,
        Depends(require_permission(Permission.VIEW)),
    ],
) -> WorkflowResponse:
    runtime = _runtime(request)
    try:
        stored = await runtime.workflow_service.get(
            workflow_id,
            owner_principal_id=context.principal_id,
        )
    except WorkflowNotFound as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND) from error
    except WorkflowServiceUnavailable as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE) from error
    return _workflow_response(stored)


def _runtime(request: Request) -> WorkflowRuntimeProtocol:
    return cast(WorkflowRuntimeProtocol, request.app.state.authentication)


def _create_steps(
    requests: list[CreateWorkflowStepRequest],
    task_types: TaskTypeRegistry,
) -> tuple[DraftWorkflowStep, ...]:
    steps: list[DraftWorkflowStep] = []
    issues: list[WorkflowValidationIssue] = []
    for index, step in enumerate(requests):
        try:
            steps.append(
                create_draft_step(
                    step_id=uuid4(),
                    identifier=step.identifier,
                    task_type=step.task_type,
                    parameters=step.parameters,
                    execution_policy=step.execution_policy,
                    task_types=task_types,
                )
            )
        except WorkflowValidationError as error:
            issues.extend(_prefix_issues(("steps", index), error.issues))
    if issues:
        raise WorkflowValidationError(tuple(issues))
    return tuple(steps)


def _create_dependencies(
    requests: list[CreateWorkflowDependencyRequest],
) -> tuple[DraftDependency, ...]:
    dependencies: list[DraftDependency] = []
    issues: list[WorkflowValidationIssue] = []
    for index, dependency in enumerate(requests):
        try:
            dependencies.append(
                create_draft_dependency(
                    dependency_id=uuid4(),
                    predecessor_identifier=dependency.predecessor,
                    successor_identifier=dependency.successor,
                )
            )
        except WorkflowValidationError as error:
            issues.extend(_prefix_issues(("dependencies", index), error.issues))
    if issues:
        raise WorkflowValidationError(tuple(issues))
    return tuple(dependencies)


def _prefix_issues(
    prefix: tuple[str | int, ...],
    issues: tuple[WorkflowValidationIssue, ...],
) -> tuple[WorkflowValidationIssue, ...]:
    return tuple(
        WorkflowValidationIssue(issue.code, (*prefix, *issue.path), issue.message)
        for issue in issues
    )


def _validation_error(
    request: Request,
    issues: tuple[WorkflowValidationIssue, ...],
) -> Response:
    code, message = ERROR_CONTRACTS[422]
    return error_response(
        request,
        status_code=422,
        code=code,
        message=message,
        details=tuple(
            ErrorDetail(code=issue.code, path=list(issue.path), message=issue.message)
            for issue in issues
        ),
    )


_DAG_ERROR_MESSAGES = {
    DAGViolationCode.EMPTY_GRAPH: "A workflow must contain at least one step.",
    DAGViolationCode.TOO_MANY_STEPS: "The workflow contains too many steps.",
    DAGViolationCode.TOO_MANY_DEPENDENCIES: (
        "The workflow contains too many dependencies."
    ),
    DAGViolationCode.DUPLICATE_STEP_IDENTIFIER: "Step identifier is duplicated.",
    DAGViolationCode.MISSING_DEPENDENCY_REFERENCE: (
        "Dependency references an unknown step."
    ),
    DAGViolationCode.SELF_DEPENDENCY: "A step cannot depend on itself.",
    DAGViolationCode.DUPLICATE_DEPENDENCY: "Dependency is duplicated.",
    DAGViolationCode.CYCLE: "Workflow dependencies must not contain a cycle.",
}


def _graph_validation_error(
    request: Request,
    issues: tuple[DAGValidationIssue, ...],
) -> Response:
    return _validation_error(
        request,
        tuple(
            WorkflowValidationIssue(
                issue.code.value,
                issue.path,
                _DAG_ERROR_MESSAGES[issue.code],
            )
            for issue in issues
        ),
    )


def _workflow_response(stored: StoredWorkflowDraft) -> WorkflowResponse:
    draft = stored.draft
    return WorkflowResponse(
        id=draft.id,
        owner_principal_id=draft.owner_principal_id,
        name=draft.name,
        description=draft.description,
        status=draft.status,
        execution_policy=draft.execution_policy,
        created_at=stored.created_at,
        updated_at=stored.updated_at,
        steps=[
            WorkflowStepResponse(
                id=step.id,
                identifier=step.identifier,
                task_type=step.task_type,
                parameters=step.parameters,
                execution_policy=step.execution_policy,
            )
            for step in draft.steps
        ],
        dependencies=[
            WorkflowDependencyResponse(
                id=dependency.id,
                predecessor=dependency.predecessor_identifier,
                successor=dependency.successor_identifier,
            )
            for dependency in draft.dependencies
        ],
    )


def _workflow_summary_response(summary: WorkflowSummary) -> WorkflowSummaryResponse:
    return WorkflowSummaryResponse(
        id=summary.id,
        name=summary.name,
        description=summary.description,
        status=summary.status,
        created_at=summary.created_at,
        updated_at=summary.updated_at,
    )


def _published_version_response(
    published: PublishedWorkflowVersion,
) -> PublishedWorkflowVersionResponse:
    return PublishedWorkflowVersionResponse(
        id=published.id,
        workflow_definition_id=published.workflow_definition_id,
        version_number=published.version_number,
        published_at=published.published_at,
    )


def _version_summary_response(
    summary: WorkflowVersionSummary,
) -> WorkflowVersionSummaryResponse:
    return WorkflowVersionSummaryResponse(
        id=summary.id,
        version_number=summary.version_number,
        published_at=summary.published_at,
    )


def _workflow_version_response(
    version: WorkflowVersionSnapshot,
) -> WorkflowVersionResponse:
    return WorkflowVersionResponse(
        id=version.id,
        workflow_definition_id=version.workflow_definition_id,
        version_number=version.version_number,
        name=version.name,
        description=version.description,
        execution_policy=version.execution_policy,
        published_at=version.published_at,
        steps=[
            WorkflowVersionStepResponse(
                identifier=step.identifier,
                task_type=step.task_type,
                parameters=step.parameters,
                execution_policy=step.execution_policy,
            )
            for step in version.steps
        ],
        dependencies=[
            WorkflowVersionDependencyResponse(
                predecessor=item.predecessor_identifier,
                successor=item.successor_identifier,
            )
            for item in version.dependencies
        ],
    )


def _encode_cursor(cursor: WorkflowPageCursor) -> str:
    payload = json.dumps(
        {
            "v": _CURSOR_VERSION,
            "created_at": cursor.created_at.astimezone(UTC)
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z"),
            "id": str(cursor.workflow_id),
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")


def _decode_cursor(value: str) -> WorkflowPageCursor:
    if not value or len(value) > MAX_CURSOR_LENGTH or not value.isascii():
        raise ValueError("invalid cursor")
    padding = "=" * (-len(value) % 4)
    try:
        decoded = base64.b64decode(
            f"{value}{padding}",
            altchars=b"-_",
            validate=True,
        )
        if len(decoded) > MAX_DECODED_CURSOR_BYTES:
            raise ValueError("invalid cursor")
        payload = json.loads(decoded)
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("invalid cursor") from error
    if not isinstance(payload, dict) or set(payload) != {"v", "created_at", "id"}:
        raise ValueError("invalid cursor")
    if type(payload["v"]) is not int or payload["v"] != _CURSOR_VERSION:
        raise ValueError("invalid cursor")
    created_at_value, workflow_id_value = payload["created_at"], payload["id"]
    if not isinstance(created_at_value, str) or not isinstance(workflow_id_value, str):
        raise ValueError("invalid cursor")
    try:
        created_at = datetime.fromisoformat(created_at_value.replace("Z", "+00:00"))
        workflow_id = UUID(workflow_id_value)
    except ValueError as error:
        raise ValueError("invalid cursor") from error
    if created_at.tzinfo is None:
        raise ValueError("invalid cursor")
    return WorkflowPageCursor(created_at.astimezone(UTC), workflow_id)


def _encode_version_cursor(cursor: WorkflowVersionPageCursor) -> str:
    payload = json.dumps(
        {"v": _CURSOR_VERSION, "version_number": cursor.version_number},
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")


def _decode_version_cursor(value: str) -> WorkflowVersionPageCursor:
    if not value or len(value) > MAX_CURSOR_LENGTH or not value.isascii():
        raise ValueError("invalid cursor")
    padding = "=" * (-len(value) % 4)
    try:
        decoded = base64.b64decode(f"{value}{padding}", altchars=b"-_", validate=True)
        if len(decoded) > MAX_DECODED_CURSOR_BYTES:
            raise ValueError("invalid cursor")
        payload = json.loads(decoded)
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("invalid cursor") from error
    if not isinstance(payload, dict) or set(payload) != {"v", "version_number"}:
        raise ValueError("invalid cursor")
    if type(payload["v"]) is not int or payload["v"] != _CURSOR_VERSION:
        raise ValueError("invalid cursor")
    number = payload["version_number"]
    if type(number) is not int or number <= 0:
        raise ValueError("invalid cursor")
    return WorkflowVersionPageCursor(number)
