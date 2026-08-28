"""Application services for transactional workflow draft persistence."""

from __future__ import annotations

from uuid import UUID, uuid4

from taskforge.audit.domain import (
    AuditActor,
    AuditActorKind,
    AuditOutcome,
    AuditRecord,
    AuditRejected,
)
from taskforge.identity.authorization import OwnerFilter
from taskforge.persistence.audit import RejectedAuditRecorder
from taskforge.workflows.dag_validation import DAGEdge, validate_dag
from taskforge.workflows.domain import (
    PublishedWorkflowVersion,
    WorkflowAvailabilityIntent,
    WorkflowAvailabilityResult,
    WorkflowAvailabilityTransitionRejected,
    WorkflowDraft,
    WorkflowVersionSnapshot,
    availability_requires_published_version,
    change_workflow_availability,
    create_draft_dependency,
    create_draft_step,
    create_workflow_draft,
)
from taskforge.workflows.persistence_ports import (
    ResolvedDependency,
    StoredWorkflowDraft,
    WorkflowOwnerRecordDisabled,
    WorkflowOwnerRecordNotFound,
    WorkflowPage,
    WorkflowPageCursor,
    WorkflowPersistenceUnavailable,
    WorkflowRecordConflict,
    WorkflowRepository,
    WorkflowVersionPage,
    WorkflowVersionPageCursor,
)
from taskforge.workflows.task_types import (
    TaskTypeRegistry,
    WorkflowValidationError,
    WorkflowValidationIssue,
)


class WorkflowNotFound(Exception):
    """A workflow is absent from the requested owner's scope."""


class WorkflowOwnerNotFound(Exception):
    """A workflow owner does not exist."""


class WorkflowOwnerDisabled(Exception):
    """A workflow owner is disabled."""


class WorkflowPersistenceConflict(Exception):
    """A workflow could not be persisted without violating an invariant."""


class WorkflowServiceUnavailable(Exception):
    """Workflow persistence was operationally unavailable."""


class InvalidWorkflowListQuery(ValueError):
    """A workflow list request is not valid."""


_WORKFLOW_AUDIT_REASONS: dict[type[Exception], str] = {
    WorkflowValidationError: "workflow_invalid",
    WorkflowNotFound: "workflow_not_visible",
    WorkflowOwnerRecordNotFound: "owner_not_found",
    WorkflowOwnerRecordDisabled: "owner_disabled",
    WorkflowRecordConflict: "persistence_conflict",
    WorkflowAvailabilityTransitionRejected: "availability_transition_rejected",
}


class WorkflowService:
    def __init__(
        self,
        repository: WorkflowRepository,
        task_types: TaskTypeRegistry,
        rejected_audit: RejectedAuditRecorder | None = None,
    ) -> None:
        self._repository = repository
        self._task_types = task_types
        self._rejected_audit = rejected_audit

    async def create(
        self, workflow: WorkflowDraft, *, correlation_id: UUID | None = None
    ) -> StoredWorkflowDraft:
        graph_result = validate_dag(
            tuple(step.identifier for step in workflow.steps),
            tuple(
                DAGEdge(
                    dependency.predecessor_identifier,
                    dependency.successor_identifier,
                )
                for dependency in workflow.dependencies
            ),
        )
        if not graph_result.is_valid:
            error = WorkflowValidationError.from_graph(graph_result)
            await self._audit_rejection(
                error,
                action="workflow.create",
                workflow_id=workflow.id,
                principal_id=workflow.owner_principal_id,
                correlation_id=correlation_id,
                provenance={
                    "step_count": len(workflow.steps),
                    "dependency_count": len(workflow.dependencies),
                },
            )
            raise error
        try:
            dependencies = _resolve_dependencies(workflow)
        except WorkflowValidationError as error:
            await self._audit_rejection(
                error,
                action="workflow.create",
                workflow_id=workflow.id,
                principal_id=workflow.owner_principal_id,
                correlation_id=correlation_id,
                provenance={
                    "step_count": len(workflow.steps),
                    "dependency_count": len(workflow.dependencies),
                },
            )
            raise
        try:
            async with self._repository.transaction() as transaction:
                await transaction.require_enabled_owner(workflow.owner_principal_id)
                timestamps = await transaction.insert_definition(
                    workflow, str(correlation_id) if correlation_id else None
                )
                await transaction.insert_steps(workflow.id, workflow.steps)
                await transaction.insert_dependencies(workflow.id, dependencies)
                await transaction.commit()
        except WorkflowOwnerRecordNotFound as error:
            await self._audit_rejection(
                error,
                action="workflow.create",
                workflow_id=workflow.id,
                principal_id=workflow.owner_principal_id,
                correlation_id=correlation_id,
            )
            raise WorkflowOwnerNotFound from error
        except WorkflowOwnerRecordDisabled as error:
            await self._audit_rejection(
                error,
                action="workflow.create",
                workflow_id=workflow.id,
                principal_id=workflow.owner_principal_id,
                correlation_id=correlation_id,
            )
            raise WorkflowOwnerDisabled from error
        except WorkflowRecordConflict as error:
            await self._audit_rejection(
                error,
                action="workflow.create",
                workflow_id=workflow.id,
                principal_id=workflow.owner_principal_id,
                correlation_id=correlation_id,
            )
            raise WorkflowPersistenceConflict from error
        except WorkflowPersistenceUnavailable as error:
            raise WorkflowServiceUnavailable from error
        return StoredWorkflowDraft(
            draft=workflow,
            created_at=timestamps.created_at,
            updated_at=timestamps.updated_at,
        )

    async def get(
        self,
        workflow_id: UUID,
        *,
        owner_filter: OwnerFilter,
    ) -> StoredWorkflowDraft:
        try:
            stored = await self._repository.find_draft(
                workflow_id,
                owner_filter,
            )
        except WorkflowPersistenceUnavailable as error:
            raise WorkflowServiceUnavailable from error
        if stored is None:
            raise WorkflowNotFound
        return stored

    async def publish(
        self,
        workflow_id: UUID,
        *,
        owner_filter: OwnerFilter,
        actor_principal_id: UUID,
        correlation_id: UUID | None = None,
    ) -> PublishedWorkflowVersion:
        """Revalidate and atomically snapshot one owner-scoped draft."""
        version_id = uuid4()
        try:
            async with self._repository.transaction() as transaction:
                stored = await transaction.lock_draft_for_publication(
                    workflow_id,
                    owner_filter,
                )
                if stored is None:
                    raise WorkflowNotFound
                await transaction.require_enabled_owner(stored.draft.owner_principal_id)
                validated = _revalidate_for_publication(
                    stored.draft,
                    self._task_types,
                )
                version_number = await transaction.next_version_number(workflow_id)
                published_at = await transaction.insert_version(
                    version_id,
                    version_number,
                    validated,
                    actor_principal_id,
                    str(correlation_id) if correlation_id else None,
                )
                await transaction.insert_version_steps(
                    version_id,
                    tuple(sorted(validated.steps, key=lambda step: step.identifier)),
                )
                await transaction.insert_version_dependencies(
                    version_id,
                    tuple(
                        sorted(
                            validated.dependencies,
                            key=lambda dependency: (
                                dependency.predecessor_identifier,
                                dependency.successor_identifier,
                            ),
                        )
                    ),
                )
                await transaction.commit()
        except WorkflowOwnerRecordNotFound as error:
            await self._audit_rejection(
                error,
                action="workflow.publish",
                workflow_id=workflow_id,
                principal_id=actor_principal_id,
                correlation_id=correlation_id,
            )
            raise WorkflowOwnerNotFound from error
        except WorkflowOwnerRecordDisabled as error:
            await self._audit_rejection(
                error,
                action="workflow.publish",
                workflow_id=workflow_id,
                principal_id=actor_principal_id,
                correlation_id=correlation_id,
            )
            raise WorkflowOwnerDisabled from error
        except WorkflowRecordConflict as error:
            await self._audit_rejection(
                error,
                action="workflow.publish",
                workflow_id=workflow_id,
                principal_id=actor_principal_id,
                correlation_id=correlation_id,
            )
            raise WorkflowPersistenceConflict from error
        except (WorkflowNotFound, WorkflowValidationError) as error:
            await self._audit_rejection(
                error,
                action="workflow.publish",
                workflow_id=workflow_id,
                principal_id=actor_principal_id,
                correlation_id=correlation_id,
            )
            raise
        except WorkflowPersistenceUnavailable as error:
            raise WorkflowServiceUnavailable from error
        return PublishedWorkflowVersion(
            id=version_id,
            workflow_definition_id=workflow_id,
            version_number=version_number,
            published_at=published_at,
        )

    async def set_availability(
        self,
        workflow_id: UUID,
        *,
        owner_filter: OwnerFilter,
        actor_principal_id: UUID,
        intent: WorkflowAvailabilityIntent,
        correlation_id: UUID | None = None,
    ) -> WorkflowAvailabilityResult:
        """Apply one owner-scoped availability change transactionally."""
        try:
            async with self._repository.transaction() as transaction:
                definition = await transaction.lock_definition_for_availability(
                    workflow_id,
                    owner_filter,
                )
                if definition is None:
                    raise WorkflowNotFound
                await transaction.require_enabled_owner(definition.owner_principal_id)
                requires_published_version = availability_requires_published_version(
                    definition.status,
                    intent,
                )
                has_published_version = (
                    await transaction.has_published_version(workflow_id)
                    if requires_published_version
                    else False
                )
                result = change_workflow_availability(
                    workflow_id=workflow_id,
                    current_status=definition.status,
                    intent=intent,
                    has_published_version=has_published_version,
                )
                if result.changed:
                    await transaction.update_availability(
                        workflow_id,
                        result.status,
                        actor_principal_id,
                        str(correlation_id) if correlation_id else None,
                    )
                await transaction.commit()
        except WorkflowOwnerRecordNotFound as error:
            await self._audit_rejection(
                error,
                action="workflow.availability_change",
                workflow_id=workflow_id,
                principal_id=actor_principal_id,
                correlation_id=correlation_id,
            )
            raise WorkflowOwnerNotFound from error
        except WorkflowOwnerRecordDisabled as error:
            await self._audit_rejection(
                error,
                action="workflow.availability_change",
                workflow_id=workflow_id,
                principal_id=actor_principal_id,
                correlation_id=correlation_id,
            )
            raise WorkflowOwnerDisabled from error
        except WorkflowRecordConflict as error:
            await self._audit_rejection(
                error,
                action="workflow.availability_change",
                workflow_id=workflow_id,
                principal_id=actor_principal_id,
                correlation_id=correlation_id,
            )
            raise WorkflowPersistenceConflict from error
        except (WorkflowNotFound, WorkflowAvailabilityTransitionRejected) as error:
            await self._audit_rejection(
                error,
                action="workflow.availability_change",
                workflow_id=workflow_id,
                principal_id=actor_principal_id,
                correlation_id=correlation_id,
            )
            raise
        except WorkflowPersistenceUnavailable as error:
            raise WorkflowServiceUnavailable from error
        return result

    async def _audit_rejection(
        self,
        error: Exception,
        *,
        action: str,
        workflow_id: UUID,
        principal_id: UUID,
        correlation_id: UUID | None,
        provenance: dict[str, object] | None = None,
    ) -> None:
        if self._rejected_audit is None:
            return
        try:
            await self._rejected_audit.record(
                AuditRecord(
                    uuid4(),
                    AuditActor(
                        AuditActorKind.API_PRINCIPAL, api_principal_id=principal_id
                    ),
                    action,
                    AuditOutcome.REJECTED,
                    "workflow",
                    workflow_id,
                    str(correlation_id) if correlation_id else None,
                    provenance or {},
                    _WORKFLOW_AUDIT_REASONS[type(error)],
                )
            )
        except AuditRejected as audit_error:
            raise WorkflowServiceUnavailable from audit_error

    async def list(
        self,
        *,
        owner_filter: OwnerFilter,
        limit: int,
        cursor: WorkflowPageCursor | None = None,
    ) -> WorkflowPage:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise InvalidWorkflowListQuery("limit must be a positive integer")
        try:
            return await self._repository.list_summaries(
                owner_filter,
                limit=limit,
                cursor=cursor,
            )
        except WorkflowPersistenceUnavailable as error:
            raise WorkflowServiceUnavailable from error

    async def list_versions(
        self,
        workflow_id: UUID,
        *,
        owner_filter: OwnerFilter,
        limit: int,
        cursor: WorkflowVersionPageCursor | None = None,
    ) -> WorkflowVersionPage:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise InvalidWorkflowListQuery("limit must be a positive integer")
        try:
            page = await self._repository.list_versions(
                workflow_id,
                owner_filter,
                limit=limit,
                cursor=cursor,
            )
        except WorkflowPersistenceUnavailable as error:
            raise WorkflowServiceUnavailable from error
        if page is None:
            raise WorkflowNotFound
        return page

    async def get_version(
        self,
        workflow_id: UUID,
        version_number: int,
        *,
        owner_filter: OwnerFilter,
    ) -> WorkflowVersionSnapshot:
        if (
            isinstance(version_number, bool)
            or not isinstance(version_number, int)
            or version_number <= 0
        ):
            raise ValueError("version number must be positive")
        try:
            version = await self._repository.find_version(
                workflow_id,
                version_number,
                owner_filter,
            )
        except WorkflowPersistenceUnavailable as error:
            raise WorkflowServiceUnavailable from error
        if version is None:
            raise WorkflowNotFound
        return version


def _resolve_dependencies(
    workflow: WorkflowDraft,
) -> tuple[ResolvedDependency, ...]:
    identifiers = {step.identifier: step.id for step in workflow.steps}
    resolved: list[ResolvedDependency] = []
    for dependency in workflow.dependencies:
        try:
            predecessor_id = identifiers[dependency.predecessor_identifier]
            successor_id = identifiers[dependency.successor_identifier]
        except KeyError as error:
            raise WorkflowPersistenceConflict(
                "dependency references an unknown step"
            ) from error
        resolved.append(
            ResolvedDependency(
                id=dependency.id,
                predecessor_step_id=predecessor_id,
                successor_step_id=successor_id,
            )
        )
    return tuple(resolved)


def _revalidate_for_publication(
    workflow: WorkflowDraft,
    task_types: TaskTypeRegistry,
) -> WorkflowDraft:
    steps = []
    dependencies = []
    issues: list[WorkflowValidationIssue] = []
    for index, step in enumerate(workflow.steps):
        try:
            steps.append(
                create_draft_step(
                    step_id=step.id,
                    identifier=step.identifier,
                    task_type=step.task_type,
                    parameters=step.parameters,
                    execution_policy=step.execution_policy,
                    task_types=task_types,
                )
            )
        except WorkflowValidationError as error:
            issues.extend(_prefix_issues(("steps", index), error.issues))
    for index, dependency in enumerate(workflow.dependencies):
        try:
            dependencies.append(
                create_draft_dependency(
                    dependency_id=dependency.id,
                    predecessor_identifier=dependency.predecessor_identifier,
                    successor_identifier=dependency.successor_identifier,
                )
            )
        except WorkflowValidationError as error:
            issues.extend(_prefix_issues(("dependencies", index), error.issues))
    if issues:
        raise WorkflowValidationError(tuple(issues))
    return create_workflow_draft(
        workflow_id=workflow.id,
        owner_principal_id=workflow.owner_principal_id,
        name=workflow.name,
        description=workflow.description,
        status=workflow.status,
        steps=tuple(steps),
        dependencies=tuple(dependencies),
        execution_policy=workflow.execution_policy,
    )


def _prefix_issues(
    prefix: tuple[str | int, ...],
    issues: tuple[WorkflowValidationIssue, ...],
) -> tuple[WorkflowValidationIssue, ...]:
    return tuple(
        WorkflowValidationIssue(issue.code, (*prefix, *issue.path), issue.message)
        for issue in issues
    )
