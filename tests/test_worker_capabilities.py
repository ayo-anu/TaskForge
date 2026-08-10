"""Worker capability replacement domain, service, and SQL lock tests."""

from __future__ import annotations

import asyncio
from uuid import UUID, uuid4

import pytest
from sqlalchemy.dialects import postgresql

from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.persistence.workers import (
    SQLAlchemyWorkerCapabilityRepository,
    _capability_session_lock_statement,
)
from taskforge.worker.domain import (
    MAX_WORKER_CAPABILITIES,
    InvalidWorkerRegistration,
    ReplacedWorkerCapabilities,
    WorkerCapabilityReplacement,
    validate_worker_capabilities,
    validate_worker_registration,
)
from taskforge.worker.persistence_ports import (
    WorkerCapabilityAuthorityRejected,
    WorkerCapabilityInvariantViolation,
    WorkerCapabilityPersistenceUnavailable,
    WorkerCapabilitySessionInactive,
    WorkerCapabilitySessionUnavailable,
)
from taskforge.worker.service import (
    WorkerCapabilityInvariantError,
    WorkerCapabilityRejected,
    WorkerCapabilityService,
    WorkerCapabilityServiceUnavailable,
    WorkerCapabilitySessionInactiveError,
    WorkerCapabilitySessionUnavailableError,
)
from taskforge.workflows.task_types import (
    JSONMapping,
    TaskTypeDefinition,
    TaskTypeRegistry,
    WorkflowValidationIssue,
)


class Parameters:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        del parameters
        return ()


def registry(
    capabilities: tuple[str, ...] = ("documents", "notifications.email"),
) -> TaskTypeRegistry:
    return TaskTypeRegistry(
        tuple(
            TaskTypeDefinition(f"task.{index}", capability, Parameters())
            for index, capability in enumerate(capabilities)
        )
    )


class CapabilityRepository:
    def __init__(self) -> None:
        self.calls: list[
            tuple[AuthenticatedWorker, UUID, WorkerCapabilityReplacement]
        ] = []
        self.error: Exception | None = None

    async def replace_capabilities(
        self,
        authenticated_worker: AuthenticatedWorker,
        worker_session_id: UUID,
        replacement: WorkerCapabilityReplacement,
    ) -> ReplacedWorkerCapabilities:
        self.calls.append((authenticated_worker, worker_session_id, replacement))
        if self.error:
            raise self.error
        return ReplacedWorkerCapabilities(worker_session_id, replacement.capabilities)


class Result:
    def __init__(self, *, row: object = None, scalars: tuple[str, ...] = ()) -> None:
        self.row = row
        self.scalar_values = scalars

    def one_or_none(self) -> object:
        return self.row

    def scalars(self) -> tuple[str, ...]:
        return self.scalar_values


class Session:
    def __init__(self, results: list[Result]) -> None:
        self.results = results

    async def execute(self, statement: object, parameters: object = None) -> Result:
        del statement, parameters
        return self.results.pop(0)


class Begin:
    def __init__(self, session: Session) -> None:
        self.session = session

    async def __aenter__(self) -> Session:
        return self.session

    async def __aexit__(self, *args: object) -> None:
        del args


class Sessions:
    def __init__(self, session: Session) -> None:
        self.session = session

    def begin(self) -> Begin:
        return Begin(self.session)


def test_shared_validator_accepts_empty_and_canonicalizes_registration_and_replacement() -> (
    None
):
    known = registry().required_capabilities
    assert validate_worker_capabilities((), known_capabilities=known) == ()
    expected = ("documents", "notifications.email")
    advertised = ("notifications.email", "documents")
    assert (
        validate_worker_capabilities(advertised, known_capabilities=known) == expected
    )
    assert (
        validate_worker_registration(advertised, known_capabilities=known).capabilities
        == expected
    )


def test_shared_validator_rejects_count_lexical_duplicate_and_unknown_failures() -> (
    None
):
    known = registry().required_capabilities
    cases = (
        (("Invalid",), "invalid_capability"),
        (("documents", "documents"), "duplicate_capability"),
        (("unknown",), "unknown_capability"),
        (
            tuple(f"cap-{index}" for index in range(MAX_WORKER_CAPABILITIES + 1)),
            "too_many_capabilities",
        ),
    )
    for values, expected in cases:
        with pytest.raises(InvalidWorkerRegistration) as caught:
            validate_worker_capabilities(values, known_capabilities=known)
        assert expected in {issue.code for issue in caught.value.issues}


def test_shared_validator_accepts_maximum_capability_count() -> None:
    capabilities = tuple(f"cap-{index}" for index in range(MAX_WORKER_CAPABILITIES))
    assert validate_worker_capabilities(
        tuple(reversed(capabilities)), known_capabilities=frozenset(capabilities)
    ) == tuple(sorted(capabilities))


def test_service_validates_then_delegates_once_with_authenticated_scope() -> None:
    repository = CapabilityRepository()
    service = WorkerCapabilityService(repository, registry())
    authenticated = AuthenticatedWorker(uuid4(), uuid4())
    session_id = uuid4()

    result = asyncio.run(
        service.replace(
            authenticated,
            session_id,
            ("notifications.email", "documents"),
        )
    )

    replacement = WorkerCapabilityReplacement(("documents", "notifications.email"))
    assert result == ReplacedWorkerCapabilities(session_id, replacement.capabilities)
    assert repository.calls == [(authenticated, session_id, replacement)]


@pytest.mark.parametrize(
    ("persistence_error", "service_error"),
    (
        (WorkerCapabilityAuthorityRejected(), WorkerCapabilityRejected),
        (
            WorkerCapabilitySessionUnavailable(),
            WorkerCapabilitySessionUnavailableError,
        ),
        (WorkerCapabilitySessionInactive(), WorkerCapabilitySessionInactiveError),
        (WorkerCapabilityInvariantViolation(), WorkerCapabilityInvariantError),
        (
            WorkerCapabilityPersistenceUnavailable(),
            WorkerCapabilityServiceUnavailable,
        ),
    ),
)
def test_service_normalizes_declared_replacement_failures(
    persistence_error: Exception,
    service_error: type[Exception],
) -> None:
    repository = CapabilityRepository()
    repository.error = persistence_error
    service = WorkerCapabilityService(repository, registry())
    with pytest.raises(service_error):
        asyncio.run(
            service.replace(
                AuthenticatedWorker(uuid4(), uuid4()), uuid4(), ("documents",)
            )
        )


def test_postgresql_session_lock_compiles_as_for_no_key_update() -> None:
    compiled = str(
        _capability_session_lock_statement(uuid4(), uuid4()).compile(
            dialect=postgresql.dialect()  # type: ignore[no-untyped-call]
        )
    )
    assert compiled.endswith("FOR NO KEY UPDATE")


def test_sqlalchemy_repository_maps_idempotent_and_differential_replacements() -> None:
    authenticated = AuthenticatedWorker(uuid4(), uuid4())
    session_id = uuid4()
    authority_row = type("Authority", (), {"disabled_at": None})()
    session_row = type("WorkerSession", (), {"ended_at": None})()

    identical_session = Session(
        [
            Result(row=authority_row),
            Result(row=object()),
            Result(row=session_row),
            Result(scalars=("documents",)),
        ]
    )
    identical_repository = SQLAlchemyWorkerCapabilityRepository(
        Sessions(identical_session)  # type: ignore[arg-type]
    )
    identical = asyncio.run(
        identical_repository.replace_capabilities(
            authenticated,
            session_id,
            WorkerCapabilityReplacement(("documents",)),
        )
    )
    assert identical.capabilities == ("documents",)
    assert identical_session.results == []

    changed_session = Session(
        [
            Result(row=authority_row),
            Result(row=object()),
            Result(row=session_row),
            Result(scalars=("documents", "notifications.email")),
            Result(),
            Result(),
        ]
    )
    changed_repository = SQLAlchemyWorkerCapabilityRepository(
        Sessions(changed_session)  # type: ignore[arg-type]
    )
    changed = asyncio.run(
        changed_repository.replace_capabilities(
            authenticated,
            session_id,
            WorkerCapabilityReplacement(("documents", "images")),
        )
    )
    assert changed.capabilities == ("documents", "images")
    assert changed_session.results == []
