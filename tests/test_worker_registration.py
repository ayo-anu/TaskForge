"""Worker registration domain and service contract tests."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.worker.domain import (
    MAX_WORKER_CAPABILITIES,
    InvalidWorkerRegistration,
    RegisteredWorkerSession,
    WorkerRegistration,
    validate_worker_registration,
)
from taskforge.worker.persistence_ports import (
    WorkerRegistrationAuthorityRejected,
    WorkerRegistrationPersistenceUnavailable,
    WorkerRegistrationRecordConflict,
)
from taskforge.worker.service import (
    WorkerRegistrationConflict,
    WorkerRegistrationRejected,
    WorkerRegistrationService,
    WorkerRegistrationServiceUnavailable,
)
from taskforge.workflows.task_types import (
    JSONMapping,
    TaskTypeDefinition,
    TaskTypeRegistry,
    WorkflowValidationIssue,
)


class ValidParameters:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        del parameters
        return ()


class RegistrationRepository:
    def __init__(self) -> None:
        self.calls: list[tuple[AuthenticatedWorker, UUID, WorkerRegistration]] = []
        self.error: BaseException | None = None

    async def register_session(
        self,
        authenticated_worker: AuthenticatedWorker,
        session_id: UUID,
        registration: WorkerRegistration,
    ) -> RegisteredWorkerSession:
        self.calls.append((authenticated_worker, session_id, registration))
        if self.error is not None:
            raise self.error
        return RegisteredWorkerSession(
            session_id,
            datetime.now(UTC),
            registration.capabilities,
        )


def registry() -> TaskTypeRegistry:
    validator = ValidParameters()
    return TaskTypeRegistry(
        (
            TaskTypeDefinition("documents.extract", "documents", validator),
            TaskTypeDefinition("documents.render", "documents", validator),
            TaskTypeDefinition("email.send", "notifications.email", validator),
        )
    )


def test_registration_accepts_empty_and_canonicalizes_known_capabilities() -> None:
    known = registry().required_capabilities

    assert validate_worker_registration((), known_capabilities=known).capabilities == ()
    assert validate_worker_registration(
        ("notifications.email", "documents"), known_capabilities=known
    ).capabilities == ("documents", "notifications.email")


def test_registration_domain_rejects_empty_issues_and_naive_timestamp() -> None:
    with pytest.raises(ValueError, match="at least one"):
        InvalidWorkerRegistration(())
    with pytest.raises(ValueError, match="timezone-aware"):
        RegisteredWorkerSession(uuid4(), datetime.now(), ())


def test_registration_rejects_duplicates_malformed_unknown_and_over_limit() -> None:
    known = registry().required_capabilities
    cases = (
        (("documents", "documents"), "duplicate_capability"),
        (("Invalid",), "invalid_capability"),
        (("unknown",), "unknown_capability"),
        (
            tuple(
                f"capability-{index}" for index in range(MAX_WORKER_CAPABILITIES + 1)
            ),
            "too_many_capabilities",
        ),
    )
    for capabilities, expected_code in cases:
        with pytest.raises(InvalidWorkerRegistration) as caught:
            validate_worker_registration(capabilities, known_capabilities=known)
        assert expected_code in {issue.code for issue in caught.value.issues}


def test_service_generates_fresh_session_and_propagates_authenticated_authority() -> (
    None
):
    repository = RegistrationRepository()
    session_id = uuid4()
    authenticated = AuthenticatedWorker(uuid4(), uuid4())
    service = WorkerRegistrationService(
        repository,
        registry(),
        identifier_factory=lambda: session_id,
    )

    registered = asyncio.run(
        service.register(authenticated, ("notifications.email", "documents"))
    )

    assert registered.id == session_id
    assert registered.capabilities == ("documents", "notifications.email")
    assert repository.calls == [
        (
            authenticated,
            session_id,
            WorkerRegistration(("documents", "notifications.email")),
        )
    ]


@pytest.mark.parametrize(
    ("persistence_error", "service_error"),
    (
        (WorkerRegistrationAuthorityRejected(), WorkerRegistrationRejected),
        (WorkerRegistrationRecordConflict(), WorkerRegistrationConflict),
        (
            WorkerRegistrationPersistenceUnavailable(),
            WorkerRegistrationServiceUnavailable,
        ),
    ),
)
def test_service_normalizes_only_declared_persistence_failures(
    persistence_error: Exception,
    service_error: type[Exception],
) -> None:
    repository = RegistrationRepository()
    repository.error = persistence_error
    service = WorkerRegistrationService(repository, registry())

    with pytest.raises(service_error):
        asyncio.run(service.register(AuthenticatedWorker(uuid4(), uuid4()), ()))


def test_service_does_not_swallow_programming_or_cancellation_failures() -> None:
    for failure in (RuntimeError("defect"), asyncio.CancelledError()):
        repository = RegistrationRepository()
        repository.error = failure
        service = WorkerRegistrationService(repository, registry())
        with pytest.raises(type(failure)):
            asyncio.run(service.register(AuthenticatedWorker(uuid4(), uuid4()), ()))
