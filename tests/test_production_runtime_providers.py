"""Installed production provider and process import-boundary tests."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from datetime import UTC, datetime
from importlib import metadata
from uuid import UUID, uuid4

import pytest

from taskforge.identity.authentication import AuthenticatedWorker
from taskforge.identity.credentials import CredentialScope, generate_credential
from taskforge.runtime_provider import (
    ResolvedWorkerProfile,
    RuntimeProviderError,
    load_installed_task_catalog,
    load_installed_worker_profile,
)
from taskforge.settings import WorkerSettings
from taskforge.tasks.catalog import (
    INGEST_TASK_TYPE,
    INGESTION_CAPABILITY,
    NOTIFICATION_CAPABILITY,
    NOTIFY_TASK_TYPE,
    PROCESSING_CAPABILITY,
    TRANSFORM_TASK_TYPE,
    VALIDATE_TASK_TYPE,
)
from taskforge.tasks.handlers import ingest, notify, transform, validate
from taskforge.worker.application import WorkerApplication, WorkerApplicationState
from taskforge.worker.domain import RegisteredWorkerSession, WorkerRegistration
from taskforge.worker.service import WorkerRegistrationService

CATALOG_VALUE = "taskforge.tasks.catalog:provide_task_catalog"
PROFILE_VALUES = {
    "pipeline": "taskforge.tasks.profiles:provide_pipeline_profile",
    "ingestion": "taskforge.tasks.profiles:provide_ingestion_profile",
    "processing": "taskforge.tasks.profiles:provide_processing_profile",
    "notification": "taskforge.tasks.profiles:provide_notification_profile",
}


class RegistrationRepository:
    def __init__(self) -> None:
        self.registration: WorkerRegistration | None = None

    async def register_session(
        self,
        authenticated_worker: AuthenticatedWorker,
        session_id: UUID,
        registration: WorkerRegistration,
    ) -> RegisteredWorkerSession:
        del authenticated_worker
        self.registration = registration
        return RegisteredWorkerSession(
            session_id, datetime(2030, 1, 1, tzinfo=UTC), registration.capabilities
        )


class ApplicationEngine:
    def __init__(self) -> None:
        self.dispose_calls = 0

    async def dispose(self) -> None:
        self.dispose_calls += 1


class ApplicationHeartbeat:
    def __init__(self) -> None:
        self.initial_calls = 0
        self.start_calls = 0
        self.close_calls = 0

    async def send_initial(self) -> None:
        self.initial_calls += 1

    def start(self) -> None:
        self.start_calls += 1

    async def close(self) -> None:
        self.close_calls += 1


def entry_points(group: str) -> tuple[metadata.EntryPoint, ...]:
    return tuple(entry for entry in metadata.entry_points() if entry.group == group)


def test_installed_metadata_contains_exact_production_entry_points() -> None:
    catalogs = entry_points("taskforge.task_catalog")
    profiles = entry_points("taskforge.worker_profile")

    assert [(entry.name, entry.value) for entry in catalogs] == [
        ("taskforge", CATALOG_VALUE)
    ]
    assert {entry.name: entry.value for entry in profiles} == PROFILE_VALUES
    assert len(profiles) == 4


def test_every_real_profile_resolves_exact_trusted_bindings_and_capabilities() -> None:
    catalog = load_installed_task_catalog()
    expected = {
        "pipeline": (
            {
                INGEST_TASK_TYPE: ingest,
                VALIDATE_TASK_TYPE: validate,
                TRANSFORM_TASK_TYPE: transform,
                NOTIFY_TASK_TYPE: notify,
            },
            (
                INGESTION_CAPABILITY,
                NOTIFICATION_CAPABILITY,
                PROCESSING_CAPABILITY,
            ),
        ),
        "ingestion": (
            {INGEST_TASK_TYPE: ingest},
            (INGESTION_CAPABILITY,),
        ),
        "processing": (
            {
                VALIDATE_TASK_TYPE: validate,
                TRANSFORM_TASK_TYPE: transform,
            },
            (PROCESSING_CAPABILITY,),
        ),
        "notification": (
            {NOTIFY_TASK_TYPE: notify},
            (NOTIFICATION_CAPABILITY,),
        ),
    }

    assert catalog.names == {
        INGEST_TASK_TYPE,
        VALIDATE_TASK_TYPE,
        TRANSFORM_TASK_TYPE,
        NOTIFY_TASK_TYPE,
    }
    for name, (bindings, capabilities) in expected.items():
        resolved = load_installed_worker_profile(name, catalog)
        assert resolved.handlers.task_types == set(bindings)
        assert resolved.capabilities == capabilities
        for task_type, handler in bindings.items():
            definition = resolved.handlers.definition(task_type)
            assert definition is not None
            task_definition = catalog.definition(task_type)
            assert task_definition is not None
            assert definition.handler is handler
            assert definition.handler.__module__ == "taskforge.tasks.handlers"
            assert definition.required_capability == task_definition.required_capability


def test_unknown_real_profile_still_fails_closed() -> None:
    with pytest.raises(RuntimeProviderError):
        load_installed_worker_profile("module:callable", load_installed_task_catalog())
    with pytest.raises(RuntimeProviderError):
        load_installed_worker_profile("unknown", load_installed_task_catalog())


def test_real_profile_capabilities_are_the_registration_advertisement() -> None:
    catalog = load_installed_task_catalog()
    profile = load_installed_worker_profile("pipeline", catalog)
    repository = RegistrationRepository()
    service = WorkerRegistrationService(repository, catalog, identifier_factory=uuid4)

    registered = asyncio.run(
        service.register(AuthenticatedWorker(uuid4(), uuid4()), profile.capabilities)
    )

    assert repository.registration == WorkerRegistration(profile.capabilities)
    assert registered.capabilities == profile.capabilities


def test_worker_application_loads_real_pipeline_profile_for_registration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        credential = generate_credential(CredentialScope.WORKER)
        application = WorkerApplication(
            WorkerSettings(
                postgres_password="postgres-secret",
                rabbitmq_password="rabbit-secret",
                worker_credential=credential.take_presented_value(),
                worker_profile="pipeline",
            )
        )
        engine = ApplicationEngine()
        heartbeat = ApplicationHeartbeat()
        authenticated = AuthenticatedWorker(uuid4(), uuid4())
        session_id = uuid4()
        registration_capabilities: list[tuple[str, ...]] = []
        resolved_profiles: list[ResolvedWorkerProfile] = []

        class Authenticator:
            async def authenticate(self, presented: object) -> AuthenticatedWorker:
                del presented
                return authenticated

        class Registration:
            async def register(
                self, worker: AuthenticatedWorker, capabilities: tuple[str, ...]
            ) -> RegisteredWorkerSession:
                assert worker is authenticated
                registration_capabilities.append(capabilities)
                return RegisteredWorkerSession(
                    session_id, datetime(2030, 1, 1, tzinfo=UTC), capabilities
                )

        async def connect_broker(
            catalog: object, profile: ResolvedWorkerProfile
        ) -> None:
            assert catalog is not None
            resolved_profiles.append(profile)

        async def start_consumers(
            profile: ResolvedWorkerProfile, execution: object
        ) -> None:
            assert execution is not None
            assert profile is resolved_profiles[0]

        monkeypatch.setattr(application, "_configure_telemetry", lambda: None)
        monkeypatch.setattr(application, "_connect_broker", connect_broker)
        monkeypatch.setattr(application, "_start_consumers", start_consumers)
        monkeypatch.setattr(
            "taskforge.worker.application.build_async_engine", lambda value: engine
        )
        monkeypatch.setattr(
            "taskforge.worker.application.build_session_factory", lambda value: object()
        )
        monkeypatch.setattr(
            "taskforge.worker.application.WorkerAuthenticator",
            lambda *args, **kwargs: Authenticator(),
        )
        monkeypatch.setattr(
            "taskforge.worker.application.WorkerRegistrationService",
            lambda *args, **kwargs: Registration(),
        )
        monkeypatch.setattr(
            "taskforge.worker.application.WorkerHeartbeatSupervisor",
            lambda *args, **kwargs: heartbeat,
        )

        await application.start()
        try:
            assert application.state is WorkerApplicationState.RUNNING
            assert len(resolved_profiles) == 1
            profile = resolved_profiles[0]
            assert profile.name == "pipeline"
            assert profile.handlers.task_types == {
                INGEST_TASK_TYPE,
                VALIDATE_TASK_TYPE,
                TRANSFORM_TASK_TYPE,
                NOTIFY_TASK_TYPE,
            }
            assert registration_capabilities == [
                (
                    INGESTION_CAPABILITY,
                    NOTIFICATION_CAPABILITY,
                    PROCESSING_CAPABILITY,
                )
            ]
            assert heartbeat.initial_calls == 1
            assert heartbeat.start_calls == 1
        finally:
            await application.close()

        assert heartbeat.close_calls == 1
        assert engine.dispose_calls == 1

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "script",
    (
        "from taskforge.api.application import create_production_app; "
        "create_production_app()",
        "import taskforge.orchestrator.application as application; "
        "application.load_installed_task_catalog()",
    ),
)
def test_catalog_only_process_composition_does_not_import_concrete_handlers(
    script: str,
) -> None:
    assertion = (
        "; import sys; "
        "assert 'taskforge.tasks.catalog' in sys.modules; "
        "assert 'taskforge.tasks.handlers' not in sys.modules; "
        "assert 'taskforge.tasks.profiles' not in sys.modules"
    )
    completed = subprocess.run(
        [sys.executable, "-c", f"{script}{assertion}"],
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "POSTGRES_PASSWORD": "task3-test-password",
            "RABBITMQ_DEFAULT_PASS": "task3-test-password",
        },
    )

    assert completed.returncode == 0, completed.stderr
