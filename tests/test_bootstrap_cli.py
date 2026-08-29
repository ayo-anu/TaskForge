"""Tests for safe local bootstrap CLI parsing and output behavior."""

from __future__ import annotations

import asyncio
import io
from argparse import Namespace
from datetime import UTC, datetime, timedelta
from types import TracebackType
from uuid import UUID, uuid4

import pytest
from pydantic import SecretStr

from taskforge.bootstrap import cli
from taskforge.identity.credentials import GeneratedCredential
from taskforge.identity.provisioning import (
    CredentialNotFound,
    DuplicateIdentity,
    IdentityDisabled,
    IdentityNotFound,
    InvalidProvisioningRequest,
    ProvisioningUnavailable,
)
from taskforge.settings import Settings


def settings(environment: str = "development") -> Settings:
    return Settings(
        environment=environment,
        postgres_password=SecretStr("postgres-test-secret"),
        rabbitmq_password=SecretStr("rabbitmq-test-secret"),
        task_claim_result_authority_secret=SecretStr(
            "test-claim-result-authority-secret"
        ),
    )


def invoke(
    monkeypatch: pytest.MonkeyPatch,
    arguments: list[str],
    *,
    environment: str = "development",
    stdin_value: str = "yes\n",
    result: GeneratedCredential | None = None,
) -> tuple[int, str, str, list[datetime | None]]:
    expirations: list[datetime | None] = []

    async def fake_execute(
        parsed: object,
        runtime_settings: Settings,
        expiration: datetime | None,
    ) -> GeneratedCredential | None:
        del parsed, runtime_settings
        expirations.append(expiration)
        return result

    monkeypatch.setattr(cli, "_execute", fake_execute)
    stdout, stderr = io.StringIO(), io.StringIO()
    exit_code = cli.main(
        arguments,
        stdin=io.StringIO(stdin_value),
        stdout=stdout,
        stderr=stderr,
        settings_factory=lambda: settings(environment),
    )
    return exit_code, stdout.getvalue(), stderr.getvalue(), expirations


def generated(value: str = "runtime-only-sensitive-value") -> GeneratedCredential:
    from uuid import uuid4

    return GeneratedCredential(
        credential_id=uuid4(),
        credential_verifier="runtime-only-verifier",
        presented_value=value,
    )


def test_success_outputs_new_credential_exactly_once() -> None:
    credential = generated()
    monkeypatch = pytest.MonkeyPatch()
    try:
        code, stdout, stderr, _ = invoke(
            monkeypatch,
            [
                "create-worker",
                "--name",
                "worker",
                "--expires-in",
                "30d",
                "--yes",
            ],
            result=credential,
        )
    finally:
        monkeypatch.undo()

    assert code == 0
    assert stdout == "runtime-only-sensitive-value\n"
    assert stdout.count("runtime-only-sensitive-value") == 1
    assert "runtime-only-sensitive-value" not in stderr
    assert "runtime-only-verifier" not in stderr


@pytest.mark.parametrize(
    ("command", "identity_option"),
    (
        ("rotate-api-credential", "--principal-id"),
        ("rotate-worker-credential", "--worker-identity-id"),
    ),
)
def test_rotation_preserves_overlap_and_explains_explicit_revocation(
    monkeypatch: pytest.MonkeyPatch,
    command: str,
    identity_option: str,
) -> None:
    code, stdout, stderr, _ = invoke(
        monkeypatch,
        [
            command,
            identity_option,
            str(uuid4()),
            "--expires-in",
            "30d",
            "--yes",
        ],
        result=generated(),
    )

    assert code == 0
    assert stdout == "runtime-only-sensitive-value\n"
    assert "existing credentials remain active" in stderr.lower()
    assert "explicitly revoking" in stderr.lower()
    assert "runtime-only-sensitive-value" not in stderr
    assert "runtime-only-verifier" not in stderr


@pytest.mark.parametrize("duration", ("30d", "90d", "365d", "24h", "2w"))
def test_expires_in_computes_a_future_utc_instant(
    monkeypatch: pytest.MonkeyPatch,
    duration: str,
) -> None:
    code, stdout, _, expirations = invoke(
        monkeypatch,
        ["create-worker", "--name", "worker", "--expires-in", duration, "--yes"],
        result=generated(duration),
    )

    assert code == 0
    assert stdout
    assert expirations[0] is not None
    assert expirations[0].tzinfo is UTC
    assert expirations[0] > datetime.now(UTC)


def test_expires_at_preserves_the_explicit_instant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = datetime.now(UTC) + timedelta(days=30)
    code, _, _, expirations = invoke(
        monkeypatch,
        [
            "create-worker",
            "--name",
            "worker",
            "--expires-at",
            expected.isoformat(),
            "--yes",
        ],
        result=generated(),
    )

    assert code == 0
    assert expirations == [expected]


def test_interactive_confirmation_defaults_to_no(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    code, stdout, stderr, expirations = invoke(
        monkeypatch,
        ["create-worker", "--name", "worker", "--expires-in", "30d"],
        stdin_value="\n",
        result=generated(),
    )

    assert code == 2
    assert stdout == ""
    assert "cancelled" in stderr.lower()
    assert expirations == []


def test_yes_bypasses_prompt_but_not_development_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    code, stdout, stderr, expirations = invoke(
        monkeypatch,
        [
            "create-worker",
            "--name",
            "worker",
            "--expires-in",
            "30d",
            "--yes",
        ],
        environment="production",
        result=generated(),
    )

    assert code == 2
    assert stdout == ""
    assert "only in development" in stderr.lower()
    assert expirations == []


@pytest.mark.parametrize("duration", ("0d", "30", "+1d", "forever"))
def test_invalid_durations_fail_without_secret_output(
    monkeypatch: pytest.MonkeyPatch,
    duration: str,
) -> None:
    code, stdout, stderr, expirations = invoke(
        monkeypatch,
        [
            "create-worker",
            "--name",
            "worker",
            "--expires-in",
            duration,
            "--yes",
        ],
        result=generated(),
    )

    assert code == 2
    assert stdout == ""
    assert "invalid" in stderr.lower()
    assert expirations == []


def test_revocation_has_no_secret_output_and_supports_yes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from uuid import uuid4

    code, stdout, stderr, expirations = invoke(
        monkeypatch,
        [
            "revoke-api-credential",
            "--principal-id",
            str(uuid4()),
            "--credential-id",
            str(uuid4()),
            "--yes",
        ],
        result=None,
    )

    assert code == 0
    assert stdout == ""
    assert "revoked" in stderr.lower()
    assert expirations == [None]


@pytest.mark.parametrize(
    ("failure", "expected_message", "expected_code"),
    (
        (InvalidProvisioningRequest(), "invalid", 2),
        (DuplicateIdentity(), "already exists", 2),
        (IdentityNotFound(), "not found", 2),
        (CredentialNotFound(), "not found", 2),
        (IdentityDisabled(), "disabled", 2),
        (ProvisioningUnavailable(), "unavailable", 1),
    ),
)
def test_failures_are_normalized_without_secret_output(
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception,
    expected_message: str,
    expected_code: int,
) -> None:
    async def fail(*values: object) -> None:
        del values
        raise failure

    monkeypatch.setattr(cli, "_execute", fail)
    stdout, stderr = io.StringIO(), io.StringIO()

    code = cli.main(
        ["create-worker", "--name", "worker", "--expires-in", "30d", "--yes"],
        stdout=stdout,
        stderr=stderr,
        settings_factory=settings,
    )

    assert code == expected_code
    assert stdout.getvalue() == ""
    assert expected_message in stderr.getvalue().lower()
    assert "hostname" not in stderr.getvalue()
    assert "secret" not in stderr.getvalue()


def test_unexpected_failures_propagate_during_development(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail(*values: object) -> None:
        del values
        raise RuntimeError("unexpected programming failure")

    monkeypatch.setattr(cli, "_execute", fail)

    with pytest.raises(RuntimeError, match="unexpected programming failure"):
        cli.main(
            [
                "create-worker",
                "--name",
                "worker",
                "--expires-in",
                "30d",
                "--yes",
            ],
            settings_factory=settings,
        )


class FakeEngine:
    def __init__(self) -> None:
        self.disposed = False

    async def dispose(self) -> None:
        self.disposed = True


class FakeProvisioningTransaction:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    async def __aenter__(self) -> FakeProvisioningTransaction:
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exception_type, exception, traceback

    async def create_api_principal(self, principal_id: UUID, name: str) -> None:
        self.calls.append(("create_api_principal", principal_id, name))

    async def assign_api_roles(self, principal_id: UUID, roles: object) -> None:
        self.calls.append(("assign_api_roles", principal_id, roles))

    async def create_worker_identity(self, worker_id: UUID, name: str) -> None:
        self.calls.append(("create_worker_identity", worker_id, name))

    async def add_api_credential(self, *values: object) -> None:
        self.calls.append(("add_api_credential", *values))

    async def add_worker_credential(self, *values: object) -> None:
        self.calls.append(("add_worker_credential", *values))

    async def revoke_api_credential(self, *values: object) -> None:
        self.calls.append(("revoke_api_credential", *values))

    async def revoke_worker_credential(self, *values: object) -> None:
        self.calls.append(("revoke_worker_credential", *values))

    async def commit(self) -> None:
        self.calls.append(("commit",))


class FakeProvisioningRepository:
    def __init__(self, transaction: FakeProvisioningTransaction) -> None:
        self._transaction = transaction

    def transaction(self) -> FakeProvisioningTransaction:
        return self._transaction


@pytest.mark.parametrize(
    ("arguments", "expected_calls", "returns_credential"),
    (
        (
            [
                "create-api-principal",
                "--name",
                "administrator",
                "--role",
                "administrator",
                "--expires-in",
                "30d",
                "--yes",
            ],
            ("create_api_principal", "assign_api_roles", "add_api_credential"),
            True,
        ),
        (
            ["create-worker", "--name", "worker", "--expires-in", "30d", "--yes"],
            ("create_worker_identity", "add_worker_credential"),
            True,
        ),
        (
            [
                "rotate-api-credential",
                "--principal-id",
                str(uuid4()),
                "--expires-in",
                "30d",
                "--yes",
            ],
            ("add_api_credential",),
            True,
        ),
        (
            [
                "rotate-worker-credential",
                "--worker-identity-id",
                str(uuid4()),
                "--expires-in",
                "30d",
                "--yes",
            ],
            ("add_worker_credential",),
            True,
        ),
        (
            [
                "revoke-api-credential",
                "--principal-id",
                str(uuid4()),
                "--credential-id",
                str(uuid4()),
                "--yes",
            ],
            ("revoke_api_credential",),
            False,
        ),
        (
            [
                "revoke-worker-credential",
                "--worker-identity-id",
                str(uuid4()),
                "--credential-id",
                str(uuid4()),
                "--yes",
            ],
            ("revoke_worker_credential",),
            False,
        ),
    ),
)
def test_execute_composes_each_command_in_one_transaction(
    monkeypatch: pytest.MonkeyPatch,
    arguments: list[str],
    expected_calls: tuple[str, ...],
    returns_credential: bool,
) -> None:
    transaction = FakeProvisioningTransaction()
    repository = FakeProvisioningRepository(transaction)
    engine = FakeEngine()
    monkeypatch.setattr(cli, "build_async_engine", lambda runtime_settings: engine)
    monkeypatch.setattr(cli, "build_session_factory", lambda value: object())
    monkeypatch.setattr(
        cli,
        "SQLAlchemyProvisioningRepository",
        lambda sessions: repository,
    )
    parsed: Namespace = cli.build_parser().parse_args(arguments)

    result = asyncio.run(cli._execute(parsed, settings(), cli._expiration(parsed)))

    call_names = tuple(str(call[0]) for call in transaction.calls)
    assert call_names == (*expected_calls, "commit")
    assert (result is not None) is returns_credential
    assert engine.disposed
