"""Development-only CLI for one-time credential provisioning and rotation."""

from __future__ import annotations

import argparse
import asyncio
import re
import sys
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import TextIO
from uuid import UUID

from pydantic import ValidationError
from pydantic_settings import SettingsError

from taskforge.identity.authorization import Role
from taskforge.identity.credentials import GeneratedCredential
from taskforge.identity.provisioning import (
    CredentialIssuanceService,
    CredentialNotFound,
    CredentialRevocationService,
    DuplicateIdentity,
    IdentityDisabled,
    IdentityNotFound,
    IdentityProvisioningService,
    InvalidProvisioningRequest,
    ProvisioningUnavailable,
)
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.provisioning import SQLAlchemyProvisioningRepository
from taskforge.settings import OwnerSettings, Settings

DURATION_PATTERN = re.compile(r"\A([1-9][0-9]*)([hdw])\Z")
MUTATING_COMMANDS = frozenset(
    {
        "create-api-principal",
        "create-worker",
        "rotate-api-credential",
        "rotate-worker-credential",
        "revoke-api-credential",
        "revoke-worker-credential",
    }
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m taskforge.bootstrap",
        description="Development-only Taskforge credential bootstrap",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    api = subparsers.add_parser("create-api-principal")
    api.add_argument("--name", required=True)
    api.add_argument(
        "--role",
        action="append",
        required=True,
        choices=[role.value for role in Role],
    )
    _add_expiration(api)
    _add_confirmation(api)

    worker = subparsers.add_parser("create-worker")
    worker.add_argument("--name", required=True)
    _add_expiration(worker)
    _add_confirmation(worker)

    rotate_api = subparsers.add_parser(
        "rotate-api-credential",
        help="issue a new API credential without revoking existing credentials",
    )
    rotate_api.add_argument("--principal-id", required=True, type=UUID)
    _add_expiration(rotate_api)
    _add_confirmation(rotate_api)

    rotate_worker = subparsers.add_parser(
        "rotate-worker-credential",
        help="issue a new worker credential without revoking existing credentials",
    )
    rotate_worker.add_argument("--worker-identity-id", required=True, type=UUID)
    _add_expiration(rotate_worker)
    _add_confirmation(rotate_worker)

    revoke_api = subparsers.add_parser("revoke-api-credential")
    revoke_api.add_argument("--principal-id", required=True, type=UUID)
    revoke_api.add_argument("--credential-id", required=True, type=UUID)
    _add_confirmation(revoke_api)

    revoke_worker = subparsers.add_parser("revoke-worker-credential")
    revoke_worker.add_argument("--worker-identity-id", required=True, type=UUID)
    revoke_worker.add_argument("--credential-id", required=True, type=UUID)
    _add_confirmation(revoke_worker)
    return parser


def main(
    argv: list[str] | None = None,
    *,
    stdin: TextIO | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
    settings_factory: Callable[[], Settings] = OwnerSettings,
) -> int:
    input_stream = stdin or sys.stdin
    output_stream = stdout or sys.stdout
    error_stream = stderr or sys.stderr
    arguments = build_parser().parse_args(argv)

    try:
        settings = settings_factory()
    except (SettingsError, ValidationError):
        _write_error(error_stream, "Bootstrap configuration is invalid.")
        return 2
    if settings.environment != "development":
        _write_error(error_stream, "Bootstrap is allowed only in development.")
        return 2
    if arguments.command not in MUTATING_COMMANDS:
        _write_error(error_stream, "Unsupported bootstrap command.")
        return 2
    if not arguments.yes and not _confirm(input_stream, error_stream):
        _write_error(error_stream, "Bootstrap cancelled.")
        return 2

    try:
        expiration = _expiration(arguments)
        generated = asyncio.run(_execute(arguments, settings, expiration))
    except (InvalidProvisioningRequest, ValueError, OverflowError):
        _write_error(error_stream, "Bootstrap request is invalid.")
        return 2
    except DuplicateIdentity:
        _write_error(error_stream, "Identity already exists.")
        return 2
    except (IdentityNotFound, CredentialNotFound):
        _write_error(error_stream, "Identity or credential was not found.")
        return 2
    except IdentityDisabled:
        _write_error(error_stream, "Identity is disabled.")
        return 2
    except ProvisioningUnavailable:
        _write_error(error_stream, "Credential operation is unavailable.")
        return 1

    if generated is None:
        _write_error(error_stream, "Credential revoked.")
        return 0
    _write_error(
        error_stream,
        "Store the following credential now; it cannot be retrieved again.",
    )
    if arguments.command.startswith("rotate-"):
        _write_error(
            error_stream,
            "Existing credentials remain active; deploy and verify this credential "
            "before explicitly revoking its predecessor.",
        )
    try:
        output_stream.write(generated.take_presented_value() + "\n")
        output_stream.flush()
    except OSError:
        _write_error(error_stream, "Credential output failed after creation.")
        return 1
    return 0


async def _execute(
    arguments: argparse.Namespace,
    settings: Settings,
    expiration: datetime | None,
) -> GeneratedCredential | None:
    engine = build_async_engine(settings)
    repository = SQLAlchemyProvisioningRepository(build_session_factory(engine))
    identities = IdentityProvisioningService()
    issuance = CredentialIssuanceService()
    revocation = CredentialRevocationService()
    try:
        async with repository.transaction() as transaction:
            generated: GeneratedCredential | None
            if arguments.command == "create-api-principal":
                principal_id = await identities.create_api_principal(
                    transaction,
                    name=arguments.name,
                    roles=frozenset(Role(role) for role in arguments.role),
                )
                assert expiration is not None
                generated = await issuance.issue_api_credential(
                    transaction,
                    principal_id=principal_id,
                    expires_at=expiration,
                )
            elif arguments.command == "create-worker":
                worker_id = await identities.create_worker_identity(
                    transaction,
                    name=arguments.name,
                )
                assert expiration is not None
                generated = await issuance.issue_worker_credential(
                    transaction,
                    worker_id=worker_id,
                    expires_at=expiration,
                )
            elif arguments.command == "rotate-api-credential":
                assert expiration is not None
                generated = await issuance.issue_api_credential(
                    transaction,
                    principal_id=arguments.principal_id,
                    expires_at=expiration,
                )
            elif arguments.command == "rotate-worker-credential":
                assert expiration is not None
                generated = await issuance.issue_worker_credential(
                    transaction,
                    worker_id=arguments.worker_identity_id,
                    expires_at=expiration,
                )
            elif arguments.command == "revoke-api-credential":
                await revocation.revoke_api_credential(
                    transaction,
                    principal_id=arguments.principal_id,
                    credential_id=arguments.credential_id,
                )
                generated = None
            else:
                await revocation.revoke_worker_credential(
                    transaction,
                    worker_id=arguments.worker_identity_id,
                    credential_id=arguments.credential_id,
                )
                generated = None
            await transaction.commit()
            return generated
    finally:
        await engine.dispose()


def _add_expiration(parser: argparse.ArgumentParser) -> None:
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--expires-at")
    group.add_argument("--expires-in")


def _add_confirmation(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--yes",
        action="store_true",
        help="bypass the interactive development mutation confirmation",
    )


def _expiration(arguments: argparse.Namespace) -> datetime | None:
    if arguments.command.startswith("revoke-"):
        return None
    if arguments.expires_at:
        value = arguments.expires_at
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError
        return parsed.astimezone(UTC)
    match = DURATION_PATTERN.fullmatch(arguments.expires_in)
    if match is None:
        raise ValueError
    amount, unit = int(match.group(1)), match.group(2)
    durations = {
        "h": timedelta(hours=amount),
        "d": timedelta(days=amount),
        "w": timedelta(weeks=amount),
    }
    return datetime.now(UTC) + durations[unit]


def _confirm(stdin: TextIO, stderr: TextIO) -> bool:
    stderr.write("Proceed with the local development credential mutation? [y/N] ")
    stderr.flush()
    return stdin.readline().strip().lower() in {"y", "yes"}


def _write_error(stream: TextIO, message: str) -> None:
    stream.write(message + "\n")
    stream.flush()
