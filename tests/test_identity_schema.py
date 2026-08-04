"""Structural guarantees for identity and credential persistence."""

from __future__ import annotations

from sqlalchemy import CheckConstraint, Index, Text, UniqueConstraint

from taskforge.identity.schema import (
    API_ROLES,
    api_credentials,
    api_principal_roles,
    api_principals,
    worker_credentials,
    worker_identities,
)
from taskforge.persistence.metadata import metadata


def test_identity_metadata_contains_only_the_task_one_tables() -> None:
    assert set(metadata.tables) == {
        "api_credentials",
        "api_principal_roles",
        "api_principals",
        "worker_credentials",
        "worker_identities",
    }


def test_required_api_roles_are_constrained() -> None:
    role_checks = {
        str(constraint.sqltext)
        for constraint in api_principal_roles.constraints
        if isinstance(constraint, CheckConstraint)
    }

    assert API_ROLES == ("viewer", "workflow_operator", "administrator")
    assert any(
        all(role in expression for role in API_ROLES) for expression in role_checks
    )


def test_api_and_worker_credentials_have_separate_identity_boundaries() -> None:
    api_targets = {
        foreign_key.target_fullname for foreign_key in api_credentials.foreign_keys
    }
    worker_targets = {
        foreign_key.target_fullname for foreign_key in worker_credentials.foreign_keys
    }

    assert api_targets == {"api_principals.id"}
    assert worker_targets == {"worker_identities.id"}


def test_credential_verifiers_are_opaque_unindexed_non_unique_text() -> None:
    for table in (api_credentials, worker_credentials):
        verifier = table.c.credential_verifier
        indexed_columns = {
            column.name
            for index in table.indexes
            if isinstance(index, Index)
            for column in index.columns
        }
        unique_columns = {
            column.name
            for constraint in table.constraints
            if isinstance(constraint, UniqueConstraint)
            for column in constraint.columns
        }

        assert isinstance(verifier.type, Text)
        assert verifier.nullable is False
        assert verifier.primary_key is False
        assert verifier.unique is not True
        assert verifier.index is not True
        assert "credential_verifier" not in indexed_columns
        assert "credential_verifier" not in unique_columns


def test_credential_record_ids_are_the_lookup_identifiers() -> None:
    for table in (api_credentials, worker_credentials):
        assert [column.name for column in table.primary_key.columns] == ["id"]


def test_schema_has_no_plaintext_credential_columns() -> None:
    forbidden_names = {"credential", "password", "secret", "token", "api_key"}

    for table in (api_credentials, worker_credentials):
        assert forbidden_names.isdisjoint(table.c.keys())


def test_identity_names_are_unique_and_lifecycle_fields_are_constrained() -> None:
    for table in (api_principals, worker_identities):
        assert any(
            isinstance(constraint, UniqueConstraint)
            and list(constraint.columns.keys()) == ["name"]
            for constraint in table.constraints
        )
        assert table.c.created_at.nullable is False
        assert table.c.disabled_at.nullable is True

    for table in (api_credentials, worker_credentials):
        check_expressions = {
            str(constraint.sqltext)
            for constraint in table.constraints
            if isinstance(constraint, CheckConstraint)
        }
        assert any("expires_at" in expression for expression in check_expressions)
        assert any("revoked_at" in expression for expression in check_expressions)
