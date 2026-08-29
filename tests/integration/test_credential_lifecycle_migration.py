"""Real PostgreSQL credential-fact and revocation lifecycle protection."""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL

from tests.integration.postgresql import (
    asyncpg_dsn,
    migration_database_url,
    temporary_database,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_MIGRATION_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_MIGRATION_INTEGRATION=1 explicitly",
    ),
]


async def verify_credential_lifecycle(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    created_at = datetime.now(UTC) - timedelta(minutes=1)
    expires_at = created_at + timedelta(days=30)
    principal_ids = (uuid4(), uuid4())
    worker_ids = (uuid4(), uuid4())
    api_credential_id = uuid4()
    worker_credential_id = uuid4()
    try:
        for table in ("api_credentials", "worker_credentials"):
            for privilege in ("INSERT", "UPDATE", "DELETE", "TRUNCATE"):
                assert not await connection.fetchval(
                    "SELECT has_table_privilege('taskforge_runtime', $1, $2)",
                    table,
                    privilege,
                )
        assert not await connection.fetchval(
            "SELECT has_function_privilege("
            "'taskforge_runtime','protect_credential_lifecycle()','EXECUTE')"
        )
        assert not await connection.fetchval(
            "SELECT has_function_privilege("
            "0,'protect_credential_lifecycle()','EXECUTE')"
        )
        await connection.executemany(
            "INSERT INTO api_principals (id, name) VALUES ($1, $2)",
            [
                (principal_ids[0], f"principal-{uuid4().hex}"),
                (principal_ids[1], f"principal-{uuid4().hex}"),
            ],
        )
        await connection.executemany(
            "INSERT INTO worker_identities (id, name) VALUES ($1, $2)",
            [
                (worker_ids[0], f"worker-{uuid4().hex}"),
                (worker_ids[1], f"worker-{uuid4().hex}"),
            ],
        )
        await connection.execute(
            "INSERT INTO api_credentials "
            "(id, principal_id, credential_verifier, created_at, expires_at) "
            "VALUES ($1, $2, $3, $4, $5)",
            api_credential_id,
            principal_ids[0],
            "v1$sha256$api-verifier",
            created_at,
            expires_at,
        )
        await connection.execute(
            "INSERT INTO worker_credentials "
            "(id, worker_identity_id, credential_verifier, created_at, expires_at) "
            "VALUES ($1, $2, $3, $4, $5)",
            worker_credential_id,
            worker_ids[0],
            "v1$sha256$worker-verifier",
            created_at,
            expires_at,
        )

        # Mentioning every protected column in a value-preserving UPDATE is valid.
        await connection.execute(
            "UPDATE api_credentials SET id=id, principal_id=principal_id, "
            "credential_verifier=credential_verifier, created_at=created_at, "
            "expires_at=expires_at, revoked_at=revoked_at WHERE id=$1",
            api_credential_id,
        )
        await connection.execute(
            "UPDATE worker_credentials SET id=id, "
            "worker_identity_id=worker_identity_id, "
            "credential_verifier=credential_verifier, created_at=created_at, "
            "expires_at=expires_at, revoked_at=revoked_at WHERE id=$1",
            worker_credential_id,
        )

        first_revoked_at = await connection.fetchval(
            "UPDATE api_credentials SET revoked_at=COALESCE(revoked_at, "
            "statement_timestamp()) WHERE id=$1 RETURNING revoked_at",
            api_credential_id,
        )
        repeated_revoked_at = await connection.fetchval(
            "UPDATE api_credentials SET revoked_at=COALESCE(revoked_at, "
            "statement_timestamp()) WHERE id=$1 RETURNING revoked_at",
            api_credential_id,
        )
        assert first_revoked_at == repeated_revoked_at

        await _assert_rejected(
            connection,
            "UPDATE api_credentials SET id=$2 WHERE id=$1",
            api_credential_id,
            uuid4(),
        )
        await _assert_rejected(
            connection,
            "UPDATE api_credentials SET principal_id=$2 WHERE id=$1",
            api_credential_id,
            principal_ids[1],
        )
        await _assert_rejected(
            connection,
            "UPDATE api_credentials SET credential_verifier=$2 WHERE id=$1",
            api_credential_id,
            "v1$sha256$replacement",
        )
        await _assert_rejected(
            connection,
            "UPDATE api_credentials SET created_at=$2 WHERE id=$1",
            api_credential_id,
            created_at - timedelta(seconds=1),
        )
        await _assert_rejected(
            connection,
            "UPDATE api_credentials SET expires_at=$2 WHERE id=$1",
            api_credential_id,
            expires_at + timedelta(days=1),
        )
        await _assert_rejected(
            connection,
            "UPDATE api_credentials SET revoked_at=NULL WHERE id=$1",
            api_credential_id,
        )
        await _assert_rejected(
            connection,
            "UPDATE api_credentials SET revoked_at=$2 WHERE id=$1",
            api_credential_id,
            first_revoked_at + timedelta(seconds=1),
        )
        await _assert_rejected(
            connection,
            "UPDATE worker_credentials SET worker_identity_id=$2 WHERE id=$1",
            worker_credential_id,
            worker_ids[1],
        )
        await _assert_rejected(
            connection,
            "UPDATE worker_credentials SET credential_verifier=$2 WHERE id=$1",
            worker_credential_id,
            "v1$sha256$replacement",
        )
        await _assert_rejected(
            connection,
            "DELETE FROM api_credentials WHERE id=$1",
            api_credential_id,
        )
        await _assert_rejected(
            connection,
            "DELETE FROM worker_credentials WHERE id=$1",
            worker_credential_id,
        )
        await _assert_rejected(connection, "TRUNCATE api_credentials")
        await _assert_rejected(connection, "TRUNCATE worker_credentials")

        assert (
            await connection.fetchval(
                "SELECT count(*) FROM api_credentials WHERE id=$1",
                api_credential_id,
            )
            == 1
        )
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM worker_credentials WHERE id=$1",
                worker_credential_id,
            )
            == 1
        )
    finally:
        await connection.close()


async def _assert_rejected(
    connection: asyncpg.Connection,
    statement: str,
    *arguments: object,
) -> None:
    with pytest.raises(asyncpg.PostgresError) as error:
        await connection.execute(statement, *arguments)
    assert error.value.sqlstate == "TF011"


def test_credential_lifecycle_upgrade_downgrade_reupgrade() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL",
        "taskforge_cred_lifecycle",
    ) as database_url:
        rendered = database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        configuration = Config("alembic.ini")
        with migration_database_url(rendered):
            command.upgrade(configuration, "head")
            asyncio.run(verify_credential_lifecycle(database_url))
            command.downgrade(configuration, "0029_create_rate_limit_counters")
            command.upgrade(configuration, "head")
