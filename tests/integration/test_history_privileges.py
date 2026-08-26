"""PostgreSQL role, ACL, and immutable-history defense-in-depth contract."""

from __future__ import annotations

import asyncio
import os
import subprocess
from importlib import import_module
from pathlib import Path
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

privileges = import_module("migrations.versions.0027_enforce_history_privileges")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("TASKFORGE_RUN_MIGRATION_INTEGRATION") != "1",
        reason="set TASKFORGE_RUN_MIGRATION_INTEGRATION=1 explicitly",
    ),
]

RUNTIME_PASSWORD = "taskforge-privilege-test-runtime"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
BOOTSTRAP_SCRIPT = PROJECT_ROOT / "docker/postgres/init-taskforge-roles.sh"


def _run_bootstrap(
    database_url: URL, *, check: bool = True
) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment.update(
        {
            "PGHOST": database_url.host or "",
            "PGPORT": str(database_url.port or 5432),
            "PGPASSWORD": database_url.password or "",
            "POSTGRES_DB": database_url.database or "",
            "POSTGRES_USER": database_url.username or "",
            "TASKFORGE_RUNTIME_USER": "taskforge_runtime",
            "TASKFORGE_RUNTIME_PASSWORD": RUNTIME_PASSWORD,
        }
    )
    return subprocess.run(
        ["sh", str(BOOTSTRAP_SCRIPT)],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        check=check,
        text=True,
    )


def test_fresh_privilege_bootstrap_is_idempotent() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_history_privileges"
    ) as database_url:
        asyncio.run(_drop_unconfigured_runtime_role(database_url))
        _run_bootstrap(database_url)
        _run_bootstrap(database_url)
        asyncio.run(_assert_bootstrap_boundary(database_url))


def test_existing_database_bootstrap_rejects_incompatible_administrator() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_history_privileges"
    ) as database_url:
        configuration = Config("alembic.ini")
        rendered = database_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        with migration_database_url(rendered):
            command.upgrade(configuration, "0026_standardize_audit_semantics")
        incompatible_url = asyncio.run(_create_incompatible_administrator(database_url))

        result = _run_bootstrap(incompatible_url, check=False)

        assert result.returncode != 0
        assert (
            "must run as the database owner" in result.stderr
            or "does not own every existing TaskForge table" in result.stderr
        )
        asyncio.run(_assert_failed_bootstrap_changed_nothing(database_url))


def test_history_privilege_upgrade_downgrade_reupgrade() -> None:
    with temporary_database(
        "TASKFORGE_MIGRATION_TEST_DATABASE_URL", "taskforge_history_privileges"
    ) as database_url:
        configuration = Config("alembic.ini")
        alembic_url = database_url.set(
            drivername="postgresql+asyncpg"
        ).render_as_string(hide_password=False)
        with migration_database_url(alembic_url):
            command.upgrade(configuration, "0026_standardize_audit_semantics")
            owner_name = asyncio.run(_prepare_cycle(database_url))
            _run_bootstrap(database_url)
            _run_bootstrap(database_url)
            command.upgrade(configuration, "head")
            asyncio.run(_assert_0027(database_url, owner_name))
            command.downgrade(configuration, "0026_standardize_audit_semantics")
            asyncio.run(_assert_0026(database_url))
            command.upgrade(configuration, "head")
            asyncio.run(_assert_0027(database_url, owner_name))


async def _prepare_cycle(database_url: object) -> str:
    owner = await asyncpg.connect(asyncpg_dsn(database_url))  # type: ignore[arg-type]
    owner_name = await owner.fetchval("SELECT current_user")
    try:
        await owner.execute("DROP ROLE taskforge_runtime")
        await owner.execute(
            "CREATE FUNCTION unrelated_privilege_probe() RETURNS integer LANGUAGE sql AS 'SELECT 1'"
        )
        await owner.execute(
            "GRANT EXECUTE ON FUNCTION unrelated_privilege_probe() TO PUBLIC"
        )
    finally:
        await owner.close()
    return owner_name


async def _drop_unconfigured_runtime_role(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        await connection.execute("DROP ROLE taskforge_runtime")
    finally:
        await connection.close()


async def _assert_bootstrap_boundary(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        role = await connection.fetchrow(
            "SELECT rolsuper, rolcreatedb, rolcreaterole, rolinherit, rolreplication, "
            "rolbypassrls FROM pg_roles WHERE rolname='taskforge_runtime'"
        )
        assert role is not None and not any(role.values())
        assert await connection.fetchval(
            "SELECT has_database_privilege('taskforge_runtime',current_database(),'CONNECT')"
        )
        assert not await connection.fetchval(
            "SELECT has_database_privilege('taskforge_runtime',current_database(),'TEMPORARY')"
        )
        assert await connection.fetchval(
            "SELECT has_schema_privilege('taskforge_runtime','public','USAGE')"
        )
        assert not await connection.fetchval(
            "SELECT has_schema_privilege('taskforge_runtime','public','CREATE')"
        )
    finally:
        await connection.close()


async def _create_incompatible_administrator(database_url: URL) -> URL:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    password = "incompatible-administrator-password"
    try:
        await connection.execute(
            f"CREATE ROLE incompatible_taskforge_admin LOGIN SUPERUSER PASSWORD '{password}'"
        )
        await connection.execute("DROP ROLE taskforge_runtime")
    finally:
        await connection.close()
    return database_url.set(username="incompatible_taskforge_admin", password=password)


async def _assert_failed_bootstrap_changed_nothing(database_url: URL) -> None:
    connection = await asyncpg.connect(asyncpg_dsn(database_url))
    try:
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_roles WHERE rolname='taskforge_runtime')"
        )
        assert await connection.fetchval(
            "SELECT has_database_privilege(0,current_database(),'TEMPORARY')"
        )
        assert await connection.fetchval(
            "SELECT has_schema_privilege(0,'public','USAGE')"
        )
    finally:
        await connection.close()


async def _assert_0027(database_url: object, owner_name: str) -> None:
    owner = await asyncpg.connect(asyncpg_dsn(database_url))  # type: ignore[arg-type]
    runtime_url = database_url.set(
        username="taskforge_runtime", password=RUNTIME_PASSWORD
    )  # type: ignore[union-attr]
    runtime = await asyncpg.connect(asyncpg_dsn(runtime_url))
    try:
        role = await owner.fetchrow(
            "SELECT rolsuper, rolcreatedb, rolcreaterole, rolinherit, rolreplication, "
            "rolbypassrls FROM pg_roles WHERE rolname='taskforge_runtime'"
        )
        assert role is not None and not any(role.values())
        assert await owner.fetchval(
            "SELECT has_database_privilege('taskforge_runtime',current_database(),'CONNECT')"
        )
        assert not await owner.fetchval(
            "SELECT has_database_privilege('taskforge_runtime',current_database(),'TEMPORARY')"
        )
        assert await owner.fetchval(
            "SELECT has_schema_privilege('taskforge_runtime','public','USAGE')"
        )
        assert not await owner.fetchval(
            "SELECT has_schema_privilege('taskforge_runtime','public','CREATE')"
        )
        assert not await owner.fetchval(
            "SELECT EXISTS(SELECT FROM pg_class WHERE relowner=(SELECT oid FROM "
            "pg_roles WHERE rolname='taskforge_runtime')) OR EXISTS(SELECT FROM "
            "pg_namespace WHERE nspowner=(SELECT oid FROM pg_roles WHERE "
            "rolname='taskforge_runtime')) OR EXISTS(SELECT FROM pg_proc WHERE "
            "proowner=(SELECT oid FROM pg_roles WHERE rolname='taskforge_runtime'))"
        )
        assert not await owner.fetchval(
            "WITH RECURSIVE memberships(roleid) AS (SELECT roleid FROM pg_auth_members "
            "WHERE member=(SELECT oid FROM pg_roles WHERE rolname='taskforge_runtime') "
            "UNION SELECT m.roleid FROM pg_auth_members m JOIN memberships p ON "
            "m.member=p.roleid) SELECT EXISTS(SELECT FROM memberships)"
        )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await runtime.execute(f'SET ROLE "{owner_name}"')
        for role_name in ("pg_read_all_data", "pg_write_all_data", "pg_database_owner"):
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await runtime.execute(f"SET ROLE {role_name}")
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await runtime.execute("CREATE TEMP TABLE forbidden_temp (id integer)")
        assert await runtime.fetchval("SELECT 1") == 1
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await runtime.execute("CREATE TABLE forbidden_ddl (id integer)")

        for table in privileges.ALL_TABLES:
            expected = {
                "SELECT": table in privileges.SELECT_TABLES,
                "INSERT": table in privileges.INSERT_TABLES,
                "UPDATE": table in privileges.UPDATE_TABLES,
                "DELETE": table in privileges.DELETE_TABLES,
                "TRUNCATE": False,
                "REFERENCES": False,
                "TRIGGER": False,
            }
            for privilege, allowed in expected.items():
                assert (
                    await owner.fetchval(
                        "SELECT has_table_privilege('taskforge_runtime', $1, $2)",
                        table,
                        privilege,
                    )
                    is allowed
                )
        assert not await owner.fetchval(
            "SELECT has_table_privilege('taskforge_runtime','alembic_version','SELECT')"
        )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await runtime.fetchval("SELECT count(*) FROM audit_records")

        principal_id = uuid4()
        await owner.execute(
            "INSERT INTO api_principals(id,name) VALUES($1,$2)",
            principal_id,
            f"acl-principal-{principal_id.hex}",
        )
        await runtime.execute(
            "INSERT INTO audit_records(id,actor_kind,api_principal_id,action,outcome,"
            "resource_type,resource_id) VALUES($1,'api_principal',$2,'workflow.create',"
            "'accepted','workflow',$3)",
            uuid4(),
            principal_id,
            uuid4(),
        )
        runtime_workflow_id = uuid4()
        await runtime.execute(
            "INSERT INTO workflow_definitions(id,owner_principal_id,name) "
            "VALUES($1,$2,$3)",
            runtime_workflow_id,
            principal_id,
            f"runtime-workflow-{runtime_workflow_id.hex}",
        )
        await runtime.execute(
            "UPDATE workflow_definitions SET description='runtime mutation' WHERE id=$1",
            runtime_workflow_id,
        )
        for statement in (
            "UPDATE audit_records SET action='workflow.publish'",
            "DELETE FROM audit_records",
            "TRUNCATE audit_records",
            "ALTER TABLE audit_records ADD COLUMN forbidden integer",
            "DROP TABLE audit_records",
        ):
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await runtime.execute(statement)
        for statement in (
            "UPDATE audit_records SET action='workflow.publish'",
            "DELETE FROM audit_records",
            "TRUNCATE audit_records",
        ):
            with pytest.raises(asyncpg.PostgresError) as error:
                await owner.execute(statement)
            assert error.value.sqlstate is not None
            assert error.value.sqlstate.startswith("TF")

        for function in privileges.TASKFORGE_FUNCTIONS:
            assert not await owner.fetchval(
                "SELECT has_function_privilege(0, $1, 'EXECUTE')", f"{function}()"
            )
            assert not await owner.fetchval(
                "SELECT has_function_privilege('taskforge_runtime', $1, 'EXECUTE')",
                f"{function}()",
            )
        assert await owner.fetchval(
            "SELECT has_function_privilege('taskforge_runtime',"
            "'unrelated_privilege_probe()', 'EXECUTE')"
        )

        await owner.execute(
            "CREATE OR REPLACE FUNCTION future_acl_probe() RETURNS integer "
            "LANGUAGE sql AS 'SELECT 1'"
        )
        await owner.execute(
            "CREATE TABLE IF NOT EXISTS future_acl_table(id bigserial primary key)"
        )
        assert not await owner.fetchval(
            "SELECT has_function_privilege(0,'future_acl_probe()','EXECUTE')"
        )
        assert not await owner.fetchval(
            "SELECT has_function_privilege('taskforge_runtime','future_acl_probe()','EXECUTE')"
        )
        assert not await owner.fetchval(
            "SELECT has_table_privilege('taskforge_runtime','future_acl_table','SELECT')"
        )
        assert not await owner.fetchval(
            "SELECT has_sequence_privilege('taskforge_runtime','future_acl_table_id_seq','USAGE')"
        )

        triggers = await owner.fetchval(
            "SELECT count(*) FROM pg_trigger WHERE tgname = ANY($1::text[]) AND NOT tgisinternal",
            [f"trg_{table}_reject_truncate" for table in privileges.SNAPSHOT_TABLES],
        )
        assert triggers == 3
        definition_id, version_id = uuid4(), uuid4()
        await owner.execute(
            "INSERT INTO workflow_definitions(id,owner_principal_id,name) VALUES($1,$2,$3)",
            definition_id,
            principal_id,
            f"immutable-{definition_id.hex}",
        )
        await owner.execute(
            "INSERT INTO workflow_versions(id,workflow_definition_id,version_number,name) "
            "VALUES($1,$2,1,'version')",
            version_id,
            definition_id,
        )
        for statement in (
            "UPDATE workflow_versions SET name='changed'",
            "DELETE FROM workflow_versions",
            "TRUNCATE workflow_versions CASCADE",
        ):
            with pytest.raises(asyncpg.PostgresError) as error:
                await owner.execute(statement)
            assert error.value.sqlstate == "TF001"
    finally:
        await runtime.close()
        await owner.close()


async def _assert_0026(database_url: object) -> None:
    owner = await asyncpg.connect(asyncpg_dsn(database_url))  # type: ignore[arg-type]
    try:
        for function in privileges.TASKFORGE_FUNCTIONS:
            assert await owner.fetchval(
                "SELECT has_function_privilege(0, $1, 'EXECUTE')", f"{function}()"
            )
            assert await owner.fetchval(
                "SELECT has_function_privilege('taskforge_runtime', $1, 'EXECUTE')",
                f"{function}()",
            )
        assert await owner.fetchval(
            "SELECT has_function_privilege(0,'unrelated_privilege_probe()','EXECUTE')"
        )
        assert not await owner.fetchval(
            "SELECT EXISTS(SELECT FROM pg_trigger WHERE tgname LIKE "
            "'trg_workflow_version%_reject_truncate')"
        )
    finally:
        await owner.close()
