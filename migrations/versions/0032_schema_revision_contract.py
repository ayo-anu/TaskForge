"""Expose the exact schema revision through a narrow runtime function.

Revision ID: 0032_schema_revision_contract
Revises: 0031_lock_worker_authority
Create Date: 2026-09-12
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0032_schema_revision_contract"
down_revision: str | None = "0031_lock_worker_authority"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

RUNTIME_ROLE = "taskforge_runtime"
FUNCTION_SIGNATURE = "public.taskforge_schema_revisions()"


def upgrade() -> None:
    """Grant runtime only a safe exact-revision observation."""
    op.execute(
        f"""
        DO $block$
        DECLARE
            database_owner oid;
            runtime_oid oid;
        BEGIN
            SELECT datdba INTO database_owner
            FROM pg_catalog.pg_database
            WHERE datname = pg_catalog.current_database();

            IF database_owner <> (
                SELECT oid FROM pg_catalog.pg_roles WHERE rolname = current_user
            ) THEN
                RAISE EXCEPTION
                    'schema revision contract migration must run as database owner';
            END IF;

            SELECT oid INTO runtime_oid
            FROM pg_catalog.pg_roles
            WHERE rolname = '{RUNTIME_ROLE}';
            IF runtime_oid IS NULL THEN
                RAISE EXCEPTION '{RUNTIME_ROLE} must be provisioned before migration';
            END IF;
            IF EXISTS (
                SELECT FROM pg_catalog.pg_roles
                WHERE oid = runtime_oid
                  AND (rolsuper OR rolcreatedb OR rolcreaterole OR rolinherit
                       OR rolreplication OR rolbypassrls)
            ) THEN
                RAISE EXCEPTION '{RUNTIME_ROLE} has forbidden role attributes';
            END IF;
            IF EXISTS (
                WITH RECURSIVE memberships(roleid) AS (
                    SELECT roleid FROM pg_catalog.pg_auth_members
                    WHERE member = runtime_oid
                    UNION
                    SELECT membership.roleid
                    FROM pg_catalog.pg_auth_members AS membership
                    JOIN memberships AS parent
                      ON membership.member = parent.roleid
                )
                SELECT FROM memberships
            ) THEN
                RAISE EXCEPTION '{RUNTIME_ROLE} must have no role memberships';
            END IF;
            IF EXISTS (
                SELECT FROM pg_catalog.pg_class WHERE relowner = runtime_oid
            ) OR EXISTS (
                SELECT FROM pg_catalog.pg_namespace WHERE nspowner = runtime_oid
            ) OR EXISTS (
                SELECT FROM pg_catalog.pg_proc WHERE proowner = runtime_oid
            ) THEN
                RAISE EXCEPTION '{RUNTIME_ROLE} must not own schema objects';
            END IF;
            IF pg_catalog.has_schema_privilege(
                '{RUNTIME_ROLE}', 'public', 'CREATE'
            ) THEN
                RAISE EXCEPTION '{RUNTIME_ROLE} must not create public schema objects';
            END IF;
        END
        $block$
        """
    )
    op.execute(
        """
        CREATE FUNCTION public.taskforge_schema_revisions()
        RETURNS text[]
        LANGUAGE sql
        SECURITY DEFINER
        STABLE
        PARALLEL SAFE
        SET search_path = pg_catalog
        AS $function$
            SELECT COALESCE(
                pg_catalog.array_agg(
                    version.version_num::pg_catalog.text
                    ORDER BY version.version_num
                ),
                ARRAY[]::pg_catalog.text[]
            )
            FROM public.alembic_version AS version
        $function$
        """
    )
    op.execute(f"REVOKE ALL ON FUNCTION {FUNCTION_SIGNATURE} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {FUNCTION_SIGNATURE} TO {RUNTIME_ROLE}")
    op.execute(
        f"COMMENT ON FUNCTION {FUNCTION_SIGNATURE} IS "
        "'Reports migration revisions without exposing Alembic metadata privileges'"
    )


def downgrade() -> None:
    """Remove only the exact schema-revision contract signature."""
    op.execute(f"REVOKE EXECUTE ON FUNCTION {FUNCTION_SIGNATURE} FROM {RUNTIME_ROLE}")
    op.execute(f"DROP FUNCTION {FUNCTION_SIGNATURE}")
