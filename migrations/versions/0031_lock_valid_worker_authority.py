"""Allow runtime worker authority validation to retain row locks.

Revision ID: 0031_lock_worker_authority
Revises: 0030_credential_lifecycle
Create Date: 2026-09-09
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0031_lock_worker_authority"
down_revision: str | None = "0030_credential_lifecycle"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

RUNTIME_ROLE = "taskforge_runtime"
FUNCTION_SIGNATURE = "public.lock_valid_worker_authority(uuid, uuid)"


def upgrade() -> None:
    """Grant runtime only the ability to lock and validate worker authority."""
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

            IF database_owner <> (SELECT oid FROM pg_catalog.pg_roles
                                  WHERE rolname = current_user) THEN
                RAISE EXCEPTION
                    'worker authority function migration must run as database owner';
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
            IF EXISTS (
                SELECT FROM pg_catalog.pg_class AS object
                JOIN pg_catalog.pg_namespace AS namespace
                  ON namespace.oid = object.relnamespace
                WHERE namespace.nspname = 'public'
                  AND object.relname IN ('worker_identities', 'worker_credentials')
                  AND object.relowner <> database_owner
            ) THEN
                RAISE EXCEPTION
                    'database owner must own worker authority tables';
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
        CREATE FUNCTION public.lock_valid_worker_authority(
            p_worker_identity_id uuid,
            p_credential_id uuid
        )
        RETURNS boolean
        LANGUAGE plpgsql
        SECURITY DEFINER
        VOLATILE
        PARALLEL UNSAFE
        SET search_path = pg_catalog
        AS $function$
        DECLARE
            credential_checked_at timestamp with time zone;
        BEGIN
            PERFORM 1
            FROM public.worker_identities AS identity
            WHERE identity.id = p_worker_identity_id
              AND identity.disabled_at IS NULL
            FOR SHARE OF identity;

            IF NOT FOUND THEN
                RETURN false;
            END IF;

            credential_checked_at := pg_catalog.clock_timestamp();
            PERFORM 1
            FROM public.worker_credentials AS credential
            WHERE credential.id = p_credential_id
              AND credential.worker_identity_id = p_worker_identity_id
              AND credential.revoked_at IS NULL
              AND (
                  credential.expires_at IS NULL
                  OR credential.expires_at > credential_checked_at
              )
            FOR SHARE OF credential;

            RETURN FOUND;
        END;
        $function$
        """
    )
    op.execute(f"REVOKE ALL ON FUNCTION {FUNCTION_SIGNATURE} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {FUNCTION_SIGNATURE} TO {RUNTIME_ROLE}")
    op.execute(
        f"COMMENT ON FUNCTION {FUNCTION_SIGNATURE} IS "
        "'Validates worker authority and retains identity/credential row locks "
        "through the caller transaction. Runtime EXECUTE can delay authority "
        "mutation while that transaction remains open.'"
    )


def downgrade() -> None:
    """Remove the narrow runtime authority-lock capability."""
    op.execute(f"REVOKE EXECUTE ON FUNCTION {FUNCTION_SIGNATURE} FROM {RUNTIME_ROLE}")
    op.execute(f"DROP FUNCTION {FUNCTION_SIGNATURE}")
