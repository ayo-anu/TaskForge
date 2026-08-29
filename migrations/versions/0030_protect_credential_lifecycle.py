"""Protect immutable credential facts and monotonic revocation.

Revision ID: 0030_credential_lifecycle
Revises: 0029_create_rate_limit_counters
Create Date: 2026-08-28
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0030_credential_lifecycle"
down_revision: str | None = "0029_create_rate_limit_counters"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

FUNCTION_NAME = "protect_credential_lifecycle"
IMMUTABILITY_SQLSTATE = "TF011"
IMMUTABILITY_MESSAGE = "credential facts and revocation are immutable"
TABLES = ("api_credentials", "worker_credentials")


def upgrade() -> None:
    """Allow only value-preserving updates and one-way credential revocation."""
    op.execute(
        f"""
        CREATE FUNCTION {FUNCTION_NAME}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        BEGIN
            IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
                RAISE EXCEPTION USING
                    ERRCODE = '{IMMUTABILITY_SQLSTATE}',
                    MESSAGE = '{IMMUTABILITY_MESSAGE}';
            END IF;

            IF TG_TABLE_NAME = 'api_credentials' THEN
                IF NEW.principal_id IS DISTINCT FROM OLD.principal_id THEN
                    RAISE EXCEPTION USING
                        ERRCODE = '{IMMUTABILITY_SQLSTATE}',
                        MESSAGE = '{IMMUTABILITY_MESSAGE}';
                END IF;
            ELSIF TG_TABLE_NAME = 'worker_credentials' THEN
                IF NEW.worker_identity_id IS DISTINCT FROM OLD.worker_identity_id THEN
                    RAISE EXCEPTION USING
                        ERRCODE = '{IMMUTABILITY_SQLSTATE}',
                        MESSAGE = '{IMMUTABILITY_MESSAGE}';
                END IF;
            END IF;

            IF NEW.id IS DISTINCT FROM OLD.id
               OR NEW.credential_verifier IS DISTINCT FROM OLD.credential_verifier
               OR NEW.created_at IS DISTINCT FROM OLD.created_at
               OR NEW.expires_at IS DISTINCT FROM OLD.expires_at
               OR (
                    OLD.revoked_at IS NOT NULL
                    AND NEW.revoked_at IS DISTINCT FROM OLD.revoked_at
               )
            THEN
                RAISE EXCEPTION USING
                    ERRCODE = '{IMMUTABILITY_SQLSTATE}',
                    MESSAGE = '{IMMUTABILITY_MESSAGE}';
            END IF;

            RETURN NEW;
        END;
        $function$
        """
    )
    for table in TABLES:
        op.execute(
            f"""
            CREATE TRIGGER trg_{table}_protect_lifecycle
            BEFORE UPDATE OR DELETE ON {table}
            FOR EACH ROW
            EXECUTE FUNCTION {FUNCTION_NAME}()
            """
        )
        op.execute(
            f"""
            CREATE TRIGGER trg_{table}_reject_truncate
            BEFORE TRUNCATE ON {table}
            FOR EACH STATEMENT
            EXECUTE FUNCTION {FUNCTION_NAME}()
            """
        )


def downgrade() -> None:
    """Restore the previous owner-mutable credential lifecycle."""
    for table in reversed(TABLES):
        op.execute(f"DROP TRIGGER trg_{table}_reject_truncate ON {table}")
        op.execute(f"DROP TRIGGER trg_{table}_protect_lifecycle ON {table}")
    op.execute(f"DROP FUNCTION {FUNCTION_NAME}()")
