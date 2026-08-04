.PHONY: install format format-check lint typecheck test coverage migrations-check migration-test authentication-test authorization-test protected-route-test credential-bootstrap-test workflow-persistence-test check clean

install:
	uv sync --locked --dev

format:
	uv run ruff format src tests migrations

format-check:
	uv run ruff format --check src tests migrations

lint:
	uv run ruff check src tests migrations

typecheck:
	uv run mypy src tests

test:
	uv run pytest

coverage:
	uv run pytest --cov=taskforge --cov-report=term-missing

migrations-check:
	uv run alembic heads --verbose

migration-test:
	@test "$${TASKFORGE_RUN_MIGRATION_INTEGRATION:-}" = "1" || (echo "TASKFORGE_RUN_MIGRATION_INTEGRATION=1 is required" >&2; exit 2)
	@test -n "$${TASKFORGE_MIGRATION_TEST_DATABASE_URL:-}" || (echo "TASKFORGE_MIGRATION_TEST_DATABASE_URL is required" >&2; exit 2)
	uv run pytest tests/integration/test_identity_migrations.py tests/integration/test_workflow_definition_migrations.py

authentication-test:
	@test "$${TASKFORGE_RUN_AUTHENTICATION_INTEGRATION:-}" = "1" || (echo "TASKFORGE_RUN_AUTHENTICATION_INTEGRATION=1 is required" >&2; exit 2)
	@test -n "$${TASKFORGE_AUTHENTICATION_TEST_DATABASE_URL:-}" || (echo "TASKFORGE_AUTHENTICATION_TEST_DATABASE_URL is required" >&2; exit 2)
	uv run pytest tests/integration/test_authentication_persistence.py

authorization-test:
	@test "$${TASKFORGE_RUN_AUTHORIZATION_INTEGRATION:-}" = "1" || (echo "TASKFORGE_RUN_AUTHORIZATION_INTEGRATION=1 is required" >&2; exit 2)
	@test -n "$${TASKFORGE_AUTHORIZATION_TEST_DATABASE_URL:-}" || (echo "TASKFORGE_AUTHORIZATION_TEST_DATABASE_URL is required" >&2; exit 2)
	uv run pytest tests/integration/test_authorization_persistence.py

protected-route-test:
	@test "$${TASKFORGE_RUN_PROTECTED_ROUTE_INTEGRATION:-}" = "1" || (echo "TASKFORGE_RUN_PROTECTED_ROUTE_INTEGRATION=1 is required" >&2; exit 2)
	@test -n "$${TASKFORGE_PROTECTED_ROUTE_TEST_DATABASE_URL:-}" || (echo "TASKFORGE_PROTECTED_ROUTE_TEST_DATABASE_URL is required" >&2; exit 2)
	uv run pytest tests/integration/test_protected_principal_route.py

credential-bootstrap-test:
	@test "$${TASKFORGE_RUN_CREDENTIAL_BOOTSTRAP_INTEGRATION:-}" = "1" || (echo "TASKFORGE_RUN_CREDENTIAL_BOOTSTRAP_INTEGRATION=1 is required" >&2; exit 2)
	@test -n "$${TASKFORGE_CREDENTIAL_BOOTSTRAP_TEST_DATABASE_URL:-}" || (echo "TASKFORGE_CREDENTIAL_BOOTSTRAP_TEST_DATABASE_URL is required" >&2; exit 2)
	uv run pytest tests/integration/test_credential_bootstrap.py

workflow-persistence-test:
	@test "$${TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION:-}" = "1" || (echo "TASKFORGE_RUN_WORKFLOW_PERSISTENCE_INTEGRATION=1 is required" >&2; exit 2)
	@test -n "$${TASKFORGE_WORKFLOW_PERSISTENCE_TEST_DATABASE_URL:-}" || (echo "TASKFORGE_WORKFLOW_PERSISTENCE_TEST_DATABASE_URL is required" >&2; exit 2)
	uv run pytest tests/integration/test_workflow_persistence.py

check: format-check lint typecheck coverage migrations-check

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov
	find src tests migrations -type d -name __pycache__ -prune -exec rm -rf {} +
