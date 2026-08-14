.PHONY: install format format-check lint typecheck test coverage migrations-check migration-test claim-test renewal-test retry-test recovery-test authentication-test authorization-test protected-route-test credential-bootstrap-test workflow-persistence-test workflow-route-test broker-dispatch-test check clean

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
	uv run pytest tests/integration/test_identity_migrations.py tests/integration/test_workflow_definition_migrations.py tests/integration/test_workflow_run_migrations.py tests/integration/test_task_dispatch_migrations.py tests/integration/test_task_claim_migrations.py tests/integration/test_task_claim_event_migrations.py tests/integration/test_retry_persistence_migrations.py tests/integration/test_retry_event_migrations.py

claim-test:
	@test "$${TASKFORGE_RUN_CLAIM_INTEGRATION:-}" = "1" || (echo "TASKFORGE_RUN_CLAIM_INTEGRATION=1 is required" >&2; exit 2)
	@test -n "$${TASKFORGE_CLAIM_TEST_DATABASE_URL:-}" || (echo "TASKFORGE_CLAIM_TEST_DATABASE_URL is required" >&2; exit 2)
	uv run pytest tests/integration/test_task_claim_acquisition.py tests/integration/test_task_claim_events.py

renewal-test:
	@test "$${TASKFORGE_RUN_CLAIM_INTEGRATION:-}" = "1" || (echo "TASKFORGE_RUN_CLAIM_INTEGRATION=1 is required" >&2; exit 2)
	@test -n "$${TASKFORGE_CLAIM_TEST_DATABASE_URL:-}" || (echo "TASKFORGE_CLAIM_TEST_DATABASE_URL is required" >&2; exit 2)
	uv run pytest tests/integration/test_task_claim_renewal.py

retry-test:
	@test "$${TASKFORGE_RUN_RETRY_INTEGRATION:-}" = "1" || (echo "TASKFORGE_RUN_RETRY_INTEGRATION=1 is required" >&2; exit 2)
	@test -n "$${TASKFORGE_RETRY_TEST_DATABASE_URL:-}" || (echo "TASKFORGE_RETRY_TEST_DATABASE_URL is required" >&2; exit 2)
	uv run pytest tests/integration/test_retry_transition.py tests/integration/test_retry_scanner.py tests/integration/test_retry_inspection.py

recovery-test:
	@test "$${TASKFORGE_RUN_RECOVERY_INTEGRATION:-}" = "1" || (echo "TASKFORGE_RUN_RECOVERY_INTEGRATION=1 is required" >&2; exit 2)
	@test -n "$${TASKFORGE_RECOVERY_TEST_DATABASE_URL:-}" || (echo "TASKFORGE_RECOVERY_TEST_DATABASE_URL is required" >&2; exit 2)
	uv run pytest tests/integration/test_recovery_scanner.py

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
	uv run pytest tests/integration/test_workflow_persistence.py tests/integration/test_workflow_version_resolution.py tests/integration/test_workflow_run_creation.py tests/integration/test_workflow_run_idempotency.py tests/integration/test_task_dispatch_creation.py tests/integration/test_dispatch_publisher_persistence.py

workflow-route-test:
	@test "$${TASKFORGE_RUN_WORKFLOW_ROUTE_INTEGRATION:-}" = "1" || (echo "TASKFORGE_RUN_WORKFLOW_ROUTE_INTEGRATION=1 is required" >&2; exit 2)
	@test -n "$${TASKFORGE_WORKFLOW_ROUTE_TEST_DATABASE_URL:-}" || (echo "TASKFORGE_WORKFLOW_ROUTE_TEST_DATABASE_URL is required" >&2; exit 2)
	uv run pytest tests/integration/test_workflow_routes.py tests/integration/test_workflow_run_routes.py

broker-dispatch-test:
	@test "$${TASKFORGE_RUN_BROKER_INTEGRATION:-}" = "1" || (echo "TASKFORGE_RUN_BROKER_INTEGRATION=1 is required" >&2; exit 2)
	@test -n "$${TASKFORGE_BROKER_TEST_DATABASE_URL:-}" || (echo "TASKFORGE_BROKER_TEST_DATABASE_URL is required" >&2; exit 2)
	@test -n "$${TASKFORGE_BROKER_TEST_AMQP_URL:-}" || (echo "TASKFORGE_BROKER_TEST_AMQP_URL is required" >&2; exit 2)
	uv run pytest tests/integration/test_dispatch_publisher_broker.py tests/integration/test_dispatch_topology_broker.py

check: format-check lint typecheck coverage migrations-check

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov
	find src tests migrations -type d -name __pycache__ -prune -exec rm -rf {} +
