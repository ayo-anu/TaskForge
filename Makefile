.PHONY: install format format-check lint typecheck test coverage migrations-check check clean

install:
	uv sync --locked --dev

format:
	uv run ruff format src tests migrations/env.py

format-check:
	uv run ruff format --check src tests migrations/env.py

lint:
	uv run ruff check src tests migrations/env.py

typecheck:
	uv run mypy src tests

test:
	uv run pytest

coverage:
	uv run pytest --cov=taskforge --cov-report=term-missing

migrations-check:
	uv run alembic heads --verbose

check: format-check lint typecheck coverage migrations-check

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov
	find src tests migrations -type d -name __pycache__ -prune -exec rm -rf {} +
