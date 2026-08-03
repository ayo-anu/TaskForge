.PHONY: install format format-check lint typecheck test coverage check clean

install:
	uv sync --locked --dev

format:
	uv run ruff format src tests

format-check:
	uv run ruff format --check src tests

lint:
	uv run ruff check src tests

typecheck:
	uv run mypy src tests

test:
	uv run pytest

coverage:
	uv run pytest --cov=taskforge --cov-report=term-missing

check: format-check lint typecheck coverage

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov
	find src tests -type d -name __pycache__ -prune -exec rm -rf {} +
