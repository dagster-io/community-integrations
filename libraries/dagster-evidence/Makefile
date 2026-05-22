install:
	uv sync

build:
	uv build

test:
	uv run pytest

ruff:
	uv run ruff check --fix
	uv run ruff format

check:
	uv run ty check .

quality:
	uv run ruff check
	uv run ruff format --check
	uv run ty check .
	uv run pytest
