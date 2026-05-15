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
	uv run pyright

quality:
	uv run ruff check
	uv run ruff format --check
	uv run pyright
	uv run pytest
