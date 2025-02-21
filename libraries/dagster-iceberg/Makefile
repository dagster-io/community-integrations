install:
	uv sync --all-extras --group docs

build:
	uv build

test:
	uv run pytest

ruff:
	uv run ruff check --fix
	uv run ruff format

check:
	uv run pyright

docs:
	uv run mkdocs serve
