install:
	uv sync --all-extras --group docs

build:
	uv build

test:
	uv run pytest

e2e-test:
	uv run pytest kitchen-sink

ruff:
	uv run ruff check --fix
	uv run ruff format

check:
	uv run pyright

docs:
	uv run mkdocs serve
