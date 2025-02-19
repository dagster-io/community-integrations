install:
	uv sync --all-extras --group dev --group docs

build:
	uv build

test:
	uv run pytest tests

lint:
	uv run ruff check --fix .
	uv run ruff format .

check:
	uv run pyright

docs:
	uv run mkdocs serve
