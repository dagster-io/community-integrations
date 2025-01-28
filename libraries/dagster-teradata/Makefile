install:
	uv sync --all-extras

build:
	uv build

test:
	uv run pytest --ignore=dagster_teradata_tests/functional/

ruff:
	uv run ruff check --fix .
	uv run ruff format .

check:
	uv run pyright
