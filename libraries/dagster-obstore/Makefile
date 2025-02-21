install:
	uv sync

build:
	uv build

test:
	uv run pytest -vv

ruff:
	uv run ruff check --fix
	uv run ruff format

check:
	uv run pyright
