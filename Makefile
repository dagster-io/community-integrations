ruff:
	-ruff check --fix
	ruff format

check:
	@for d in libraries/dagster-*; do if [ -d "$$d" ]; then (echo "$$d" && cd "$$d" && make check) || exit 1; fi; done
