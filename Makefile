ruff:
	-ruff check --fix
	ruff format

check:
	@for d in libraries/dagster-*; do if [ -d "$$d" ]; then (echo "$$d" && cd "$$d" && make check) || exit 1; fi; done


test:
	@failed=0; \
	find . -not -path "*/.venv/*" -name "pyproject.toml" -exec dirname {} \; | \
	while read -r dir; do \
		echo "$$dir" && \
		(cd "$$dir" && make test) || failed=1; \
	done; \
	exit $$failed

install:
	@failed=0; \
	find . -not -path "*/.venv/*" -name "pyproject.toml" -exec dirname {} \; | \
	while read -r dir; do \
		echo "$$dir" && \
		(cd "$$dir" && uv sync --all-extras) || failed=1; \
	done; \
	exit $$failed