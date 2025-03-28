up:
	docker compose up --build --wait

catalog: up
	python -c 'from pyiceberg.catalog import load_catalog; catalog = load_catalog("rest", **{"type": "rest", "uri": "http://localhost:8181", "s3.endpoint": "http://localhost:9000", "s3.access-key-id": "admin", "s3.secret-access-key": "password"}); catalog.create_namespace_if_not_exists("nyc")'

dev: export DAGSTER_HOME = $(shell mktemp -d)
dev: catalog
	@echo 'concurrency:\n  runs:\n    max_concurrent_runs: 1' > "$${DAGSTER_HOME}/dagster.yaml"
	dagster dev -f kitchen_sink.py

test:
	uv run pytest

down:
	docker compose down --remove-orphans --volumes
