up:
	docker compose up --build --wait

catalog: up
	python -c 'from pyiceberg.catalog import load_catalog; catalog = load_catalog("local", **{"type": "rest", "uri": "http://localhost:8181", "s3.endpoint": "http://localhost:9000", "s3.access-key-id": "admin", "s3.secret-access-key": "password"}); catalog.create_namespace_if_not_exists("demo.nyc")'

dev: catalog
	dagster dev -f kitchen_sink.py

test:
	uv run pytest

down:
	docker compose down --remove-orphans --volumes
