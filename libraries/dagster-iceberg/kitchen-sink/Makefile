catalog:
	mkdir -p /tmp/warehouse
	python -c 'from pyiceberg.catalog.sql import SqlCatalog; catalog = SqlCatalog("default", **{"uri": "sqlite:////tmp/warehouse/iceberg_catalog.db", "warehouse": "file:///tmp/warehouse"}); catalog.create_namespace_if_not_exists("default")'

dev: catalog
	dagster dev -f kitchen_sink.py

test:
	uv run pytest

clean:
	rm -rf /tmp/warehouse
