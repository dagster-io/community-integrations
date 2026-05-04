# Changelog

## [Unreleased]

## [0.0.1]

### Added

- Initial release of `dagster-elasticsearch`.
- `ElasticsearchResource` — `ConfigurableResource` over the official Elasticsearch 9.x Python client with a `get_client()` context manager.
- `ElasticsearchIOManager` — bulk-indexes asset outputs (`dict`, `list[dict]`, `pandas.DataFrame`, or polars `DataFrame`/`LazyFrame`) via `elasticsearch.helpers.bulk`. DataFrames are streamed in `bulk_chunk_size` chunks; polars LazyFrames are collected with the streaming engine, so memory stays bounded for very large parquet-backed assets.
- `ElasticsearchIndexConfig` — explicit mappings and settings applied when the IO manager creates an index during alias rollover.
- `ElasticsearchBulkIndexError` — wraps `elasticsearch.helpers.BulkIndexError`, exposing per-document errors via `.errors`.
- IO manager options: `index_config`, `fail_fast`, `max_chunk_bytes`, `server_timeout`.
- Optional alias rollover: write each materialisation to a fresh physical index and atomically swap a stable alias, with `auto`/`timestamp`/`run_id`/`partition`/`none` strategies and `keep_last` cleanup.
- Partitioned-asset support: per-partition physical indices when alias rollover is disabled.
- `HostsConfig` and `CloudConfig` connection configurations supporting API key, basic auth (username/password), bearer token, and TLS options.
- Test suite using `testcontainers` to spin up an Elasticsearch 9.x cluster for round-trip and alias-swap coverage.
