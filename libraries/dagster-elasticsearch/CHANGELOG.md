# Changelog

## [Unreleased]

### Added

- Per-asset overrides via `definition_metadata` and `output_metadata` for `index`, `id_field`, `bulk_chunk_size`, `max_chunk_bytes`, `refresh`, `rollover_strategy`, and `index_config` (mirrors the `dagster-iceberg` and `dagster-polars` patterns).
- `backoff` retries on initial Elasticsearch client construction (`connect_max_retries` field, default 5) for both the resource and the IO manager (mirrors `dagster-chroma` and `dagster-weaviate`).
- PyArrow `Table` and `RecordBatchReader` support in the IO manager. Optional dependency group `[arrow]`.
- Iterator/generator input support (when wired through a custom op or passed directly to `handle_output`).
- Pydantic v1/v2 model and dataclass elements supported in `list[...]` inputs.
- `lazy_load: bool` on the IO manager — when True, `load_input` returns an iterator instead of materialising a list. New `scan_size` and `scroll_keep_alive` fields tune the underlying scroll.
- `BaseConnectionConfig.to_client_kwargs` is now a declared abstract method, surfacing missing implementations under `mypy`.
- Loosened the `elasticsearch` client pin to `>=8.10,<11`. The integration only uses stable bulk and alias APIs unchanged across Elasticsearch 8, 9, and 10; pin a specific major in your own project to match your server (the client refuses cross-major). Test fixtures honour the `ES_TEST_IMAGE` environment variable so a single test run can target any supported major.

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
