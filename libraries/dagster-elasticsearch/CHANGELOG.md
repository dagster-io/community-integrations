# Changelog

## [Unreleased]

## [0.0.1]

### Added

- Initial release of `dagster-elasticsearch`.
- `ElasticsearchResource` — `ConfigurableResource` over the official Elasticsearch 9.x Python client with a `get_client()` context manager.
- `ElasticsearchIOManager` — bulk-indexes asset outputs (`dict`, `list[dict]`, or `pandas.DataFrame`) via `elasticsearch.helpers.bulk`.
- Optional alias rollover: write each materialisation to a fresh physical index and atomically swap a stable alias, with `auto`/`timestamp`/`run_id`/`partition`/`none` strategies and `keep_last` cleanup.
- Partitioned-asset support: per-partition physical indices when alias rollover is disabled.
- `HostsConfig` and `CloudConfig` connection configurations supporting API key, basic auth (username/password), bearer token, and TLS options.
- Test suite using `testcontainers` to spin up an Elasticsearch 9.x cluster for round-trip and alias-swap coverage.
