## [Unreleased]

## 0.27.13

### Fixes

- `PolarsParquetIOManager` reads no longer fail on Polars 2.0, which removed the `rechunk` and `retries` arguments to `scan_parquet`. From Polars 1.37.1 (where `retries` was deprecated), `retries` metadata is forwarded as `max_retries` in `storage_options`, and `max_retries` is allowed in the IO manager's own `storage_options` (an explicit `max_retries` there takes priority). When `retries` metadata is not set on Polars >= 1.37.1, Polars' own retry default now applies instead of the previous `0`. `polars` is now capped at `<3`.

## 0.27.11

### Fixes

- Fixed `PolarsParquetIOManager` reads and writes failing on S3 when fsspec-style `storage_options` (`key`, `secret`, `client_kwargs`) were passed to the IO manager. Polars uses `object_store` under the hood, which expects different key names (`aws_access_key_id`, `aws_secret_access_key`, etc.). The IO manager now translates fsspec keys to their `object_store` equivalents on every read and write, and drops unknown keys against an allowlist so typos surface as missing config rather than opaque Rust errors. Applies on `polars >= 1.17`; older versions keep the previous fsspec passthrough. Closes [#257](https://github.com/dagster-io/community-integrations/issues/257).

## 0.27.10

### Changed

- Bumped minimum `polars` version to `>=1.0.0`.

### Fixes

- The `extension` overrides on `PolarsParquetIOManager` and `PolarsDeltaIOManager` are now declared as `ClassVar[Optional[str]]`, which stops pydantic from treating them as model fields. Previously every import surfaced a `UserWarning: Field name "extension" in "PolarsParquetIOManager" shadows an attribute in parent "BasePolarsUPathIOManager"` (and the same for the delta manager) — that warning is now gone. Includes a regression test asserting no shadow warning is emitted on import.
- `PolarsParquetIOManager.write_df_to_path` now passes `storage_options` to Polars `write_parquet` for cloud storage writes (requires Polars >= 1.17.0).
- `PolarsParquetIOManager.sink_df_to_path` now uses Polars native `sink_parquet` with `storage_options` (requires Polars >= 1.17.0, falls back to collecting for older versions).

## 0.27.9

### Fixes

- Partitioned assets/outputs now log `dagster/partition_row_count` metadata instead of `dagster/row_count`. See https://docs.dagster.io/guides/build/assets/metadata-and-tags for more details. 

## 0.27.7

### Added

- Added new `schema_mode` (defaults to `None`, can be set to `overwrite` or `merge`) parameter to `PolarsDeltaIOManager`. Previously schema mode had to be configured for each asset individually.

## 0.27.6

- Use new deltalake (>=1.0.0) syntax and arguments for delta io manager while retaining compatibility via version parsing and legacy syntax.

## Fixes

- Fixed use of deprecated streaming engine selector in polars collect.
- Bump polars dev dependency to support latest deltalake syntax
- Fixed `ImportError` when `patito` is not installed
- Fixed groupings of iomanager config allowing inclusion of s3fs and polars options.

## 0.27.2

### Fixed

- Fixed sinking `polars.LazyFrame` in append mode with `PolarsDeltaIOManager`

## 0.27.1

### Added

- The Patito data validation library for Polars is now support in IO managers. DagsterType instances can be built with `dagster_polars.patito.patito_model_to_dagster_type`.
