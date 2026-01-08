## [Unreleased]

## Unreleased

## Added

- Added new `schema_mode` (defaults to `None`, can be set to `overwrite` or `merge`) parameter to `PolarsDeltaIOManager`. Previously schema mode had to be configured for each asset individually.
- Added `get_polars_storage_options()` method to `BasePolarsUPathIOManager` to expose cloud storage options to Polars native readers/writers.
- `PolarsParquetIOManager` now passes `cloud_storage_options` directly to Polars `scan_parquet`, `write_parquet`, and `sink_parquet` methods, enabling support for S3 Express One Zone and other cloud storage backends that require explicit storage options.
- `sink_df_to_path` now uses Polars native `sink_parquet` with `storage_options` for cloud storage instead of falling back to PyArrow (Polars has supported cloud sinks natively since v0.20).

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
