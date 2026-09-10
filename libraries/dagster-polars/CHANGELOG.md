## [Unreleased]

## Unreleased

### Fixes

- The `extension` overrides on `PolarsParquetIOManager` and `PolarsDeltaIOManager` are now declared as `ClassVar[Optional[str]]`, which stops pydantic from treating them as model fields. Previously every import surfaced a `UserWarning: Field name "extension" in "PolarsParquetIOManager" shadows an attribute in parent "BasePolarsUPathIOManager"` (and the same for the delta manager) — that warning is now gone. Includes a regression test asserting no shadow warning is emitted on import.
- Fixed `PolarsParquetIOManager` reads and writes failing on S3 when fsspec-style `storage_options` (`key`, `secret`, `client_kwargs`) were passed to the IO manager. Polars uses `object_store` under the hood, which expects different key names (`aws_access_key_id`, `aws_secret_access_key`, etc.). The IO manager now translates fsspec keys to their `object_store` equivalents on every read and write, and drops unknown keys against an allowlist so typos surface as missing config rather than opaque Rust errors. Applies on `polars >= 1.17`; older versions keep the previous fsspec passthrough. Closes [#257](https://github.com/dagster-io/community-integrations/issues/257).
- Partitioned assets/outputs now log `dagster/partition_row_count` metadata instead of `dagster/row_count`. See https://docs.dagster.io/guides/build/assets/metadata-and-tags for more details.

## Added

- Added new `schema_mode` (defaults to `None`, can be set to `overwrite` or `merge`) parameter to `PolarsDeltaIOManager`. Previously schema mode had to be configured for each asset individually.

## Changed

- Bumped minimum `polars` version to `>=1.0.0`.

## Fixed

- `PolarsParquetIOManager.write_df_to_path` now passes `storage_options` to Polars `write_parquet` for cloud storage writes (requires Polars >= 1.17.0).
- `PolarsParquetIOManager.sink_df_to_path` now uses Polars native `sink_parquet` with `storage_options` (requires Polars >= 1.17.0, falls back to collecting for older versions).

## [0.27.12] - 2026-05-22

- Update dagster-polars lockfile (#314)

## [0.27.11] - 2026-05-22

- remap fsspec storage_options for object_store reads (closes #257) (#280)

## [0.27.10] - 2026-05-22

- silence pydantic shadow warning on extension override (#281)
- Standardize shared ty configuration (#295)
- Migrate type checking to ty (#291)
- dagster-polars: parquet writes to cloud storage (#264)

## [0.27.9] - 2026-02-03

- log dagster/partition_row_count instead of dagster/row_count for partitioned outputs (#267)

## [0.27.8] - 2025-11-12

- Drop support for Python 3.9 (#243)
- fix typo in docs (#239)

## [0.27.7] - 2025-09-22

- add top-level schema_mode PolarsDeltaIOManager parameter (#233)

## 0.27.6

- Use new deltalake (>=1.0.0) syntax and arguments for delta io manager while retaining compatibility via version parsing and legacy syntax.

## Fixes

- Fixed use of deprecated streaming engine selector in polars collect.
- Bump polars dev dependency to support latest deltalake syntax
- Fixed `ImportError` when `patito` is not installed
- Fixed groupings of iomanager config allowing inclusion of s3fs and polars options.

## [0.27.5] - 2025-08-15

- fix: sanitize config passed to polars.scan_parquet (#220)
- Removes setuptools-scm dep for dagster-polars

## [0.27.4] - 2025-07-23

- Fix/use new streaming collect (#215)

## [0.27.3] - 2025-07-21

- refactor/update delta write options (#217)
- fix ImportError with patito and ensure optional deps are not top-level imports (#196)
- Update `is_dagster_package=False` for each package (#193)
- update changelog (#190)

## 0.27.2

### Fixed

- Fixed sinking `polars.LazyFrame` in append mode with `PolarsDeltaIOManager`

## 0.27.1

### Added

- The Patito data validation library for Polars is now support in IO managers. DagsterType instances can be built with `dagster_polars.patito.patito_model_to_dagster_type`.
