# Changelog

## [Unreleased]

### Changed

- Allowed `path` to be unspecified when instantiating `DuckLakeLocalDirectory`, because [the data storage location only has to be specified when creating a new DuckLake](https://ducklake.select/docs/stable/duckdb/usage/connecting).
- Changed the `get_ducklake_sql_parts` functions in `DuckLakeLocalDirectory` and `S3Config` output formatting to include leading commas, so that they are simple to exclude in `_setup_ducklake_connection`.

## [0.0.4] - 2026-05-22

- Update dagster-ducklake lockfile (#305)
- Standardize shared ty configuration (#295)
- Migrate type checking to ty (#291)

## [0.0.3] - 2026-04-28

- ducklake optional DATA_PATH for local storage directories (#275)

## [0.0.2] - 2025-11-12

- Drop support for Python 3.9 (#243)

## [0.0.1] - 2025-10-06

- feat: add ducklake integration (#238)
