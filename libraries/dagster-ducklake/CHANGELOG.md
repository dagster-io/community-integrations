# Changelog

## [Unreleased]

### Changed

- Allowed `path` to be unspecified when instantiating `DuckLakeLocalDirectory`, because [the data storage location only has to be specified when creating a new DuckLake](https://ducklake.select/docs/stable/duckdb/usage/connecting).
- Changed the `get_ducklake_sql_parts` functions in `DuckLakeLocalDirectory` and `S3Config` output formatting to include leading commas, so that they are simple to exclude in `_setup_ducklake_connection`.