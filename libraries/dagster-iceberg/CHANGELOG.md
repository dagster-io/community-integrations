# Changelog

## [Unreleased]

### Added

- `PolarsIcebergIOManager.reader_override` — configurable option (`'native'` | `'pyiceberg'` | `None`) that maps directly to the `reader_override` parameter of `polars.scan_iceberg`. Setting this to `'pyiceberg'` delegates all reads to the PyIceberg library instead of the Polars native Rust reader. This fixes a bug where the native reader leaves a deadlocked background thread open after reading from S3, preventing Kubernetes Dagster runs from finalising and reporting success.
- `S3TablesCatalogConfig` — typed `IcebergCatalogConfig` subclass for AWS S3 Tables. Derives the Glue REST endpoint URL, SigV4 properties, and warehouse string from a `region` plus `table_bucket_arn` so users don't have to remember the format. User-supplied `properties` keys override the derived defaults.
- Spark I/O manager.
- Support for append mode in Iceberg I/O manager. Write mode options can be set via asset definition metadata using the `write_mode` key, or at runtime via output metadata with the same key. Runtime output metadata setting overrides asset definition metadata setting.
- Support for upsert mode in Iceberg I/O manager and its variants.

### Changed

- Rename I/O managers from `<Storage><Engine>IOManager` to `<Engine><Storage>IOManager`.
- Support for pyiceberg 0.10.0 (#241). Change behavior for how partition field names are calculated to address validation introduced in [pyiceberg #2505](https://github.com/apache/iceberg-python/pull/2305). Partition field names calculated for Dagster asset partitions now include a configurable prefix before the column name they reference. **Migration note**: When updating partition specs on existing tables, new partition field names will be generated using the configured prefix (default: `part_`). For example, a partition field previously named `timestamp` will become `part_timestamp`. Existing data remains accessible, but queries referencing partition fields will need to be updated to use the new naming convention. To maintain backward compatibility, existing tables with unchanged partition specs will retain their original field names until their partition specs are updated.

### Fixed

- A static partition key that contains a quote or a backslash now matches only its own rows. The Spark I/O manager passes keys to Spark as values instead of writing them into SQL, so this holds with `spark.sql.parser.escapedStringLiterals` on or off, and the Polars and Daft I/O managers escape quotes in the SQL filters they build.
- Table, column, and partition names can no longer change the query an I/O manager runs. The Spark I/O manager reads with `SparkSession.table` and filters and selects with `col()`, so `columns` and `partition_expr` must be column names rather than SQL expressions, and `ensure_schema_exists` quotes the schema name and refuses one containing `${`. The Polars and Daft I/O managers quote column and partition names in the SQL they build, so a name must not carry its own SQL quotes. The "Query" output metadata quotes names that are not plain identifiers.
- The Polars and Daft I/O managers filter and select with their engines' expression APIs instead of building SQL, so partition keys, time window bounds, and column names never pass through SQL text. This also fixes Daft reads of a time window partition on a timestamp column, which raised a `to_date` parse error.
- A time window partition can be stored in a `timestamptz` column, which was rejected before. Rows are selected and overwritten by instant, so both 1 a.m. hours of a daylight saving fall-back day load and rewrite their own rows. A `timestamp` column without a timezone keeps selecting by wall-clock time, and for a partition whose wall-clock window is empty, such as the first 1 a.m. hour of a fall-back day, loads and writes now raise an error instead of loading nothing and leaving the old rows in place on a rewrite.
