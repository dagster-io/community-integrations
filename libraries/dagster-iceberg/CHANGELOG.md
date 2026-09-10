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

## [0.3.14] - 2026-05-27

- fix(dagster-iceberg): add reader_override option to PolarsIcebergIOManager (#323)

## [0.3.13] - 2026-05-22

- Update dagster-iceberg lockfile (#310)
- Standardize shared ty configuration (#295)
- Migrate type checking to ty (#291)

## [0.3.12] - 2026-05-22

- Fix Iceberg partition expression typing (#290)
- chore(dagster-iceberg): loosen pyiceberg upper bound to <0.12 (#289)
- Add S3TablesCatalogConfig to dagster-iceberg (#279)

## [0.3.11] - 2026-02-11

- include future annotations

## [0.3.10] - 2026-02-10

- set upper bound for compatibility (#269)

## [0.3.9] - 2026-02-10

- Include future annotations in dagster-iceberg

## [0.3.8] - 2025-11-18

- Adding upsert support  (#242)

## [0.3.7] - 2025-11-12

- Drop support for Python 3.9 (#243)

## [0.3.6] - 2025-10-23

- Assign configurable prefix to partition field names to prevent conflict with schema field names (#241)
- colton/consolidate github workflows (#236)

## [0.3.5] - 2025-10-03

- Fix indentation of docstring

## [0.3.4] - 2025-10-03

- Add append mode support (#235)

## [0.3.3] - 2025-06-30

- less specific import (#212)

## [0.3.2] - 2025-06-30

- less specific import (#211)

## [0.3.1] - 2025-06-09

- Provide extensible load function (#203)
- Upgrade Spark<4.0 and other deps (#204)
- Remove pandas for Polars example (#194)
- Ensure all APIs are in "preview" (#192)
- Update `is_dagster_package=False` for each package (#193)
- Make docs build for Dagster site (#189)
- Test Spark with multi partitions
- Test Spark with daily partitions
- Test Spark with hourly partition
- Stop aliasing `datetime` as `dt`
- Add unit test coverage for Spark (#176)
- Update dependencies in `uv.lock`
- Remove usage of `testcontainers`
- Use Compose env to test existing
- Set up Spark Iceberg Compose env
- Kill unused `CustomDbIOManager`s

## [0.3.0] - 2025-04-16

- Rename I/O managers engine-first (#169)
- Standardize capitalization of IO (#168)
- Use `COL` suffix in kitchen sink
- Test daily partitions with Spark
- Test time partitions with Polars
- Add test for Spark 2D partitions
- Reload 2D-partitioned Spark data
- Check 2D partitioning with Spark
- Add multi partitioning unit test
- Test 2D partitioning with Polars
- Rename `PARTITION_EXPR` variable
- Import, use `dg` in kitchen sink
- Add partition type to asset name
- Drop an unnecessary length check
- Ensure partitioned reads, writes
- Group kitchen sink assets by lib
- Upgrade Dagster for typing fixes
- Add partitioned Spark asset test
- Don't run Spark jobs in parallel
- Support partitioned Spark assets
- Add partitioned Polars data test
- Restore `print` of reloaded data
- Support configuring Spark remote
- Provide URL using `spark.remote`
- Don't use Pythonic configuration
- Do not require `dagster-pyspark`
- Ruff more and drop other configs
- Enable telemetry across all community integrations
- Set package version dynamically, ensure it matches
- Remove duplicate kitchen_sink.py (#148)
- Wait for Docker services to rise
- Upgrade deps (including PySpark)
- Use a faster, non-archive mirror
- Test with `SPARK_REMOTE` env var
- Reduce `NUM_PARTS` for faster CI
- Test end-to-end Spark read/write
- Only materialize Polars in tests
- Ignore the bad typing in Dagster
- Make `NUM_PARTS` a config option
- Create a `SparkIcebergIOManager`

## [0.2.2] - 2025-03-10

- ignore pyright failures
- inspect dagster version
- amend experimental tag => preview
- Use "rest" catalog with "nyc" db
- Use "demo" catalog with "nyc" db
- Update tests to use REST catalog
- Don't format copied provision.py
- Add Spark Connect to Compose env
- Copy PyIceberg integration setup
- Add `spark-iceberg` Compose file
- Define the `spark` package extra
- Materialize the downstream asset
- Test reloading data from Iceberg
- Set database to the catalog name (#137)
- Update links in documentation (#131)
- Clean up trailing whitespace, add missing newlines
- Add end-to-end kitchen sink test (#122)
- Add `Makefile` for local testing (#121)
- Create minimal `kitchen_sink.py` (#120)

## [0.2.1] - 2025-02-20

- Feat: Add dagster-iceberg (#115)
