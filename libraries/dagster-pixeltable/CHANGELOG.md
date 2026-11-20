# Changelog

## [Unreleased]

### Added

- `PixeltableResource`, a `ConfigurableResource` that checks the Pixeltable home of the Dagster process against
  `expected_home`, initializes the SDK during resource setup, and opens existing local tables with `get_table()`.
- `examples/basic`: a table deployed with `pxt schema update`, upserted by primary key, with materialization
  metadata and an asset check.
- `examples/video`: a video table and frame view, appended with `insert`.
