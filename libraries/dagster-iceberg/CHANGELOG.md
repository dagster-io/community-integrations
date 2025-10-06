# Changelog

## [Unreleased]

### Added

- Spark I/O manager.
- Support for append mode in Iceberg I/O manager. Write mode options can be set via asset definition metadata using the `write_mode` key, or at runtime via output metadata with the same key. Runtime output metadata setting overrides asset definition metadata setting.

### Changed

- Rename I/O managers from `<Storage><Engine>IOManager` to `<Engine><Storage>IOManager`.
