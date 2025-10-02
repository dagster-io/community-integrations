# Changelog

## [Unreleased]

### Added

- Spark I/O manager.

### Changed

- Rename I/O managers from `<Storage><Engine>IOManager` to `<Engine><Storage>IOManager`.

### Fixed
- TimeWindow partitioning no longer fails due to schema conflicts
