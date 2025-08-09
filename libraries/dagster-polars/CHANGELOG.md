## [Unreleased]

## Added

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
