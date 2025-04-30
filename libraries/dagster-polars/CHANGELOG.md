## [Unreleased]

## Fixed

- Fixed sinking `polars.LazyFrame` in append mode with `PolarsDeltaIOManager`

## 0.27,1

### Added

- The Patito data validation library for Polars is now support in IO managers. DagsterType instances can be built with `dagster_polars.patito.patito_model_to_dagster_type`.
