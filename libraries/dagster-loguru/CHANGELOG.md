# Changelog

## [Unreleased]

## [0.0.1]

### Added

- Initial release of dagster-loguru
- `capture_loguru_logs` function that forwards Loguru log messages to the logger
  returned by `dagster.get_dagster_logger()`, so they appear in Dagster's event log
  and UI with no configuration
- Mapping of Loguru-only levels (`SUCCESS` -> `INFO`, `TRACE` -> `DEBUG`)
- Exception information (`exc_info`) forwarding for `logger.exception()` calls
