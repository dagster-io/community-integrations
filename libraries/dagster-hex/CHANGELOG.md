# Changelog

## [Unreleased]

---

## [0.1.10] - 2026-05-22

- Update dagster-hex lockfile (#309)
- Standardize shared ty configuration (#295)
- Migrate type checking to ty (#291)

## [0.1.9] - 2025-11-12

- Drop support for Python 3.9 (#243)
- Update `is_dagster_package=False` for each package (#193)

## [0.1.8] - 2025-04-10

- Get version from `__version__` instead of metadata (#175)
- Enable telemetry across all community integrations
- Set package version dynamically, ensure it matches

## [0.1.7] - 2025-03-10

- ignore pyright failures
- inspect dagster version
- amend experimental tag => preview

## 0.1.6

- Add support for new Hex API parameters in `run_project`: `dry_run`, `update_published_results`, and `view_id`
- Improve deprecation notice for `update_cache` to clarify both alternatives
- Add comprehensive test coverage for new parameters

## 0.1.5

- Hex project asset definition with `build_hex_asset` function using the project_id
- Update to hex resource to sync with new hex API updates to deprecate `update_cache` and add `use_cached_sql` instead.

## 0.1.4

- Mark `hex_resource` with deprecation warning and recommendation to use `HexResource`
- `HexResource` has been refactored from instantiation a `hex_resource` function to a `ConfigurableResource`

## 0.1.3

- Support for notifications

## 0.1.2

- Don't sell input parameters when they are null

## 0.1.0
