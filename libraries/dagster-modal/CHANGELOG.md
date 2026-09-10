# Changelog

## [Unreleased]

## [0.0.5] - 2026-05-22

- Update dagster-modal lockfile (#311)
- Standardize shared ty configuration (#295)
- Migrate type checking to ty (#291)

## [0.0.4] - 2025-11-12

- Drop support for Python 3.9 (#243)
- Update `is_dagster_package=False` for each package (#193)

## [0.0.3] - 2025-04-09

- Rename I/O managers engine-first (#169)
- Enable telemetry across all community integrations
- Set package version dynamically, ensure it matches
- Remove extraneous `.`s at the end of Ruff commands
- dagster-contrib-modal -> dagster-modal (#101)

## [0.0.2]

### Updated

- Updated type of `modal.run` `context` parameter to `Union[AssetExecutionContext, OpExecutionContext]`

## [0.0.1] - 2024-10-25

- add docstring examples (#18)
- ignore incompatible method overrides pyright check (#17)
- rename dagster-modal to dagster-contrib-modal
- maint: add pyright check directive for each makefile
- maint: remove .python-version files from version control
- add CI for quality checks: ruff and pytest (#12)
- add github action for unit testing
- fix: builds for dagster-hex and dagster-modal (#10)
- add unit test (#7)
- add make ruff directive; run ruff fix and format
- simplify uv publish command
- add publish url; add note about releases in README
- add note about publisher
- introduce dagster-modal integration (#1)
