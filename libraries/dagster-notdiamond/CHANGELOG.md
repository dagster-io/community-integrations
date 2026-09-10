# Changelog

> **Note on version ordering.** Releases below are listed newest first by
> release date, which is not the same as descending version order. This
> library shipped 0.1.2 and 0.1.3, then commit `ca78a07` moved the version
> out of `pyproject.toml` and into `__init__.py` as 0.0.2, and releases have
> continued from that lower base. PyPI serves the highest version, so
> `pip install dagster-notdiamond` still resolves to 0.1.3; 0.0.3 onwards are
> published but only reachable by pinning an exact version. See
> [#340](https://github.com/dagster-io/community-integrations/issues/340).

## [Unreleased]

## [0.0.5] - 2026-05-22

- Update NotDiamond SDK (#297)
- Standardize shared ty configuration (#295)
- Migrate type checking to ty (#291)

## [0.0.4] - 2025-11-12

- Drop support for Python 3.9 (#243)
- Update `is_dagster_package=False` for each package (#193)

## [0.0.3] - 2025-04-09

- Rename I/O managers engine-first (#169)
- Fix Ruff violation in example
- Enable telemetry across all community integrations
- Set package version dynamically, ensure it matches

## [0.1.3] - 2025-03-10

- ignore pyright failures
- inspect dagster version
- amend experimental tag => preview
- Remove extraneous `.`s at the end of Ruff commands
- Run Ruff formatter on libraries that would fail CI
- adds example of using the model gateway (#110)
- rename `dagster-contrib-notdiamond` to `dagster-notdiamond` (#108)
- create example showcasing model routing usage with OAI (#94)

## [0.1.2]

- Removing TOML config.
- Adds example `examples/example_book_reviews_summary.py` showing model usage after `model_select` for book review summarization

## [0.1.1]

- Simplifying API to remove usage_metadata wrapper.

## [0.1.0]

- Initial Release, 2024-10-30

## [0.0.1] - 2024-11-07

- add setuptools configuration
- make ruff
- fixing expected user_agent param
- Docs and example job
- test run
- forgot to save pyproject
- feedback from colton
- rename to dagster-contrib-notdiamond
- initial
