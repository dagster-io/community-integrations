# Changelog

## [Unreleased]

## [0.0.10] - 2026-05-22

- Update dagster-contrib-gcp lockfile (#303)
- Standardize shared ty configuration (#295)
- Migrate type checking to ty (#291)
- Fix changelog for dagster-contrib-gcp

## 0.0.9

### Updated

- (pull/259) Added the `container_name` field option to support multi-container job launching

## [0.0.8] - 2025-11-12

- See the git history for the changes in this release.

## [0.0.7] - 2025-11-12

- Drop support for Python 3.9 (#243)
- Update `is_dagster_package=False` for each package (#193)

## [0.0.6] - 2025-04-16

- Bug: job dict mutation (#181)

## [0.0.5] - 2025-04-09

- Enable telemetry across all community integrations
- Set package version dynamically, ensure it matches

## 0.0.4

### Updated

- (pull/181) Fixed mutation of job configuration preventing repeat launches of the same job due to KeyError

## 0.0.4

### Updated

- (pull/145) Added support for launching CloudRun runs in multiple GCP Projects


## 0.0.3

### Updated

- (pull/88) Added job timeout as dagster.yaml instance config

## [0.0.2] - 2024-11-21

- fix build to detect sub-packages (#27)

## [0.0.1] - 2024-11-21

- add cloud runner (#26)
