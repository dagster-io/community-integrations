# Changelog

## [Unreleased]

## [0.0.3] - 2026-05-22

- Update dagster-apprise lockfile (#300)
- Standardize shared ty configuration (#295)
- Migrate type checking to ty (#291)
- chore: prevent registry warnings for community integrations (#270)

## [0.0.2] - 2025-11-12

- Drop support for Python 3.9 (#243)

## [0.0.1]

### Added

- Initial release of dagster-apprise
- Integration with Apprise for sending notifications across 70+ services - Slack, Discord, Pushover, Telegram, email, and more
- `AppriseResource` for configuring notification services
- `AppriseConfig` for resource configuration
- `apprise_notifications` function for integration with definitions
- `AppriseNotificationsConfig` for notification configuration
- `apprise_failure_hook` and `apprise_success_hook` for manual hook usage
- `apprise_on_failure` and `apprise_on_success` decorators for asset/op-level notifications
- Comprehensive test coverage
- Support for filtering notifications by job names and event types
