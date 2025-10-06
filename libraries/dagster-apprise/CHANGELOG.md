# Changelog

## [Unreleased]

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
