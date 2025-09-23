"""Dagster Apprise integration for notifications."""

from dagster._core.libraries import DagsterLibraryRegistry

from .components import AppriseNotificationsConfig, apprise_notifications
from .hooks import (
    apprise_failure_hook,
    apprise_on_failure,
    apprise_on_success,
    apprise_success_hook,
)
from .resource import AppriseConfig, AppriseResource

__version__ = "0.0.1"


__all__ = [
    "AppriseResource",
    "AppriseConfig",
    "apprise_notifications",
    "AppriseNotificationsConfig",
    "apprise_failure_hook",
    "apprise_success_hook",
    "apprise_on_failure",
    "apprise_on_success",
]


DagsterLibraryRegistry.register(
    "example-integration", __version__, is_dagster_package=False
)
