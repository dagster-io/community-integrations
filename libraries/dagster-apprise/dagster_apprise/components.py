"""Apprise component for Dagster notifications."""

import logging
from typing import Optional

import dagster as dg
from pydantic import Field

from .resource import AppriseConfig, AppriseResource

logger = logging.getLogger(__name__)


class AppriseNotificationsConfig(dg.Config):
    """Configuration for Apprise notifications component."""

    urls: list[str] = Field(
        default_factory=list, description="List of Apprise notification URLs"
    )
    config_file: Optional[str] = Field(
        default=None, description="Path to Apprise config file"
    )
    base_url: Optional[str] = Field(
        default=None, description="Base URL for Dagster UI links"
    )
    webserver_base_url: Optional[str] = Field(
        default=None,
        description="Alias for base_url to align with Dagster patterns",
    )
    title_prefix: str = Field(
        default="Dagster", description="Prefix for notification titles"
    )
    events: list[str] = Field(
        default_factory=lambda: ["FAILURE"],
        description="List of events to send notifications for (SUCCESS, FAILURE, CANCELED, RUNNING)",
    )
    include_jobs: list[str] = Field(
        default_factory=lambda: ["*"],
        description="List of job name patterns to include (supports wildcards)",
    )
    exclude_jobs: list[str] = Field(
        default_factory=list,
        description="List of job name patterns to exclude (supports wildcards)",
    )


def apprise_notifications(config: AppriseNotificationsConfig) -> dg.Definitions:
    """Create Dagster definitions for Apprise notifications.

    Args:
        config: Configuration for the Apprise notifications

    Returns:
        Dagster definitions with resource and sensors
    """
    # Create the Apprise resource
    resource_config = AppriseConfig(
        urls=config.urls,
        config_file=config.config_file,
        base_url=config.base_url,
        webserver_base_url=config.webserver_base_url,
        title_prefix=config.title_prefix,
    )

    apprise_resource = AppriseResource(config=resource_config)

    # Create sensors for each event type
    sensors = []
    for event in config.events:
        sensor = _create_run_status_sensor(event, config)
        if sensor:
            sensors.append(sensor)

    return dg.Definitions(resources={"apprise": apprise_resource}, sensors=sensors)


def _create_run_status_sensor(
    event: str, config: AppriseNotificationsConfig
) -> Optional[dg.SensorDefinition]:
    """Create a run status sensor for the given event."""
    event_lower = event.lower()

    def sensor_fn(context: dg.RunStatusSensorContext, apprise: AppriseResource) -> None:
        """Sensor function that sends notifications.

        Accepts the `apprise` resource as a parameter to enable clean injection in tests
        and proper resource dependency wiring in Dagster.
        """
        run = context.dagster_run

        # Check if this job should be included
        if not _should_include_job(run.job_name, config.include_jobs):
            logger.debug(
                f"Skipping notification for job {run.job_name} (not in include list)"
            )
            return

        # Check if this job should be excluded
        if _should_exclude_job(run.job_name, config.exclude_jobs):
            logger.debug(
                f"Skipping notification for job {run.job_name} (in exclude list)"
            )
            return

        # Send notification
        success = apprise.notify_run_status(
            run=run, status=event, message=f"Run {event_lower} for job {run.job_name}"
        )

        if success:
            logger.info(f"Sent {event_lower} notification for run {run.run_id}")
        else:
            logger.error(
                f"Failed to send {event_lower} notification for run {run.run_id}"
            )

    # Map event names to Dagster run status
    status_mapping = {
        "SUCCESS": dg.DagsterRunStatus.SUCCESS,
        "FAILURE": dg.DagsterRunStatus.FAILURE,
        "CANCELED": dg.DagsterRunStatus.CANCELED,
        "RUNNING": dg.DagsterRunStatus.STARTED,
    }

    dagster_status = status_mapping.get(event.upper())
    if not dagster_status:
        logger.warning(f"Unknown event type: {event}")
        return None

    return dg.run_status_sensor(
        name=f"apprise_{event_lower}_sensor",
        run_status=dagster_status,
        description=f"Send {event_lower} notifications via Apprise",
    )(sensor_fn)


def _should_include_job(job_name: str, include_jobs: list[str]) -> bool:
    """Check if a job should be included based on include_jobs patterns."""
    if not include_jobs or include_jobs == ["*"]:
        return True

    return any(_matches_pattern(job_name, pattern) for pattern in include_jobs)


def _should_exclude_job(job_name: str, exclude_jobs: list[str]) -> bool:
    """Check if a job should be excluded based on exclude_jobs patterns."""
    if not exclude_jobs:
        return False

    return any(_matches_pattern(job_name, pattern) for pattern in exclude_jobs)


def _matches_pattern(text: str, pattern: str) -> bool:
    """Check if text matches a wildcard pattern."""
    import fnmatch

    return fnmatch.fnmatch(text, pattern)


__all__ = ["apprise_notifications", "AppriseNotificationsConfig"]
