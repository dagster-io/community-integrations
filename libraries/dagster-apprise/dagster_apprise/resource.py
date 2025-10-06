"""Apprise resource for Dagster notifications."""

import logging
from typing import Any, Optional

import apprise
import dagster as dg
from apprise import NotifyType
from pydantic import Field

logger = logging.getLogger(__name__)


class AppriseConfig(dg.Config):
    """Configuration for Apprise notifications."""

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
        description="Alias for base_url to align with Dagster patterns (used if base_url not set)",
    )
    title_prefix: str = Field(
        default="Dagster", description="Prefix for notification titles"
    )


class AppriseResource(dg.ConfigurableResource):
    """Dagster resource for sending notifications via Apprise."""

    config: AppriseConfig = Field(default_factory=AppriseConfig)

    def __init__(self, **data):
        super().__init__(**data)
        self._apprise = None

    @property
    def apprise(self) -> apprise.Apprise:
        """Get or create Apprise instance."""
        if self._apprise is None:
            self._apprise = apprise.Apprise()

            # Add URLs from config
            for url in self.config.urls:
                self._apprise.add(url)

            # Add config file if specified
            if self.config.config_file:
                self._apprise.add(self.config.config_file)

        return self._apprise

    def notify(
        self,
        body: str,
        title: Optional[str] = None,
        notify_type: NotifyType = NotifyType.INFO,
        **kwargs: Any,
    ) -> bool:
        """Send a notification via Apprise.

        Args:
            body: The notification body text
            title: Optional title for the notification
            notify_type: Type of notification (INFO, SUCCESS, WARNING, FAILURE)
            **kwargs: Additional arguments passed to Apprise

        Returns:
            True if notification was sent successfully, False otherwise
        """
        try:
            full_title = (
                f"{self.config.title_prefix}: {title}"
                if title
                else self.config.title_prefix
            )

            result = self.apprise.notify(
                body=body, title=full_title, notify_type=notify_type, **kwargs
            )

            if result:
                logger.info(f"Notification sent successfully: {title}")
            else:
                logger.warning(f"Failed to send notification: {title}")

            return bool(result)

        except Exception as e:
            logger.error(f"Error sending notification: {e}")
            return False

    def notify_run_status(
        self, run: dg.DagsterRun, status: str, message: Optional[str] = None
    ) -> bool:
        """Send a notification for a Dagster run status change.

        Args:
            run: The Dagster run
            status: The run status (RUNNING, SUCCESS, FAILURE, etc.)
            message: Optional custom message

        Returns:
            True if notification was sent successfully, False otherwise
        """
        # Map Dagster status to Apprise notification type
        status_mapping = {
            "SUCCESS": NotifyType.SUCCESS,
            "FAILURE": NotifyType.FAILURE,
            "CANCELED": NotifyType.WARNING,
            "RUNNING": NotifyType.INFO,
        }

        notify_type = status_mapping.get(status, NotifyType.INFO)

        # Build notification content
        title = f"Run {status.lower()}"
        body_parts = [
            f"Job: {run.job_name}",
            f"Run ID: {run.run_id}",
            f"Status: {status}",
        ]

        if message:
            body_parts.append(f"Message: {message}")

        # Add Dagster UI link if base_url is configured
        base = self.config.base_url or self.config.webserver_base_url
        if base:
            run_url = f"{base}/runs/{run.run_id}"
            body_parts.append(f"View run: {run_url}")

        body = "\n".join(body_parts)

        return self.notify(body=body, title=title, notify_type=notify_type)
