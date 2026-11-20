from __future__ import annotations

from typing import Any

from dagster import ConfigurableResource, EnvVar
from pydantic import Field

from dagster_darkmoon.client import DarkmoonClient


class DarkmoonResource(ConfigurableResource):
    """Dagster resource for the Darkmoon dashboard API (a Darkmoon Pro component).

    Provide either ``token`` or both ``username`` and ``password``.

    Example:
        .. code-block:: python

            from dagster import Definitions, EnvVar
            from dagster_darkmoon import DarkmoonResource

            defs = Definitions(
                resources={
                    "darkmoon": DarkmoonResource(
                        base_url="https://darkmoon.example.com:8000",
                        token=EnvVar("DARKMOON_TOKEN"),
                    )
                }
            )
    """

    base_url: str = Field(
        description="Dashboard API URL, with or without the /api/v1 suffix."
    )
    token: str | None = Field(default=None, description="API bearer token.")
    username: str | None = Field(
        default=None, description="Username, used with password when no token is set."
    )
    password: str | None = Field(default=None, description="Password for username.")
    timeout_seconds: float = Field(default=30.0, description="HTTP timeout in seconds.")

    def get_client(self) -> DarkmoonClient:
        return DarkmoonClient(
            base_url=self.base_url,
            token=self.token,
            username=self.username,
            password=self.password,
            timeout_seconds=self.timeout_seconds,
        )

    def list_campaigns(self) -> list[dict[str, Any]]:
        return self.get_client().list_campaigns()

    def get_campaign(self, campaign_id: str) -> dict[str, Any]:
        return self.get_client().get_campaign(campaign_id)

    def get_campaign_report(self, campaign_id: str) -> dict[str, Any]:
        return self.get_client().get_campaign_report(campaign_id)

    def list_findings(
        self,
        campaign_id: str | None = None,
        severity: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        return self.get_client().list_findings(campaign_id, severity, status)

    def launch_campaign(
        self,
        target: str,
        out_of_scope: list[str] | None = None,
        focus: list[str] | None = None,
        noise: str | None = None,
        safe_harbor: str | None = None,
    ) -> dict[str, Any]:
        return self.get_client().launch_campaign(
            target, out_of_scope, focus, noise, safe_harbor
        )


__all__ = ["DarkmoonResource", "EnvVar"]
