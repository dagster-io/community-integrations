from dagster import ConfigurableResource
from typing import Any

from dagster_xquik.client import (
    DEFAULT_BASE_URL,
    DEFAULT_CONTRACT_VERSION,
    XquikClient,
)


class XquikResource(ConfigurableResource[Any]):
    """Dagster resource for the Xquik REST API."""

    api_key: str
    base_url: str = DEFAULT_BASE_URL
    contract_version: str = DEFAULT_CONTRACT_VERSION
    timeout_seconds: float = 30.0

    def get_client(self) -> XquikClient:
        """Return a configured Xquik client."""

        return XquikClient(
            api_key=self.api_key,
            base_url=self.base_url,
            contract_version=self.contract_version,
            timeout_seconds=self.timeout_seconds,
        )
